// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package runtime is the connector harness: the glue that turns the five
// contract packages (spec, config, status, admin, check) into one entrypoint
// a connector's main calls with its hooks.
//
// It owns the mechanics no connector should reimplement: the D-11 mode
// selection (`spec`/`check`/run), loading the config document (D-2, D-14),
// connecting to NATS from the nexlet's launch-contract env vars, publishing
// status (D-5), serving the admin surface (D-8), the park state machine
// (D-15), and the SIGTERM drain (D-16).
//
// A connector supplies Hooks and calls Main:
//
//	func main() {
//	    cfg := &Config{}
//	    flag.Parse()
//	    os.Exit(runtime.Main("example", version, cfg, caps, runtime.Hooks{
//	        Probe: probe,
//	        Run:   run,
//	    }))
//	}
package runtime

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/admin"
	"github.com/synadia-io/connectors-sdk/sdk-go/check"
	"github.com/synadia-io/connectors-sdk/sdk-go/config"
	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// The D-11 mode tokens. Any other first argument — including none, and
// including a flag — selects the run mode (spec/contract.md, "Entrypoint
// mechanics").
const (
	modeSpec  = "spec"
	modeCheck = "check"
	modeRun   = ""
)

// Machine-readable park/stop reasons the harness itself produces. A
// connector's own park reasons come from its Park() call.
const (
	// reasonUnresolvedSecret is D-4's mandated reason for a config value
	// that still carries a whole-value "secret://" prefix.
	reasonUnresolvedSecret = "unresolved-secret"

	// reasonConfigInvalid is the park reason for a config document that
	// fails to load for any other reason once the admin surface exists.
	reasonConfigInvalid = "config-invalid"

	// reasonRuntimeError prefixes the park reason for an error the Run hook
	// returned without naming a park reason of its own (lifecycle.md,
	// "Errors that are not parks"). The message follows the prefix.
	reasonRuntimeError = "runtime-error: "

	// reasonStartupError prefixes the failed reason for the one fail-stop
	// that happens after the status publisher exists but before the admin
	// surface does — the only place the harness ever reports `failed`.
	reasonStartupError = "startup-error: "

	// reasonCheckpointConflict is spec/checkpoint.md's machine-readable park
	// reason for a lost checkpoint CAS: another writer is live right now,
	// or an operator reset the key mid-run; either needs an operator.
	reasonCheckpointConflict = "checkpoint-conflict"
)

// Hooks is the connector-supplied behavior the harness drives. Only Run is
// required; a nil Probe, Resume, or Skip means the connector does not
// implement that surface, and the harness answers accordingly instead of
// pretending it does.
type Hooks struct {
	// Probe validates config and probes the external system for the D-11
	// `check` mode. It is called after the config document is loaded, so it
	// may read the config struct the connector handed to Main. Its ctx
	// carries the check mode's deadline, 10s for the config load and the
	// probe together, so a connector needs no timeout of its own. A probe
	// that honors ctx returns the error ctx led to, and check prints that,
	// URL credentials redacted. One that ignores ctx is cut off: check waits
	// a moment past the deadline, prints its Result and exits without it.
	// Probe must not write to stdout, which carries check's one Result line.
	Probe func(context.Context) error

	// Run is the connector's main loop. It returns when the loop finishes
	// (nil), when it wants to park (an error from Park), or when it fails
	// (any other error). It MUST return promptly once ctx is done — the
	// D-16 drain budget covers this unwind, not just the NATS drain;
	// rt.ShutdownContext bounds the work it does on the way out.
	//
	// An error wrapping checkpoint.ErrConflict parks with reason
	// "checkpoint-conflict" (spec/checkpoint.md), unless it also wraps a
	// Park, which still wins. Parkf never puts its own reason over either;
	// it only prefixes the message. An error returned after ctx is done is treated
	// as a drain, not a park — it is the ordinary way a loop unwinds on
	// SIGTERM, so no status document is published for it.
	//
	// Run is re-entered after every successful resume, so it must be safe
	// to call more than once.
	Run func(ctx context.Context, rt *Runtime) error

	// Resume is invoked by the admin `resume` endpoint before the loop is
	// re-entered, giving the connector a chance to reset whatever the park
	// left behind, including after the park on a config error, where Run has
	// not been entered yet. ctx carries the admin request's deadline, is
	// cancelled once the request is answered, and is cancelled when the
	// harness shuts down, so a Resume that dials or waits should honor it.
	// The harness waits for Resume to return whatever ctx says: one that
	// ignores ctx holds the admin request, and blocks every other resume or
	// skip, until it does. rt is the same Runtime Run is handed, so Resume
	// reaches the connection and the status publisher without the connector
	// keeping a reference of its own. A nil Resume means "nothing to do",
	// not "unsupported": resume itself is always available once parked
	// (spec/admin.md), it is only skip that a connector can decline.
	Resume func(ctx context.Context, rt *Runtime) error

	// Skip discards the poison item identified by key and then behaves like
	// Resume (spec/admin.md); ctx and rt are what Resume gets. A nil Skip
	// means the connector does not support skipping; the endpoint answers
	// every call with an error.
	Skip func(ctx context.Context, rt *Runtime, key string) error
}

// Runtime is what the harness hands to Hooks.Run: the resolved runtime
// identity (D-3), the connector's own NATS connection (the one status and
// admin already share — a connector must not open a second), and the status
// publisher it reports progress through (SetCursor/SetBehind/SetBackfill/
// Add/SetState).
type Runtime struct {
	ID     identity.Identity
	Conn   *nats.Conn
	Status *status.Publisher

	// Version is the connector's version: the one passed to Main without
	// the "v" of a "v1.2.3" tag, or, when that was empty, the main module's
	// version from the build info, or "0.0.0-dev" for a build with none (a
	// test binary, or a plain go build of a module below the repository
	// root). It is the admin service's version (D-8), which accepts
	// pseudo-versions and build metadata such as "+dirty", so record the
	// same value wherever the connector stamps its writes.
	Version string

	// clock is the harness's shutdown clock, for ShutdownContext.
	clock *shutdownClock
}

// ShutdownContext returns a context for the work Run does after its ctx is
// done, such as flushing a batch or saving a final checkpoint. It carries
// ctx's values but is not cancelled with it. Its deadline falls shortly
// before the harness stops waiting for Run (D-16's shutdown budget, less the
// share kept for the NATS drain, counted from the shutdown signal), so a Run
// that returns when it ends is never abandoned. Before a signal has arrived,
// or on a Runtime the harness didn't build, as in a test, it counts from now.
func (rt *Runtime) ShutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	var clock *shutdownClock
	if rt != nil {
		clock = rt.clock
	}
	deadline := clock.hookDeadline(shutdownBudget).Add(-hookGrace(shutdownBudget))
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

// Main is a connector's entrypoint. It selects the D-11 mode from the
// process arguments, installs the D-16 signal handling, and returns the
// process exit code the caller passes to os.Exit.
//
// name is the connector's manifest name (the D-3 identity fallback),
// version is its own version (the D-8 admin service version: SemVer, a
// leading "v" allowed, or empty to take it from the build info; anything else
// fails startup), cfg is a pointer to the connector's config struct (nil for
// a flags-only connector), and caps is the D-12 capability list.
//
// Main reads the mode from flag.Args() when the caller has already run
// flag.Parse() — the documented order, since a connector parses its flags
// before calling Main — and from os.Args otherwise.
//
// opts adjust the harness; see Option. The normal call passes none.
func Main(name, version string, cfg any, caps []string, h Hooks, opts ...Option) int {
	// SIGINT gets the same treatment as SIGTERM: D-16 only names SIGTERM
	// (that is what a supervisor sends), but a developer's Ctrl-C should
	// drain the same way rather than killing the process mid-flush.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	return run(ctx, commandArgs(), name, version, cfg, caps, h, applyOptions(opts))
}

// Option adjusts how Main runs the harness. It is the extension point for the
// overrides the SDK plans — the NATS connection, the Identity, and the source
// of the config document — none of which exists yet, so no Option is defined
// today: zero options is the normal call, and every connector written
// against it keeps compiling when the first Option lands.
type Option func(*options)

// options is the harness configuration the Options build. It has no fields
// yet; see Option.
type options struct{}

// applyOptions folds opts into the harness configuration Main hands to run.
func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// run is Main's body with the argument source, the shutdown context and the
// applied options injected, so the harness is testable without
// process-global signals.
func run(
	ctx context.Context, args []string, name, version string, cfg any, caps []string, h Hooks, _ options,
) int {
	switch modeFromArgs(args) {
	case modeSpec:
		return runSpec(os.Stdout, cfg, caps)
	case modeCheck:
		return runCheck(ctx, cfg, h)
	default:
		return runConnector(ctx, name, connectorVersion(version), cfg, h)
	}
}

// commandArgs returns the connector's positional arguments. A connector
// parses its flags before calling Main (D-11 keeps flags valid in every
// mode), so flag.Args() is the authoritative source once flag.Parse() has
// run; os.Args is the fallback for a connector with no flags at all.
func commandArgs() []string {
	if flag.Parsed() {
		return flag.Args()
	}
	if len(os.Args) < 2 {
		return nil
	}
	return os.Args[1:]
}

// modeFromArgs implements D-11's mode selection: the first positional
// argument selects the mode, and only the exact tokens "spec" and "check"
// select a non-run mode — "any value other than spec/check" is the run mode
// (spec/contract.md).
func modeFromArgs(args []string) string {
	if len(args) == 0 {
		return modeRun
	}
	switch args[0] {
	case modeSpec:
		return modeSpec
	case modeCheck:
		return modeCheck
	default:
		return modeRun
	}
}

// runSpec implements D-11's `spec` mode: print the D-12 spec document to
// stdout and exit 0. It reads no config document and opens no NATS
// connection — a connector must be able to describe itself with neither
// present (spec/contract.md).
func runSpec(w io.Writer, cfg any, caps []string) int {
	// A flags-only connector hands the harness no config struct. It still
	// has to answer `spec`, so it describes itself as taking the empty
	// object: an anonymous empty struct produces exactly that schema,
	// rather than this package hand-rolling a second copy of the shape
	// sdk-go/spec already owns.
	target := cfg
	if target == nil {
		target = struct{}{}
	}
	if caps == nil {
		caps = []string{} // D-12's capabilities is a list, never null
	}

	s, err := spec.FromStruct(target, caps)
	if err != nil {
		log.Printf("runtime: spec: %v", err)
		return 1
	}
	data, err := s.JSON()
	if err != nil {
		log.Printf("runtime: spec: %v", err)
		return 1
	}
	if _, err := fmt.Fprintln(w, string(data)); err != nil {
		log.Printf("runtime: spec: write stdout: %v", err)
		return 1
	}
	return 0
}

// runCheck implements D-11's `check` mode. The config document is loaded
// first — the probe dials the system that document names — but no NATS
// connection is opened: check runs as a throwaway job against config that
// has not been deployed yet (spec/contract.md), where no runtime identity
// or status subject exists.
//
// A config-load failure is reported as a failed Result rather than a bare
// non-zero exit, so the caller gets the same one-line answer either way.
func runCheck(ctx context.Context, cfg any, h Hooks) int {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	return check.Run(ctx, func(ctx context.Context) error {
		if err := loadConfig(cfg); err != nil {
			return err
		}
		if h.Probe == nil {
			return errors.New("check: this connector provides no probe")
		}
		return h.Probe(ctx)
	})
}

// runConnector implements the D-11 run mode: the full harness. It returns 0
// for every outcome once the admin surface is up — a failure there parks
// (D-15) rather than exiting non-zero into a supervisor's restart loop — so
// the non-zero returns below are exactly lifecycle.md's fail-stop window.
//
// Startup order is load-bearing. The config document is read first, because
// an unparseable one is lifecycle.md's fail-stop case — an unrecoverable
// error before the admin surface exists, where parking would buy nothing
// since no operator can reach the process. The one exception is D-4's
// unresolved secret, which spec/config.md requires be reported as a park
// with reason "unresolved-secret"; that error is carried past startup and
// handed to the park loop below, so the connector comes up far enough to
// report it and to accept a resume.
func runConnector(ctx context.Context, name, version string, cfg any, h Hooks) int {
	if h.Run == nil {
		log.Print("runtime: Hooks.Run is required in run mode")
		return 1
	}

	id, err := identity.Detect(name)
	if err != nil {
		// The earliest fail-stop of all: a malformed identity is caught
		// before the status publisher or admin surface exist, so there is
		// nothing to park on and nowhere to report a `failed` document — the
		// only honest move is to refuse to start (lifecycle.md's pre-admin
		// fail-stop). Parking would derive its own park subject from the same
		// poisoned identity.
		log.Printf("runtime: %v", err)
		return 1
	}

	// A cancellable child of the shutdown context: the NATS ClosedHandler
	// cancels it so an unrecoverable connection loss unwinds the serve
	// loop and the process exits, rather than blocking forever on ctx/resume
	// with a dead connection nothing can reach.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// shuttingDown gates the ClosedHandler off once the harness has begun its
	// own intentional drain, so a Closed reached by that drain is not
	// mistaken for an unexpected loss. natsClosed records that the exit was
	// forced by connection loss, which is a failure exit, not a clean stop.
	var shuttingDown, natsClosed atomic.Bool

	cfgErr := loadConfig(cfg)
	if cfgErr != nil && !errors.Is(cfgErr, config.ErrUnresolvedSecret) {
		log.Printf("runtime: %v", cfgErr)
		return 1
	}

	nc, err := connectFromEnv(name, func() {
		if shuttingDown.Load() {
			return // the harness's own drain closed it; expected.
		}
		natsClosed.Store(true)
		log.Printf("runtime: NATS connection reached Closed unexpectedly; " +
			"cancelling to exit for a supervisor restart")
		cancel()
	})
	if err != nil {
		log.Printf("runtime: %v", err)
		return 1
	}

	pub := status.NewPublisher(nc, id, status.WithErrorHandler(func(err error) {
		log.Printf("runtime: %v", err)
	}))
	if pub == nil {
		// NewPublisher never returns nil. If a refactor ever made it, the
		// no-op nil Publisher would let the admin status endpoint serve the
		// zero Document, which status.schema.json rejects: fail here
		// instead.
		panic("runtime: no status publisher")
	}
	// D-16's 10 seconds are counted from the signal, not from the moment
	// the Run hook finally returns, so a slow unwind eats into the same
	// budget the NATS drain draws on. The clock is armed below, once the
	// admin surface is up.
	clock := &shutdownClock{}

	// The one Runtime of the process: Run is re-entered with it after every
	// resume, and Resume and Skip are handed the same value.
	rt := &Runtime{ID: id, Conn: nc, Status: pub, Version: version, clock: clock}
	ctl := newController(pub, h, rt, ctx)

	svc, err := admin.Start(nc, id, version, ctl)
	if err != nil {
		// The last fail-stop, and the only one with a publisher already in
		// hand: without an admin surface there is nothing for an operator to
		// resume, so parking would buy nothing (lifecycle.md). The status
		// document is best-effort — it tells whoever is watching the subject
		// why the process is about to disappear.
		log.Printf("runtime: %v", err)
		pub.SetState(status.Failed, reasonStartupError+err.Error())
		nc.Close()
		return 1
	}

	// The status ticker outlives ctx: it must keep republishing (D-5)
	// while the connector drains, and is stopped explicitly once the final
	// document is out.
	pubCtx, stopPub := context.WithCancel(context.Background())
	defer stopPub()
	go pub.Run(pubCtx)

	stopClock := context.AfterFunc(ctx, clock.mark)
	defer stopClock()

	hs := &harness{cfg: cfg, hooks: h, ctl: ctl, rt: rt}

	// Publish the seeded `running` document so an observer sees the
	// connector the moment it is up, rather than waiting out the first
	// ticker interval — unless it is about to park on the config error
	// carried in from startup, where announcing `running` first would be a
	// transient lie.
	if cfgErr == nil {
		pub.SetState(status.Running, "")
	}
	awaitServe(ctx, hs, cfgErr, clock)

	// From here the harness is draining on purpose: silence the ClosedHandler
	// so the drain's own Close is not read as an unexpected loss (it must not
	// fight the normal drain-exit path).
	shuttingDown.Store(true)

	// Nothing is published on the way out. D-6 has no terminal state, and a
	// synthetic last document — `paused`, say — would sit in the retained
	// stream describing a process that no longer exists, so a consumer
	// reading it would show "paused" for a stopped workload. The connector's
	// last document is the true one it was in when it stopped (`running`, or
	// `parked` with its reason); silence plus the workload's own lifecycle is
	// the stop signal (spec/lifecycle.md, "Drain").
	if err := svc.Stop(); err != nil {
		log.Printf("runtime: stop admin service: %v", err)
	}
	stopPub()

	if err := drain(nc, clock.remaining(shutdownBudget)); err != nil {
		// A drain that overran its budget is a D-16 violation worth
		// reporting, but it does not change the exit code: every path that
		// reaches here is a clean stop, and failures parked rather than
		// getting this far.
		log.Printf("runtime: %v", err)
	}

	// The one non-zero return once the admin surface is up: NATS went
	// away for good, so the park machinery — which needs NATS to publish
	// `parked` and to serve resume — could not run. Exiting non-zero tells an
	// on-failure supervisor to restart the connector against a recovered
	// NATS, rather than the process zombieing with a dead connection.
	if natsClosed.Load() {
		return 1
	}
	return 0
}

// awaitServe runs the park state machine and waits for it, but only for as
// long as D-16 allows once the shutdown signal has arrived.
//
// The loop is given the budget less the NATS drain's reserve, counted from
// the signal (shutdownClock.hookDeadline, which Runtime.ShutdownContext's
// deadline is also derived from); if it has not returned by then it is
// abandoned. Abandoning means exactly that — a Go goroutine cannot be
// killed, so the Run hook keeps running until the process exits moments
// later, and any status document it tries to publish on the way loses a
// race with the drain or fails on the closed connection. That is the
// deliberate trade: D-16 says exit within 10 seconds, and a connector whose
// Run hook ignores its cancelled context must not be able to turn that into
// "never exits, supervisor sends SIGKILL".
func awaitServe(ctx context.Context, hs *harness, cfgErr error, clock *shutdownClock) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		hs.serve(ctx, cfgErr)
	}()

	select {
	case <-done:
		// Stopped on its own; no signal, no budget to honor.
		return
	case <-ctx.Done():
	}

	if wait := time.Until(clock.hookDeadline(shutdownBudget)); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
		}
	}
	log.Printf("runtime: the run hook did not return within its share of the %s shutdown budget; "+
		"abandoning it and draining (D-16). Bound the work it does on the way out with "+
		"Runtime.ShutdownContext", shutdownBudget)
}

// loadConfig applies the config document to cfg (D-2, D-14). A connector
// with no config struct, or a run with no document mounted, is not an error:
// the caller's already-parsed flag values stand, which is the documented
// fallback (spec/config.md, "Loader precedence").
func loadConfig(cfg any) error {
	if cfg == nil {
		return nil
	}
	err := config.Load(cfg)
	if errors.Is(err, config.ErrNoDocument) {
		return nil
	}
	return err
}

// checkTimeout bounds the D-11 check mode: loading the config and running
// Hooks.Probe. A variable so tests can shorten it.
var checkTimeout = 10 * time.Second

// connectorVersion returns version without the "v" of a "v1.2.3" tag, or
// when it is empty the main module's version from the build info, or
// "0.0.0-dev". The admin service requires SemVer (D-8); any other explicit
// version is passed on unchanged, so a malformed release version fails
// startup instead of being hidden.
func connectorVersion(version string) string {
	if version != "" {
		return trimV(version)
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v := semverFromModule(bi.Main.Version); v != "" {
			return v
		}
	}
	return "0.0.0-dev"
}

// semverFromModule turns a Go module version ("v1.2.3", or a pseudo-version)
// into SemVer by dropping the "v". It returns "" for a build with no module
// version ("(devel)" or empty).
func semverFromModule(v string) string {
	if v == "" || v == "(devel)" {
		return ""
	}
	return trimV(v)
}

// trimV drops the leading "v" of a version such as "v1.2.3", and leaves any
// other string as it is.
func trimV(v string) string {
	if len(v) > 1 && v[0] == 'v' && '0' <= v[1] && v[1] <= '9' {
		return v[1:]
	}
	return v
}
