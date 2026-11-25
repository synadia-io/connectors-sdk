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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
	"github.com/synadia-io/connectors-sdk/sdk-go/config"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// parkError is the sentinel Park returns. It is matched with errors.As, so a
// connector may wrap it (fmt.Errorf("%w", runtime.Park("poison"))) and still
// park. cause is the error Parkf was given, if any.
type parkError struct {
	reason string
	cause  error
}

func (e *parkError) Error() string { return "park: " + e.reason }

func (e *parkError) Unwrap() error { return e.cause }

// Park builds the sentinel error a Hooks.Run implementation returns to halt
// its loop in-process (D-15): the harness publishes `parked` with reason,
// keeps serving status and admin, and re-enters Hooks.Run when an operator
// calls `resume` or `skip`.
//
// reason is the machine-readable string that lands in the status document's
// `reason` field (status.md); an empty one is reported as "unspecified"
// rather than leaving an operator with a parked connector and no clue.
func Park(reason string) error {
	if reason == "" {
		reason = "unspecified"
	}
	return &parkError{reason: reason}
}

// Parkf parks with the reason "<where>: <err>": the phase that failed, then
// the error. where is a phase name, not a format string, and an empty one
// adds no prefix. It is the usual way a Run hook stops on a failure it can't
// get past, such as Parkf("stream-ensure", err). The result wraps err, so
// errors.Is and errors.As still see it. The harness redacts URL credentials
// from every reason it publishes; other secrets in err are the connector's to
// scrub first.
//
// An err that already carries its reason keeps it, and where only prefixes
// the message: a Park, whose reason is the connector's own, and an err that
// wraps checkpoint.ErrConflict, which spec/checkpoint.md requires be
// reported as "checkpoint-conflict".
func Parkf(where string, err error) error {
	if err == nil {
		return Park(where)
	}
	if _, ok := parkReason(err); ok || errors.Is(err, checkpoint.ErrConflict) {
		if where == "" {
			return err
		}
		return fmt.Errorf("%s: %w", where, err)
	}
	reason := err.Error()
	if where != "" {
		reason = where + ": " + reason
	}
	return &parkError{reason: reason, cause: err}
}

// parkReason reports whether err is (or wraps) a Park sentinel, and the
// reason it carries.
func parkReason(err error) (string, bool) {
	var pe *parkError
	if errors.As(err, &pe) {
		return pe.reason, true
	}
	return "", false
}

// controller implements admin.Controller over the harness's park state
// (D-8, D-15). It is the only writer of parked outside the serve loop. The
// park state is guarded by mu because micro invokes these methods from its
// own subscription callbacks; the rest is immutable after construction.
type controller struct {
	pub   *status.Publisher
	hooks Hooks

	// rt is the Runtime Hooks.Run is handed; Resume and Skip get the same
	// one.
	rt *Runtime

	// shutdown is the harness's context, done once the shutdown signal has
	// arrived; the ctx a hook gets is cancelled with it.
	shutdown context.Context

	mu     sync.Mutex
	parked bool

	// resume carries the unpark signal to the serve loop. Buffered so a
	// resume that lands in the window between "parked" being set and the
	// loop reaching its select is not lost.
	resume chan struct{}
}

func newController(
	pub *status.Publisher, h Hooks, rt *Runtime, shutdown context.Context,
) *controller {
	return &controller{
		pub: pub, hooks: h, rt: rt, shutdown: shutdown, resume: make(chan struct{}, 1),
	}
}

// hookContext derives the ctx a Resume or Skip hook gets from the admin
// request's: it keeps the request's deadline and is also cancelled when the
// harness shuts down, so a hook in flight when SIGTERM arrives is told, and
// the drain is not held behind it (unpark holds the lock across the hook).
func (c *controller) hookContext(req context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(req)
	stop := context.AfterFunc(c.shutdown, cancel)
	return ctx, func() { stop(); cancel() }
}

// Status returns the current status document, the same value published to
// the status subject (D-5, spec/admin.md: "byte-for-byte the same shape").
func (c *controller) Status() status.Document { return c.pub.Snapshot() }

// Resume implements the admin `resume` endpoint: invoke Hooks.Resume with the
// request's ctx (hookContext), then release the parked loop. It errors when
// the connector is not parked — resume reverses a park, it is not a generic
// restart (spec/admin.md).
func (c *controller) Resume(ctx context.Context) error {
	return c.unpark("resume", func() error {
		if c.hooks.Resume == nil {
			return nil
		}
		ctx, cancel := c.hookContext(ctx)
		defer cancel()
		return c.hooks.Resume(ctx, c.rt)
	})
}

// Skip implements the admin `skip` endpoint: discard the poison item named
// by key, then behave like Resume. It errors when the connector declines to
// support skipping, and (like Resume) when it is not parked.
func (c *controller) Skip(ctx context.Context, key string) error {
	if c.hooks.Skip == nil {
		return errors.New("skip: unsupported by this connector")
	}
	return c.unpark("skip", func() error {
		ctx, cancel := c.hookContext(ctx)
		defer cancel()
		return c.hooks.Skip(ctx, c.rt, key)
	})
}

// unpark runs action and, only if it succeeds, releases the parked loop.
// The lock is held across action so two concurrent admin requests cannot
// both unpark the same park; admin.Controller already requires hooks that
// do not block indefinitely.
func (c *controller) unpark(endpoint string, action func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.parked {
		return fmt.Errorf("%s: connector is not parked", endpoint)
	}
	if err := action(); err != nil {
		// The hook refused: stay parked so the operator can retry after
		// fixing whatever it complained about.
		return fmt.Errorf("%s: %w", endpoint, err)
	}

	c.parked = false
	select {
	case c.resume <- struct{}{}:
	default:
	}
	return nil
}

// park marks the connector parked with reason, publishes it immediately
// (D-5), and blocks until an admin resume/skip unparks it — returning true,
// with the status already back to `running` — or until ctx is done, meaning
// the connector is draining while parked (lifecycle.md), returning false.
//
// If ctx is already done when park is called, it returns false at once
// without beginning a park: no park is begun once the shutdown signal has
// arrived, so a stopping process can't leave a `parked` document on a
// subject the next instance, with the same InstanceID, also uses. This only
// guarantees that a park observing ctx.Err() != nil at its start doesn't
// begin; a park that started just before the signal arrived is already past
// this check and may still publish a then-true `parked` document before it
// unwinds.
func (c *controller) park(ctx context.Context, reason string) bool {
	// A park that hasn't begun yet when the shutdown signal has already
	// arrived is not begun at all — see the doc comment above.
	if ctx.Err() != nil {
		log.Printf("runtime: not parking (%s): shutting down", status.RedactReason(reason))
		return false
	}
	log.Printf("runtime: parked (%s)", status.RedactReason(reason))

	c.mu.Lock()
	c.parked = true
	select {
	case <-c.resume: // discard a signal left over from a raced shutdown
	default:
	}
	// Published while mu is held: an admin resume arriving right now blocks
	// in unpark until the parked document is on the wire, so an observer can
	// never see the resume's `running` document without the `parked` one
	// that preceded it.
	c.pub.SetState(status.Parked, reason)
	c.mu.Unlock()

	select {
	case <-c.resume:
		c.pub.SetState(status.Running, "")
		return true
	case <-ctx.Done():
		c.mu.Lock()
		c.parked = false
		c.mu.Unlock()
		return false
	}
}

// harness is the run-mode state the serve loop needs, bundled so the loop
// reads as the state machine it is rather than a seven-argument function.
type harness struct {
	cfg   any
	hooks Hooks
	ctl   *controller
	rt    *Runtime
}

// serve runs the D-15 park state machine until the connector stops. It
// returns only on a clean stop — Hooks.Run finishing, or the shutdown
// context cancelling the loop or a park. Every failure inside it parks
// instead (lifecycle.md: "once the admin surface is up, every subsequent
// failure that would otherwise crash the process parks instead"), so there
// is no failure outcome for it to report: exiting non-zero under a
// supervisor's on-failure restart policy is the crash loop D-15 exists to
// prevent.
//
// cfgErr is a config-document failure carried over from startup (D-4's
// unresolved secret): it parks before the loop is ever entered, and a
// resume re-attempts the load rather than re-entering Hooks.Run with
// half-applied config.
func (hs *harness) serve(ctx context.Context, cfgErr error) {
	pending := cfgErr

	for {
		if pending != nil {
			reason := configParkReason(pending)
			log.Printf("runtime: config: %s", status.RedactReason(pending.Error()))
			if !hs.ctl.park(ctx, reason) {
				return
			}
			// The document is mounted read-only, but a resume is an
			// operator asserting they fixed the environment, so the load
			// is re-attempted rather than assumed still broken.
			pending = loadConfig(hs.cfg)
			continue
		}

		err := hs.runHook(ctx)

		switch {
		case err == nil:
			return

		case errors.Is(err, context.Canceled) && ctx.Err() != nil:
			// A loop unwinding on its own cancelled context is a drain,
			// not a failure — the most common way a connector returns
			// after SIGTERM.
			return

		default:
			// A Park sentinel names its own reason; a lost checkpoint CAS
			// gets spec/checkpoint.md's fixed reason, whatever the connector
			// wrapped it in; any other error is still a park, reported under
			// the generic prefix. The paths are otherwise identical, resume
			// included: a connector that forgot to wrap an error in Park()
			// gets the same operator-inspectable halt rather than a process
			// exit.
			reason, ok := parkReason(err)
			switch {
			case ok:
			case errors.Is(err, checkpoint.ErrConflict):
				reason = reasonCheckpointConflict
				log.Printf("runtime: %s", status.RedactReason(err.Error()))
			default:
				reason = reasonRuntimeError + err.Error()
			}
			if !hs.ctl.park(ctx, reason) {
				return
			}
		}
	}
}

// runHook calls the connector's Run hook, converting a panic into an
// ordinary error the loop can report. Mirrors sdk-go/check's treatment of
// Probe: a crashing connector should still park with a `runtime-error`
// reason (lifecycle.md) and keep serving its admin surface, rather than
// vanishing mid-loop with the operator left to guess from an empty status
// subject.
func (hs *harness) runHook(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("run: panic: %v", r)
		}
	}()
	return hs.hooks.Run(ctx, hs.rt)
}

// configParkReason maps a config-document failure to its machine-readable
// park reason: D-4 names "unresolved-secret" specifically, everything else
// is reported generically.
func configParkReason(err error) string {
	if errors.Is(err, config.ErrUnresolvedSecret) {
		return reasonUnresolvedSecret
	}
	return reasonConfigInvalid
}
