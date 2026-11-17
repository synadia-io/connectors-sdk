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

// Package driver builds and black-box-drives a connector binary over the nex
// launch contract (NEX_WORKLOAD_* env seam) — the language-agnostic mechanism
// of the conformance tier: any connector directory that produces a binary can
// be driven, whatever it is written in.
package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// buildCache dedups binary builds across tests in one run. The binaries live
// under buildDir, a directory scoped to the whole test binary process, not
// any one *testing.T — a per-call t.TempDir() would get removed the moment
// the test (or subtest) that happened to trigger the build finishes,
// breaking every later Build call for the same connector from a different
// test function despite the cache hit returning a path that no longer
// exists. os.MkdirTemp is left for the OS to reap; conformance binaries are
// small and test runs are short-lived.
var (
	buildMu    sync.Mutex
	buildCache = map[string]string{}
	buildDir   string
)

// Build compiles the connector in dir (a directory containing a main package)
// once per test run and returns the binary path.
func Build(t *testing.T, dir string) string {
	t.Helper()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	buildMu.Lock()
	defer buildMu.Unlock()
	if bin, ok := buildCache[abs]; ok {
		return bin
	}
	if _, err := os.Stat(filepath.Join(abs, "go.mod")); err != nil {
		t.Skipf("connector source not found at %s: %v", abs, err)
	}
	if buildDir == "" {
		d, err := os.MkdirTemp("", "connectors-conformance-bin-")
		if err != nil {
			t.Fatalf("create build dir: %v", err)
		}
		buildDir = d
	}
	bin := filepath.Join(buildDir, filepath.Base(abs)+"-conformance")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = abs
	var out bytes.Buffer
	build.Stdout, build.Stderr = &out, &out
	if err := build.Run(); err != nil {
		t.Fatalf("build %s: %v\n%s", abs, err, out.String())
	}
	buildCache[abs] = bin
	return bin
}

// Run executes bin with args to completion and returns its combined
// stdout+stderr output and exit error (nil iff exit 0). Unlike Spawn, it
// sets no NEX_WORKLOAD_* env at all — for the D-11 modes that need neither a
// NATS connection nor a config document, namely `spec` (contract.md: "takes
// no config... opens no NATS connection").
func Run(t *testing.T, bin string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// Identity is the spec/status.md D-3 runtime-identity env vars a nexlet
// injects. Tests that don't care about a specific instanceID can leave this
// zero-valued: Env returns no entries, and the connector falls back to its
// own "<name>-dev" identity (D-3) — still deterministic for a single Spawn
// per test.
type Identity struct {
	ConnectorID string
	Namespace   string
	WorkloadID  string
}

// Env returns id's NEX_WORKLOAD_* identity entries, omitting any field left
// zero so a caller can leave individual fields to their D-3 fallback.
func (id Identity) Env() []string {
	var env []string
	if id.ConnectorID != "" {
		env = append(env, "NEX_WORKLOAD_CONNECTOR_ID="+id.ConnectorID)
	}
	if id.Namespace != "" {
		env = append(env, "NEX_WORKLOAD_NAMESPACE="+id.Namespace)
	}
	if id.WorkloadID != "" {
		env = append(env, "NEX_WORKLOAD_ID="+id.WorkloadID)
	}
	return env
}

// StatusSubject returns the D-5 status subject for namespace/instanceID.
func StatusSubject(namespace, instanceID string) string {
	return "$CONNECTOR." + namespace + ".status." + instanceID
}

// AdminRoot returns the D-8 admin service root subject for
// namespace/instanceID.
func AdminRoot(namespace, instanceID string) string {
	return "$CONNECTOR." + namespace + ".admin." + instanceID
}

// Proc is a running connector binary.
type Proc struct {
	cmd *exec.Cmd
	out *safeBuf
}

// Spawn launches the binary with the nex env contract (NATS servers; extra env
// entries appended verbatim) and the given argv. The process is killed on test
// cleanup unless the test already stopped it.
func Spawn(t *testing.T, bin, natsURL string, env []string, args ...string) *Proc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "NEX_WORKLOAD_NATS_SERVERS="+natsURL)
	cmd.Env = append(cmd.Env, env...)
	out := &safeBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	p := &Proc{cmd: cmd, out: out}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return p
}

// SpawnWithConfig launches bin in config-document mode (spec/config.md,
// D-2): config is marshaled to JSON, written to a temp file, and mounted via
// NEX_WORKLOAD_CONFIG_FILE; id's identity vars (D-3) are set alongside it so
// the caller knows the resulting status/admin subjects up front instead of
// reading them back off the fallback chain. extraEnv is appended after both,
// so a caller can still override an individual entry.
func SpawnWithConfig(t *testing.T, bin, natsURL string, id Identity, config any, extraEnv []string, args ...string) *Proc {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	env := append([]string{"NEX_WORKLOAD_CONFIG_FILE=" + path}, id.Env()...)
	env = append(env, extraEnv...)
	return Spawn(t, bin, natsURL, env, args...)
}

// Output returns everything the process has written so far.
func (p *Proc) Output() string { return p.out.String() }

// WaitOutput blocks until the process output contains substr.
func (p *Proc) WaitOutput(t *testing.T, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(p.out.String(), substr) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in output:\n%s", substr, p.out.String())
}

// KillNow sends SIGKILL — an uncatchable, unrecoverable termination, unlike
// StopGraceful's SIGTERM — and blocks until the OS has reaped the process.
// This is the real-crash primitive: every other "restart" path exercises an
// in-process ctx-cancel or a graceful shutdown, never an actual crash window
// where the process has no chance to run any unwind code at all. A caller
// restarts by spawning a fresh Proc (Spawn/SpawnWithConfig) with the same
// identity/config after calling this, exactly as a supervisor would after an
// OOM-kill or a `kill -9`.
func (p *Proc) KillNow(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9: %v", err)
	}
	if err := p.cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("wait after kill -9: %v", err)
		}
	}
}

// WaitExit blocks until the process exits, or fails the test after timeout,
// and returns its exit code (0 for a clean exit).
func (p *Proc) WaitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			return 0
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		t.Fatalf("wait: %v", err)
		return -1
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		t.Fatalf("process did not exit within %s\noutput:\n%s", timeout, p.Output())
		return -1
	}
}

// DrainBudget is lifecycle.md's D-16 normative bound: SIGTERM to exit 0
// within this long, covering the connector's own unwind and its NATS drain
// together. StopGraceful hardcodes it rather than taking a per-call
// timeout — lifecycle.md's "Drain" section flags exactly the gap this
// closes: a driver that checked "exits 0 within some generous outer bound"
// rather than the normative number itself would pass a connector that took,
// say, 12 seconds. A fixed driver constant means no suite can quietly loosen
// it back to the old 15s.
const DrainBudget = 10 * time.Second

// StopGraceful sends SIGTERM and asserts the spec/lifecycle.md D-16 drain
// contract: the connector drains and exits 0 within DrainBudget.
func (p *Proc) StopGraceful(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SIGTERM must exit 0 (drain contract), got: %v\noutput:\n%s", err, p.Output())
		}
	case <-time.After(DrainBudget):
		_ = p.cmd.Process.Kill()
		t.Fatalf("no exit within %s of SIGTERM (drain contract, D-16)\noutput:\n%s", DrainBudget, p.Output())
	}
}

// StatusDoc is the subset of spec/status.schema.json's shape (D-5..D-7) a
// conformance test reads as typed fields. A test that needs to check the
// full wire shape validates the paired StatusMsg.Raw against
// status.schema.json directly instead of extending this struct.
type StatusDoc struct {
	Contract string           `json:"contract"`
	State    string           `json:"state"`
	Reason   string           `json:"reason"`
	Counters map[string]int64 `json:"counters"`
}

// StatusMsg pairs a decoded StatusDoc with the raw bytes it arrived as.
type StatusMsg struct {
	Raw []byte
	Doc StatusDoc
}

// StatusWatcher subscribes to a connector's D-5 status subject and
// accumulates every document published to it, so a test can subscribe
// before starting the connector — never racing the first publish — and then
// wait for a specific transition.
type StatusWatcher struct {
	mu      sync.Mutex
	msgs    []StatusMsg
	checked int // number of msgs already scanned by a completed WaitFor call; see WaitFor
	got     chan struct{}
	sub     *nats.Subscription
}

// WatchStatus subscribes to namespace/instanceID's status subject (D-5) on
// nc. Call it before Spawn/SpawnWithConfig so the connector's first publish
// is never missed to a race between subscribe and start.
func WatchStatus(t *testing.T, nc *nats.Conn, namespace, instanceID string) *StatusWatcher {
	t.Helper()
	w := &StatusWatcher{got: make(chan struct{}, 64)}
	sub, err := nc.Subscribe(StatusSubject(namespace, instanceID), func(m *nats.Msg) {
		var doc StatusDoc
		_ = json.Unmarshal(m.Data, &doc) // a malformed doc still lands in Msgs, for a caller to fail on
		w.mu.Lock()
		w.msgs = append(w.msgs, StatusMsg{Raw: append([]byte(nil), m.Data...), Doc: doc})
		w.mu.Unlock()
		select {
		case w.got <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe status: %v", err)
	}
	w.sub = sub
	t.Cleanup(func() { _ = sub.Drain() })
	return w
}

// WaitFor blocks until a received document satisfies pred, or fails the test
// after timeout, returning the first match. It replays already-received
// documents before waiting for new ones, so a predicate for a transition
// that already happened (e.g. the connector parked before WaitFor was
// called) still matches instead of hanging.
//
// The scan cursor belongs to the watcher, not to the call: a document, once
// scanned by any WaitFor (matched or not), is never reconsidered by a later
// one. Without this, a second WaitFor for a predicate a document earlier in
// the stream already satisfied — e.g. a second WaitFor(state == "running")
// after a resume, following a first WaitFor(state == "running") for the
// initial boot — would trivially re-match that first, already-consumed
// document instead of waiting for the genuinely new one, silently turning
// the second wait into a no-op rather than the synchronization point the
// caller relies on it to be.
func (w *StatusWatcher) WaitFor(t *testing.T, timeout time.Duration, pred func(StatusDoc) bool) StatusMsg {
	t.Helper()
	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		for ; w.checked < len(w.msgs); w.checked++ {
			if pred(w.msgs[w.checked].Doc) {
				m := w.msgs[w.checked]
				w.checked++
				w.mu.Unlock()
				return m
			}
		}
		w.mu.Unlock()
		select {
		case <-w.got:
		case <-deadline:
			t.Fatalf("status: no matching document within %s", timeout)
			return StatusMsg{}
		}
	}
}

// Messages returns a copy of every status document observed so far, in
// arrival order. Used by tests that must inspect the WHOLE sequence (every
// document validates against the schema, not just the first; no adjacent
// pair in the sequence is an illegal state transition) rather than
// a single matching one.
func (w *StatusWatcher) Messages() []StatusMsg {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]StatusMsg, len(w.msgs))
	copy(out, w.msgs)
	return out
}

// WaitForCount blocks until at least n documents have been observed in
// total, or fails the test after timeout, then returns the full sequence
// (Messages). Unlike WaitFor (which matches on document content), this
// waits purely on COUNT — the tool for observing an event with no
// distinguishing content of its own, e.g. the periodic republish tick
// (same state, same reason, nothing to match on except "one more arrived")
// or TestCrashAndRestart's restart (a fresh process's first status document, which a
// content-based WaitFor("running") would confuse with the old process's
// already-recorded running document via WaitFor's own replay behavior).
func (w *StatusWatcher) WaitForCount(t *testing.T, n int, timeout time.Duration) []StatusMsg {
	t.Helper()
	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		got := len(w.msgs)
		w.mu.Unlock()
		if got >= n {
			return w.Messages()
		}
		select {
		case <-w.got:
		case <-deadline:
			t.Fatalf("status: only %d/%d documents observed within %s", got, n, timeout)
			return nil
		}
	}
}

// AdminRequest sends body to <root>.<endpoint> (spec/admin.md, D-8) and
// returns the raw reply, failing the test on a NATS-level send/timeout
// error. A micro endpoint error (e.g. resume when not parked) still arrives
// as an ordinary reply with the Nats-Service-Error headers set — AdminErr
// reads them.
func AdminRequest(t *testing.T, nc *nats.Conn, root, endpoint string, body []byte, timeout time.Duration) *nats.Msg {
	t.Helper()
	msg, err := nc.Request(root+"."+endpoint, body, timeout)
	if err != nil {
		t.Fatalf("admin request %s.%s: %v", root, endpoint, err)
	}
	return msg
}

// AdminErr reports whether reply carries a NATS micro error (spec/admin.md
// "Errors": the standard Nats-Service-Error/Nats-Service-Error-Code headers)
// and, if so, its message.
func AdminErr(reply *nats.Msg) (message string, ok bool) {
	if reply.Header == nil {
		return "", false
	}
	if e := reply.Header.Get("Nats-Service-Error"); e != "" {
		return e, true
	}
	return "", false
}

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// LastJSONLine returns the last line of s that starts with "{": check and
// spec mode print exactly one JSON line, but log lines, captured into the same
// combined output by Run, may precede it.
func LastJSONLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") {
			return line
		}
	}
	return ""
}
