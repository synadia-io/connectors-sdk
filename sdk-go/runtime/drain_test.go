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

package runtime_test

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// TestMain_SIGTERM_DrainsAndExitsZero exercises the real signal wiring
// Main installs (D-16): a SIGTERM delivered to this process must cancel the
// Run hook's context, publish a final status document, close the NATS
// connection, and exit 0 — inside the 10 seconds D-16 makes normative. Every
// other test in this package drives the harness through RunHarness with an
// explicit context, so this is the one place the process-global signal path
// is covered.
func TestMain_SIGTERM_DrainsAndExitsZero(t *testing.T) {
	_, sub := startHarnessEnv(t)

	entered := make(chan struct{})
	var conn atomic.Value
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			conn.Store(rt.Conn)
			close(entered)
			<-ctx.Done()
			// Returning ctx.Err() is what a connector unwinding on
			// cancellation naturally does; it is a clean drain, not a failure.
			return ctx.Err()
		},
	}

	codes := make(chan int, 1)
	go func() {
		codes <- runtime.Main("test-connector", "1.0.0", nil, testCaps, h)
	}()

	select {
	case <-entered:
	case <-time.After(harnessTimeout):
		t.Fatal("Hooks.Run was never entered; not sending SIGTERM into an unguarded process")
	}

	signalledAt := time.Now()
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("syscall.Kill(SIGTERM): %v", err)
	}

	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("Main() = %d, want 0 after SIGTERM (D-16)", code)
	}
	if elapsed := time.Since(signalledAt); elapsed > 10*time.Second {
		t.Errorf("Main() took %s to return after SIGTERM, want under the 10s D-16 bound", elapsed)
	}

	nc, _ := conn.Load().(*nats.Conn)
	if nc == nil {
		t.Fatal("Runtime.Conn was nil inside Hooks.Run")
	}
	if !nc.IsClosed() {
		t.Error("NATS connection is still open after the drain (D-16)")
	}

	// Nothing synthetic goes out on the way down: the last document is the
	// `running` one the connector genuinely was in. A trailing `paused`
	// would sit in the retained stream describing a process that no longer
	// exists (spec/lifecycle.md, "Drain").
	last := drainStatusDocs(t, sub)
	if last == nil {
		t.Fatal("no status document was published at all, want the initial running one")
	}
	if last.State != status.Running {
		t.Errorf("last published state = %q, want %q — the drain must publish nothing new",
			last.State, status.Running)
	}
}

// TestMain_SIGTERM_WhileParkedKeepsTheParkedDocument guards the other half
// of the same rule: a connector that was parked when the signal arrived
// exits on its parked document, reason intact, rather than overwriting the
// one thing an operator would want to read afterwards.
func TestMain_SIGTERM_WhileParkedKeepsTheParkedDocument(t *testing.T) {
	_, sub := startHarnessEnv(t)

	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			return runtime.Park("poison")
		},
	}

	codes := make(chan int, 1)
	go func() {
		codes <- runtime.Main("test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("syscall.Kill(SIGTERM): %v", err)
	}
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("Main() = %d, want 0 after SIGTERM while parked (D-16)", code)
	}

	// Anything published after the parked document must still be that same
	// document (a periodic republish is fine, D-5); a new state on the way
	// out would erase the one thing an operator wants to read afterwards.
	if extra := drainStatusDocs(t, sub); extra != nil {
		if extra.State != status.Parked || extra.Reason != parked.Reason {
			t.Errorf("the drain published %q/%q, want nothing after parked/%q",
				extra.State, extra.Reason, parked.Reason)
		}
	}
}

// TestMain_SIGTERM_AbandonsARunHookThatIgnoresCancellation guards D-16's
// bound end to end, against the connector that would otherwise defeat it: a
// Run hook that never looks at its context. The harness must stop waiting
// for it, drain, and exit 0 inside the budget rather than hanging until the
// supervisor escalates to SIGKILL. The budget is shortened so the test does
// not have to sit through the real 10 seconds.
func TestMain_SIGTERM_AbandonsARunHookThatIgnoresCancellation(t *testing.T) {
	_, sub := startHarnessEnv(t)

	const budget = 300 * time.Millisecond
	restore := runtime.SetShutdownBudget(budget)

	// The hook blocks on this channel, not on ctx. It is released at
	// cleanup so the goroutine does not outlive the test any longer than
	// the abandonment itself requires.
	stuck := make(chan struct{})
	defer close(stuck)

	entered := make(chan struct{})
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			close(entered)
			<-stuck
			return nil
		},
	}

	codes := make(chan int, 1)
	go func() {
		codes <- runtime.Main("test-connector", "1.0.0", nil, testCaps, h)
	}()

	select {
	case <-entered:
	case <-time.After(harnessTimeout):
		t.Fatal("Hooks.Run was never entered; not sending SIGTERM into an unguarded process")
	}

	signalledAt := time.Now()
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("syscall.Kill(SIGTERM): %v", err)
	}

	code := waitForExit(t, codes)
	elapsed := time.Since(signalledAt)
	restore()

	if code != 0 {
		t.Errorf("Main() = %d, want 0 after abandoning a stuck run hook", code)
	}
	// Generous slack over the budget: the assertion that matters is
	// "bounded", not the exact millisecond. Without the bound this would not
	// return at all.
	if limit := 20 * budget; elapsed > limit {
		t.Errorf("Main() took %s to return after SIGTERM, want it bounded near the %s budget", elapsed, budget)
	}

	// Abandoning publishes nothing: the last document is still the running
	// one the connector genuinely was in.
	if last := drainStatusDocs(t, sub); last == nil || last.State != status.Running {
		t.Errorf("last published document = %v, want the initial running one", last)
	}
}

// TestDrain_ClosesTheConnection guards the happy half of D-16: a drain
// inside its budget flushes and closes, reporting no error.
func TestDrain_ClosesTheConnection(t *testing.T) {
	url, _ := startTestServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}

	if err := runtime.Drain(nc, 5*time.Second); err != nil {
		t.Fatalf("Drain() error = %v, want nil", err)
	}
	if !nc.IsClosed() {
		t.Error("connection is still open after a clean drain")
	}
}

// TestDrain_ExhaustedBudgetClosesHard guards the other half: once the D-16
// budget is gone the connection is closed outright rather than draining past
// the deadline, and the overrun is reported.
func TestDrain_ExhaustedBudgetClosesHard(t *testing.T) {
	url, _ := startTestServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}

	err = runtime.Drain(nc, 0)
	if err == nil {
		t.Error("Drain() with no budget left returned nil, want an error reporting the overrun")
	}
	if !nc.IsClosed() {
		t.Error("connection is still open after an exhausted-budget drain")
	}
}

// TestDrain_AlreadyClosedIsNotAnError guards the shutdown path where the
// connection died on its own before the drain ran.
func TestDrain_AlreadyClosedIsNotAnError(t *testing.T) {
	url, _ := startTestServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	nc.Close()

	if err := runtime.Drain(nc, 5*time.Second); err != nil {
		t.Errorf("Drain() on a closed connection = %v, want nil", err)
	}
}

// TestShutdownClock_BudgetRunsFromTheSignal guards the D-16 accounting: the
// 10 seconds start when the shutdown signal arrives, so a slow Run-hook
// unwind eats into the same budget the NATS drain draws on. An unmarked
// clock — a connector that stopped on its own — gets the whole budget.
func TestShutdownClock_BudgetRunsFromTheSignal(t *testing.T) {
	mark, remaining := runtime.NewShutdownClock()

	if got := remaining(10 * time.Second); got != 10*time.Second {
		t.Errorf("remaining() before any signal = %v, want the full 10s", got)
	}

	mark()
	time.Sleep(20 * time.Millisecond)

	got := remaining(10 * time.Second)
	if got >= 10*time.Second {
		t.Errorf("remaining() after a 20ms unwind = %v, want less than the full 10s", got)
	}
	if got <= 9*time.Second {
		t.Errorf("remaining() after a 20ms unwind = %v, want nearly the full 10s", got)
	}
}
