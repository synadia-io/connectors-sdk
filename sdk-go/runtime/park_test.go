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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// TestRunHarness_Park_ResumeReentersRunLoop guards the D-15 park cycle end to
// end: Hooks.Run returning Park(reason) publishes `parked` with that reason
// and keeps the process alive; an admin `resume` invokes Hooks.Resume, moves
// the status back to `running`, and re-enters Hooks.Run.
func TestRunHarness_Park_ResumeReentersRunLoop(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs, resumes atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			if runs.Add(1) == 1 {
				return runtime.Park("poison")
			}
			<-ctx.Done()
			return nil
		},
		Resume: func() error { resumes.Add(1); return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "poison" {
		t.Errorf("parked reason = %q, want %q", parked.Reason, "poison")
	}
	if parked.Admin != identity.AdminRoot(testIdentity()) {
		t.Errorf("parked admin = %q, want %q", parked.Admin, identity.AdminRoot(testIdentity()))
	}

	// A parked connector still answers its admin surface (lifecycle.md).
	if errHeader := adminRequest(t, obs, "status", nil); errHeader != "" {
		t.Fatalf("admin status while parked returned an error: %s", errHeader)
	}

	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("admin resume returned an error: %s", errHeader)
	}

	running := waitForState(t, sub, status.Running)
	if running.Reason != "" {
		t.Errorf("resumed reason = %q, want empty", running.Reason)
	}
	if got := resumes.Load(); got != 1 {
		t.Errorf("Hooks.Resume called %d times, want 1", got)
	}
	waitFor(t, "Hooks.Run to be re-entered", func() bool { return runs.Load() == 2 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_Park_MultipleCycles guards that park is a loop state, not a
// one-shot: two park/resume cycles must both complete.
func TestRunHarness_Park_MultipleCycles(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			switch runs.Add(1) {
			case 1:
				return runtime.Park("poison-1")
			case 2:
				return runtime.Park("poison-2")
			default:
				<-ctx.Done()
				return nil
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	for _, want := range []string{"poison-1", "poison-2"} {
		parked := waitForState(t, sub, status.Parked)
		if parked.Reason != want {
			t.Fatalf("parked reason = %q, want %q", parked.Reason, want)
		}
		if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
			t.Fatalf("admin resume (%s) returned an error: %s", want, errHeader)
		}
		waitForState(t, sub, status.Running)
	}
	waitFor(t, "the third Hooks.Run entry", func() bool { return runs.Load() == 3 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_Resume_WhileRunningIsAnError guards spec/admin.md's "error
// if the connector is not currently parked" — resume reverses a park, it is
// not a generic restart.
func TestRunHarness_Resume_WhileRunningIsAnError(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var resumes atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			<-ctx.Done()
			return nil
		},
		Resume: func() error { resumes.Add(1); return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	waitForState(t, sub, status.Running)

	if errHeader := adminRequest(t, obs, "resume", nil); errHeader == "" {
		t.Error("admin resume while running succeeded, want a micro error")
	}
	if got := resumes.Load(); got != 0 {
		t.Errorf("Hooks.Resume called %d times while running, want 0", got)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_Skip_ForwardsKeyAndResumes guards spec/admin.md's skip
// endpoint: the key reaches Hooks.Skip and, on success, skip behaves like
// resume — the loop re-enters and the status returns to running.
func TestRunHarness_Skip_ForwardsKeyAndResumes(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs atomic.Int32
	var skipped atomic.Value
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			if runs.Add(1) == 1 {
				return runtime.Park("poison")
			}
			<-ctx.Done()
			return nil
		},
		Skip: func(key string) error { skipped.Store(key); return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	waitForState(t, sub, status.Parked)

	body, err := json.Marshal(map[string]string{"key": "doc-42"})
	if err != nil {
		t.Fatalf("marshal skip body: %v", err)
	}
	if errHeader := adminRequest(t, obs, "skip", body); errHeader != "" {
		t.Fatalf("admin skip returned an error: %s", errHeader)
	}

	waitForState(t, sub, status.Running)
	if got := skipped.Load(); got != "doc-42" {
		t.Errorf("Hooks.Skip received key = %v, want %q", got, "doc-42")
	}
	waitFor(t, "Hooks.Run to be re-entered", func() bool { return runs.Load() == 2 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_Skip_UnsupportedIsAnError guards spec/admin.md's "error if
// unsupported by this connector": a connector with no Skip hook answers the
// endpoint with an error and stays parked, it does not omit the endpoint.
func TestRunHarness_Skip_UnsupportedIsAnError(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			return runtime.Park("poison")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	waitForState(t, sub, status.Parked)

	body, _ := json.Marshal(map[string]string{"key": "doc-42"})
	errHeader := adminRequest(t, obs, "skip", body)
	if errHeader == "" {
		t.Fatal("admin skip with no Skip hook succeeded, want a micro error")
	}

	// Still parked: an unsupported skip must not unpark the connector.
	if errHeader := adminRequest(t, obs, "status", nil); errHeader != "" {
		t.Fatalf("admin status returned an error: %s", errHeader)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_PlainErrorParksAndResumes guards lifecycle.md's "once the
// admin surface is up, every subsequent failure that would otherwise crash
// the process parks instead": an ordinary error from Hooks.Run — one that
// never went through Park() — parks under the "runtime-error: " prefix, and
// resume re-enters the loop exactly as it does after a deliberate park.
// Exiting non-zero here would hand a supervisor's on-failure restart policy
// the crash loop D-15 exists to prevent.
func TestRunHarness_PlainErrorParksAndResumes(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			if runs.Add(1) == 1 {
				return errors.New("boom")
			}
			<-ctx.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "runtime-error: boom" {
		t.Errorf("parked reason = %q, want %q", parked.Reason, "runtime-error: boom")
	}

	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("admin resume after an error park returned an error: %s", errHeader)
	}
	waitForState(t, sub, status.Running)
	waitFor(t, "Hooks.Run to be re-entered after the error park", func() bool { return runs.Load() == 2 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0 — a post-startup error parks, it does not fail-stop", code)
	}
}

// TestRunHarness_Resume_HookErrorStaysParked guards the resume refusal path:
// when Hooks.Resume returns an error the connector must stay parked — the
// hook is the connector saying "I am not ready" — and a later resume that
// succeeds must still work.
func TestRunHarness_Resume_HookErrorStaysParked(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs, attempts atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			if runs.Add(1) == 1 {
				return runtime.Park("poison")
			}
			<-ctx.Done()
			return nil
		},
		Resume: func() error {
			if attempts.Add(1) == 1 {
				return errors.New("not ready yet")
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	waitForState(t, sub, status.Parked)

	errHeader := adminRequest(t, obs, "resume", nil)
	if errHeader == "" {
		t.Fatal("resume succeeded despite Hooks.Resume returning an error")
	}
	if !strings.Contains(errHeader, "not ready yet") {
		t.Errorf("resume error = %q, want it to carry the hook's message", errHeader)
	}

	// Still parked: the refused resume must not have released the loop.
	statusHeader := adminRequest(t, obs, "status", nil)
	if statusHeader != "" {
		t.Fatalf("admin status returned an error: %s", statusHeader)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("Hooks.Run entered %d times after a refused resume, want 1", got)
	}

	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("second resume returned an error: %s", errHeader)
	}
	waitForState(t, sub, status.Running)
	waitFor(t, "Hooks.Run to be re-entered after the accepted resume", func() bool { return runs.Load() == 2 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_PanickingRunHookParks guards that a connector crashing
// inside its loop leaves an operator something to read and something to act
// on: the panic becomes an error like any other, so it parks with the panic
// value in the reason instead of killing the process.
func TestRunHarness_PanickingRunHookParks(t *testing.T) {
	_, sub := startHarnessEnv(t)

	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			panic("boom")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if !strings.HasPrefix(parked.Reason, "runtime-error: ") {
		t.Errorf("parked reason = %q, want the runtime-error prefix", parked.Reason)
	}
	if !strings.Contains(parked.Reason, "panic") || !strings.Contains(parked.Reason, "boom") {
		t.Errorf("parked reason = %q, want it to name the panic and its value", parked.Reason)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_CheckpointConflictParksWithFixedReason guards the
// spec/checkpoint.md contract: a lost checkpoint CAS returned from Run parks
// under the one machine-readable reason, whatever the connector wrapped it in,
// and resume re-enters the loop.
func TestRunHarness_CheckpointConflictParksWithFixedReason(t *testing.T) {
	obs, sub := startHarnessEnv(t)

	var runs atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			if runs.Add(1) == 1 {
				return fmt.Errorf("save checkpoint: %w", checkpoint.ErrConflict)
			}
			<-ctx.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "checkpoint-conflict" {
		t.Errorf("parked reason = %q, want %q", parked.Reason, "checkpoint-conflict")
	}
	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("admin resume after a conflict park returned an error: %s", errHeader)
	}
	waitFor(t, "Hooks.Run to be re-entered after the conflict park", func() bool { return runs.Load() == 2 })

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_ExplicitParkWinsOverCheckpointConflict guards precedence: a
// connector that names its own reason with Park keeps it, even when the error
// also wraps ErrConflict.
func TestRunHarness_ExplicitParkWinsOverCheckpointConflict(t *testing.T) {
	_, sub := startHarnessEnv(t)

	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			return fmt.Errorf("%w: %w", runtime.Park("custom-reason"), checkpoint.ErrConflict)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "custom-reason" {
		t.Errorf("parked reason = %q, want %q (explicit Park wins)", parked.Reason, "custom-reason")
	}

	cancel()
	waitForExit(t, codes)
}

// TestRunHarness_ErrorAfterShutdownPublishesNoParked guards "nothing is
// published on the way out" (runConnector): a Run that returns an error after
// the shutdown signal (a final checkpoint save losing to the successor, say)
// must not leave a `parked` document on the subject the successor shares.
func TestRunHarness_ErrorAfterShutdownPublishesNoParked(t *testing.T) {
	_, sub := startHarnessEnv(t)

	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			<-ctx.Done()
			return fmt.Errorf("final checkpoint save: %w", checkpoint.ErrConflict)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
	}()
	waitForState(t, sub, status.Running)

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if last := drainStatusDocs(t, sub); last != nil && last.State == status.Parked {
		t.Fatalf("published a %q document (reason %q) on the way out", last.State, last.Reason)
	}
}

func TestParkf_NamesThePhaseAndWrapsTheError(t *testing.T) {
	cause := errors.New("connection refused")
	err := runtime.Parkf("mongo-connect", cause)
	if reason, ok := runtime.ParkReason(err); !ok || reason != "mongo-connect: connection refused" {
		t.Errorf("ParkReason = %q, %v; want %q, true", reason, ok, "mongo-connect: connection refused")
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is(Parkf(...), cause) = false, want the cause wrapped")
	}

	if reason, ok := runtime.ParkReason(runtime.Parkf("config", nil)); !ok || reason != "config" {
		t.Errorf("Parkf with a nil error: reason = %q, %v; want %q, true", reason, ok, "config")
	}
	if reason, _ := runtime.ParkReason(runtime.Parkf("", cause)); reason != "connection refused" {
		t.Errorf("Parkf with no phase: reason = %q, want %q", reason, "connection refused")
	}
}

// TestParkf_KeepsAReasonTheErrorCarries: an explicit Park reason is the
// connector's own, and spec/checkpoint.md requires "checkpoint-conflict" for
// a lost compare-and-swap. Parkf keeps either, and adds the phase to the
// message only, so the harness's conflict log line still names it.
func TestParkf_KeepsAReasonTheErrorCarries(t *testing.T) {
	t.Run("explicit park", func(t *testing.T) {
		inner := runtime.Park("poison")
		got := runtime.Parkf("run", inner)
		if reason, ok := runtime.ParkReason(got); !ok || reason != "poison" {
			t.Errorf("ParkReason = %q, %v; want %q, true", reason, ok, "poison")
		}
		if !errors.Is(got, inner) || !strings.HasPrefix(got.Error(), "run: ") {
			t.Errorf("Parkf(run, Park(poison)) = %q, want the park wrapped, prefixed with the phase", got)
		}
	})
	t.Run("checkpoint conflict", func(t *testing.T) {
		got := runtime.Parkf("run", fmt.Errorf("save checkpoint: %w", checkpoint.ErrConflict))
		if reason, ok := runtime.ParkReason(got); ok {
			t.Errorf("ParkReason = %q, want none, so the harness reports checkpoint-conflict", reason)
		}
		if !errors.Is(got, checkpoint.ErrConflict) || !strings.HasPrefix(got.Error(), "run: save checkpoint: ") {
			t.Errorf("Parkf(run, conflict) = %q, want the conflict wrapped, prefixed with the phase", got)
		}
	})
}

// TestRunHarness_ParkfKeepsTheReasonItsErrorCarries: through the harness, a
// Parkf over a conflict publishes "checkpoint-conflict", and one over an
// explicit Park publishes that Park's reason.
func TestRunHarness_ParkfKeepsTheReasonItsErrorCarries(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"checkpoint conflict", fmt.Errorf("save checkpoint: %w", checkpoint.ErrConflict), "checkpoint-conflict"},
		{"explicit park", runtime.Park("poison"), "poison"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, sub := startHarnessEnv(t)
			h := runtime.Hooks{
				Run: func(ctx context.Context, rt *runtime.Runtime) error {
					return runtime.Parkf("run", tc.err)
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			codes := make(chan int, 1)
			go func() {
				codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, h)
			}()
			if parked := waitForState(t, sub, status.Parked); parked.Reason != tc.want {
				t.Errorf("parked reason = %q, want %q", parked.Reason, tc.want)
			}
			cancel()
			if code := waitForExit(t, codes); code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}
		})
	}
}
