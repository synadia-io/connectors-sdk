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
	"testing"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// TestRunHarness_UnresolvedSecretParks guards D-4's normative reaction to a
// surviving secret:// reference: the connector parks with reason
// "unresolved-secret" (spec/config.md, spec/lifecycle.md) rather than
// connecting with a token that is obviously not a credential — and never
// enters the run loop.
func TestRunHarness_UnresolvedSecretParks(t *testing.T) {
	_, sub := startHarnessEnv(t)
	writeConfigDoc(t, `{"database":"orders","token":"secret://prod/token"}`)

	var cfg testConfig
	var runs atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			runs.Add(1)
			<-ctx.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", &cfg, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "unresolved-secret" {
		t.Errorf("parked reason = %q, want %q", parked.Reason, "unresolved-secret")
	}
	if got := runs.Load(); got != 0 {
		t.Errorf("Hooks.Run entered %d times with an unresolved secret, want 0", got)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_UnresolvedSecret_ResumeRetriesTheLoad guards the resume
// path out of a config park: the harness re-reads the document rather than
// entering Hooks.Run with config it never managed to apply, so a document
// that is still broken parks again instead of running half-configured.
func TestRunHarness_UnresolvedSecret_ResumeRetriesTheLoad(t *testing.T) {
	obs, sub := startHarnessEnv(t)
	writeConfigDoc(t, `{"database":"orders","token":"secret://prod/token"}`)

	var cfg testConfig
	var runs atomic.Int32
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			runs.Add(1)
			<-ctx.Done()
			return nil
		},
		// Run has never been entered when this park is resumed; the hook
		// still gets a usable Runtime.
		Resume: func(_ context.Context, rt *runtime.Runtime) error {
			if rt == nil || rt.Status == nil || rt.Conn == nil {
				t.Errorf("Resume before Run: rt = %+v, want a Runtime with Status and Conn", rt)
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", &cfg, testCaps, h)
	}()

	waitForState(t, sub, status.Parked)

	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("admin resume returned an error: %s", errHeader)
	}

	// The document is unchanged, so the retried load fails the same way and
	// the harness parks again with the same reason.
	waitForState(t, sub, status.Running)
	reparked := waitForState(t, sub, status.Parked)
	if reparked.Reason != "unresolved-secret" {
		t.Errorf("re-parked reason = %q, want %q", reparked.Reason, "unresolved-secret")
	}
	if got := runs.Load(); got != 0 {
		t.Errorf("Hooks.Run entered %d times with an unresolved secret, want 0", got)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_ConfigInvalidParkReason guards the second config park
// reason (spec/lifecycle.md, "Reason strings the harness itself produces"):
// a resume re-reads the document, and one that has become unparseable in
// the meantime parks as `config-invalid` rather than `unresolved-secret`.
func TestRunHarness_ConfigInvalidParkReason(t *testing.T) {
	obs, sub := startHarnessEnv(t)
	path := writeConfigDoc(t, `{"database":"orders","token":"secret://prod/token"}`)

	var cfg testConfig
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			<-ctx.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", &cfg, testCaps, h)
	}()

	parked := waitForState(t, sub, status.Parked)
	if parked.Reason != "unresolved-secret" {
		t.Fatalf("first parked reason = %q, want %q", parked.Reason, "unresolved-secret")
	}

	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatalf("corrupt the config document: %v", err)
	}
	if errHeader := adminRequest(t, obs, "resume", nil); errHeader != "" {
		t.Fatalf("admin resume returned an error: %s", errHeader)
	}

	waitForState(t, sub, status.Running)
	reparked := waitForState(t, sub, status.Parked)
	if reparked.Reason != "config-invalid" {
		t.Errorf("re-parked reason = %q, want %q", reparked.Reason, "config-invalid")
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

// TestRunHarness_NoConfigDocumentLeavesFlagValues guards D-2's mode switch:
// with no document mounted, the loader is a no-op and the values the
// connector already parsed from its flags stand — absence of a document is
// not a startup failure.
func TestRunHarness_NoConfigDocumentLeavesFlagValues(t *testing.T) {
	_, sub := startHarnessEnv(t)
	unsetEnv(t, envConfig)

	// As if the connector's flags had already been parsed into cfg.
	cfg := testConfig{Database: "from-flags", Batch: 7}

	entered := make(chan struct{})
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			close(entered)
			<-ctx.Done()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", &cfg, testCaps, h)
	}()

	waitForState(t, sub, status.Running)
	select {
	case <-entered:
	case <-time.After(harnessTimeout):
		t.Fatal("Hooks.Run was never entered with no config document mounted")
	}
	if cfg.Database != "from-flags" || cfg.Batch != 7 {
		t.Errorf("config = %+v, want the flag values untouched (D-2)", cfg)
	}

	cancel()
	if code := waitForExit(t, codes); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}
