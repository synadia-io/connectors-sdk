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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/check"
	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// TestRunHarness_SpecMode_PrintsSpecDocument guards D-11's `spec` mode: the
// spec document (D-12) goes to stdout, the process exits 0, and no NATS
// connection is attempted (the env points at an unreachable server, so an
// attempt would fail the exit-code assertion).
func TestRunHarness_SpecMode_PrintsSpecDocument(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)

	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"spec"},
			"test-connector", "1.0.0", testConfig{}, testCaps, runtime.Hooks{})
	})
	if code != 0 {
		t.Fatalf("RunHarness([spec]) = %d, want 0; stdout = %q", code, out)
	}

	doc := decodeJSONObject(t, []byte(out))
	if got := doc["contract"]; got != spec.Contract {
		t.Errorf("spec contract = %v, want %q", got, spec.Contract)
	}
	caps, ok := doc["capabilities"].([]any)
	if !ok || len(caps) != len(testCaps) {
		t.Errorf("spec capabilities = %v, want %v", doc["capabilities"], testCaps)
	}

	schemaDoc, ok := doc["config_schema"].(map[string]any)
	if !ok {
		t.Fatalf("spec config_schema = %v, want a JSON object", doc["config_schema"])
	}
	compileInlineSchema(t, "spec-mode-config-schema.json", schemaDoc)

	xdisplay := compileSchemaFile(t, filepath.Join("..", "..", "spec", "x-display.schema.json"))
	hints := collectXDisplays(schemaDoc)
	if len(hints) == 0 {
		t.Fatal("collectXDisplays() = 0 hints, want at least one (testConfig carries display tags)")
	}
	for _, h := range hints {
		if err := xdisplay.Validate(h); err != nil {
			t.Errorf("x-display %v failed x-display.schema.json: %v", h, err)
		}
	}
}

// TestRunHarness_SpecMode_NilConfigDescribesAnEmptyObject guards
// spec/contract.md's "a connector must be able to describe itself without a
// config document present" for a flags-only connector that hands the harness
// no config struct at all: it still answers spec, with a schema that accepts
// only the empty object.
func TestRunHarness_SpecMode_NilConfigDescribesAnEmptyObject(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"spec"},
			"test-connector", "1.0.0", nil, nil, runtime.Hooks{})
	})
	if code != 0 {
		t.Fatalf("RunHarness([spec]) = %d, want 0; stdout = %q", code, out)
	}

	doc := decodeJSONObject(t, []byte(out))
	if _, ok := doc["capabilities"].([]any); !ok {
		t.Errorf("spec capabilities = %v, want [] for a nil capability list (D-12)", doc["capabilities"])
	}
	schemaDoc, ok := doc["config_schema"].(map[string]any)
	if !ok {
		t.Fatalf("spec config_schema = %v, want a JSON object", doc["config_schema"])
	}
	if schemaDoc["type"] != "object" {
		t.Errorf("config_schema type = %v, want %q", schemaDoc["type"], "object")
	}
	compileInlineSchema(t, "spec-mode-empty-schema.json", schemaDoc)
}

// TestRunHarness_AcceptsOptions guards Main's variadic Option tail: the
// harness runs the same with no option and with one that changes nothing, so
// the call every existing connector makes stays valid when the first real
// Option lands.
func TestRunHarness_AcceptsOptions(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)

	for _, tt := range []struct {
		name string
		opts []runtime.Option
	}{
		{name: "zero options", opts: nil},
		{name: "one no-op option", opts: []runtime.Option{runtime.NoOption}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			out := captureStdout(t, func() {
				code = runtime.RunHarness(context.Background(), []string{"spec"},
					"test-connector", "1.0.0", testConfig{}, testCaps, runtime.Hooks{}, tt.opts...)
			})
			if code != 0 {
				t.Fatalf("RunHarness([spec], %d options) = %d, want 0; stdout = %q", len(tt.opts), code, out)
			}
			if got := decodeJSONObject(t, []byte(out))["contract"]; got != spec.Contract {
				t.Errorf("spec contract = %v, want %q", got, spec.Contract)
			}
		})
	}
}

// TestRunHarness_CheckMode_FailingProbe guards D-11's `check` mode contract:
// exactly one Result line on stdout and exit 1 when the probe fails —
// signaling failure only in the body while exiting 0 is a contract violation.
func TestRunHarness_CheckMode_FailingProbe(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)

	probeErr := "connection refused"
	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", nil, testCaps, runtime.Hooks{
				Probe: func(context.Context) error { return errors.New(probeErr) },
			})
	})
	if code != 1 {
		t.Errorf("RunHarness([check]) = %d, want 1 for a failing probe", code)
	}
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("stdout = %q, want exactly one line", out)
	}

	var result check.Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("decode check.Result from %q: %v", out, err)
	}
	if result.OK {
		t.Error("Result.OK = true, want false")
	}
	if !strings.Contains(result.Message, probeErr) {
		t.Errorf("Result.Message = %q, want it to mention %q", result.Message, probeErr)
	}
}

// TestRunHarness_CheckMode_LoadsConfigBeforeProbing guards the ordering the
// check mode depends on: the probe dials the system the config document
// names, so the document (defaults included, D-14) must already be applied
// when Hooks.Probe runs.
func TestRunHarness_CheckMode_LoadsConfigBeforeProbing(t *testing.T) {
	writeConfigDoc(t, `{"database":"orders"}`)

	var cfg testConfig
	var seenDatabase string
	var seenBatch int

	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", &cfg, testCaps, runtime.Hooks{
				Probe: func(context.Context) error {
					seenDatabase = cfg.Database
					seenBatch = cfg.Batch
					return nil
				},
			})
	})
	if code != 0 {
		t.Fatalf("RunHarness([check]) = %d, want 0; stdout = %q", code, out)
	}
	if seenDatabase != "orders" {
		t.Errorf("probe saw Database = %q, want %q", seenDatabase, "orders")
	}
	if seenBatch != 100 {
		t.Errorf("probe saw Batch = %d, want the schema default 100 (D-14)", seenBatch)
	}
}

// TestRunHarness_CheckMode_ConfigErrorIsAFailedResult guards that a config
// document the loader rejects still produces check's one-line Result rather
// than a bare non-zero exit with no explanation.
func TestRunHarness_CheckMode_ConfigErrorIsAFailedResult(t *testing.T) {
	writeConfigDoc(t, `{"unknown_field":1}`)

	var cfg testConfig
	probed := false
	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", &cfg, testCaps, runtime.Hooks{
				Probe: func(context.Context) error { probed = true; return nil },
			})
	})
	if code != 1 {
		t.Errorf("RunHarness([check]) = %d, want 1 for an invalid config document", code)
	}
	if probed {
		t.Error("Hooks.Probe ran despite the config document failing to load")
	}
	var result check.Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("decode check.Result from %q: %v", out, err)
	}
	if result.OK {
		t.Error("Result.OK = true, want false")
	}
}

// TestRunHarness_CheckMode_NoProbeFails guards a connector that routes into
// check mode without supplying a probe: the harness reports it as a failed
// check rather than claiming success it never verified.
func TestRunHarness_CheckMode_NoProbeFails(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", nil, nil, runtime.Hooks{})
	})
	if code != 1 {
		t.Errorf("RunHarness([check]) with no Probe = %d, want 1", code)
	}
	if !strings.Contains(out, `"ok":false`) {
		t.Errorf("stdout = %q, want a failed Result", out)
	}
}

// TestModeFromArgs guards D-11's mode selection: only the exact tokens
// "spec" and "check" select a non-run mode; everything else, including a
// flag or no argument at all, is the run mode.
func TestModeFromArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"no arguments", nil, ""},
		{"spec", []string{"spec"}, "spec"},
		{"check", []string{"check"}, "check"},
		{"spec with trailing flags", []string{"spec", "-mode=inlet"}, "spec"},
		{"unknown positional runs", []string{"serve"}, ""},
		{"leading flag runs", []string{"-mode=inlet"}, ""},
		{"mode after a flag is not first", []string{"-mode=inlet", "spec"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtime.ModeFromArgs(tt.args); got != tt.want {
				t.Errorf("ModeFromArgs(%q) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}

// TestRunHarness_RunMode_UnparseableConfigFailsStop guards lifecycle.md's
// narrow fail-stop window: an unparseable config document is an
// unrecoverable startup error that happens before the admin surface exists,
// so the harness exits 1 without ever connecting to NATS.
func TestRunHarness_RunMode_UnparseableConfigFailsStop(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)
	writeConfigDoc(t, `{not json`)

	var cfg testConfig
	ran := false
	code := runtime.RunHarness(context.Background(), nil, "test-connector", "1.0.0", &cfg, testCaps,
		runtime.Hooks{Run: func(context.Context, *runtime.Runtime) error { ran = true; return nil }})

	if code != 1 {
		t.Errorf("RunHarness() = %d, want 1 for an unparseable config document (D-15)", code)
	}
	if ran {
		t.Error("Hooks.Run was entered despite an unparseable config document")
	}
}

// TestRunHarness_RunMode_NoRunHookFailsStop guards the other pre-admin
// fail-stop: a connector that reaches run mode without a Run hook is
// misprogrammed, not parked.
func TestRunHarness_RunMode_NoRunHookFailsStop(t *testing.T) {
	if code := runtime.RunHarness(context.Background(), nil, "test-connector", "1.0.0", nil, nil, runtime.Hooks{}); code != 1 {
		t.Errorf("RunHarness() with no Run hook = %d, want 1", code)
	}
}

// TestRunHarness_RunMode_MissingServersEnvFailsStop guards the launch
// contract: without NEX_WORKLOAD_NATS_SERVERS there is no connection to
// carry status or admin, so the harness fail-stops before the admin surface.
func TestRunHarness_RunMode_MissingServersEnvFailsStop(t *testing.T) {
	t.Setenv(envServers, "")

	code := runtime.RunHarness(context.Background(), nil, "test-connector", "1.0.0", nil, testCaps,
		runtime.Hooks{Run: func(context.Context, *runtime.Runtime) error { return nil }})
	if code != 1 {
		t.Errorf("RunHarness() = %d, want 1 when %s is unset", code, envServers)
	}
}

// TestRunHarness_AdminStartFailurePublishesFailed guards the contract's sole
// producer of the `failed` state (spec/lifecycle.md): a fail-stop late
// enough in startup to hold a status publisher — here an admin service that
// will not register because the version is not SemVer — publishes `failed`
// with the "startup-error: " reason prefix before exiting 1.
func TestRunHarness_AdminStartFailurePublishesFailed(t *testing.T) {
	_, sub := startHarnessEnv(t)

	ran := false
	h := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error { ran = true; return nil },
	}

	code := runtime.RunHarness(context.Background(), nil, "test-connector", "not-a-semver", nil, testCaps, h)
	if code != 1 {
		t.Errorf("exit code = %d, want 1 — an unregistrable admin service is a fail-stop", code)
	}
	if ran {
		t.Error("Hooks.Run was entered despite the admin service failing to start")
	}

	failed := waitForState(t, sub, status.Failed)
	if !strings.HasPrefix(failed.Reason, "startup-error: ") {
		t.Errorf("failed reason = %q, want the startup-error prefix", failed.Reason)
	}
}

// TestRunHarness_CheckMode_TimesOutAProbeThatHangs: check mode has a default
// timeout, so a probe that ignores its ctx can't hold the process open.
func TestRunHarness_CheckMode_TimesOutAProbeThatHangs(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)
	defer runtime.SetCheckTimeout(50 * time.Millisecond)()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var code int
	out := captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", nil, testCaps, runtime.Hooks{
				Probe: func(context.Context) error { <-release; return nil },
			})
	})
	if code != 1 || !strings.Contains(out, "did not finish") {
		t.Errorf("RunHarness([check]) = %d, stdout %q; want 1 and a timeout message", code, out)
	}
}

// TestRunHarness_CheckMode_ProbeHasADeadline: the probe's ctx carries the
// check timeout, so a connector needs no timeout of its own.
func TestRunHarness_CheckMode_ProbeHasADeadline(t *testing.T) {
	t.Setenv(envServers, unreachableNATS)
	var code int
	_ = captureStdout(t, func() {
		code = runtime.RunHarness(context.Background(), []string{"check"},
			"test-connector", "1.0.0", nil, testCaps, runtime.Hooks{
				Probe: func(ctx context.Context) error {
					if _, ok := ctx.Deadline(); !ok {
						return errors.New("probe ctx has no deadline")
					}
					return nil
				},
			})
	})
	if code != 0 {
		t.Errorf("RunHarness([check]) = %d, want 0", code)
	}
}

func TestSemverFromModule(t *testing.T) {
	for in, want := range map[string]string{
		"v1.2.3":                             "1.2.3",
		"v0.0.0-20261102120000-abcdef123456": "0.0.0-20261102120000-abcdef123456",
		"(devel)":                            "",
		"":                                   "",
	} {
		if got := runtime.SemverFromModule(in); got != want {
			t.Errorf("SemverFromModule(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRunHarness_RuntimeCarriesTheVersion: Run sees the version Main was
// given, a "v1.2.3" tag loses its "v", and an empty one falls back to a valid
// SemVer string, since the admin service rejects anything else.
func TestRunHarness_RuntimeCarriesTheVersion(t *testing.T) {
	for _, tt := range []struct{ given, want string }{
		{"1.2.3", "1.2.3"},
		{"v1.2.3", "1.2.3"},
		{"", "0.0.0-dev"},
	} {
		t.Run(fmt.Sprintf("%q", tt.given), func(t *testing.T) {
			_, sub := startHarnessEnv(t)
			got := make(chan string, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			codes := make(chan int, 1)
			go func() {
				codes <- runtime.RunHarness(ctx, nil, "test-connector", tt.given, nil, testCaps, runtime.Hooks{
					Run: func(ctx context.Context, rt *runtime.Runtime) error {
						got <- rt.Version
						<-ctx.Done()
						return nil
					},
				})
			}()
			waitForState(t, sub, status.Running)
			select {
			case v := <-got:
				if v != tt.want {
					t.Errorf("Runtime.Version = %q, want %q", v, tt.want)
				}
			case <-time.After(harnessTimeout):
				t.Fatal("Run hook never ran")
			}
			cancel()
			if code := waitForExit(t, codes); code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}
		})
	}
}

// TestRuntime_ShutdownContext: the context outlives Run's ctx and keeps its
// values, and its deadline is the hook's share of the shutdown budget less
// the grace, counted from the signal.
func TestRuntime_ShutdownContext(t *testing.T) {
	type key struct{}

	t.Run("outside the harness", func(t *testing.T) {
		const want = 8*time.Second - 250*time.Millisecond
		for _, rt := range []*runtime.Runtime{{}, nil} {
			sctx, cancel := rt.ShutdownContext(context.Background())
			deadline, ok := sctx.Deadline()
			cancel()
			if in := time.Until(deadline); !ok || in > want || in < want-time.Second {
				t.Errorf("Runtime %v: deadline in %v (set %v), want about %v from now", rt, in, ok, want)
			}
		}
	})

	t.Run("keeps ctx's values, not its cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace"))
		cancel()
		sctx, scancel := (&runtime.Runtime{}).ShutdownContext(ctx)
		defer scancel()
		if sctx.Err() != nil || sctx.Value(key{}) != "trace" {
			t.Errorf("Err() = %v, Value = %v; want nil, %q", sctx.Err(), sctx.Value(key{}), "trace")
		}
	})

	t.Run("after the signal", func(t *testing.T) {
		// A 1s budget: the hook's share is 800ms and the grace 80ms.
		defer runtime.SetShutdownBudget(time.Second)()
		const want = 720 * time.Millisecond
		_, sub := startHarnessEnv(t)
		type seen struct {
			deadline time.Time
			err      error
		}
		got := make(chan seen, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		codes := make(chan int, 1)
		go func() {
			codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, runtime.Hooks{
				Run: func(ctx context.Context, rt *runtime.Runtime) error {
					<-ctx.Done()
					// Unwinding takes a while; the deadline still counts
					// from the signal, not from this call.
					time.Sleep(200 * time.Millisecond)
					sctx, scancel := rt.ShutdownContext(ctx)
					defer scancel()
					d, _ := sctx.Deadline()
					got <- seen{d, sctx.Err()}
					return nil
				},
			})
		}()
		waitForState(t, sub, status.Running)
		signalled := time.Now()
		cancel()
		var s seen
		select {
		case s = <-got:
		case <-time.After(harnessTimeout):
			t.Fatal("the Run hook never called ShutdownContext")
		}
		if s.err != nil {
			t.Errorf("ShutdownContext().Err() = %v, want it alive after Run's ctx is done", s.err)
		}
		if off := s.deadline.Sub(signalled.Add(want)); off < -100*time.Millisecond || off > 100*time.Millisecond {
			t.Errorf("deadline = signal + %v, want signal + %v", s.deadline.Sub(signalled), want)
		}
		if code := waitForExit(t, codes); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	})
}

// TestRunHarness_ARunThatHonorsShutdownContextIsNotAbandoned: a Run hook that
// spends its whole ShutdownContext on a final flush and returns when it ends
// is doing what D-16 asks, so the harness must not log it as abandoned.
func TestRunHarness_ARunThatHonorsShutdownContextIsNotAbandoned(t *testing.T) {
	defer runtime.SetShutdownBudget(500 * time.Millisecond)()
	logs := &syncBuffer{}
	prev := log.Writer()
	log.SetOutput(logs)
	defer log.SetOutput(prev)

	for range 3 {
		_, sub := startHarnessEnv(t)
		ctx, cancel := context.WithCancel(context.Background())
		codes := make(chan int, 1)
		go func() {
			codes <- runtime.RunHarness(ctx, nil, "test-connector", "1.0.0", nil, testCaps, runtime.Hooks{
				Run: func(ctx context.Context, rt *runtime.Runtime) error {
					<-ctx.Done()
					sctx, scancel := rt.ShutdownContext(ctx)
					defer scancel()
					<-sctx.Done() // a final flush that uses its whole budget
					return nil
				},
			})
		}()
		waitForState(t, sub, status.Running)
		cancel()
		if code := waitForExit(t, codes); code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	}
	if strings.Contains(logs.String(), "abandoning") {
		t.Errorf("the harness logged a Run that honored ShutdownContext as abandoned:\n%s", logs)
	}
}

// syncBuffer is a bytes.Buffer the harness's goroutines can log to while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
