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

package check_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/check"
)

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return string(out)
}

func decodeResult(t *testing.T, line string) check.Result {
	t.Helper()
	var r check.Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &r); err != nil {
		t.Fatalf("decode Result from %q: %v", line, err)
	}
	return r
}

func TestRun_SucceedingProbe(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = check.Run(context.Background(), func(context.Context) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		})
	})

	if code != 0 {
		t.Errorf("Run() exit code = %d, want 0", code)
	}
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("stdout = %q, want exactly one line, got %d newlines", out, got)
	}
	result := decodeResult(t, out)
	if !result.OK {
		t.Errorf("Result.OK = false, want true")
	}
	if result.Message != "" {
		t.Errorf("Result.Message = %q, want empty on success", result.Message)
	}
	if result.LatencyMs < 2 {
		t.Errorf("Result.LatencyMs = %d, want >= 2 (probe slept 2ms)", result.LatencyMs)
	}
}

func TestRun_FailingProbe(t *testing.T) {
	probeErr := errors.New("connection refused")
	var code int
	out := captureStdout(t, func() {
		code = check.Run(context.Background(), func(context.Context) error {
			return probeErr
		})
	})

	if code != 1 {
		t.Errorf("Run() exit code = %d, want 1", code)
	}
	result := decodeResult(t, out)
	if result.OK {
		t.Errorf("Result.OK = true, want false")
	}
	if result.Message != probeErr.Error() {
		t.Errorf("Result.Message = %q, want %q", result.Message, probeErr.Error())
	}
}

func TestRun_PanickingProbe(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = check.Run(context.Background(), func(context.Context) error {
			panic("boom")
		})
	})

	if code != 1 {
		t.Errorf("Run() exit code = %d, want 1", code)
	}
	result := decodeResult(t, out)
	if result.OK {
		t.Errorf("Result.OK = true, want false")
	}
	if !strings.Contains(result.Message, "panic") {
		t.Errorf("Result.Message = %q, want it to mention panic", result.Message)
	}
	if !strings.Contains(result.Message, "boom") {
		t.Errorf("Result.Message = %q, want it to include the panic value", result.Message)
	}
}

func TestRun_ContextPassedThroughToProbe(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "probe-saw-me")

	var seen any
	_ = captureStdout(t, func() {
		check.Run(ctx, func(pctx context.Context) error {
			seen = pctx.Value(ctxKey{})
			return nil
		})
	})

	if seen != "probe-saw-me" {
		t.Errorf("probe's context value = %v, want %q", seen, "probe-saw-me")
	}
}

// TestRun_StopsWaitingForAProbeThatIgnoresItsDeadline: check is a one-shot
// process, so a probe that ignores its ctx must not hold it open. Run reports
// the timeout as a failed Result once ctx and the grace after it are over.
func TestRun_StopsWaitingForAProbeThatIgnoresItsDeadline(t *testing.T) {
	defer check.SetProbeGrace(50 * time.Millisecond)()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var code int
	start := time.Now()
	out := captureStdout(t, func() {
		code = check.Run(ctx, func(context.Context) error {
			<-release // ignores ctx
			return nil
		})
	})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Run took %v, want it to return when ctx ends", elapsed)
	}
	if code != 1 {
		t.Errorf("Run() = %d, want 1", code)
	}
	var result check.Result
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if result.OK || !strings.Contains(result.Message, "did not finish") {
		t.Errorf("Result = %+v, want a failure saying the probe did not finish", result)
	}
}

// TestRun_PrintsTheProbesOwnErrorAtItsDeadline: a probe that honors its ctx
// returns its own error a moment after the deadline. That error names the
// host and the operation, so it is the one printed, every time.
func TestRun_PrintsTheProbesOwnErrorAtItsDeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after time.Duration
	}{
		{"at the deadline", 0},
		{"just after it", 5 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 25 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				out := captureStdout(t, func() {
					check.Run(ctx, func(ctx context.Context) error {
						<-ctx.Done()
						time.Sleep(tc.after)
						return fmt.Errorf("dial db:27017: %w", ctx.Err())
					})
				})
				cancel()
				if !strings.Contains(out, "dial db:27017") {
					t.Fatalf("stdout = %q, want the probe's own error", out)
				}
			}
		})
	}
}

// TestRun_ADoneCtxStillReportsAPassingProbe: with ctx already done, a probe
// that returns nil at once passes every time, rather than whenever the select
// happens to pick its result over ctx.Done.
func TestRun_ADoneCtxStillReportsAPassingProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 50 {
		var code int
		_ = captureStdout(t, func() {
			code = check.Run(ctx, func(context.Context) error { return nil })
		})
		if code != 0 {
			t.Fatalf("Run() = %d, want 0", code)
		}
	}
}

// TestRun_RedactsURLCredentials: a probe error often quotes a connection URL
// verbatim. The printed message must not carry its userinfo.
func TestRun_RedactsURLCredentials(t *testing.T) {
	out := captureStdout(t, func() {
		check.Run(context.Background(), func(context.Context) error {
			return errors.New(`dial "mongodb://admin:s3cret@db:27017/shop": connection refused`)
		})
	})
	if strings.Contains(out, "s3cret") || !strings.Contains(out, "mongodb://REDACTED@db:27017") {
		t.Errorf("stdout = %q, want the URL userinfo redacted", out)
	}
}
