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

// Package check implements the D-11 "check" entrypoint mode: a connector
// validates its config and probes the external system it names, then prints
// exactly one Result as JSON to stdout, exiting 0 iff the probe succeeded.
// Signaling failure only through "ok": false while exiting 0 is a contract
// violation (spec/contract.md, D-11).
package check

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// Result is the JSON document Run prints to stdout (D-11).
type Result struct {
	OK        bool   `json:"ok"`
	Message   string `json:"message,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
}

// probeGrace is how long Run keeps waiting for probe after ctx ends. A probe
// that honors its ctx returns within milliseconds of the deadline. A variable
// so tests can shorten it.
var probeGrace = time.Second

// Run executes probe, measuring its wall-clock latency, and prints exactly
// one Result line to stdout. It returns the process exit code the caller
// should pass to os.Exit: 0 iff probe returned nil, 1 otherwise (D-11).
//
// A panic inside probe is recovered and reported as a failed Result rather
// than crashing the process — probe is arbitrary connector code (typically
// dialing an external system), and check's exactly-one-line contract must
// hold even when it misbehaves. For the same reason, Run waits for probe only
// until ctx ends plus a short grace (probeGrace). A probe that honors its ctx
// returns within the grace, and its own error, which names the host and the
// operation, is the one printed. A probe that ignores its ctx is reported as
// not finished, and Run returns without it: check is a one-shot process, so
// such a probe can't hold it open. The probe goroutine keeps running until
// probe returns, so an in-process caller, such as a test, must release a
// probe it left blocked.
//
// The message has the userinfo of any URL in it redacted
// (status.RedactReason), since a probe error often quotes a connection URL
// verbatim and the Result is printed for whoever ran check.
func Run(ctx context.Context, probe func(context.Context) error) int {
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- runProbe(ctx, probe) }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		// ctx.Done is ready before a probe that honors it has returned, so
		// give the probe a moment to report its own error.
		grace := time.NewTimer(probeGrace)
		defer grace.Stop()
		select {
		case err = <-done:
		case <-grace.C:
			err = fmt.Errorf("check: the probe did not finish: %w", ctx.Err())
		}
	}
	result := Result{LatencyMs: time.Since(start).Milliseconds()}
	if err != nil {
		result.OK = false
		result.Message = status.RedactReason(err.Error())
	} else {
		result.OK = true
	}

	printResult(result)

	if !result.OK {
		return 1
	}
	return 0
}

// runProbe calls probe, recovering a panic into an error so Run's
// exactly-one-line contract holds even when probe is buggy.
func runProbe(ctx context.Context, probe func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("check: probe panic: %v", r)
		}
	}()
	return probe(ctx)
}

// printResult writes result to stdout as one JSON line. Result's fields are
// always marshalable (plain bool/string/int64), but printResult still fails
// closed with a hand-built failure line rather than printing nothing if
// json.Marshal ever errors, preserving the "exactly one line" guarantee.
func printResult(result Result) {
	line, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stdout, `{"ok":false,"message":%q,"latency_ms":%d}`+"\n",
			"check: marshal result: "+err.Error(), result.LatencyMs)
		return
	}
	fmt.Fprintln(os.Stdout, string(line))
}
