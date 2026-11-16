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
	"time"
)

// This file exposes the harness's internals to the package's external test
// (package runtime_test), keeping the public API to Main/Park/Hooks/Runtime
// while still letting the tests drive the core without process-global
// signals or os.Args surgery. The one test that must exercise the real
// signal wiring calls Main directly.

// RunHarness is run: Main's body with the argument list and the shutdown
// context injected.
func RunHarness(ctx context.Context, args []string, name, version string, cfg any, caps []string, h Hooks) int {
	return run(ctx, args, name, version, cfg, caps, h)
}

// ModeFromArgs is modeFromArgs: D-11 mode selection.
var ModeFromArgs = modeFromArgs

// Drain is drain: the D-16 bounded NATS drain.
var Drain = drain

// SetShutdownBudget shortens D-16's 10-second budget so a test can watch it
// expire, and returns the restore func. Callers must set it before the
// harness starts and restore it after the harness has returned — the
// harness reads it from its own goroutines.
func SetShutdownBudget(d time.Duration) func() {
	previous := shutdownBudget
	shutdownBudget = d
	return func() { shutdownBudget = previous }
}

// NewShutdownClock builds the D-16 budget clock, exposing mark/remaining as
// the two closures the tests need.
func NewShutdownClock() (mark func(), remaining func(total time.Duration) time.Duration) {
	c := &shutdownClock{}
	return c.mark, c.remaining
}

// SetReconnect shortens the reconnect settings so a test can exhaust the
// reconnect budget in milliseconds instead of the production -1/forever, and
// returns the restore func. Callers must set it before the harness starts
// and restore it after it has returned.
func SetReconnect(max int, wait time.Duration) func() {
	prevMax, prevWait := maxReconnects, reconnectWait
	maxReconnects, reconnectWait = max, wait
	return func() { maxReconnects, reconnectWait = prevMax, prevWait }
}

// ParkReason is parkReason: the reason a Park error carries.
var ParkReason = parkReason

// SemverFromModule is semverFromModule: a module version as a SemVer string.
var SemverFromModule = semverFromModule

// SetCheckTimeout shortens the check mode's timeout so a test can watch it
// expire, and returns the restore func.
func SetCheckTimeout(d time.Duration) func() {
	previous := checkTimeout
	checkTimeout = d
	return func() { checkTimeout = previous }
}
