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
	"testing"

	"go.uber.org/goleak"
)

// TestMain wraps the runtime test binary in goleak's leak detector.
// -race catches data races but not a ticker, subscription, or goroutine the
// harness left running after it was meant to stop, and the harness launches
// several bare `go func`s (the status ticker, the serve loop, the drain
// watcher, the shutdown-clock AfterFunc). This fails the suite if any of them
// — or a nats-server/nats.go goroutine a test forgot to shut down — outlive
// the test that started them. The suite passes with no ignore list, so none
// currently leak; goleak's built-in defaults cover the standard runtime's own
// background goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
