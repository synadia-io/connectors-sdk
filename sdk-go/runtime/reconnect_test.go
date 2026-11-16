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
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"

	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
)

// TestRunHarness_ExitsWhenNATSClosesForGood is the exit guard, scaled
// down: with the reconnect budget shortened, a NATS server that goes away for
// good drives the connection to Closed, whose handler cancels the harness
// context so the process exits — instead of the pre-fix zombie that blocked
// forever on ctx/resume with an unreachable admin surface and a silent status
// subject. A production connector reconnects forever (MaxReconnect == -1, see
// TestConnectFromEnv_RetriesForever); the seam only removes the wait.
func TestRunHarness_ExitsWhenNATSClosesForGood(t *testing.T) {
	restore := runtime.SetReconnect(2, 20*time.Millisecond)
	defer restore()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	var shutdownOnce sync.Once
	shutdown := func() { shutdownOnce.Do(func() { srv.Shutdown(); srv.WaitForShutdown() }) }
	defer shutdown()

	t.Setenv(envServers, srv.ClientURL())
	t.Setenv("NEX_WORKLOAD_NAMESPACE", "test")
	t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", "conn-1")
	t.Setenv("NEX_WORKLOAD_ID", "wl-1")

	// A normal long-running connector: Run blocks until its context is
	// cancelled. It never parks, so it is entered exactly once.
	var startOnce sync.Once
	started := make(chan struct{})
	h := runtime.Hooks{
		Run: func(ctx context.Context, _ *runtime.Runtime) error {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		},
	}

	codes := make(chan int, 1)
	go func() {
		codes <- runtime.RunHarness(context.Background(), nil, "test", "1.0.0", nil, testCaps, h)
	}()

	select {
	case <-started:
	case <-time.After(harnessTimeout):
		t.Fatal("Run hook never started; harness did not come up")
	}

	// Kill NATS for good. The client exhausts its (scaled) reconnect budget,
	// reaches Closed, and the harness exits.
	shutdown()

	code := waitForExit(t, codes)
	if code == 0 {
		t.Errorf("RunHarness exit code = 0, want non-zero after an unrecoverable NATS close")
	}
}
