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
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// unreachableNATS is a server URL nothing listens on. The spec and check
// modes set it so that a connection attempt the mode contract forbids
// (spec/contract.md: neither mode reads runtime identity or needs NATS)
// would surface as a failed exit code rather than passing unnoticed.
const unreachableNATS = "nats://127.0.0.1:1"

const (
	envServers = "NEX_WORKLOAD_NATS_SERVERS"
	envConfig  = "NEX_WORKLOAD_CONFIG_FILE"
)

// harnessTimeout bounds every wait in these tests. It is deliberately
// generous: they assert ordering, not latency.
const harnessTimeout = 10 * time.Second

var testCaps = []string{"check", "status", "park"}

// testConfig is a connector config struct exercising the tag features the
// harness has to carry end to end: a default applied by the loader (D-14)
// and a whole-value secret reference the D-4 guard scans for.
type testConfig struct {
	Database string `json:"database" schema:"required" display:"label=Database"`
	Token    string `json:"token" display:"secret"`
	Batch    int    `json:"batch" schema:"default=100"`
}

// --- process-level helpers ---

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. Mirrors sdk-go/check/check_test.go's helper —
// check.Run writes straight to os.Stdout, so the harness's check mode can
// only be observed the same way.
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

// unsetEnv removes key for the duration of the test, restoring whatever the
// environment held before. t.Setenv registers the restore; the Unsetenv that
// follows is what actually makes the variable absent rather than empty —
// config.Path distinguishes the two.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
}

// writeConfigDoc writes body to a temp file and points
// NEX_WORKLOAD_CONFIG_FILE at it (D-2). It returns the path so a test can
// rewrite the document underneath a running connector.
func writeConfigDoc(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config document: %v", err)
	}
	t.Setenv(envConfig, path)
	return path
}

// --- schema helpers (mirroring sdk-go/spec/gen_test.go) ---

func decodeJSONObject(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal JSON object from %q: %v", b, err)
	}
	return m
}

// compileInlineSchema asserts doc is a valid 2020-12 schema in its own
// right, not merely a JSON object.
func compileInlineSchema(t *testing.T, name string, doc any) {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(name, doc); err != nil {
		t.Fatalf("add resource %s: %v", name, err)
	}
	if _, err := c.Compile(name); err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
}

func compileSchemaFile(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	sch, err := c.Compile(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return sch
}

// collectXDisplays walks a config_schema's properties and returns every
// "x-display" hint object, so each can be validated against the closed D-13
// vocabulary in spec/x-display.schema.json.
func collectXDisplays(node map[string]any) []map[string]any {
	var out []map[string]any
	props, _ := node["properties"].(map[string]any)
	for _, v := range props {
		propSchema, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if xd, ok := propSchema["x-display"].(map[string]any); ok {
			out = append(out, xd)
		}
		out = append(out, collectXDisplays(propSchema)...)
	}
	return out
}

// --- NATS / harness helpers ---

// startTestServer boots an in-process NATS server plus an observer client,
// returning the server URL (for NEX_WORKLOAD_NATS_SERVERS) and the observer.
// Mirrors sdk-go/status/publisher_test.go and sdk-go/admin/admin_test.go.
func startTestServer(t *testing.T) (string, *nats.Conn) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1 // random free port, hermetic across parallel test runs

	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	return srv.ClientURL(), nc
}

// testIdentity is the identity the env vars set by startHarnessEnv resolve
// to (D-3), so tests can compute the status subject and admin root directly.
func testIdentity() identity.Identity {
	return identity.Identity{
		Namespace:   "test",
		ConnectorID: "conn-1",
		WorkloadID:  "wl-1",
		InstanceID:  "conn-1",
	}
}

// startHarnessEnv boots a server and injects the nexlet launch contract the
// harness reads: the NATS endpoint and the D-3 identity vars. It returns the
// observer connection and a subscription already listening on the status
// subject, so no publish is missed between startup and the first assertion.
func startHarnessEnv(t *testing.T) (*nats.Conn, *nats.Subscription) {
	t.Helper()
	url, obs := startTestServer(t)
	t.Setenv(envServers, url)
	t.Setenv("NEX_WORKLOAD_NAMESPACE", "test")
	t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", "conn-1")
	t.Setenv("NEX_WORKLOAD_ID", "wl-1")

	sub, err := obs.SubscribeSync(identity.StatusSubject(testIdentity()))
	if err != nil {
		t.Fatalf("SubscribeSync(status subject): %v", err)
	}
	// Flush so the server has registered the subscription before the harness
	// (on its own connection) publishes its first status document — without
	// this, an early publish can be missed and the wait then races the 10s
	// republish ticker against the 10s harnessTimeout.
	if err := obs.Flush(); err != nil {
		t.Fatalf("flush status subscription: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	return obs, sub
}

// waitForState reads status documents until one reports want, failing the
// test if none arrives in time. The harness publishes on every state change
// (D-5), so intermediate documents are expected and skipped.
func waitForState(t *testing.T, sub *nats.Subscription, want status.State) status.Document {
	t.Helper()
	deadline := time.Now().Add(harnessTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("no status document with state %q within %s", want, harnessTimeout)
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			t.Fatalf("waiting for state %q: NextMsg: %v", want, err)
		}
		var doc status.Document
		if err := json.Unmarshal(msg.Data, &doc); err != nil {
			t.Fatalf("unmarshal status document %q: %v", msg.Data, err)
		}
		if doc.State == want {
			return doc
		}
	}
}

// drainStatusDocs reads every status document already queued on sub and
// returns the last one, or nil if there were none. Used after the harness
// has exited, where "what did it publish last, and did it add anything on
// the way out" is the assertion.
func drainStatusDocs(t *testing.T, sub *nats.Subscription) *status.Document {
	t.Helper()
	var last *status.Document
	for {
		msg, err := sub.NextMsg(200 * time.Millisecond)
		if err != nil {
			return last
		}
		var doc status.Document
		if err := json.Unmarshal(msg.Data, &doc); err != nil {
			t.Fatalf("unmarshal status document %q: %v", msg.Data, err)
		}
		last = &doc
	}
}

// adminRequest sends one admin request and fails the test if no reply
// arrives. The micro error header (if any) is returned for the caller to
// assert on, since spec/admin.md puts errors there rather than in the body.
func adminRequest(t *testing.T, obs *nats.Conn, endpoint string, body []byte) string {
	t.Helper()
	subject := identity.AdminRoot(testIdentity()) + "." + endpoint
	msg, err := obs.Request(subject, body, harnessTimeout)
	if err != nil {
		t.Fatalf("Request(%q): %v", subject, err)
	}
	return msg.Header.Get(micro.ErrorHeader)
}

// waitFor blocks until cond holds, polling; it fails the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(harnessTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", harnessTimeout, what)
}

// waitForExit collects the harness's exit code, failing the test if it never
// returns.
func waitForExit(t *testing.T, codes <-chan int) int {
	t.Helper()
	select {
	case code := <-codes:
		return code
	case <-time.After(harnessTimeout):
		t.Fatal("harness did not return")
		return -1
	}
}
