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

// Black-box conformance for the connector-contract-v1 surfaces themselves
// (spec/contract.md, config.md, status.md, admin.md, lifecycle.md), driven
// against conformance/testconn — a minimal fixture connector built on the
// sdk-go/runtime harness that exists only to exercise the contract, not to
// do real work. Each test names the contract section it enforces.
package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/conformance/assert"
	"github.com/synadia-io/connectors-sdk/conformance/driver"
	"github.com/synadia-io/connectors-sdk/conformance/natsenv"
	"github.com/synadia-io/connectors-sdk/conformance/specschema"
)

// testconnBin returns the fixture connector this run targets:
// conformance/testconn by default, or the prebuilt fixture binary
// CONFORMANCE_TESTCONN_BIN names, so another SDK's fixture can be held to the
// same suite.
func testconnBin(t *testing.T) string {
	t.Helper()
	if bin := os.Getenv("CONFORMANCE_TESTCONN_BIN"); bin != "" {
		return bin
	}
	return driver.Build(t, "./testconn")
}

// testconnConfig mirrors conformance/testconn/main.go's Config shape, kept
// as a plain map by callers below rather than an imported type: the driver
// module drives testconn as a black box (over env/argv/NATS), the same way
// it drives s3 and mongodb, so it never imports the fixture's own package.
func testconnConfig(subject string, overrides map[string]any) map[string]any {
	cfg := map[string]any{"subject": subject}
	for k, v := range overrides {
		cfg[k] = v
	}
	return cfg
}

// TestSpecMode enforces contract.md's `spec` entrypoint mode and D-12 spec
// document shape: printed to stdout, exit 0, no config or NATS required,
// `config_schema` is a valid 2020-12 schema whose `x-display` hint objects
// validate against x-display.schema.json (D-13).
func TestSpecMode(t *testing.T) {
	bin := testconnBin(t)

	out, err := driver.Run(t, bin, "spec")
	if err != nil {
		t.Fatalf("spec mode: %v\noutput:\n%s", err, out)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("spec output is not JSON: %v\noutput:\n%s", err, out)
	}

	// contract.md: "{"contract": "synadia.connector/v1", "config_schema":
	// <JSON Schema 2020-12>, "capabilities": [...]}" (D-12).
	if doc["contract"] != "synadia.connector/v1" {
		t.Errorf("contract = %v, want synadia.connector/v1", doc["contract"])
	}
	configSchema, ok := doc["config_schema"].(map[string]any)
	if !ok {
		t.Fatalf("config_schema missing or not an object: %v", doc)
	}
	caps, ok := doc["capabilities"].([]any)
	if !ok {
		t.Fatalf("capabilities missing or not a list: %v", doc)
	}
	wantCaps := map[string]bool{"check": false, "status": false, "park": false}
	for _, c := range caps {
		if s, ok := c.(string); ok {
			if _, known := wantCaps[s]; known {
				wantCaps[s] = true
			}
		}
	}
	for c, seen := range wantCaps {
		if !seen {
			t.Errorf("capabilities missing %q (declared but testconn/main.go should advertise it): %v", c, caps)
		}
	}

	// config_schema must itself compile as a JSON Schema 2020-12 document.
	sch := specschema.CompileInline(t, "testconn-config-schema.json", configSchema)

	// Every property's x-display hint object (D-13) must validate against
	// the closed vocabulary in x-display.schema.json.
	xdisplaySchema := specschema.Compile(t, "x-display.schema.json")
	props, _ := configSchema["properties"].(map[string]any)
	sawSecret, sawDefault := false, false
	for name, raw := range props {
		propSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, hasDefault := propSchema["default"]; hasDefault {
			sawDefault = true
		}
		xd, ok := propSchema["x-display"]
		if !ok {
			continue
		}
		if err := xdisplaySchema.Validate(xd); err != nil {
			t.Errorf("property %q x-display fails x-display.schema.json: %v", name, err)
		}
		if xdMap, ok := xd.(map[string]any); ok && xdMap["secret"] == true {
			sawSecret = true
		}
	}
	if !sawSecret {
		t.Errorf("config_schema has no field with x-display.secret=true (fixture must include one)")
	}
	if !sawDefault {
		t.Errorf("config_schema has no field with a schema default (fixture must include one)")
	}

	// A config document actually satisfying the schema must validate.
	if err := sch.Validate(map[string]any{"subject": "conf.testconn.spec"}); err != nil {
		t.Errorf("minimal config document fails config_schema: %v", err)
	}
}

// TestCheckMode enforces contract.md's `check` entrypoint mode (D-11): exit
// 0 iff the printed Result's "ok" is true, exit 1 otherwise — never a
// contradiction between exit code and body. Config-driven via the
// fail_check field, exercised through a mounted config document (config.md,
// D-2) so this also proves check mode loads the document before probing.
func TestCheckMode(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)

	t.Run("success", func(t *testing.T) {
		p := driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-check-ok"},
			testconnConfig("conf.testconn.check", nil), nil, "check")
		code := p.WaitExit(t, 10*time.Second)
		var res struct {
			OK        bool   `json:"ok"`
			LatencyMs int64  `json:"latency_ms"`
			Message   string `json:"message"`
		}
		if err := json.Unmarshal([]byte(driver.LastJSONLine(p.Output())), &res); err != nil {
			t.Fatalf("check result is not JSON: %v\noutput:\n%s", err, p.Output())
		}
		if !res.OK || code != 0 {
			t.Fatalf("ok=%v exit=%d, want ok=true exit=0\noutput:\n%s", res.OK, code, p.Output())
		}
	})

	t.Run("failure", func(t *testing.T) {
		p := driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-check-fail"},
			testconnConfig("conf.testconn.check", map[string]any{"fail_check": true}), nil, "check")
		code := p.WaitExit(t, 10*time.Second)
		var res struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal([]byte(driver.LastJSONLine(p.Output())), &res); err != nil {
			t.Fatalf("check result is not JSON: %v\noutput:\n%s", err, p.Output())
		}
		if res.OK || code == 0 {
			t.Fatalf("ok=%v exit=%d, want ok=false and a non-zero exit (D-11: a non-zero exit on probe "+
				"failure is a contract violation even the other way around)\noutput:\n%s", res.OK, code, p.Output())
		}
	})

	// contract.md D-11: "Check reads the config document... before probing —
	// the config is the thing under test... A document that fails to load is
	// itself a failed check: it is reported as "ok": false with the loader's
	// message, not as a bare non-zero exit." Exit code must still be
	// non-zero (ok=false <=> exit!=0), but the process must still print
	// exactly one Result line rather than dying before check.Run runs.
	t.Run("config document load failure", func(t *testing.T) {
		p := driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-check-badfield"},
			testconnConfig("conf.testconn.check", map[string]any{"not_a_real_field": true}), nil, "check")
		code := p.WaitExit(t, 10*time.Second)
		var res struct {
			OK      bool   `json:"ok"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(driver.LastJSONLine(p.Output())), &res); err != nil {
			t.Fatalf("check result is not JSON: %v\noutput:\n%s", err, p.Output())
		}
		if res.OK || code == 0 {
			t.Fatalf("ok=%v exit=%d, want ok=false and a non-zero exit for a config-document load failure\n"+
				"output:\n%s", res.OK, code, p.Output())
		}
		if !strings.Contains(res.Message, "not_a_real_field") {
			t.Errorf("message = %q, want it to name the rejected field %q", res.Message, "not_a_real_field")
		}
	})
}

// TestConfigDocumentBoot enforces config.md D-2: a valid config document
// boots the connector normally, and a document carrying a field the schema
// doesn't know about is a startup failure (not a warning) reported before
// any NATS connection, naming the offending field in stderr.
func TestConfigDocumentBoot(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)

	t.Run("valid document runs", func(t *testing.T) {
		nc, err := srv.Connect()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		watch := driver.WatchStatus(t, nc, "default", "testconn-configboot")

		driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-configboot"},
			testconnConfig("conf.testconn.configboot", nil), nil)

		watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	})

	t.Run("unknown field rejected before connecting", func(t *testing.T) {
		p := driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-badfield"},
			testconnConfig("conf.testconn.badfield", map[string]any{"not_a_real_field": true}), nil)
		code := p.WaitExit(t, 10*time.Second)
		if code == 0 {
			t.Fatalf("want non-zero exit for an unknown config field, got 0\noutput:\n%s", p.Output())
		}
		if !strings.Contains(p.Output(), "not_a_real_field") {
			t.Errorf("stderr does not name the rejected field %q:\n%s", "not_a_real_field", p.Output())
		}
	})
}

// TestUnresolvedSecret enforces config.md D-4: a whole-value "secret://"
// prefix surviving into any field is fatal, reported as a
// park (not a fail-stop) with reason "unresolved-secret", observed on the
// D-5 status subject — the admin surface stays up.
//
// The point is that resolution and the connector-side
// guard are SCHEMA-BLIND: "the connector-side guard mirrors this on
// purpose: it too scans every string field, at every nesting level, with no
// schema lookup... checking only x-display.secret fields would miss exactly
// the failure mode this guard exists to catch" (config.md, "Secret
// references"). So this covers both a marked (api_key, display:"secret")
// and an unmarked (greeting, no display tag at all) field — a regression
// that narrowed the scan to only marked fields would still pass the first
// case and only be caught by the second.
func TestUnresolvedSecret(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	tests := []struct {
		name       string
		instanceID string
		overrides  map[string]any
	}{
		{
			name:       "x-display.secret-marked field",
			instanceID: "testconn-secret-marked",
			overrides:  map[string]any{"api_key": "secret://vault/testconn-key"},
		},
		{
			name:       "unmarked field (schema-blind guard)",
			instanceID: "testconn-secret-unmarked",
			overrides:  map[string]any{"greeting": "secret://vault/testconn-key"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watch := driver.WatchStatus(t, nc, "default", tt.instanceID)
			driver.SpawnWithConfig(t, bin, srv.URL, driver.Identity{ConnectorID: tt.instanceID},
				testconnConfig("conf.testconn.secret", tt.overrides), nil)

			msg := watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })
			if !strings.Contains(msg.Doc.Reason, "unresolved-secret") {
				t.Errorf("reason = %q, want it to contain %q", msg.Doc.Reason, "unresolved-secret")
			}
		})
	}
}

// TestStatus enforces status.md D-5/D-6: the status document appears on
// $CONNECTOR.<namespace>.status.<instanceID> within 15s of start, validates
// against status.schema.json, and reports state "running".
func TestStatus(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	watch := driver.WatchStatus(t, nc, "default", "testconn-status")
	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-status"}.Env(),
		"-subject=conf.testconn.status")

	msg := watch.WaitFor(t, 15*time.Second, func(driver.StatusDoc) bool { return true })

	sch := specschema.Compile(t, "status.schema.json")
	var raw map[string]any
	if err := json.Unmarshal(msg.Raw, &raw); err != nil {
		t.Fatalf("status document is not JSON: %v", err)
	}
	if err := sch.Validate(raw); err != nil {
		t.Errorf("status document fails status.schema.json: %v\ndocument: %s", err, msg.Raw)
	}
	if msg.Doc.State != "running" {
		t.Errorf("state = %q, want running", msg.Doc.State)
	}
}

// TestParkResumeSkip enforces admin.md D-8 and lifecycle.md D-15: resume
// errors outside a park; a poison message parks the connector with a
// machine-readable reason; admin resume un-parks it and the loop resumes
// processing; a second poison park is cleared by admin skip, which records
// the skipped key and resumes processing identically to resume.
func TestParkResumeSkip(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.park"
	watch := driver.WatchStatus(t, nc, "default", "testconn-park")
	counts, err := assert.Collect(nc, subject+".count")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(counts.Close)

	root := driver.AdminRoot("default", "testconn-park")
	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-park"}.Env(), "-subject="+subject)

	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	// admin.md: resume errors when the connector is not currently parked.
	reply := driver.AdminRequest(t, nc, root, "resume", nil, 5*time.Second)
	if _, isErr := driver.AdminErr(reply); !isErr {
		t.Errorf("resume while running: want a micro error, got none (body %s)", reply.Data)
	}

	// Prove the loop is alive before poisoning it.
	if err := nc.Publish(subject, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := counts.WaitForAtLeast(1, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// Poison -> parked with a reason naming the key.
	poison1 := nats.NewMsg(subject)
	poison1.Data = []byte("poison")
	poison1.Header.Set("Key", "item-1")
	if err := nc.PublishMsg(poison1); err != nil {
		t.Fatal(err)
	}
	msg := watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })
	awaitUnsubscribed(t, srv, subject)
	if !strings.Contains(msg.Doc.Reason, "item-1") {
		t.Errorf("park reason = %q, want it to name the poison key %q", msg.Doc.Reason, "item-1")
	}

	// admin resume un-parks it.
	reply = driver.AdminRequest(t, nc, root, "resume", nil, 5*time.Second)
	if desc, isErr := driver.AdminErr(reply); isErr {
		t.Fatalf("resume failed: %s", desc)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	// The loop actually resumed processing, not just the status flag.
	if err := nc.Publish(subject, []byte("hello-again")); err != nil {
		t.Fatal(err)
	}
	if _, err := counts.WaitForAtLeast(2, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// Second poison -> parked again, this time cleared with admin skip.
	poison2 := nats.NewMsg(subject)
	poison2.Data = []byte("poison")
	poison2.Header.Set("Key", "item-2")
	if err := nc.PublishMsg(poison2); err != nil {
		t.Fatal(err)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool {
		return d.State == "parked" && strings.Contains(d.Reason, "item-2")
	})
	awaitUnsubscribed(t, srv, subject)

	skipBody, err := json.Marshal(map[string]string{"key": "item-2"})
	if err != nil {
		t.Fatal(err)
	}
	reply = driver.AdminRequest(t, nc, root, "skip", skipBody, 5*time.Second)
	if desc, isErr := driver.AdminErr(reply); isErr {
		t.Fatalf("skip failed: %s", desc)
	}

	skipMsg := watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool {
		return d.State == "running" && d.Counters["skipped:item-2"] > 0
	})
	if skipMsg.Doc.Counters["skipped:item-2"] != 1 {
		t.Errorf("skipped:item-2 counter = %d, want 1", skipMsg.Doc.Counters["skipped:item-2"])
	}
	awaitSubscribed(t, srv, subject)

	// And the loop is alive a third time, post-skip.
	if err := nc.Publish(subject, []byte("hello-thrice")); err != nil {
		t.Fatal(err)
	}
	if _, err := counts.WaitForAtLeast(3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestDrainOnSigterm enforces lifecycle.md D-16: SIGTERM stops intake,
// flushes/acks in flight, closes NATS, and exits 0 within the driver's
// 10-second normative bound.
func TestDrainOnSigterm(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	watch := driver.WatchStatus(t, nc, "default", "testconn-drain")
	p := driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-drain"}.Env(),
		"-subject=conf.testconn.drain")
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })

	p.StopGraceful(t)
}

// credsPoisonReason mirrors testconn/main.go's credsPoisonReason so this
// test asserts against the exact secret it expects to have been redacted,
// rather than a substring that happens not to appear in redacted output.
const credsPoisonSecret = "secretpw"

// TestRedactionEndToEnd enforces status.md's redaction requirement (the
// paragraph beginning "An implementation MUST NOT let a `reason` string
// carry credentials") at the HARNESS level: sdk-go/status.Publisher.SetState
// redacts URI userinfo in every published reason and every admin `status`
// endpoint response, automatically, for every connector wired through
// runtime.Main — a connector that forgot its own redaction still cannot leak
// a credential this way. Each connector's own redaction (mongodb's
// redactURI, s3's redact) is tested in that connector; this test is what
// proves the HARNESS itself redacts for a connector that has none.
//
// testconn's "poison-creds" trigger parks with a reason quoting a mongodb
// URI's userinfo verbatim (credsPoisonReason in testconn/main.go) — testconn
// does no redaction of its own, so a "REDACTED" marker appearing on the wire
// can only have come from the harness. The test asserts both places D-8/D-5
// require it: the status subject document AND the admin `status` endpoint's
// response (admin.md), the second surface SetState's redaction covers.
func TestRedactionEndToEnd(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.redact"
	watch := driver.WatchStatus(t, nc, "default", "testconn-redact")
	root := driver.AdminRoot("default", "testconn-redact")
	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-redact"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	if err := nc.Publish(subject, []byte("poison-creds")); err != nil {
		t.Fatal(err)
	}
	msg := watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })

	if !strings.Contains(msg.Doc.Reason, "REDACTED") {
		t.Errorf("status subject reason = %q, want it to contain %q", msg.Doc.Reason, "REDACTED")
	}
	if strings.Contains(msg.Doc.Reason, credsPoisonSecret) {
		t.Errorf("status subject reason = %q, leaks the unredacted secret %q", msg.Doc.Reason, credsPoisonSecret)
	}

	// admin.md's `status` endpoint returns byte-for-byte the same document
	// shape — assert the SAME two properties hold there, independently: a
	// harness that redacted only the publish path (not SetState's stored
	// Document, which both surfaces read) would pass the first half above
	// and fail this one.
	reply := driver.AdminRequest(t, nc, root, "status", nil, 5*time.Second)
	if desc, isErr := driver.AdminErr(reply); isErr {
		t.Fatalf("admin status failed: %s", desc)
	}
	var adminDoc driver.StatusDoc
	if err := json.Unmarshal(reply.Data, &adminDoc); err != nil {
		t.Fatalf("admin status response is not JSON: %v\nbody: %s", err, reply.Data)
	}
	if !strings.Contains(adminDoc.Reason, "REDACTED") {
		t.Errorf("admin status reason = %q, want it to contain %q", adminDoc.Reason, "REDACTED")
	}
	if strings.Contains(adminDoc.Reason, credsPoisonSecret) {
		t.Errorf("admin status reason = %q, leaks the unredacted secret %q", adminDoc.Reason, credsPoisonSecret)
	}
}

// TestDrainFlushesInFlight enforces lifecycle.md D-16's "flushes or acks
// whatever is in flight" clause specifically, not just the exit-0-within-
// budget half TestDrainOnSigterm already covers. testconn's "flush-then-
// term" trigger (testconn/main.go) publishes a marker message and then
// self-delivers SIGTERM in the same handler invocation, with no
// synchronization between the two — the marker sits in the NATS client's
// own outbound buffer, not yet flushed to the server, at the exact moment
// shutdown begins. A harness that closed the connection (nc.Close) instead
// of draining it (nc.Drain, which flushes the outbound buffer before
// closing) would silently drop this message while still exiting 0 — the gap
// this test exists to catch, since TestDrainOnSigterm's exit-0 assertion
// alone cannot distinguish a clean drain from a silent drop.
func TestDrainFlushesInFlight(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.flush"
	watch := driver.WatchStatus(t, nc, "default", "testconn-flush")
	flushed, err := assert.Collect(nc, subject+".flushed")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(flushed.Close)

	p := driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-flush"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	if err := nc.Publish(subject, []byte("flush-then-term")); err != nil {
		t.Fatal(err)
	}

	// The process self-signals immediately after queuing the flushed
	// message; WaitExit (not StopGraceful, which would send its own
	// redundant SIGTERM) just waits for the exit this test did not itself
	// trigger.
	if code := p.WaitExit(t, driver.DrainBudget); code != 0 {
		t.Errorf("exit code = %d, want 0 (a clean drain, D-16)", code)
	}
	if _, err := flushed.WaitForAtLeast(1, 2*time.Second); err != nil {
		t.Errorf("marker message published immediately before self-SIGTERM was not flushed: %v", err)
	}
}

// TestStatusAllDocumentsValid enforces status.md D-5/D-6/D-14 against EVERY
// status document a connector publishes over a window that includes a state
// change and a periodic republish tick, not just the first one observed.
// contract_test.go's TestStatus (unchanged) validates only the first
// document WaitFor happens to return; a connector whose schema drifted
// specifically on its SECOND-or-later publish (e.g. a field it only
// populates after the first state transition, or a stale/malformed doc left
// over from a ticker bug) would pass that test and fail this one.
func TestStatusAllDocumentsValid(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.allvalid"
	watch := driver.WatchStatus(t, nc, "default", "testconn-allvalid")
	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-allvalid"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	// A state change (park then resume) gives two more documents beyond the
	// initial running one, each with a genuinely different shape (a
	// non-empty reason while parked).
	poisonMsg := nats.NewMsg(subject)
	poisonMsg.Data = []byte("poison")
	poisonMsg.Header.Set("Key", "allvalid-key")
	if err := nc.PublishMsg(poisonMsg); err != nil {
		t.Fatal(err)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })
	awaitUnsubscribed(t, srv, subject)

	root := driver.AdminRoot("default", "testconn-allvalid")
	reply := driver.AdminRequest(t, nc, root, "resume", nil, 5*time.Second)
	if desc, isErr := driver.AdminErr(reply); isErr {
		t.Fatalf("resume failed: %s", desc)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	// A periodic republish tick (status.md D-5: every 10s, no state change
	// involved) beyond every document collected so far — the specific gap
	// TestStatus's single-document check cannot reach at all.
	before := len(watch.Messages())
	msgs := watch.WaitForCount(t, before+1, 13*time.Second)

	if len(msgs) < 4 {
		t.Fatalf("only %d status documents observed, want at least 4 (running, parked, running, tick)", len(msgs))
	}

	sch := specschema.Compile(t, "status.schema.json")
	for i, m := range msgs {
		var raw map[string]any
		if err := json.Unmarshal(m.Raw, &raw); err != nil {
			t.Fatalf("document %d is not JSON: %v\ndocument: %s", i, err, m.Raw)
		}
		if err := sch.Validate(raw); err != nil {
			t.Errorf("document %d fails status.schema.json: %v\ndocument: %s", i, err, m.Raw)
		}
	}
}

// TestStatusCountersIncrement enforces status.md's `counters` field against
// a RUNNING connector processing ordinary traffic, not just the park-
// triggered skip counter TestParkResumeSkip already exercises. It sends N
// ordinary messages, then forces an immediate status publish (a park, which
// publishes on every state change per D-5) so the test does not need to wait
// out the 10s periodic tick to observe the accumulated counter value —
// Publisher.Add mutates the shared Document immediately (sdk-go/status/
// publisher.go), independent of when the next publish happens, so the
// park's status document already carries the fully-accumulated count.
func TestStatusCountersIncrement(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.counters"
	watch := driver.WatchStatus(t, nc, "default", "testconn-counters")
	counts, err := assert.Collect(nc, subject+".count")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(counts.Close)

	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-counters"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	const n = 5
	for i := 0; i < n; i++ {
		if err := nc.Publish(subject, []byte("hello")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := counts.WaitForAtLeast(n, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	poisonMsg := nats.NewMsg(subject)
	poisonMsg.Data = []byte("poison")
	poisonMsg.Header.Set("Key", "counters-key")
	if err := nc.PublishMsg(poisonMsg); err != nil {
		t.Fatal(err)
	}
	msg := watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })

	if got := msg.Doc.Counters["received"]; got != n {
		t.Errorf("counters[received] = %d, want %d (status doc: %+v)", got, n, msg.Doc)
	}
}

// TestRoutingGuard enforces transformer.md's routing guard through the
// fixture's `route:<target>` probe (README.md, "Fixture contract"): a
// transform output may be published only to a literal subject that is one of
// the connector's data subjects (here `<subject>.out.>`), never to a
// server-reserved namespace (subjects.md: `$JS.API...` would purge a stream,
// `_INBOX...` would answer someone's request), never to a subject carrying a
// reserved notice suffix (notices are unforgeable), and never with a header
// the server acts on (`Nats-*`). A refused output becomes an `.error` notice
// on the pre-transform subject, carrying a fixed class, not the error text.
//
// Every probe goes over one connection and the fixture answers each on one
// connection, so once the allowed probe, published last, has arrived, the
// server has processed everything the fixture published before it. Delivery
// to the test's subscriptions is ordered per subscription only (each has its
// own goroutine), so the test then publishes a fence on every collected
// subject and waits for it: anything the guard let through, and every
// notice, is in front of the fence on its own subscription.
func TestRoutingGuard(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.route"
	const allowedSubject = subject + ".out.a"
	forbiddenSubjects := []string{
		"$JS.API.STREAM.PURGE.CONFTEST",
		"_INBOX.conftest.x",
		subject + ".out.a.error",
		"conf.other.subject",
	}
	watch := driver.WatchStatus(t, nc, "default", "testconn-route")
	allowed, err := assert.Collect(nc, allowedSubject)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(allowed.Close)
	forbidden := make(map[string]*assert.Collector, len(forbiddenSubjects))
	for _, s := range forbiddenSubjects {
		c, err := assert.Collect(nc, s)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		forbidden[s] = c
	}
	notices, err := assert.Collect(nc, subject+".error")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(notices.Close)
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-route"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	probe := func(target, hk, hv string) {
		t.Helper()
		m := nats.NewMsg(subject)
		m.Data = []byte("route:" + target)
		if hk != "" {
			m.Header.Set(hk, hv)
		}
		if err := nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	wantClasses := []string{
		"unpublishable-subject", // $JS.API...: server-reserved namespace
		"unpublishable-subject", // _INBOX...: server-reserved namespace
		"unpublishable-subject", // <subject>.out.a.error: reserved notice suffix
		"unpublishable-subject", // conf.other.subject: not a data subject
		"reserved-header",       // Nats-Rollup on an allowed subject
	}
	for _, s := range forbiddenSubjects {
		probe(s, "", "")
	}
	probe(allowedSubject, "Nats-Rollup", "all")
	probe(allowedSubject, "X-Trace", "1")

	if _, err := allowed.WaitForAtLeast(1, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	got := allowed.Msgs()
	if len(got) != 1 {
		t.Fatalf("allowed subject received %d messages, want 1 (only the X-Trace probe)", len(got))
	}
	if string(got[0].Data) != "routed" || got[0].Header.Get("X-Trace") != "1" {
		t.Errorf("allowed message = %q %v, want body \"routed\" with X-Trace: 1", got[0].Data, got[0].Header)
	}
	if got[0].Header.Get("Nats-Rollup") != "" {
		t.Errorf("allowed message carries Nats-Rollup: the reserved-header probe was published")
	}

	// The fence: published by the test after the allowed message arrived, so
	// it lands behind anything the fixture published on these subjects.
	const fence = "fence"
	fenced := map[string]*assert.Collector{subject + ".error": notices}
	for _, s := range forbiddenSubjects {
		fenced[s] = forbidden[s]
	}
	for s := range fenced {
		if err := nc.Publish(s, []byte(fence)); err != nil {
			t.Fatal(err)
		}
	}
	for s, c := range fenced {
		deadline := time.Now().Add(10 * time.Second)
		for {
			msgs := c.Msgs()
			if n := len(msgs); n > 0 && string(msgs[n-1].Data) == fence {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("no fence on %s after 10s (%d messages)", s, len(msgs))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	ns := notices.Msgs()
	if len(ns) != len(wantClasses)+1 {
		t.Fatalf("notices on %s.error: got %d before the fence, want %d", subject, len(ns)-1, len(wantClasses))
	}
	for i, want := range wantClasses {
		n := ns[i]
		if n.Header.Get("Transform-Error") != want {
			t.Errorf("notice %d: Transform-Error = %q, want %q", i, n.Header.Get("Transform-Error"), want)
		}
		if len(n.Data) != 0 {
			t.Errorf("notice %d: payload = %q, want empty (transformer.md: notices are metadata-only)", i, n.Data)
		}
	}
	for _, s := range forbiddenSubjects {
		if msgs := forbidden[s].Msgs(); len(msgs) != 1 {
			t.Errorf("forbidden subject %q received %d messages before the fence, want 0", s, len(msgs)-1)
		}
	}
}

// legalStateTransitions is the D-6 legal adjacency set: which state may
// immediately follow which in a connector's own observed status stream.
// lifecycle.md: park (D-15) is only ever reversed by resume/skip, both of
// which land on running. status.md: paused is an explicit external
// pause/resume pair (Kafka Connect semantics) distinct from parked;
// restarting is a transient hop that only ever returns to running. `failed`
// has NO legal successor at all — lifecycle.md restricts it to the
// fail-stop window before the admin/status surface exists, so once any
// other state has been observed on the subject, `failed` must never appear;
// even as the very first document (a startup error late enough to hold a
// status publisher), it is necessarily the LAST, because the process exits
// immediately after publishing it.
var legalStateTransitions = map[string]map[string]bool{
	"running":    {"parked": true, "paused": true, "restarting": true},
	"paused":     {"running": true},
	"parked":     {"running": true},
	"restarting": {"running": true},
	"failed":     {},
}

// checkStateTransitions walks seq (in observation order) and returns an
// error naming the first adjacent pair not in legalStateTransitions. A
// repeated state (a periodic republish tick, or two identical entries) is
// not a transition and is always legal.
func checkStateTransitions(seq []string) error {
	for i := 1; i < len(seq); i++ {
		from, to := seq[i-1], seq[i]
		if from == to {
			continue
		}
		allowed, known := legalStateTransitions[from]
		if !known || !allowed[to] {
			return fmt.Errorf("illegal state transition %s -> %s at position %d (sequence: %v)", from, to, i, seq)
		}
	}
	return nil
}

// TestStateTransitionLegality enforces lifecycle.md/status.md D-6's state
// machine at the sequence level: no adjacent pair of observed status
// documents may cross an edge the contract does not define, e.g. `failed`
// appearing mid-run, or a direct `parked` -> `failed`.
//
// The table-driven cases first prove checkStateTransitions actually
// discriminates — hand-built illegal sequences it must reject, and legal
// ones (including a repeated-state tick) it must accept — before the
// live-run subtest applies it to a real captured sequence from testconn: a
// checker that always returned nil would pass the live check vacuously, so
// the live assertion alone would not prove anything without this table.
func TestStateTransitionLegality(t *testing.T) {
	illegal := [][]string{
		{"parked", "failed"},
		{"running", "failed"},
		{"failed", "running"},
		{"restarting", "parked"},
		{"paused", "parked"},
		{"running", "parked", "failed"},
	}
	for _, seq := range illegal {
		if err := checkStateTransitions(seq); err == nil {
			t.Errorf("checkStateTransitions(%v) = nil, want an error (illegal adjacency)", seq)
		}
	}

	legal := [][]string{
		{"running"},
		{"running", "parked", "running"},
		{"running", "parked", "running", "parked", "running"},
		{"running", "running", "parked"},
		{"running", "paused", "running"},
		{"running", "restarting", "running"},
	}
	for _, seq := range legal {
		if err := checkStateTransitions(seq); err != nil {
			t.Errorf("checkStateTransitions(%v) = %v, want nil", seq, err)
		}
	}

	// Live check: drive testconn through a real park/resume, then a second
	// park cleared by skip (admin.md), and assert the OBSERVED sequence has
	// no illegal adjacency.
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.legality"
	watch := driver.WatchStatus(t, nc, "default", "testconn-legality")
	root := driver.AdminRoot("default", "testconn-legality")
	driver.Spawn(t, bin, srv.URL, driver.Identity{ConnectorID: "testconn-legality"}.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	p1 := nats.NewMsg(subject)
	p1.Data = []byte("poison")
	p1.Header.Set("Key", "legality-1")
	if err := nc.PublishMsg(p1); err != nil {
		t.Fatal(err)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })
	awaitUnsubscribed(t, srv, subject)

	resumeReply := driver.AdminRequest(t, nc, root, "resume", nil, 5*time.Second)
	if desc, isErr := driver.AdminErr(resumeReply); isErr {
		t.Fatalf("resume failed: %s", desc)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	p2 := nats.NewMsg(subject)
	p2.Data = []byte("poison")
	p2.Header.Set("Key", "legality-2")
	if err := nc.PublishMsg(p2); err != nil {
		t.Fatal(err)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "parked" })
	awaitUnsubscribed(t, srv, subject)

	skipBody, err := json.Marshal(map[string]string{"key": "legality-2"})
	if err != nil {
		t.Fatal(err)
	}
	reply := driver.AdminRequest(t, nc, root, "skip", skipBody, 5*time.Second)
	if desc, isErr := driver.AdminErr(reply); isErr {
		t.Fatalf("skip failed: %s", desc)
	}
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	seq := make([]string, 0, len(watch.Messages()))
	for _, m := range watch.Messages() {
		seq = append(seq, m.Doc.State)
	}
	if err := checkStateTransitions(seq); err != nil {
		t.Errorf("observed status sequence: %v", err)
	}
}

// TestCrashAndRestart proves that the process-kill driver helper
// (driver.Proc.KillNow) works: testconn is killed with SIGKILL — an actual
// crash, with no chance to run any unwind code, unlike every prior
// "restart" scenario in this repo (SIGTERM-then-check, or an in-process
// ctx-cancel) — and a freshly spawned replacement process, as a supervisor
// would launch, re-subscribes and resumes doing real work. This unlocks the
// crash-window test class for later use (e.g. a checkpoint/dedup-durability
// scenario across a real crash rather than a graceful stop) without itself
// attempting that broader scenario.
func TestCrashAndRestart(t *testing.T) {
	bin := testconnBin(t)
	srv, err := natsenv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := srv.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	const subject = "conf.testconn.crash"
	watch := driver.WatchStatus(t, nc, "default", "testconn-crash")
	counts, err := assert.Collect(nc, subject+".count")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(counts.Close)

	id := driver.Identity{ConnectorID: "testconn-crash"}
	p1 := driver.Spawn(t, bin, srv.URL, id.Env(), "-subject="+subject)
	watch.WaitFor(t, 15*time.Second, func(d driver.StatusDoc) bool { return d.State == "running" })
	awaitSubscribed(t, srv, subject)

	if err := nc.Publish(subject, []byte("before-crash")); err != nil {
		t.Fatal(err)
	}
	if _, err := counts.WaitForAtLeast(1, 10*time.Second); err != nil {
		t.Fatalf("original process never processed a message before the crash: %v", err)
	}

	before := len(watch.Messages())
	p1.KillNow(t) // the real crash: SIGKILL, no unwind, no drain.

	// Restart: a fresh OS process, same identity/config, as a supervisor's
	// restart policy would launch. WaitForCount (not WaitFor("running"),
	// which would immediately replay the OLD process's already-recorded
	// running document instead of waiting for the new one) forces a
	// genuinely new document to arrive.
	p2 := driver.Spawn(t, bin, srv.URL, id.Env(), "-subject="+subject)
	msgs := watch.WaitForCount(t, before+1, 15*time.Second)
	if got := msgs[len(msgs)-1].Doc.State; got != "running" {
		t.Fatalf("first status document after restart = %q, want running", got)
	}

	// Prove the restarted process actually re-subscribed and does real
	// work, not just that a status document happened to appear.
	awaitSubscribed(t, srv, subject)
	if err := nc.Publish(subject, []byte("after-crash")); err != nil {
		t.Fatal(err)
	}
	if _, err := counts.WaitForAtLeast(2, 10*time.Second); err != nil {
		t.Fatalf("restarted process did not resume processing: %v", err)
	}

	p2.StopGraceful(t)
}

// awaitSubscribed blocks until the fixture's Run hook is subscribed to
// subject, by asking the embedded server whether anything subscribes to it
// (no fixture cooperation needed; the tests themselves never subscribe to
// the bare subject). A "running" status does not promise that: the harness announces running and
// then enters the Run hook, which subscribes a few milliseconds later (about
// 5 ms for the JVM fixture, measured). A test that publishes work right after
// running, or right after a resume re-enters the hook, waits here first.
func awaitSubscribed(t *testing.T, srv *natsenv.Server, subject string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if srv.HasInterest(subject) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fixture never subscribed to %q within 10s", subject)
}

// awaitUnsubscribed blocks until nothing subscribes to subject, failing the
// test if a parked fixture still does after 10s. lifecycle.md: a parked
// connector consumes no more messages, and for a fixture that reads with a
// plain NATS subscription that means dropping it. The park tests call this
// after "parked" and before resume or skip, so that the awaitSubscribed that
// follows proves the re-entered Run hook subscribed, not that the old
// subscription survived the park.
func awaitUnsubscribed(t *testing.T, srv *natsenv.Server, subject string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !srv.HasInterest(subject) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("parked fixture still subscribes to %q after 10s (lifecycle.md: a parked connector consumes no messages)", subject)
}
