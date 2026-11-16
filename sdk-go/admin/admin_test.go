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

package admin_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/connectors-sdk/sdk-go/admin"
	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// startTestServer boots an in-process NATS server and a connected client,
// returning both plus a cleanup func. Mirrors
// sdk-go/status/publisher_test.go's pattern.
func startTestServer(t *testing.T) (*nats.Conn, func()) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1 // random free port, hermetic across parallel test runs

	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats.Connect: %v", err)
	}

	return nc, func() {
		nc.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
}

func testIdentity() identity.Identity {
	return identity.Identity{
		Namespace:   "test",
		ConnectorID: "conn-1",
		WorkloadID:  "wl-1",
		InstanceID:  "conn-1",
	}
}

// fakeController is a settable Controller test double: each method's
// behavior is configured via the exported fields before Start is called,
// and Skip's received key is recorded for assertions.
type fakeController struct {
	mu sync.Mutex

	statusDoc status.Document
	resumeErr error
	skipErr   error

	lastSkipKey string
}

func (f *fakeController) Status() status.Document {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statusDoc
}

func (f *fakeController) Resume() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resumeErr
}

func (f *fakeController) Skip(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSkipKey = key
	return f.skipErr
}

func (f *fakeController) LastSkipKey() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSkipKey
}

// panicController is a Controller whose endpoints panic when the matching
// flag is set — the panic fault injector. Status returns statusDoc unless
// statusPanic is set. It never guards with a mutex: the panics are the
// point, and each method is called from at most one in-flight request in
// these tests.
type panicController struct {
	statusDoc   status.Document
	statusPanic bool
	resumePanic bool
	skipPanic   bool
}

func (p *panicController) Status() status.Document {
	if p.statusPanic {
		panic("status boom")
	}
	return p.statusDoc
}

func (p *panicController) Resume() error {
	if p.resumePanic {
		panic("resume boom")
	}
	return nil
}

func (p *panicController) Skip(string) error {
	if p.skipPanic {
		panic("skip boom")
	}
	return nil
}

// TestStart_HandlerPanicRepliesErrorAndStaysAlive is the panic guard: a
// Controller.Resume/Skip that panics must be converted to a micro error
// reply, and — the load-bearing half — the process must survive so a
// subsequent status request is still answered. Without the handler recover,
// the panic unwinds through micro's subscription goroutine and aborts the
// whole test binary, so reaching the post-panic assertions at all is part of
// the proof.
func TestStart_HandlerPanicRepliesErrorAndStaysAlive(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &panicController{
		statusDoc:   sampleStatusDocument(id),
		resumePanic: true,
		skipPanic:   true,
	}
	svc, err := admin.Start(nc, id, "1.0.0", fc)
	if err != nil {
		t.Fatalf("admin.Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })
	root := identity.AdminRoot(id)

	// resume panics -> micro error mentioning the panic, not a dead process.
	msg, err := nc.Request(root+".resume", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(resume): %v", err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got == "" {
		t.Error("resume panic: want a Nats-Service-Error header, got none")
	} else if !strings.Contains(got, "panic") {
		t.Errorf("resume panic: error header = %q, want it to mention the panic", got)
	}

	// skip panics too.
	body, _ := json.Marshal(map[string]string{"key": "k1"})
	msg, err = nc.Request(root+".skip", body, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(skip): %v", err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got == "" || !strings.Contains(got, "panic") {
		t.Errorf("skip panic: error header = %q, want it to mention the panic", got)
	}

	// The service is still alive after two handler panics: status answers.
	msg, err = nc.Request(root+".status", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(status) after panics: %v", err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got != "" {
		t.Fatalf("status after panics: unexpected error header %q", got)
	}
}

// TestStart_StatusPanicRepliesError guards the status endpoint's own recover:
// a panicking Status() must reply a micro error rather than crash the
// process. Reaching the assertion proves the handler recovered.
func TestStart_StatusPanicRepliesError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &panicController{statusPanic: true}
	svc, err := admin.Start(nc, id, "1.0.0", fc)
	if err != nil {
		t.Fatalf("admin.Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	msg, err := nc.Request(identity.AdminRoot(id)+".status", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(status): %v", err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got == "" || !strings.Contains(got, "panic") {
		t.Errorf("status panic: error header = %q, want it to mention the panic", got)
	}
}

// sampleStatusDocument mirrors sdk-go/status/status_test.go's sample shape,
// scoped to the fields Start's status endpoint needs to round-trip.
func sampleStatusDocument(id identity.Identity) status.Document {
	return status.Document{
		Contract:    spec.Contract,
		State:       status.Running,
		Reason:      "",
		Since:       time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Admin:       identity.AdminRoot(id),
		ConnectorID: id.ConnectorID,
		WorkloadID:  id.WorkloadID,
	}
}

// compileStatusSchema mirrors sdk-go/status/status_test.go's helper: the
// status endpoint's response must validate against the same
// status.schema.json a published status document does (spec/admin.md
// "status" — "byte-for-byte the same shape").
func compileStatusSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	path := filepath.Join("..", "..", "spec", "status.schema.json")
	sch, err := c.Compile(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return sch
}

// startAdmin starts the D-8 admin service against a fresh fakeController
// and returns both, plus the exact literal endpoint subjects spec/admin.md
// defines (root subject + ".status"/".resume"/".skip") so tests assert
// against the spec's subject layout directly, not through the micro
// package's group abstraction.
func startAdmin(t *testing.T, nc *nats.Conn, id identity.Identity, fc *fakeController) (root, statusSubj, resumeSubj, skipSubj string) {
	t.Helper()
	svc, err := admin.Start(nc, id, "1.0.0", fc)
	if err != nil {
		t.Fatalf("admin.Start: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	root = identity.AdminRoot(id)
	return root, root + ".status", root + ".resume", root + ".skip"
}

// TestStart_StatusEndpoint_ReturnsValidStatusDocument guards spec/admin.md's
// "status" endpoint: no request body, response is the Controller's current
// status document, validating against status.schema.json.
func TestStart_StatusEndpoint_ReturnsValidStatusDocument(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	want := sampleStatusDocument(id)
	fc := &fakeController{statusDoc: want}
	_, statusSubj, _, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(statusSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", statusSubj, err)
	}
	if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader != "" {
		t.Fatalf("unexpected error response: %s", errHeader)
	}

	var instance map[string]any
	if err := json.Unmarshal(msg.Data, &instance); err != nil {
		t.Fatalf("unmarshal response as JSON object: %v", err)
	}
	schema := compileStatusSchema(t)
	if err := schema.Validate(instance); err != nil {
		t.Errorf("status.schema.json validation failed: %v", err)
	}

	var got status.Document
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("unmarshal response as status.Document: %v", err)
	}
	if got.State != want.State || got.ConnectorID != want.ConnectorID {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestStart_ResumeEndpoint_Success guards the resume success path: a nil
// Controller.Resume() error responds without any micro error header.
func TestStart_ResumeEndpoint_Success(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", resumeSubj, err)
	}
	if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader != "" {
		t.Fatalf("unexpected error response: %s", errHeader)
	}
}

// TestStart_ResumeEndpoint_NotParkedReturnsMicroError guards spec/admin.md's
// "Error if the connector is not currently parked": the Controller's error
// must surface as a standard micro error response, not an ad hoc body.
func TestStart_ResumeEndpoint_NotParkedReturnsMicroError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	wantErr := "not parked"
	fc := &fakeController{resumeErr: errors.New(wantErr)}
	_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", resumeSubj, err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got != wantErr {
		t.Errorf("Nats-Service-Error = %q, want %q", got, wantErr)
	}
	if msg.Header.Get(micro.ErrorCodeHeader) == "" {
		t.Error("Nats-Service-Error-Code header missing, want a non-empty code")
	}
}

// TestStart_SkipEndpoint_ForwardsKeyToController guards spec/admin.md's
// skip request body {"key": string}: the parsed key must reach
// Controller.Skip verbatim, and success responds without an error header.
func TestStart_SkipEndpoint_ForwardsKeyToController(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	_, _, _, skipSubj := startAdmin(t, nc, id, fc)

	body, err := json.Marshal(map[string]string{"key": "k1"})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	msg, err := nc.Request(skipSubj, body, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", skipSubj, err)
	}
	if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader != "" {
		t.Fatalf("unexpected error response: %s", errHeader)
	}
	if got := fc.LastSkipKey(); got != "k1" {
		t.Errorf("Controller.Skip received key = %q, want %q", got, "k1")
	}
}

// TestStart_SkipEndpoint_UnsupportedOrNotParkedReturnsMicroError guards
// spec/admin.md's "Error if unsupported by this connector, or if not
// currently parked" for skip.
func TestStart_SkipEndpoint_UnsupportedOrNotParkedReturnsMicroError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	wantErr := "skip unsupported"
	fc := &fakeController{skipErr: errors.New(wantErr)}
	_, _, _, skipSubj := startAdmin(t, nc, id, fc)

	body, _ := json.Marshal(map[string]string{"key": "k1"})
	msg, err := nc.Request(skipSubj, body, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", skipSubj, err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got != wantErr {
		t.Errorf("Nats-Service-Error = %q, want %q", got, wantErr)
	}
}

// TestStart_SkipEndpoint_MalformedBodyReturnsMicroError guards the "reject
// malformed/missing key" behavior for a body that isn't even valid JSON:
// the Controller must never be called.
func TestStart_SkipEndpoint_MalformedBodyReturnsMicroError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	_, _, _, skipSubj := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(skipSubj, []byte("not-json"), 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", skipSubj, err)
	}
	if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader == "" {
		t.Error("expected a Nats-Service-Error header for a malformed body, got none")
	}
	if fc.LastSkipKey() != "" {
		t.Errorf("Controller.Skip was called with a malformed body, key = %q", fc.LastSkipKey())
	}
}

// TestStart_SkipEndpoint_MissingKeyReturnsMicroError guards the "reject
// malformed/missing key" behavior for a well-formed body that omits key.
func TestStart_SkipEndpoint_MissingKeyReturnsMicroError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	_, _, _, skipSubj := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(skipSubj, []byte(`{}`), 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", skipSubj, err)
	}
	if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader == "" {
		t.Error("expected a Nats-Service-Error header for a missing key, got none")
	}
	if fc.LastSkipKey() != "" {
		t.Errorf("Controller.Skip was called with a missing key, key = %q", fc.LastSkipKey())
	}
}

// TestStart_DiscoverableViaSRVPing guards ADR-32 discovery: a bare $SRV.PING
// request must find the "connector-admin" service.
func TestStart_DiscoverableViaSRVPing(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	startAdmin(t, nc, id, fc)

	msg, err := nc.Request("$SRV.PING", nil, 2*time.Second)
	if err != nil {
		t.Fatalf(`Request("$SRV.PING"): %v`, err)
	}
	var ping micro.Ping
	if err := json.Unmarshal(msg.Data, &ping); err != nil {
		t.Fatalf("unmarshal PING response: %v", err)
	}
	if ping.Name != "connector-admin" {
		t.Errorf("PING response Name = %q, want %q", ping.Name, "connector-admin")
	}
}

// TestStart_InvalidVersionReturnsMicroError guards the D-8 documentation
// point that Start surfaces micro's own SemVer validation error rather than
// silently accepting a non-SemVer version string.
func TestStart_InvalidVersionReturnsMicroError(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{}
	if _, err := admin.Start(nc, id, "not-a-semver", fc); err == nil {
		t.Fatal("Start() error = nil, want an error for an invalid SemVer version")
	}
}
