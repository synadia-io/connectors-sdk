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
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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

	lastSkipKey  string
	sawDeadline  bool
	lastDeadline time.Time
}

func (f *fakeController) Status() status.Document {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statusDoc
}

func (f *fakeController) Resume(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordDeadline(ctx)
	return f.resumeErr
}

func (f *fakeController) Skip(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordDeadline(ctx)
	f.lastSkipKey = key
	return f.skipErr
}

// recordDeadline notes whether ctx carries a deadline and, if so, which; the
// caller holds f.mu.
func (f *fakeController) recordDeadline(ctx context.Context) {
	if d, ok := ctx.Deadline(); ok {
		f.sawDeadline = true
		f.lastDeadline = d
	}
}

// LastDeadline returns the deadline of the most recent Resume or Skip ctx
// that carried one (zero if none did).
func (f *fakeController) LastDeadline() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastDeadline
}

// SawDeadline reports whether every Resume or Skip call so far carried a ctx
// with a deadline, the contract Controller's godoc states.
func (f *fakeController) SawDeadline() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sawDeadline
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

	// skipPanicValue, when set, is the value Skip panics with instead of
	// "skip boom", so a test can plant a credential in it.
	skipPanicValue string
}

func (p *panicController) Status() status.Document {
	if p.statusPanic {
		panic("status boom")
	}
	return p.statusDoc
}

func (p *panicController) Resume(context.Context) error {
	if p.resumePanic {
		panic("resume boom")
	}
	return nil
}

func (p *panicController) Skip(context.Context, string) error {
	if p.skipPanic {
		if p.skipPanicValue != "" {
			panic(p.skipPanicValue)
		}
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
	if !fc.SawDeadline() {
		t.Error("Resume ctx carried no deadline; Controller promises the request's")
	}
}

// TestStart_RequestTimeout guards the deadline Start puts on the ctx a resume
// or skip request hands the Controller: DefaultRequestTimeout (5s) from the
// request's arrival without an option, WithRequestTimeout's duration with
// one, and a non-positive WithRequestTimeout ignored in favor of the default.
// Measured from the client's send, the deadline lands the configured timeout
// out plus the request's trip through the server, so it is never short of
// the timeout and the slack allowed is on the far side only.
func TestStart_RequestTimeout(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	for _, tt := range []struct {
		name string
		opts []admin.Option
		want time.Duration
	}{
		{name: "default", opts: nil, want: admin.DefaultRequestTimeout},
		{
			name: "raised",
			opts: []admin.Option{admin.WithRequestTimeout(30 * time.Second)},
			want: 30 * time.Second,
		},
		{
			name: "zero keeps default",
			opts: []admin.Option{admin.WithRequestTimeout(0)},
			want: admin.DefaultRequestTimeout,
		},
		{
			name: "negative keeps default",
			opts: []admin.Option{admin.WithRequestTimeout(-time.Second)},
			want: admin.DefaultRequestTimeout,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := testIdentity()
			fc := &fakeController{}
			svc, err := admin.Start(nc, id, "1.0.0", fc, tt.opts...)
			if err != nil {
				t.Fatalf("admin.Start: %v", err)
			}
			defer func() { _ = svc.Stop() }()
			root := identity.AdminRoot(id)

			for _, ep := range []struct {
				subject string
				body    []byte
			}{
				{root + ".resume", nil},
				{root + ".skip", []byte(`{"key":"k"}`)},
			} {
				// The deadline is set on the request's arrival, so measured
				// from sent it is want plus the one-way trip, and a trip longer
				// than the request timeout cannot have produced a reply.
				const reqTimeout = 2 * time.Second
				sent := time.Now()
				msg, err := nc.Request(ep.subject, ep.body, reqTimeout)
				if err != nil {
					t.Fatalf("Request(%q): %v", ep.subject, err)
				}
				if errHeader := msg.Header.Get(micro.ErrorHeader); errHeader != "" {
					t.Fatalf("Request(%q) returned an error: %s", ep.subject, errHeader)
				}
				deadline := fc.LastDeadline()
				if deadline.IsZero() {
					t.Fatalf("%s: Controller ctx carried no deadline", ep.subject)
				}
				remaining := deadline.Sub(sent)
				if remaining < tt.want || remaining > tt.want+reqTimeout {
					t.Errorf("%s: deadline %v after the request, want within %v over %v",
						ep.subject, remaining.Round(time.Millisecond), reqTimeout, tt.want)
				}
			}
		})
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

// TestStart_ErrorMessagesAreRedacted guards the admin surface's error choke
// point: a Controller error or panic value quoting a URI with userinfo must
// reach the Nats-Service-Error header with the credential masked, the same
// way a status reason is (status.RedactReason), and the rest of the message
// intact.
func TestStart_ErrorMessagesAreRedacted(t *testing.T) {
	const secret = "p4ss"
	const uri = "mongodb://u:" + secret + "@h/db"

	t.Run("resume error", func(t *testing.T) {
		nc, cleanup := startTestServer(t)
		defer cleanup()

		id := testIdentity()
		fc := &fakeController{resumeErr: errors.New("connect " + uri + ": refused")}
		_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

		msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Request(%q): %v", resumeSubj, err)
		}
		got := msg.Header.Get(micro.ErrorHeader)
		if strings.Contains(got, secret) {
			t.Errorf("Nats-Service-Error = %q, leaks %q", got, secret)
		}
		if want := "connect mongodb://REDACTED@h/db: refused"; got != want {
			t.Errorf("Nats-Service-Error = %q, want %q", got, want)
		}
	})

	t.Run("skip panic", func(t *testing.T) {
		nc, cleanup := startTestServer(t)
		defer cleanup()

		id := testIdentity()
		fc := &panicController{skipPanic: true, skipPanicValue: "dial " + uri}
		svc, err := admin.Start(nc, id, "1.0.0", fc)
		if err != nil {
			t.Fatalf("admin.Start: %v", err)
		}
		t.Cleanup(func() { _ = svc.Stop() })

		body, _ := json.Marshal(map[string]string{"key": "k1"})
		msg, err := nc.Request(identity.AdminRoot(id)+".skip", body, 2*time.Second)
		if err != nil {
			t.Fatalf("Request(skip): %v", err)
		}
		got := msg.Header.Get(micro.ErrorHeader)
		if strings.Contains(got, secret) {
			t.Errorf("Nats-Service-Error = %q, leaks %q", got, secret)
		}
		if !strings.Contains(got, "panic") || !strings.Contains(got, "mongodb://REDACTED@h") {
			t.Errorf("Nats-Service-Error = %q, want the panic with the URI redacted", got)
		}
	})
}

// TestStart_ErrorMessagesCapOnRuneBoundary guards the cut itself: a message
// of multi-byte runes that is cut at the byte cap must still be valid UTF-8,
// so the cut lands on a rune start, not inside one.
func TestStart_ErrorMessagesCapOnRuneBoundary(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{resumeErr: errors.New(strings.Repeat("é", 600))} // 1200 bytes
	_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", resumeSubj, err)
	}
	got := msg.Header.Get(micro.ErrorHeader)
	if !utf8.ValidString(got) {
		t.Errorf("Nats-Service-Error is not valid UTF-8 after the cut: %q", got)
	}
	if !strings.HasSuffix(got, admin.TruncationMarker) {
		t.Errorf("Nats-Service-Error does not end with the truncation marker %q", admin.TruncationMarker)
	}
	if limit := admin.MaxErrorMessageBytes + len(admin.TruncationMarker); len(got) > limit {
		t.Errorf("Nats-Service-Error is %d bytes, want at most %d", len(got), limit)
	}
}

// TestStart_EmptyErrorMessageStillAnswers guards the edge the choke point
// must not fall into: micro refuses to send an error with an empty
// description, which would leave the requester to time out, so an empty
// Controller error is answered with a placeholder.
func TestStart_EmptyErrorMessageStillAnswers(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{resumeErr: errors.New("")}
	_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v, want an error response rather than a timeout", resumeSubj, err)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got == "" {
		t.Error("want a non-empty Nats-Service-Error header for an empty Controller error")
	}
}

// TestStart_ErrorMessagesAreCapped guards the cap on the same choke point: a
// Controller error of several kilobytes is cut to the documented bound,
// truncation marker included, before it reaches the header.
func TestStart_ErrorMessagesAreCapped(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	fc := &fakeController{resumeErr: errors.New(strings.Repeat("x", 5*1024))}
	_, _, resumeSubj, _ := startAdmin(t, nc, id, fc)

	msg, err := nc.Request(resumeSubj, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Request(%q): %v", resumeSubj, err)
	}
	got := msg.Header.Get(micro.ErrorHeader)
	if got == "" {
		t.Fatal("want a Nats-Service-Error header, got none")
	}
	if !strings.HasSuffix(got, admin.TruncationMarker) {
		t.Errorf("Nats-Service-Error does not end with the truncation marker %q", admin.TruncationMarker)
	}
	// ASCII input cuts exactly, so the marker-included cap is met to the byte.
	if len(got) != admin.MaxErrorMessageBytes {
		t.Errorf("Nats-Service-Error is %d bytes, want exactly the cap of %d, marker included",
			len(got), admin.MaxErrorMessageBytes)
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
