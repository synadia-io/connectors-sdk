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

package subject_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// enumerate returns every '.'-joined sequence of 1 to maxTokens tokens drawn
// from alphabet, valid or not.
func enumerate(alphabet []string, maxTokens int) []string {
	var out []string
	var walk func(prefix []string)
	walk = func(prefix []string) {
		if len(prefix) > 0 {
			out = append(out, strings.Join(prefix, "."))
		}
		if len(prefix) == maxTokens {
			return
		}
		for _, t := range alphabet {
			walk(append(prefix[:len(prefix):len(prefix)], t))
		}
	}
	walk(nil)
	return out
}

// TestAgreesWithServer cross-checks Covers and Overlap against the
// nats-server functions they were copied to match, on every pair of valid
// patterns of up to four tokens, drawn from an alphabet that includes a
// multi-character token so token comparisons aren't masked by single-byte
// equality. nats-server is a test-only dependency (spec/subjects.md).
func TestAgreesWithServer(t *testing.T) {
	var valid []string
	for _, s := range enumerate([]string{"a", "b", "ab", "*", ">"}, 4) {
		if subject.ValidPattern(s) {
			valid = append(valid, s)
		}
	}
	for _, a := range valid {
		for _, b := range valid {
			if got, want := subject.Covers(a, b), server.SubjectMatchesFilter(b, a); got != want {
				t.Errorf("Covers(%q, %q) = %v, server.SubjectMatchesFilter = %v", a, b, got, want)
			}
			if got, want := subject.Overlap(a, b), server.SubjectsCollide(a, b); got != want {
				t.Errorf("Overlap(%q, %q) = %v, server.SubjectsCollide = %v", a, b, got, want)
			}
		}
	}
}

// TestNeverLooserThanServer checks that every subject this package accepts,
// the server accepts too. The candidates are built from tokens the server
// itself rejects, so the test can actually fail. It also sweeps every rune
// below U+3000 inside a token, though inside a token longer than one byte
// the server rejects only whitespace (space, tab, CR, LF and form feed), so
// beyond TestValidToken the sweep can catch only a form feed.
func TestNeverLooserThanServer(t *testing.T) {
	alphabet := []string{"a", "*", ">", "", "a b", "a\tb", "a\nb", "a\rb", "a\fb"}
	for _, s := range enumerate(alphabet, 3) {
		if subject.ValidPattern(s) && !server.IsValidSubject(s) {
			t.Errorf("ValidPattern(%q) = true, server.IsValidSubject = false", s)
		}
		if subject.ValidLiteral(s) && !server.IsValidLiteralSubject(s) {
			t.Errorf("ValidLiteral(%q) = true, server.IsValidLiteralSubject = false", s)
		}
	}
	for r := range rune(0x3000) {
		s := "a" + string(r) + "b"
		if subject.ValidToken(s) && !server.IsValidPublishSubject(s) {
			t.Errorf("ValidToken(%q) = true, server.IsValidPublishSubject = false", s)
		}
	}
}

// TestMaxSubjectLenPublishes checks MaxSubjectLen's derivation against a
// real server at the default max_control_line: a subject of MaxSubjectLen
// bytes, in a message with a 5-digit header size and a 7-digit total size
// (the most the default max_payload allows), is acknowledged by JetStream,
// published synchronously and asynchronously over a connection whose custom
// inbox prefix is the 969 bytes the derivation allows, and the connection
// stays up. It also checks what MaxSubjectLen's doc comment says happens
// over the limit: the server closes the connection, and nats.go doesn't
// reconnect.
func TestMaxSubjectLenPublishes(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "_INBOX_" + strings.Repeat("p", 969-len("_INBOX_"))
	nc, err := nats.Connect(srv.ClientURL(), nats.CustomInboxPrefix(prefix))
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	subj := "orders." + strings.Repeat("x", subject.MaxSubjectLen-len("orders."))
	newMsg := func() *nats.Msg {
		return &nats.Msg{
			Subject: subj,
			Header:  nats.Header{"K": {strings.Repeat("v", 10000)}},
			Data:    make([]byte, nc.MaxPayload()-20000),
		}
	}
	if _, err := js.PublishMsg(ctx, newMsg()); err != nil {
		t.Fatalf("synchronous publish to a %d-byte subject: %v", len(subj), err)
	}
	f, err := js.PublishMsgAsync(newMsg())
	if err != nil {
		t.Fatalf("asynchronous publish to a %d-byte subject: %v", len(subj), err)
	}
	select {
	case <-f.Ok():
	case err := <-f.Err():
		t.Fatalf("asynchronous publish to a %d-byte subject: %v", len(subj), err)
	case <-ctx.Done():
		t.Fatalf("asynchronous publish to a %d-byte subject: no ack", len(subj))
	}
	if st, n := nc.Status(), nc.Stats().Reconnects; st != nats.CONNECTED || n != 0 {
		t.Fatalf("connection is %v after %d reconnects, want CONNECTED after none", st, n)
	}

	closed := make(chan struct{})
	over, err := nats.Connect(srv.ClientURL(), nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(over.Close)
	long := "orders." + strings.Repeat("x", server.MAX_CONTROL_LINE_SIZE)
	_ = over.Publish(long, nil)
	_ = over.Flush()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatalf("publish to a %d-byte subject: connection is %v, want it closed", len(long), over.Status())
	}
	if err := over.LastError(); err == nil || !strings.Contains(err.Error(), "maximum control line exceeded") {
		t.Errorf("publish to a %d-byte subject: LastError = %v, want maximum control line exceeded", len(long), err)
	}
}
