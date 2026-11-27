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

package status_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// TestRedactReason covers the pure redactor: a URI's userinfo is masked and
// the secret is gone, while a reason with no URI is left untouched.
func TestRedactReason(t *testing.T) {
	tests := []struct {
		name, in, secret string
	}{
		{"mongodb uri password", "connect failed: mongodb://admin:sup3rs3cret@db:27017/app timed out", "sup3rs3cret"},
		{"s3 endpoint userinfo", "put failed: https://AKIAKEY:s3cretkey@minio.local/bucket refused", "s3cretkey"},
		{"token-only userinfo", "amqp://t0ken@broker unreachable", "t0ken"},
		{"slash in password", "mongodb://user:pa/ss@host/db timed out", "pa/ss"},
		{"at sign in password", "mongodb://user:p@ss@host/db timed out", "p@ss"},
		{"json-escaped slashes", `{"uri":"mongodb:\/\/u:p\/w@h"}`, `p\/w`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := status.RedactReason(tt.in)
			if strings.Contains(got, tt.secret) {
				t.Errorf("RedactReason(%q) = %q, still contains secret %q", tt.in, got, tt.secret)
			}
			if !strings.Contains(got, "REDACTED") {
				t.Errorf("RedactReason(%q) = %q, missing REDACTED marker", tt.in, got)
			}
		})
	}
}

// TestRedactReason_Exact pins the whole output where its shape matters: the
// userinfo goes as one piece, up to the last "@" of the token, and the host
// after it survives; two URIs in one reason are redacted independently; a
// URI with no userinfo but an "@" later in the token is over-redacted, which
// is the accepted cost; and a bare mailbox with no scheme is not a URI.
func TestRedactReason_Exact(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{
			"at sign in password keeps the host",
			"mongodb://user:p@ss@host/db refused",
			"mongodb://REDACTED@host/db refused",
		},
		{
			"two uris redacted independently",
			"primary mongodb://a:s1@h1/db secondary mongodb://b:s2@h2/db",
			"primary mongodb://REDACTED@h1/db secondary mongodb://REDACTED@h2/db",
		},
		{
			"over-redaction of a path containing an at sign is accepted",
			"get https://host/path@x failed",
			"get https://REDACTED@x failed",
		},
		{
			"bare mailbox without a scheme is left alone",
			"notify user@example.com failed",
			"notify user@example.com failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.RedactReason(tt.in); got != tt.want {
				t.Errorf("RedactReason(%q)\n got %q\nwant %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestRedactReason_URLParseErrors builds its inputs from real url.Parse
// errors rather than hand-written text. net/url quotes the URL it rejected
// with its fragment cut off, so "#" in the password leaves no "@" to redact
// up to; a space in the password breaks the userinfo pattern; and the piece
// it choked on is repeated as the "port" (the password's head before "/" or
// "?") or the bad escape. Each would reach the wire through a connector that
// puts the raw error in a reason. Every piece of the password must be gone
// present.
func TestRedactReason_URLParseErrors(t *testing.T) {
	// head and tail sit on either side of the character url.Parse cuts at.
	const head, tail = "wJalrXUtn", "K7MDENG"
	for _, sep := range []string{"/", "?", "#", "%zz", " "} {
		t.Run(fmt.Sprintf("%q in password", sep), func(t *testing.T) {
			raw := "mongodb://adm1n:" + head + sep + tail + "@db:27017"
			_, err := url.Parse(raw)
			if err == nil {
				t.Fatalf("url.Parse(%q) error = nil, want the rejection this test is about", raw)
			}
			got := status.RedactReason("connect: " + err.Error())
			for _, leak := range []string{head, tail, "adm1n", "%zz"} {
				if strings.Contains(got, leak) {
					t.Errorf("RedactReason(%q)\n = %q, carries %q", err, got, leak)
				}
			}
			if !strings.Contains(got, "REDACTED") {
				t.Errorf("RedactReason(%q)\n = %q, want the REDACTED marker", err, got)
			}
		})
	}
}

// TestRedactReason_URLParseErrorExact pins the shape the masking produces
// for one real error, so a change in what is kept shows up as a diff.
func TestRedactReason_URLParseErrorExact(t *testing.T) {
	_, err := url.Parse("mongodb://adm1n:wJalrXUtn/K7MDENG@db:27017")
	if err == nil {
		t.Fatal("url.Parse error = nil, want a rejection")
	}
	want := `connect: parse "mongodb://REDACTED": invalid port "REDACTED" after host`
	if got := status.RedactReason("connect: " + err.Error()); got != want {
		t.Errorf("RedactReason\n got %q\nwant %q", got, want)
	}
}

func TestRedactReason_NoURIUnchanged(t *testing.T) {
	in := "checkpoint save failed after 3 retries"
	if got := status.RedactReason(in); got != in {
		t.Errorf("RedactReason(%q) = %q, want it unchanged (no URI to redact)", in, got)
	}
}

// TestSetState_RedactsPublishedReason is the load-bearing end-to-end guard:
// a reason carrying an embedded mongodb URI, handed to SetState, must reach
// the wire with the password gone and the REDACTED marker present — the
// harness backstop that protects a connector which forgets its own redaction.
func TestSetState_RedactsPublishedReason(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	sub, err := nc.SubscribeSync(identity.StatusSubject(id))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()

	p := status.NewPublisher(nc, id)
	p.SetState(status.Parked, "config-invalid: mongodb://u:p4ssw0rd@h:27017 unreachable")

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var doc status.Document
	if err := json.Unmarshal(msg.Data, &doc); err != nil {
		t.Fatalf("unmarshal published document: %v", err)
	}
	if strings.Contains(doc.Reason, "p4ssw0rd") {
		t.Errorf("published reason %q still contains the password", doc.Reason)
	}
	if !strings.Contains(doc.Reason, "REDACTED") {
		t.Errorf("published reason %q missing REDACTED marker", doc.Reason)
	}

	// The stored snapshot (what the admin status endpoint returns verbatim)
	// is redacted too, not just the wire copy.
	if strings.Contains(p.Snapshot().Reason, "p4ssw0rd") {
		t.Errorf("snapshot reason %q still contains the password", p.Snapshot().Reason)
	}
}
