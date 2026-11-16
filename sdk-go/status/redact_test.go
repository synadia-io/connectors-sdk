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
