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

package identity_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// clearEnv unsets the D-3 identity env vars so each test starts from a known
// empty environment regardless of what the ambient shell (or another test)
// has set. t.Setenv registers a restore-on-cleanup even for the subsequent
// Unsetenv call, matching the pattern in sdk-go/config/load_test.go. Because
// it uses t.Setenv, a test that calls it can't use t.Parallel.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"NEX_WORKLOAD_CONNECTOR_ID",
		"NEX_WORKLOAD_NAMESPACE",
		"NEX_WORKLOAD_ID",
	} {
		t.Setenv(name, "placeholder")
		os.Unsetenv(name)
	}
}

func TestDetect_ConnectorIDWins(t *testing.T) {
	clearEnv(t)
	t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", "conn-1")
	t.Setenv("NEX_WORKLOAD_ID", "wl-1")
	t.Setenv("NEX_WORKLOAD_NAMESPACE", "acme")

	id := identity.Detect("fallback")

	if id.InstanceID != "conn-1" {
		t.Errorf("InstanceID = %q, want %q", id.InstanceID, "conn-1")
	}
	if id.ConnectorID != "conn-1" {
		t.Errorf("ConnectorID = %q, want %q", id.ConnectorID, "conn-1")
	}
	if id.WorkloadID != "wl-1" {
		t.Errorf("WorkloadID = %q, want %q", id.WorkloadID, "wl-1")
	}
	if id.Namespace != "acme" {
		t.Errorf("Namespace = %q, want %q", id.Namespace, "acme")
	}
}

func TestDetect_FallsBackToWorkloadID(t *testing.T) {
	clearEnv(t)
	t.Setenv("NEX_WORKLOAD_ID", "wl-1")

	id := identity.Detect("fallback")

	if id.InstanceID != "wl-1" {
		t.Errorf("InstanceID = %q, want %q", id.InstanceID, "wl-1")
	}
	if id.ConnectorID != "" {
		t.Errorf("ConnectorID = %q, want empty", id.ConnectorID)
	}
}

func TestDetect_FallsBackToFallbackNamePlusDev(t *testing.T) {
	clearEnv(t)

	id := identity.Detect("my-connector")

	if id.InstanceID != "my-connector-dev" {
		t.Errorf("InstanceID = %q, want %q", id.InstanceID, "my-connector-dev")
	}
	if id.ConnectorID != "" || id.WorkloadID != "" {
		t.Errorf("ConnectorID=%q WorkloadID=%q, want both empty", id.ConnectorID, id.WorkloadID)
	}
}

// TestDetect_NamespaceUnsetDefaultsToDefault covers the ruling recorded in
// spec/credentials.md: an unset NEX_WORKLOAD_NAMESPACE resolves to
// "default", not the empty string — an empty namespace would make
// StatusSubject/AdminRoot emit a malformed NATS subject ("$CONNECTOR..status.<id>").
func TestDetect_NamespaceUnsetDefaultsToDefault(t *testing.T) {
	clearEnv(t)

	id := identity.Detect("fallback")

	if id.Namespace != "default" {
		t.Errorf("Namespace = %q, want %q", id.Namespace, "default")
	}
}

func TestDetect_NamespaceSetIsUsedVerbatim(t *testing.T) {
	clearEnv(t)
	t.Setenv("NEX_WORKLOAD_NAMESPACE", "acme")

	id := identity.Detect("fallback")

	if id.Namespace != "acme" {
		t.Errorf("Namespace = %q, want %q", id.Namespace, "acme")
	}
}

func TestCheckpointBucket_SanitizesInstanceID(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		want       string
	}{
		{"clean id passes through", "conn-1", "CONN-CKPT-conn-1"},
		{"slash and dot become underscores", "id/with.bad", "CONN-CKPT-id_with_bad"},
		{"each bad rune replaced individually, not collapsed", "a  b", "CONN-CKPT-a__b"},
		{"underscore and dash are already legal", "a_b-c", "CONN-CKPT-a_b-c"},
		{"unicode rune becomes one underscore", "aéb", "CONN-CKPT-a_b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := identity.CheckpointBucket(identity.Identity{InstanceID: tt.instanceID})
			if got != tt.want {
				t.Errorf("CheckpointBucket(%q) = %q, want %q", tt.instanceID, got, tt.want)
			}
		})
	}
}

func TestStatusSubject_ExactFormat(t *testing.T) {
	id := identity.Identity{Namespace: "acme", InstanceID: "conn-1"}
	got := identity.StatusSubject(id)
	want := "$CONNECTOR.acme.status.conn-1"
	if got != want {
		t.Errorf("StatusSubject() = %q, want %q (spec/status.md D-5)", got, want)
	}
}

func TestAdminRoot_ExactFormat(t *testing.T) {
	id := identity.Identity{Namespace: "acme", InstanceID: "conn-1"}
	got := identity.AdminRoot(id)
	want := "$CONNECTOR.acme.admin.conn-1"
	if got != want {
		t.Errorf("AdminRoot() = %q, want %q (spec/admin.md D-8)", got, want)
	}
}

// TestSubjects_ExactlyFourTokens is the structural guard: for any valid
// identity the status and admin subjects must be exactly four
// dot-separated tokens ($CONNECTOR / namespace / status|admin / instance).
// A namespace or instance id carrying a stray '.' would split into a fifth
// token and land the connector's control plane on the wrong subject — the
// exact corruption DetectIdentity now rejects at the boundary.
func TestSubjects_ExactlyFourTokens(t *testing.T) {
	valid := []identity.Identity{
		{Namespace: "default", InstanceID: "conn-1"},
		{Namespace: "acme", InstanceID: "mongodb-dev"},
		{Namespace: "team_a", InstanceID: "wl-42"},
	}
	for _, id := range valid {
		for name, subj := range map[string]string{
			"StatusSubject": identity.StatusSubject(id),
			"AdminRoot":     identity.AdminRoot(id),
		} {
			if got := len(strings.Split(subj, ".")); got != 4 {
				t.Errorf("%s(%+v) = %q has %d tokens, want exactly 4", name, id, subj, got)
			}
		}
	}
}

// TestDetectIdentity_ValidPasses covers the accept path: clean env values
// resolve without error and produce the expected InstanceID.
func TestDetectIdentity_ValidPasses(t *testing.T) {
	clearEnv(t)
	t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", "conn-1")
	t.Setenv("NEX_WORKLOAD_ID", "wl-1")
	t.Setenv("NEX_WORKLOAD_NAMESPACE", "acme")

	id, err := identity.DetectIdentity("fallback")
	if err != nil {
		t.Fatalf("DetectIdentity() error = %v, want nil for clean ids", err)
	}
	if id.InstanceID != "conn-1" {
		t.Errorf("InstanceID = %q, want %q", id.InstanceID, "conn-1")
	}
}

// TestDetectIdentity_FallbackUnsetIDsAllowed guards the decision documented
// on DetectIdentity: an unset connector_id/workload_id is the legitimate D-3
// fallback, NOT an injection, so it must be accepted (the resolved
// InstanceID is fallbackName+"-dev", itself a clean token).
func TestDetectIdentity_FallbackUnsetIDsAllowed(t *testing.T) {
	clearEnv(t)

	id, err := identity.DetectIdentity("my-connector")
	if err != nil {
		t.Fatalf("DetectIdentity() error = %v, want nil when connector_id/workload_id are unset", err)
	}
	if id.InstanceID != "my-connector-dev" {
		t.Errorf("InstanceID = %q, want %q", id.InstanceID, "my-connector-dev")
	}
	if id.Namespace != "default" {
		t.Errorf("Namespace = %q, want %q", id.Namespace, "default")
	}
}

// TestDetectIdentity_RejectsInjection is the admission table: a '.', ' ',
// tab, '*', '>', an ASCII control character, or invalid UTF-8 in ANY of the
// three identity env vars is rejected loudly,
// regardless of which one wins the InstanceID fallback race — a bad
// connector_id/workload_id still reaches the wire status document, and a bad
// namespace/instance reaches a subject.
func TestDetectIdentity_RejectsInjection(t *testing.T) {
	const (
		connEnv = "NEX_WORKLOAD_CONNECTOR_ID"
		wlEnv   = "NEX_WORKLOAD_ID"
		nsEnv   = "NEX_WORKLOAD_NAMESPACE"
	)
	bad := map[string]string{
		"wildcard-star":    "*",
		"wildcard-gt":      ">",
		"dot-separator":    "a.b",
		"trailing-dot":     "conn.",
		"space":            "a b",
		"tab":              "a\tb",
		"newline":          "a\nb",
		"carriage-return":  "a\rb",
		"escape":           "a\x1bb",
		"delete":           "a\x7fb",
		"invalid-utf8":     "a\xffb",
		"replacement-char": "a\ufffdb",
	}
	// Each env var, set in turn to a bad value while the other two hold a
	// clean value, must fail. The clean baseline makes the rejection
	// attributable to the one poisoned var.
	for _, target := range []string{connEnv, wlEnv, nsEnv} {
		for name, val := range bad {
			t.Run(target+"/"+name, func(t *testing.T) {
				clearEnv(t)
				t.Setenv(connEnv, "conn-1")
				t.Setenv(wlEnv, "wl-1")
				t.Setenv(nsEnv, "acme")
				t.Setenv(target, val)

				if _, err := identity.DetectIdentity("fallback"); err == nil {
					t.Errorf("DetectIdentity() error = nil, want rejection for %s=%q", target, val)
				}
			})
		}
	}
}

// TestDetectIdentity_RejectsPoisonedFallbackName guards the InstanceID path
// the env-var checks do not cover: when connector_id and workload_id are
// both unset, InstanceID derives from fallbackName+"-dev", which must still
// be a clean subject token. This covers a dot-separator, invalid UTF-8, and
// a bare wildcard.
func TestDetectIdentity_RejectsPoisonedFallbackName(t *testing.T) {
	for _, name := range []string{"bad.name", "a\xffb", "*"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			clearEnv(t)
			if _, err := identity.DetectIdentity(name); err == nil {
				t.Error("DetectIdentity() error = nil, want rejection for a fallback name that is not a clean token")
			}
		})
	}
}

// TestDetectIdentity_AcceptsNonASCII pins the other side of the shared token
// rule (spec/subjects.md): non-ASCII text, including non-ASCII spaces and C1
// control characters, is a valid token. The server accepts it, and it
// changes nothing about where a message goes.
func TestDetectIdentity_AcceptsNonASCII(t *testing.T) {
	const (
		connEnv = "NEX_WORKLOAD_CONNECTOR_ID"
		wlEnv   = "NEX_WORKLOAD_ID"
		nsEnv   = "NEX_WORKLOAD_NAMESPACE"
	)
	for _, target := range []string{connEnv, wlEnv, nsEnv} {
		for _, val := range []string{"a\u00e9b", "a\u00a0b", "a\u0085b", "a\u0080b"} {
			t.Run(target+"/"+fmt.Sprintf("%q", val), func(t *testing.T) {
				clearEnv(t)
				t.Setenv(connEnv, "conn-1")
				t.Setenv(wlEnv, "wl-1")
				t.Setenv(nsEnv, "acme")
				t.Setenv(target, val)

				id, err := identity.DetectIdentity("fallback")
				if err != nil {
					t.Fatalf("DetectIdentity() error = %v, want nil", err)
				}
				switch target {
				case connEnv:
					if id.ConnectorID != val {
						t.Errorf("ConnectorID = %q, want %q", id.ConnectorID, val)
					}
				case wlEnv:
					if id.WorkloadID != val {
						t.Errorf("WorkloadID = %q, want %q", id.WorkloadID, val)
					}
				case nsEnv:
					if id.Namespace != val {
						t.Errorf("Namespace = %q, want %q", id.Namespace, val)
					}
				}
			})
		}
	}
}

// TestDurableName: the id is used as is, never rewritten. An id that can't
// form a valid consumer name is an error, because a rewritten name could
// collide with another connector's durable.
func TestDurableName(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		want       string // "" means an error
	}{
		{"clean id", "conn-1", "CONN-conn-1"},
		{"non-ASCII id", "aéb", "CONN-aéb"},
		{"distinct non-ASCII ids stay distinct", "aüb", "CONN-aüb"},
		{"longest id that fits", strings.Repeat("a", 250), "CONN-" + strings.Repeat("a", 250)},
		{"slash", "a/b", ""},
		{"backslash", `a\b`, ""},
		{"not a token", "id/with.bad", ""},
		{"too long", strings.Repeat("a", 251), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := identity.DurableName(identity.Identity{InstanceID: tt.instanceID})
			if tt.want == "" {
				if err == nil {
					t.Errorf("DurableName(%q) = %q, want an error", tt.instanceID, got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("DurableName(%q) = %q, %v; want %q", tt.instanceID, got, err, tt.want)
			}
			if !subject.ValidConsumerName(got) {
				t.Errorf("DurableName(%q) = %q, not a valid consumer name", tt.instanceID, got)
			}
		})
	}
}

// TestDurableName_NeedsAStableID: a durable keeps its name from one launch to
// the next only if the id does. The workload id is new on every launch, so an
// identity that fell back to it can't name a durable; the connector id and
// the "-dev" fallback can.
func TestDurableName_NeedsAStableID(t *testing.T) {
	for _, tt := range []struct {
		name, connectorID, workloadID string
		want                          string // "" means ErrNoConnectorID
	}{
		{"connector id", "conn-1", "wl-1", "CONN-conn-1"},
		{"workload id only", "", "wl-1", ""},
		{"neither", "", "", "CONN-inlet-dev"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			if tt.connectorID != "" {
				t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", tt.connectorID)
			}
			if tt.workloadID != "" {
				t.Setenv("NEX_WORKLOAD_ID", tt.workloadID)
			}
			got, err := identity.DurableName(identity.Detect("inlet"))
			if tt.want == "" {
				if !errors.Is(err, identity.ErrNoConnectorID) {
					t.Errorf("DurableName = %q, %v; want ErrNoConnectorID", got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("DurableName = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
	if got, err := identity.DurableName(identity.Identity{}); err == nil {
		t.Errorf("DurableName(Identity{}) = %q, want an error", got)
	}
}
