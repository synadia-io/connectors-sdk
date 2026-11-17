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

// Package identity implements the D-3 runtime-identity resolution every
// other naming decision in this contract hangs off: the status subject
// (spec/status.md, D-5), the admin root subject (spec/admin.md, D-8), the
// checkpoint bucket name (spec/checkpoint.md, D-9), and the default durable
// consumer name are all pure functions of the Identity this package
// resolves from environment variables the nexlet injects. The nexlet is the
// nex node agent that launches a connector; it injects the NEX_WORKLOAD_*
// environment variables.
//
// This lives in its own package rather than sdk-go/runtime to avoid an
// import cycle:
// sdk-go/runtime will need to import sdk-go/admin, which needs Identity.
package identity

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

const (
	// envConnectorID is the durable connector id the launcher assigns
	// (spec/status.md, D-3).
	envConnectorID = "NEX_WORKLOAD_CONNECTOR_ID"

	// envNamespace is the namespace the connector runs in (spec/status.md,
	// D-3); defaults to defaultNamespace when unset (spec/credentials.md).
	envNamespace = "NEX_WORKLOAD_NAMESPACE"

	// defaultNamespace is Namespace's value when envNamespace is unset
	// (spec/credentials.md). An empty namespace would make StatusSubject
	// and AdminRoot emit a malformed NATS subject (a bare "..").
	defaultNamespace = "default"

	// envWorkloadID is the nexlet-injected workload id. spec/status.md's
	// D-3 fallback chain names this only as "the workload id env var if
	// the nexlet provides one"; NEX_WORKLOAD_ID is the concrete name a
	// nexlet injects.
	envWorkloadID = "NEX_WORKLOAD_ID"

	// tokenRule is the shared NATS subject token rule's description, shown
	// in every identity validation error (spec/subjects.md).
	tokenRule = "no '.', ' ', tab, '*', '>', ASCII control characters, U+FFFD, or invalid UTF-8; spec/subjects.md"
)

// Identity is the runtime identity a connector resolves once at startup;
// every subject, bucket, and default consumer name in this contract is a
// pure function of it (spec/status.md, D-3).
type Identity struct {
	Namespace   string
	ConnectorID string
	WorkloadID  string
	InstanceID  string
}

// DetectIdentity resolves the runtime Identity (see Detect) and validates it
// at the identity boundary before any subject or bucket is derived from it.
// The validation rule is subject.ValidToken (spec/subjects.md).
//
// This is the harness's entry point, and where it validates: connector_id,
// namespace, and workload_id all flow unescaped into NATS subjects
// (StatusSubject, AdminRoot) via InstanceID, exactly the way CheckpointBucket
// sanitizes for the KV bucket name — but a subject cannot be sanitized the
// way a bucket name can. Replacing a `.` with `_` in a subject would silently
// relocate a connector's control plane and collide two distinct ids onto one
// subject; a `*` or `>` would subscribe the admin service to sibling
// connectors' status/resume/skip across the namespace. So an id component
// that is not a single clean NATS subject token is REJECTED here, loudly,
// rather than sanitized: a malformed identity is a fatal misconfiguration,
// caught before the admin and status surfaces exist (lifecycle.md D-15's
// pre-admin fail-stop), not a value to be quietly rewritten.
//
// An unset NEX_WORKLOAD_CONNECTOR_ID or NEX_WORKLOAD_ID is not an error — an
// empty value is the documented D-3 fallback trigger, not an injected token.
// Only a value that is present-but-malformed, and the resolved Namespace and
// InstanceID that actually land in subjects, are validated.
func DetectIdentity(fallbackName string) (Identity, error) {
	id := Detect(fallbackName)

	// Validate each env-var value that was actually supplied: a set
	// connector_id/workload_id lands in the wire status document even when it
	// loses the InstanceID fallback race, and a set namespace always lands in
	// a subject. An unset value (the fallback case) is skipped.
	for _, f := range []struct {
		env, val string
	}{
		{envConnectorID, os.Getenv(envConnectorID)},
		{envWorkloadID, os.Getenv(envWorkloadID)},
		{envNamespace, os.Getenv(envNamespace)},
	} {
		if f.val != "" && !subject.ValidToken(f.val) {
			return Identity{}, fmt.Errorf("identity: %s=%q is not a valid NATS subject token (%s)", f.env, f.val, tokenRule)
		}
	}

	// The two components that always end up in a subject: Namespace after its
	// "default" fallback, and the resolved InstanceID (which also covers the
	// fallbackName+"-dev" branch the env checks above do not). The namespace
	// check can't fail today (a set namespace was checked above, and the
	// fallback is a valid token); it guards a future fallback.
	if !subject.ValidToken(id.Namespace) {
		return Identity{}, fmt.Errorf("identity: namespace %q is not a valid NATS subject token (%s)", id.Namespace, tokenRule)
	}
	if !subject.ValidToken(id.InstanceID) {
		return Identity{}, fmt.Errorf("identity: instance id %q (the fallback name plus \"-dev\") is not a valid NATS subject token (%s)", id.InstanceID, tokenRule)
	}

	return id, nil
}

// Detect resolves the runtime Identity from environment variables injected
// by the nexlet (D-3), without validation — DetectIdentity is the validating
// entry point the harness uses. InstanceID follows the D-3 fallback chain:
// the connector id when NEX_WORKLOAD_CONNECTOR_ID is set, else the workload
// id when the nexlet provides one, else fallbackName with "-dev" appended —
// guaranteeing an InstanceID is always resolvable, including for a connector
// run outside any launcher (e.g. a developer's laptop).
//
// Namespace defaults to "default" when NEX_WORKLOAD_NAMESPACE is unset
// (spec/credentials.md) — an empty namespace would make StatusSubject and
// AdminRoot produce a malformed NATS subject.
func Detect(fallbackName string) Identity {
	id := Identity{
		Namespace:   os.Getenv(envNamespace),
		ConnectorID: os.Getenv(envConnectorID),
		WorkloadID:  os.Getenv(envWorkloadID),
	}
	if id.Namespace == "" {
		id.Namespace = defaultNamespace
	}

	switch {
	case id.ConnectorID != "":
		id.InstanceID = id.ConnectorID
	case id.WorkloadID != "":
		id.InstanceID = id.WorkloadID
	default:
		id.InstanceID = fallbackName + "-dev"
	}

	return id
}

// CheckpointBucket returns id's default checkpoint KV bucket name:
// "CONN-CKPT-" plus id.InstanceID sanitized to the character class
// [A-Za-z0-9_-], with every other rune replaced by "_" (spec/checkpoint.md,
// D-9). It doesn't check that the InstanceID is stable; checkpoint.Open
// does, but only for a name it derives itself, so passing CheckpointBucket(id)
// in as checkpoint.Options.Bucket skips that check.
func CheckpointBucket(id Identity) string {
	return "CONN-CKPT-" + sanitize(id.InstanceID)
}

// ErrNoConnectorID means an Identity has a workload id but no connector id,
// so it can't name durable state. The workload id is new on every launch: a
// durable consumer named after it would be a new one, replaying the stream
// from its start, and a checkpoint bucket named after it would start over,
// every time the connector starts (spec/status.md, D-3). DurableName and
// checkpoint.Open return it.
var ErrNoConnectorID = errors.New("identity: no connector id, and the workload id changes on every launch")

// DurableName returns the default name for id's durable consumer: "CONN-"
// plus id.InstanceID, as is (spec/status.md, D-3). The name must stay the
// same from one launch to the next, so DurableName returns ErrNoConnectorID
// when the InstanceID is the workload id (ConnectorID empty, WorkloadID set);
// the "-dev" fallback is stable and is used. It also returns an error for an
// empty InstanceID, and when the name is not a valid consumer name
// (subject.ValidConsumerName): for an id with '/' or '\', or one longer than
// 250 bytes. The id is never rewritten, because a rewritten name could be
// another connector's, and two connectors sharing one durable split its
// messages without any error.
//
// A connector that already has a configured durable keeps passing that name
// instead (jsresource.ConsumerSpec.Durable): renaming a deployed connector's
// durable starts a new durable, which loses the old one's position. That is
// also the way to run without a connector id.
func DurableName(id Identity) (string, error) {
	switch {
	case id.InstanceID == "":
		return "", errors.New("identity: durable name: the instance id is empty")
	case id.ConnectorID == "" && id.WorkloadID != "":
		return "", fmt.Errorf("%w: set NEX_WORKLOAD_CONNECTOR_ID, or configure the durable's name", ErrNoConnectorID)
	}
	name := "CONN-" + id.InstanceID
	if !subject.ValidConsumerName(name) {
		return "", fmt.Errorf("identity: durable name %q is not a valid consumer name (no '/' or '\\', at most 255 bytes; spec/subjects.md)", name)
	}
	return name, nil
}

// StatusSubject returns id's status subject (spec/status.md, D-5).
func StatusSubject(id Identity) string {
	return "$CONNECTOR." + id.Namespace + ".status." + id.InstanceID
}

// AdminRoot returns id's admin service root subject (spec/admin.md, D-8).
func AdminRoot(id Identity) string {
	return "$CONNECTOR." + id.Namespace + ".admin." + id.InstanceID
}

// sanitize replaces every rune outside [A-Za-z0-9_-] with a single "_"
// (D-9). Runs of bad runes are not collapsed — each is replaced
// individually, matching the checkpoint.md naming boundary contract.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
