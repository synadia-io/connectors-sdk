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
	"path/filepath"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// compileStatusSchema compiles the cross-connector status.schema.json
// (spec/status.md, D-1..D-7) from its real location in the repo, mirroring
// sdk-go/spec/gen_test.go's compileSchemaFile pattern.
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

func decodeJSONObject(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal JSON object: %v", err)
	}
	return m
}

// sampleDocument mirrors spec/examples/status.parked.json's shape (with a
// caller-supplied state) so tests exercise every field the schema knows
// about, not just the ones a given state would realistically carry.
func sampleDocument(s status.State) status.Document {
	behindMs := int64(1500)
	return status.Document{
		Contract: spec.Contract,
		State:    s,
		Reason:   "",
		Since:    time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Cursor: &status.Cursor{
			BehindMs: &behindMs,
			Backfill: &status.Backfill{Copied: 128000, Done: false},
		},
		Counters:    map[string]int64{"published": 128000, "errors": 0},
		Admin:       "$CONNECTOR.prod.admin.mongodb-orders",
		ConnectorID: "mongodb-orders",
		WorkloadID:  "wl-8f3a2c",
	}
}

// TestDocument_RoundTripValidatesForEveryState guards every D-6 state value
// against status.schema.json, and that Document's json tags round-trip
// through encoding/json without loss.
func TestDocument_RoundTripValidatesForEveryState(t *testing.T) {
	schema := compileStatusSchema(t)
	states := []status.State{
		status.Running, status.Paused, status.Parked, status.Failed, status.Restarting,
	}
	for _, s := range states {
		t.Run(string(s), func(t *testing.T) {
			doc := sampleDocument(s)
			data, err := json.Marshal(doc)
			if err != nil {
				t.Fatalf("marshal Document: %v", err)
			}

			instance := decodeJSONObject(t, data)
			if err := schema.Validate(instance); err != nil {
				t.Errorf("status.schema.json validation failed for state %q: %v", s, err)
			}

			var got status.Document
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("unmarshal Document: %v", err)
			}
			if got.State != s {
				t.Errorf("round-tripped State = %q, want %q", got.State, s)
			}
			if got.Cursor == nil || got.Cursor.Backfill == nil {
				t.Fatalf("round-tripped Cursor/Backfill = %+v, want both non-nil", got.Cursor)
			}
			if got.Cursor.Backfill.Copied != 128000 || got.Cursor.Backfill.Done != false {
				t.Errorf("round-tripped Backfill = %+v, want Copied=128000 Done=false", got.Cursor.Backfill)
			}
			if !got.Since.Equal(doc.Since) {
				t.Errorf("round-tripped Since = %v, want %v", got.Since, doc.Since)
			}
		})
	}
}

// TestDocument_NilCursorAndCountersMarshalNull guards status.md's "cursor
// and counters are the only ever-null keys, but all nine keys are always
// present" rule (D-14): a Document with no Cursor/Counters set must still
// marshal both keys, as null, not omit them.
func TestDocument_NilCursorAndCountersMarshalNull(t *testing.T) {
	schema := compileStatusSchema(t)
	doc := status.Document{
		Contract:    spec.Contract,
		State:       status.Running,
		Reason:      "",
		Since:       time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Admin:       "$CONNECTOR.default.admin.foo",
		ConnectorID: "foo",
		WorkloadID:  "",
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal Document: %v", err)
	}
	instance := decodeJSONObject(t, data)

	cursor, hasCursor := instance["cursor"]
	if !hasCursor {
		t.Error(`instance missing "cursor" key, want it present (with null value)`)
	} else if cursor != nil {
		t.Errorf(`instance["cursor"] = %v, want null`, cursor)
	}

	counters, hasCounters := instance["counters"]
	if !hasCounters {
		t.Error(`instance missing "counters" key, want it present (with null value)`)
	} else if counters != nil {
		t.Errorf(`instance["counters"] = %v, want null`, counters)
	}

	if err := schema.Validate(instance); err != nil {
		t.Errorf("status.schema.json validation failed: %v", err)
	}
}

// TestDocument_ExtraFieldFailsValidation guards against tag drift: if a
// stray key ever appeared in a marshaled Document (a rename that didn't
// keep status.schema.json's additionalProperties:false in mind), the
// schema must reject it rather than silently accept it.
func TestDocument_ExtraFieldFailsValidation(t *testing.T) {
	schema := compileStatusSchema(t)
	data, err := json.Marshal(sampleDocument(status.Running))
	if err != nil {
		t.Fatalf("marshal Document: %v", err)
	}
	instance := decodeJSONObject(t, data)
	instance["unexpected_field"] = "guards-against-tag-drift"

	if err := schema.Validate(instance); err == nil {
		t.Fatal("Validate() error = nil, want an error for an unrecognized field (additionalProperties: false)")
	}
}
