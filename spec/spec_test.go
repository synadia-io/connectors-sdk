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

// Package spec_test validates the contract's JSON documents (schemas and
// examples) against each other. It has no runtime dependents — sdk-go and
// the connectors parse these documents with the standard library; this
// module exists only to keep the schemas honest in CI.
package spec_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// compileSchema compiles a schema file relative to this package's directory.
func compileSchema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	sch, err := c.Compile(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return sch
}

// loadJSONObject reads a JSON document into a map, as spec.Validate expects.
func loadJSONObject(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	return m
}

// cloneObject deep-copies a decoded JSON object so mutations in one test
// case can't leak into another.
func cloneObject(m map[string]any) map[string]any {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

// compileInlineSchema compiles a schema value already decoded from JSON
// (e.g. the config_schema embedded in a spec document), rather than a file
// on disk.
func compileInlineSchema(t *testing.T, resourceName string, schemaDoc any) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(resourceName, schemaDoc); err != nil {
		t.Fatalf("add resource %s: %v", resourceName, err)
	}
	sch, err := c.Compile(resourceName)
	if err != nil {
		t.Fatalf("compile %s: %v", resourceName, err)
	}
	return sch
}

// errorHasKeywordAt reports whether err (or any of its causes) is a
// jsonschema.ValidationError whose failing keyword is exactly keyword and
// whose instance location contains locSegment (ignored if empty). This
// asserts *which* constraint and *where* it fired, not just that validation
// failed for some reason.
func errorHasKeywordAt(err error, keyword, locSegment string) bool {
	ve, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return false
	}
	return validationErrorHasKeywordAt(ve, keyword, locSegment)
}

func validationErrorHasKeywordAt(ve *jsonschema.ValidationError, keyword, locSegment string) bool {
	matched := false
	if keyword == "not" {
		// kind.Not.KeywordPath() returns nil (the library has no keyword
		// path for a bare schema-level "not"), so it's identified by type
		// instead of by KeywordPath entry.
		_, matched = ve.ErrorKind.(*kind.Not)
	} else {
		for _, kp := range ve.ErrorKind.KeywordPath() {
			if kp == keyword {
				matched = true
				break
			}
		}
	}
	if matched {
		if locSegment == "" {
			return true
		}
		for _, loc := range ve.InstanceLocation {
			if loc == locSegment {
				return true
			}
		}
	}
	for _, cause := range ve.Causes {
		if validationErrorHasKeywordAt(cause, keyword, locSegment) {
			return true
		}
	}
	return false
}

// TestManifestSchema validates manifest.schema.json (spec/manifest.md) against
// the mongodb example and hand-mutated variants covering each required-field
// and pattern constraint called out in the contract.
func TestManifestSchema(t *testing.T) {
	sch := compileSchema(t, "manifest.schema.json")
	golden := loadJSONObject(t, filepath.Join("examples", "manifest.mongodb.json"))

	tests := []struct {
		name    string
		mutate  func(m map[string]any)
		wantErr bool
	}{
		{
			name:    "valid example",
			mutate:  func(m map[string]any) {},
			wantErr: false,
		},
		{
			name: "tag-only image has no digest",
			mutate: func(m map[string]any) {
				m["image"] = "registry.example.com/connectors/mongodb:latest"
			},
			wantErr: true,
		},
		{
			name: "missing contract",
			mutate: func(m map[string]any) {
				delete(m, "contract")
			},
			wantErr: true,
		},
		{
			name: "wrong class",
			mutate: func(m map[string]any) {
				m["class"] = "database"
			},
			wantErr: true,
		},
		{
			name: "spec is a schemeless relative path",
			mutate: func(m map[string]any) {
				m["spec"] = "config.json"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := cloneObject(golden)
			tt.mutate(m)
			err := sch.Validate(m)
			if tt.wantErr && err == nil {
				t.Errorf("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

// TestXDisplaySchema validates x-display.schema.json (D-13's closed
// vocabulary) against valid hint objects and each way a hint object can
// violate the closed set.
func TestXDisplaySchema(t *testing.T) {
	sch := compileSchema(t, "x-display.schema.json")

	tests := []struct {
		name        string
		hints       map[string]any
		wantErr     bool
		wantKeyword string
		wantLoc     string
	}{
		{
			name: "valid label and group hints",
			hints: map[string]any{
				"label": "MongoDB URI",
				"group": "Connection",
				"order": 1,
			},
			wantErr: false,
		},
		{
			name: "valid secret_parts hint",
			hints: map[string]any{
				"secret_parts": []any{"userinfo"},
			},
			wantErr: false,
		},
		{
			name: "valid code hint",
			hints: map[string]any{
				"code": map[string]any{"language": "bloblang"},
			},
			wantErr: false,
		},
		{
			name: "unknown key multiline is outside the closed vocabulary",
			hints: map[string]any{
				"multiline": true,
			},
			wantErr:     true,
			wantKeyword: "additionalProperties",
		},
		{
			name: "secret_parts value outside the closed enum",
			hints: map[string]any{
				"secret_parts": []any{"password"},
			},
			wantErr:     true,
			wantKeyword: "enum",
			wantLoc:     "secret_parts",
		},
		{
			name: "secret and secret_parts are mutually exclusive",
			hints: map[string]any{
				"secret":       true,
				"secret_parts": []any{"userinfo"},
			},
			wantErr:     true,
			wantKeyword: "not",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sch.Validate(tt.hints)
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if tt.wantErr && !errorHasKeywordAt(err, tt.wantKeyword, tt.wantLoc) {
				t.Errorf("Validate() = %v, want failure at keyword %q loc %q", err, tt.wantKeyword, tt.wantLoc)
			}
		})
	}
}

// TestStatusSchema validates status.schema.json (D-5..D-7) against the
// parked example and hand-mutated variants covering the state enum, the
// D-7 no-percentage rule, and the required `since` field.
func TestStatusSchema(t *testing.T) {
	sch := compileSchema(t, "status.schema.json")
	golden := loadJSONObject(t, filepath.Join("examples", "status.parked.json"))

	tests := []struct {
		name        string
		mutate      func(m map[string]any)
		wantErr     bool
		wantKeyword string
		wantLoc     string
	}{
		{
			name:    "parked example with reason, counters, backfill cursor",
			mutate:  func(m map[string]any) {},
			wantErr: false,
		},
		{
			name: "uppercase state is not in the wire enum",
			mutate: func(m map[string]any) {
				m["state"] = "PARKED"
			},
			wantErr:     true,
			wantKeyword: "enum",
			wantLoc:     "state",
		},
		{
			name: "backfill percentage field does not exist (D-7)",
			mutate: func(m map[string]any) {
				cursor := m["cursor"].(map[string]any)
				backfill := cursor["backfill"].(map[string]any)
				backfill["pct"] = 50
			},
			wantErr:     true,
			wantKeyword: "additionalProperties",
			wantLoc:     "backfill",
		},
		{
			name: "missing since",
			mutate: func(m map[string]any) {
				delete(m, "since")
			},
			wantErr:     true,
			wantKeyword: "required",
		},
		{
			name: "negative backfill.copied is absurd (a count has no negative value)",
			mutate: func(m map[string]any) {
				cursor := m["cursor"].(map[string]any)
				backfill := cursor["backfill"].(map[string]any)
				backfill["copied"] = -1
			},
			wantErr:     true,
			wantKeyword: "minimum",
			wantLoc:     "copied",
		},
		{
			name: "negative cursor.behind_ms is absurd (a connector cannot be ahead of its source)",
			mutate: func(m map[string]any) {
				cursor := m["cursor"].(map[string]any)
				cursor["behind_ms"] = -1000
			},
			wantErr:     true,
			wantKeyword: "minimum",
			wantLoc:     "behind_ms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := cloneObject(golden)
			tt.mutate(m)
			err := sch.Validate(m)
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if tt.wantErr && !errorHasKeywordAt(err, tt.wantKeyword, tt.wantLoc) {
				t.Errorf("Validate() = %v, want failure at keyword %q loc %q", err, tt.wantKeyword, tt.wantLoc)
			}
		})
	}
}

// TestExampleSpecConfigPair validates that examples/config.mongodb.json (a
// config document, D-2) satisfies the config_schema embedded in
// examples/spec.mongodb.json (a spec document, D-12), and that an unknown
// field in the config document is rejected — config documents have no
// envelope and no tolerance for unrecognized fields (D-2).
func TestExampleSpecConfigPair(t *testing.T) {
	specDoc := loadJSONObject(t, filepath.Join("examples", "spec.mongodb.json"))
	configSchemaDoc, ok := specDoc["config_schema"]
	if !ok {
		t.Fatalf("spec.mongodb.json: missing config_schema")
	}
	sch := compileInlineSchema(t, "mongodb-config-schema.json", configSchemaDoc)
	golden := loadJSONObject(t, filepath.Join("examples", "config.mongodb.json"))

	tests := []struct {
		name        string
		mutate      func(m map[string]any)
		wantErr     bool
		wantKeyword string
	}{
		{
			name:    "valid config",
			mutate:  func(m map[string]any) {},
			wantErr: false,
		},
		{
			name: "unknown field is rejected",
			mutate: func(m map[string]any) {
				m["extra_field"] = "not part of the schema"
			},
			wantErr:     true,
			wantKeyword: "additionalProperties",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := cloneObject(golden)
			tt.mutate(m)
			err := sch.Validate(m)
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if tt.wantErr && !errorHasKeywordAt(err, tt.wantKeyword, "") {
				t.Errorf("Validate() = %v, want failure at keyword %q", err, tt.wantKeyword)
			}
		})
	}
}

// derivedAllOfEntry builds the D-13 allOf/if/then shape a visible_when hint
// compiles to (see sdk-go/spec's generator, which produces exactly
// this shape), so this test can assert a hand-authored example config_schema
// hasn't drifted from what a generator would emit for the same hint.
func derivedAllOfEntry(dependent string, conditions map[string]any) map[string]any {
	keys := make([]string, 0, len(conditions))
	for k := range conditions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ifProps := map[string]any{}
	ifRequired := make([]any, 0, len(keys))
	for _, k := range keys {
		ifProps[k] = map[string]any{"const": conditions[k]}
		ifRequired = append(ifRequired, k)
	}

	return map[string]any{
		"if": map[string]any{
			"properties": ifProps,
			"required":   ifRequired,
		},
		"then": map[string]any{
			"required": []any{dependent},
		},
	}
}

// checkVisibleWhenHasDerivedAllOf recursively walks a config_schema (or any
// nested object schema within it, following "properties" and array "items")
// and fails if a property carrying an x-display.visible_when hint has no
// matching derived allOf/if/then entry at the SAME schema level. D-13 says
// conditional requirements are DERIVED from visible_when — schema authors
// never hand-write the allOf, but a published example's config_schema must
// still carry the derived result, matching what a generator produces.
func checkVisibleWhenHasDerivedAllOf(t *testing.T, path string, node map[string]any) {
	t.Helper()
	props, _ := node["properties"].(map[string]any)
	allOf, _ := node["allOf"].([]any)

	for name, v := range props {
		propSchema, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if xd, ok := propSchema["x-display"].(map[string]any); ok {
			if vw, ok := xd["visible_when"].(map[string]any); ok && len(vw) > 0 {
				want := derivedAllOfEntry(name, vw)
				found := false
				for _, entry := range allOf {
					if reflect.DeepEqual(entry, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s: property %q has visible_when %v with no matching derived allOf entry %v (got allOf=%v)",
						path, name, vw, want, allOf)
				}
			}
		}
		if items, ok := propSchema["items"].(map[string]any); ok {
			checkVisibleWhenHasDerivedAllOf(t, path+"."+name+"[]", items)
		}
		checkVisibleWhenHasDerivedAllOf(t, path+"."+name, propSchema)
	}
}

// TestExampleSpecsVisibleWhenHasDerivedAllOf enforces D-13's derivation rule
// against every examples/spec.*.json document: a visible_when hint's
// conditional requirement must be present in config_schema as a derived
// allOf/if/then entry, not left implicit. This is the class of drift that
// let spec.mongodb.json's transformer_mapping visible_when ship without its
// allOf.
func TestExampleSpecsVisibleWhenHasDerivedAllOf(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join("examples", "spec.*.json"))
	if err != nil {
		t.Fatalf("glob examples/spec.*.json: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("no examples/spec.*.json files found")
	}
	for _, path := range matches {
		specDoc := loadJSONObject(t, path)
		configSchema, ok := specDoc["config_schema"].(map[string]any)
		if !ok {
			t.Fatalf("%s: missing config_schema", path)
		}
		checkVisibleWhenHasDerivedAllOf(t, path, configSchema)
	}
}
