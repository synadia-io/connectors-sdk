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

package spec_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
)

// sampleCfg mirrors spec/examples/spec.mongodb.json's config_schema field for
// field, to exercise every tag feature FromStruct supports: a secret_parts
// display hint on a composite value (mongo_uri), a whole-value secret
// (mongo_password), a plain required string (database), a string array
// (collections), a cp-type hint (js_stream), an enum with a default
// (transformer_type), and a code-editor hint combined with visible_when
// (transformer_mapping) whose derived allOf/if/then this test asserts
// explicitly (D-13).
type sampleCfg struct {
	MongoURI      string   `json:"mongo_uri" schema:"required" display:"label=MongoDB URI,group=Connection,order=1,secret_parts=userinfo"`
	MongoPassword string   `json:"mongo_password" schema:"required" display:"label=MongoDB Password,group=Connection,order=2,secret"`
	Database      string   `json:"database" schema:"required" display:"label=Database,group=Connection,order=3"`
	Collections   []string `json:"collections" schema:"required" display:"label=Collections,group=Connection,order=4"`
	JSStream      string   `json:"js_stream" schema:"required" display:"label=Output stream,group=Output,order=1,cp-type=stream"`

	TransformerType    string `json:"transformer_type" schema:"default=none,enum=none|mapping" display:"label=Transformer,group=Transform,order=1"`
	TransformerMapping string `json:"transformer_mapping" schema:"default=" display:"label=Mapping,group=Transform,order=2,code=bloblang,visible_when=transformer_type:mapping"`
}

var sampleCaps = []string{"check", "status", "park", "transform:mapping"}

func TestFromStruct_Golden(t *testing.T) {
	got, err := spec.FromStruct(sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct() error = %v", err)
	}

	gotJSON, err := got.JSON()
	if err != nil {
		t.Fatalf("Spec.JSON() error = %v", err)
	}
	gotDoc := decodeJSONObject(t, gotJSON)

	wantBytes, err := os.ReadFile(filepath.Join("testdata", "gen_test_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	wantDoc := decodeJSONObject(t, wantBytes)

	if !reflect.DeepEqual(gotDoc, wantDoc) {
		t.Errorf("FromStruct() output does not match golden.\ngot:  %s\nwant: %s", gotJSON, wantBytes)
	}
}

func TestFromStruct_ValidSchema2020_12(t *testing.T) {
	got, err := spec.FromStruct(sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct() error = %v", err)
	}
	schemaDoc := decodeJSONObject(t, got.ConfigSchema)
	compileInlineSchema(t, "sample-config-schema.json", schemaDoc)
}

func TestFromStruct_XDisplayValidatesAgainstSchema(t *testing.T) {
	got, err := spec.FromStruct(sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct() error = %v", err)
	}
	schemaDoc := decodeJSONObject(t, got.ConfigSchema)

	xdisplaySchema := compileSchemaFile(t, filepath.Join("..", "..", "spec", "x-display.schema.json"))

	hints := collectXDisplays(schemaDoc)
	if len(hints) == 0 {
		t.Fatal("collectXDisplays() = 0 hints, want at least one (every field in sampleCfg carries display tags)")
	}
	for _, h := range hints {
		if err := xdisplaySchema.Validate(h); err != nil {
			t.Errorf("x-display %v failed x-display.schema.json: %v", h, err)
		}
	}
}

func TestFromStruct_DerivesVisibleWhenAllOf(t *testing.T) {
	got, err := spec.FromStruct(sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct() error = %v", err)
	}
	schemaDoc := decodeJSONObject(t, got.ConfigSchema)

	allOf, ok := schemaDoc["allOf"].([]any)
	if !ok || len(allOf) == 0 {
		t.Fatalf("config_schema[\"allOf\"] = %v, want a derived entry for visible_when (D-13)", schemaDoc["allOf"])
	}

	want := map[string]any{
		"if": map[string]any{
			"properties": map[string]any{
				"transformer_type": map[string]any{"const": "mapping"},
			},
			"required": []any{"transformer_type"},
		},
		"then": map[string]any{
			"required": []any{"transformer_mapping"},
		},
	}
	found := false
	for _, entry := range allOf {
		if reflect.DeepEqual(entry, want) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("allOf = %v, want an entry matching %v", allOf, want)
	}
}

func TestFromStruct_PointerInputMatchesValue(t *testing.T) {
	fromValue, err := spec.FromStruct(sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct(value) error = %v", err)
	}
	fromPointer, err := spec.FromStruct(&sampleCfg{}, sampleCaps)
	if err != nil {
		t.Fatalf("FromStruct(pointer) error = %v", err)
	}
	if !reflect.DeepEqual(decodeJSONObject(t, fromValue.ConfigSchema), decodeJSONObject(t, fromPointer.ConfigSchema)) {
		t.Errorf("FromStruct(pointer) config_schema differs from FromStruct(value)")
	}
}

// nestedAuth and nestedCfg exercise typeSchema's reflect.Struct case
// (nested object emission), required propagation at each nesting level
// independently, and x-display hints on a nested field — none of which
// sampleCfg exercises, since none of its fields are structs.
type nestedAuth struct {
	Username string `json:"username" schema:"required" display:"label=Username,group=Auth"`
	Token    string `json:"token" display:"secret"`
}

type nestedCfg struct {
	Name string     `json:"name" schema:"required" display:"label=Name"`
	Auth nestedAuth `json:"auth" schema:"required"`
}

func TestFromStruct_NestedStruct(t *testing.T) {
	got, err := spec.FromStruct(nestedCfg{}, nil)
	if err != nil {
		t.Fatalf("FromStruct() error = %v", err)
	}
	schemaDoc := decodeJSONObject(t, got.ConfigSchema)

	// Top-level required: "auth" itself (schema:"required" on the Auth
	// field) lands in the PARENT's required array.
	topRequired, _ := schemaDoc["required"].([]any)
	if !containsString(topRequired, "auth") {
		t.Errorf("top-level required = %v, want it to contain %q", topRequired, "auth")
	}

	props, _ := schemaDoc["properties"].(map[string]any)
	authSchema, ok := props["auth"].(map[string]any)
	if !ok {
		t.Fatalf("properties[\"auth\"] missing or not an object: %v", props["auth"])
	}
	if authSchema["type"] != "object" {
		t.Errorf("auth.type = %v, want %q (nested struct -> nested object)", authSchema["type"], "object")
	}

	authProps, ok := authSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("auth.properties missing or not an object: %v", authSchema["properties"])
	}
	usernameSchema, ok := authProps["username"].(map[string]any)
	if !ok {
		t.Fatalf("auth.properties[\"username\"] missing or not an object: %v", authProps["username"])
	}
	tokenSchema, ok := authProps["token"].(map[string]any)
	if !ok {
		t.Fatalf("auth.properties[\"token\"] missing or not an object: %v", authProps["token"])
	}

	// Nested required propagation: "username" (schema:"required" inside
	// nestedAuth) lands in auth's OWN required array, independent of the
	// top-level required array checked above; "token" (no schema tag) does
	// not.
	authRequired, _ := authSchema["required"].([]any)
	if !containsString(authRequired, "username") {
		t.Errorf("auth.required = %v, want it to contain %q", authRequired, "username")
	}
	if containsString(authRequired, "token") {
		t.Errorf("auth.required = %v, want it NOT to contain %q (no schema:\"required\" tag)", authRequired, "token")
	}

	// x-display hints on nested fields.
	usernameXD, ok := usernameSchema["x-display"].(map[string]any)
	if !ok {
		t.Fatalf("auth.properties[\"username\"][\"x-display\"] missing: %v", usernameSchema)
	}
	if usernameXD["label"] != "Username" || usernameXD["group"] != "Auth" {
		t.Errorf("nested x-display = %v, want label=Username group=Auth", usernameXD)
	}
	tokenXD, ok := tokenSchema["x-display"].(map[string]any)
	if !ok {
		t.Fatalf("auth.properties[\"token\"][\"x-display\"] missing: %v", tokenSchema)
	}
	if tokenXD["secret"] != true {
		t.Errorf("nested token x-display = %v, want secret=true", tokenXD)
	}

	// The nested object schema must itself be valid JSON Schema, and its
	// x-display hints must validate against the closed vocabulary, same as
	// top-level ones.
	compileInlineSchema(t, "nested-config-schema.json", schemaDoc)
	xdisplaySchema := compileSchemaFile(t, filepath.Join("..", "..", "spec", "x-display.schema.json"))
	for _, h := range collectXDisplays(schemaDoc) {
		if err := xdisplaySchema.Validate(h); err != nil {
			t.Errorf("nested x-display %v failed x-display.schema.json: %v", h, err)
		}
	}
}

func TestFromStruct_RejectsNonStruct(t *testing.T) {
	if _, err := spec.FromStruct("not a struct", nil); err == nil {
		t.Fatal("FromStruct(string) error = nil, want error")
	}
	if _, err := spec.FromStruct(nil, nil); err == nil {
		t.Fatal("FromStruct(nil) error = nil, want error")
	}
}

func TestFromStruct_SecretAndSecretPartsConflict(t *testing.T) {
	type badCfg struct {
		Foo string `json:"foo" display:"secret,secret_parts=userinfo"`
	}
	_, err := spec.FromStruct(badCfg{}, nil)
	if err == nil {
		t.Fatal("FromStruct() error = nil, want error for secret+secret_parts conflict (D-4)")
	}
	if !strings.Contains(err.Error(), "secret_parts") {
		t.Errorf("error = %v, want it to mention secret_parts", err)
	}
}

func TestFromStruct_UnsupportedFieldType(t *testing.T) {
	type badCfg struct {
		Handler chan int `json:"handler"`
	}
	_, err := spec.FromStruct(badCfg{}, nil)
	if err == nil {
		t.Fatal("FromStruct() error = nil, want error for unsupported field type")
	}
	if !strings.Contains(err.Error(), "handler") {
		t.Errorf("error = %v, want it to name the field %q", err, "handler")
	}
}

// TestFromStruct_RejectsSelfReferentialStruct is the cycle guard: a struct that
// contains itself — directly through a pointer, or through a slice of itself
// — cannot be expressed as a finite inline JSON Schema. Before the cycle
// guard the generator recursed until the stack overflowed, a fatal crash of
// `spec` mode (which a launcher invokes), rather than returning an error.
func TestFromStruct_RejectsSelfReferentialStruct(t *testing.T) {
	t.Run("pointer cycle", func(t *testing.T) {
		type Node struct {
			Next *Node `json:"next"`
		}
		if _, err := spec.FromStruct(Node{}, nil); err == nil {
			t.Fatal("FromStruct() error = nil, want an error for a self-referential struct")
		}
	})
	t.Run("slice cycle", func(t *testing.T) {
		type Tree struct {
			Children []Tree `json:"children"`
		}
		if _, err := spec.FromStruct(Tree{}, nil); err == nil {
			t.Fatal("FromStruct() error = nil, want an error for a struct containing a slice of itself")
		}
	})
}

// TestFromStruct_SiblingFieldsSameTypeNotACycle guards the cycle guard
// against over-rejecting: two fields of the same nested struct type are each
// a separate finite subtree, not a recursion, and must still generate.
func TestFromStruct_SiblingFieldsSameTypeNotACycle(t *testing.T) {
	type Endpoint struct {
		Host string `json:"host"`
	}
	type Config struct {
		Primary   Endpoint `json:"primary"`
		Secondary Endpoint `json:"secondary"`
	}
	if _, err := spec.FromStruct(Config{}, nil); err != nil {
		t.Fatalf("FromStruct() error = %v, want nil for sibling fields of the same nested type", err)
	}
}

// --- test helpers ---

func containsString(list []any, want string) bool {
	for _, v := range list {
		if s, ok := v.(string); ok && s == want {
			return true
		}
	}
	return false
}

func decodeJSONObject(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal JSON object: %v", err)
	}
	return m
}

func compileSchemaFile(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	sch, err := c.Compile(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return sch
}

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

// collectXDisplays walks a config_schema's properties (recursing into
// nested objects and array items) and returns every "x-display" hint object
// it finds.
func collectXDisplays(node map[string]any) []map[string]any {
	var out []map[string]any
	props, _ := node["properties"].(map[string]any)
	for _, v := range props {
		propSchema, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if xd, ok := propSchema["x-display"].(map[string]any); ok {
			out = append(out, xd)
		}
		if items, ok := propSchema["items"].(map[string]any); ok {
			out = append(out, collectXDisplays(items)...)
		}
		out = append(out, collectXDisplays(propSchema)...)
	}
	return out
}
