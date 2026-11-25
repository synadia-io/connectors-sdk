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

package spec

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/synadia-io/connectors-sdk/sdk-go/internal/cfgtags"
)

// FromStruct reflects over cfg's exported fields (a struct or pointer to
// struct) using its `json`, `schema`, and `display` tags to build a spec
// document's config_schema (D-12), then wraps it with the contract
// identifier (D-1) and caps into a Spec. Nested structs become nested JSON
// Schema objects and []T becomes an array of T's schema; a visible_when
// display hint is compiled into a derived allOf/if/then conditional
// requirement (D-13) — callers never hand-write one.
//
// The `json` tag names the property the way encoding/json does: a bare "-"
// skips the field, and an absent name falls back to the Go field name. The
// `schema` and `display` tags take comma-separated tokens:
//
//	schema:"required,default=none,enum=none|mapping,description=..."
//	display:"label=...,group=...,order=1,placeholder=...,hidden,secret,
//	         secret_parts=userinfo|query,cp-type=stream,code=bloblang,
//	         visible_when=fieldA:valueA|fieldB:valueB"
//
// A token without "=" is a bare boolean flag (required, hidden, secret). A
// list-valued token uses "|" as its separator (enum, secret_parts, and the
// multiple conditions of one visible_when). A key outside this grammar (the
// closed D-13 vocabulary for display) is an error, and so is a display tag
// setting both secret and secret_parts, which D-4 makes mutually exclusive.
// sdk-go/config reads the same tags, so a default means the same thing to
// the schema and to the loader.
func FromStruct(cfg any, caps []string) (*Spec, error) {
	if cfg == nil {
		return nil, fmt.Errorf("spec: FromStruct requires a struct or pointer to struct, got nil")
	}
	t := reflect.TypeOf(cfg)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("spec: FromStruct requires a struct or pointer to struct, got %s", t.Kind())
	}

	schema, err := structSchema(t, map[reflect.Type]bool{})
	if err != nil {
		return nil, err
	}
	schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"

	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("spec: marshal config_schema: %w", err)
	}

	return &Spec{
		Contract:     Contract,
		ConfigSchema: raw,
		Capabilities: caps,
	}, nil
}

// structSchema builds the JSON Schema object for t's exported fields.
//
// visited holds the struct types currently on the recursion path (added on
// entry, removed on return), so a self-referential type — type T struct{
// Next *T } or a []T of itself — is rejected with a clear error instead
// of recursing until the stack overflows and kills `spec` mode, which a
// launcher invokes. Two sibling fields of the same nested struct type are not a cycle:
// each is added and removed independently, so only a type that contains
// itself trips the guard.
func structSchema(t reflect.Type, visited map[reflect.Type]bool) (map[string]any, error) {
	if visited[t] {
		return nil, fmt.Errorf("spec: recursive type %s cannot be expressed as a finite inline JSON Schema", t)
	}
	visited[t] = true
	defer delete(visited, t)

	properties := map[string]any{}
	var required []string
	var allOf []any

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue // unexported
		}
		name, skip := cfgtags.JSONFieldName(field)
		if skip {
			continue
		}

		parsed, err := cfgtags.ParseTags(field)
		if err != nil {
			return nil, fmt.Errorf("spec: field %q: %w", name, err)
		}

		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		fieldSchema, err := typeSchema(fieldType, visited)
		if err != nil {
			return nil, fmt.Errorf("spec: field %q: %w", name, err)
		}

		if parsed.HasDefault {
			def, err := coerceScalar(parsed.Default, fieldType.Kind())
			if err != nil {
				return nil, fmt.Errorf("spec: field %q: default: %w", name, err)
			}
			fieldSchema["default"] = def
		}
		if len(parsed.Enum) > 0 {
			enum := make([]any, len(parsed.Enum))
			for idx, raw := range parsed.Enum {
				v, err := coerceScalar(raw, fieldType.Kind())
				if err != nil {
					return nil, fmt.Errorf("spec: field %q: enum: %w", name, err)
				}
				enum[idx] = v
			}
			fieldSchema["enum"] = enum
		}
		if parsed.Description != "" {
			fieldSchema["description"] = parsed.Description
		}
		if !parsed.Display.IsEmpty() {
			fieldSchema["x-display"] = parsed.Display
		}

		properties[name] = fieldSchema

		if parsed.Required {
			required = append(required, name)
		}
		if len(parsed.Display.VisibleWhen) > 0 {
			allOf = append(allOf, derivedCondition(name, parsed.Display.VisibleWhen))
		}
	}

	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	if len(allOf) > 0 {
		schema["allOf"] = allOf
	}
	return schema, nil
}

// typeSchema builds the JSON Schema fragment for a (already-dereferenced)
// Go type. visited carries the cycle guard through to structSchema and
// the slice-element recursion.
func typeSchema(t reflect.Type, visited map[reflect.Type]bool) (map[string]any, error) {
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, nil
	case reflect.Slice, reflect.Array:
		elemType := t.Elem()
		for elemType.Kind() == reflect.Pointer {
			elemType = elemType.Elem()
		}
		items, err := typeSchema(elemType, visited)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case reflect.Struct:
		return structSchema(t, visited)
	default:
		return nil, fmt.Errorf("unsupported type %s", t.Kind())
	}
}

// coerceScalar converts a raw tag-string value (from a `default=` or
// `enum=` token) into the Go value that marshals as the JSON type matching
// kind, so a schema's "default"/"enum" values carry the field's own type
// instead of always being strings.
func coerceScalar(raw string, kind reflect.Kind) (any, error) {
	switch kind {
	case reflect.String:
		return raw, nil
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not a bool: %w", raw, err)
		}
		return b, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer: %w", raw, err)
		}
		return n, nil
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number: %w", raw, err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("default/enum not supported for type %s", kind)
	}
}

// derivedCondition builds the allOf/if/then entry D-13 mandates for a
// visible_when hint: dependent is required only once every field named in
// conditions currently holds its required value (AND semantics, matching
// x-display.schema.json's description of visible_when). Condition keys are
// sorted so the output is deterministic regardless of map iteration order.
func derivedCondition(dependent string, conditions map[string]string) map[string]any {
	keys := make([]string, 0, len(conditions))
	for k := range conditions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ifProps := map[string]any{}
	ifRequired := make([]string, 0, len(keys))
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
			"required": []string{dependent},
		},
	}
}
