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
	"strings"
)

// XDisplay is the x-display presentation-hint object (D-13,
// x-display.schema.json). Its json tags are authoritative for the emitted
// vocabulary: marshaling an XDisplay value directly produces a conforming
// hint object.
type XDisplay struct {
	Label       string            `json:"label,omitempty"`
	Group       string            `json:"group,omitempty"`
	Order       *int              `json:"order,omitempty"`
	Placeholder string            `json:"placeholder,omitempty"`
	Hidden      bool              `json:"hidden,omitempty"`
	Secret      bool              `json:"secret,omitempty"`
	SecretParts []string          `json:"secret_parts,omitempty"`
	VisibleWhen map[string]string `json:"visible_when,omitempty"`
	CPType      string            `json:"cp-type,omitempty"`
	Code        *CodeHint         `json:"code,omitempty"`
}

// CodeHint is the x-display "code" hint: the field renders as a multiline
// code editor using this syntax-highlighting language id.
type CodeHint struct {
	Language string `json:"language"`
}

// isEmpty reports whether no display hint was set, so FromStruct can omit
// the "x-display" key entirely rather than emitting an empty object.
func (d XDisplay) isEmpty() bool {
	return d.Label == "" && d.Group == "" && d.Order == nil && d.Placeholder == "" &&
		!d.Hidden && !d.Secret && len(d.SecretParts) == 0 && len(d.VisibleWhen) == 0 &&
		d.CPType == "" && d.Code == nil
}

// ParsedTags is the decoded `schema` and `display` struct-tag information
// for one config field. sdk-go/config reuses ParseTags to apply
// Default when a field is absent from the config document (D-14).
type ParsedTags struct {
	Required    bool
	Default     string
	HasDefault  bool
	Enum        []string
	Description string
	Display     XDisplay
}

// ParseTags decodes field's `schema` and `display` struct tags. Grammar:
//
//	schema:"required,default=none,enum=none|mapping,description=..."
//	display:"label=...,group=...,order=1,placeholder=...,hidden,secret,
//	         secret_parts=userinfo|query,cp-type=stream,code=bloblang,
//	         visible_when=fieldA:valueA|fieldB:valueB"
//
// Tokens are comma-separated within each tag; a token without "=" is a bare
// boolean flag (required, hidden, secret). A list-valued token uses "|" as
// its separator (enum, secret_parts, and the multiple conditions of one
// visible_when). ParseTags rejects a key outside the grammar above (the
// closed D-13 vocabulary for display) and a display tag setting both secret
// and secret_parts, which D-4 makes mutually exclusive.
func ParseTags(field reflect.StructField) (ParsedTags, error) {
	var pt ParsedTags

	for _, tok := range splitTag(field.Tag.Get("schema")) {
		key, value, hasValue := strings.Cut(tok, "=")
		switch key {
		case "required":
			pt.Required = true
		case "default":
			pt.Default = value
			pt.HasDefault = true
		case "enum":
			if hasValue && value != "" {
				pt.Enum = strings.Split(value, "|")
			}
		case "description":
			pt.Description = value
		default:
			return pt, fmt.Errorf("schema tag: unknown key %q", key)
		}
	}

	for _, tok := range splitTag(field.Tag.Get("display")) {
		key, value, hasValue := strings.Cut(tok, "=")
		switch key {
		case "label":
			pt.Display.Label = value
		case "group":
			pt.Display.Group = value
		case "order":
			n, err := strconv.Atoi(value)
			if err != nil {
				return pt, fmt.Errorf("display tag: order: %w", err)
			}
			pt.Display.Order = &n
		case "placeholder":
			pt.Display.Placeholder = value
		case "hidden":
			pt.Display.Hidden = !hasValue || value != "false"
		case "secret":
			pt.Display.Secret = !hasValue || value != "false"
		case "secret_parts":
			if value != "" {
				pt.Display.SecretParts = strings.Split(value, "|")
			}
		case "cp-type":
			pt.Display.CPType = value
		case "code":
			pt.Display.Code = &CodeHint{Language: value}
		case "visible_when":
			cond, err := parseVisibleWhen(value)
			if err != nil {
				return pt, fmt.Errorf("display tag: visible_when: %w", err)
			}
			pt.Display.VisibleWhen = cond
		default:
			return pt, fmt.Errorf("display tag: unknown key %q", key)
		}
	}

	if pt.Display.Secret && len(pt.Display.SecretParts) > 0 {
		return pt, fmt.Errorf("display tag: secret and secret_parts are mutually exclusive (D-4)")
	}

	return pt, nil
}

// splitTag splits a comma-separated struct-tag value into non-empty tokens.
func splitTag(tag string) []string {
	if tag == "" {
		return nil
	}
	parts := strings.Split(tag, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseVisibleWhen decodes "fieldA:valueA|fieldB:valueB" into the AND
// conditions a rendered form (and the derived allOf, D-13) require.
func parseVisibleWhen(raw string) (map[string]string, error) {
	cond := map[string]string{}
	for _, pair := range strings.Split(raw, "|") {
		field, value, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("expected field:value, got %q", pair)
		}
		cond[field] = value
	}
	return cond, nil
}

// FromStruct reflects over cfg's exported fields (a struct or pointer to
// struct) using its `json`, `schema`, and `display` tags to build a spec
// document's config_schema (D-12), then wraps it with the contract
// identifier (D-1) and caps into a Spec. Nested structs become nested JSON
// Schema objects and []T becomes an array of T's schema; a visible_when
// display hint is compiled into a derived allOf/if/then conditional
// requirement (D-13) — callers never hand-write one.
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
		name, skip := JSONFieldName(field)
		if skip {
			continue
		}

		parsed, err := ParseTags(field)
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
		if !parsed.Display.isEmpty() {
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

// JSONFieldName resolves field's config-document property name from its
// `json` tag, following encoding/json's own rules: a bare "-" tag skips the
// field entirely; an absent or empty name falls back to the Go field name.
//
// It is exported because the generator and sdk-go/config's loader have to
// agree on it exactly: this function decides the property name a
// config_schema advertises, and the loader keys defaults and the D-4 secret
// guard's error paths off the same name. Two copies that drift would make a
// schema describe one field while the loader defaults another.
func JSONFieldName(field reflect.StructField) (name string, skip bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", true
	}
	name, _, _ = strings.Cut(tag, ",")
	if name == "" {
		name = field.Name
	}
	return name, false
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
