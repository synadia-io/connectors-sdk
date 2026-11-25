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

// Package cfgtags parses the `json`, `schema` and `display` struct tags of a
// connector's config struct. It is the one grammar the spec generator
// (sdk-go/spec) and the config-document loader (sdk-go/config) share: the
// generator turns the tags into a config_schema (D-12, D-13), and the loader
// keys defaults (D-14) and its error paths off the same field names and
// defaults. Two parsers that drifted would make a schema describe one field
// while the loader defaulted another, so both import this package.
package cfgtags

import (
	"fmt"
	"reflect"
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

// IsEmpty reports whether no display hint was set, so a generator can omit
// the "x-display" key entirely rather than emitting an empty object.
func (d XDisplay) IsEmpty() bool {
	return d.Label == "" && d.Group == "" && d.Order == nil && d.Placeholder == "" &&
		!d.Hidden && !d.Secret && len(d.SecretParts) == 0 && len(d.VisibleWhen) == 0 &&
		d.CPType == "" && d.Code == nil
}

// ParsedTags is the decoded `schema` and `display` struct-tag information
// for one config field.
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

// JSONFieldName resolves field's config-document property name from its
// `json` tag, following encoding/json's own rules: a bare "-" tag skips the
// field entirely; an absent or empty name falls back to the Go field name.
//
// This is the name a config_schema advertises for the field, and the name
// the loader keys defaults and the D-4 secret guard's error paths off, so
// the generator and the loader both call it rather than keeping a copy.
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
