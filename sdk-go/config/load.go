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

// Package config implements the config-document loader described in
// spec/config.md: discovery via NEX_WORKLOAD_CONFIG_FILE (D-2), default
// application for fields absent from the document (D-14), and the
// document-wide unresolved-secret guard (D-4: the "secret://" prefix is
// reserved document-wide because node-side resolution is schema-blind, so
// the connector-side guard must be too).
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
)

// configFileEnv is the single environment variable that puts a connector
// into config-document mode (D-2).
const configFileEnv = "NEX_WORKLOAD_CONFIG_FILE"

// secretPrefix is the whole-value secret reference token, reserved
// document-wide (D-4): a string field is a reference iff
// its entire value, from the start, is this prefix plus an id — never a
// substring mid-value. The nexlet resolver runs schema-blind (it has no
// access to config_schema/x-display at resolution time), so this reservation
// applies to every string field regardless of x-display.secret, which marks
// where a reference is expected.
const secretPrefix = "secret://"

// ErrNoDocument indicates NEX_WORKLOAD_CONFIG_FILE is unset: the connector is
// not in config-document mode for this run at all (D-2). Callers fall back
// to flags entirely — this is a mode switch, not a per-field merge (see
// "Loader precedence" in spec/config.md).
var ErrNoDocument = errors.New("config: NEX_WORKLOAD_CONFIG_FILE not set")

// ErrUnresolvedSecret is wrapped by every error the D-4 guard below reports,
// so a caller can tell this one failure apart from every other reason a
// document fails to load. It has to be distinguishable: spec/config.md
// requires an unresolved reference be reported as a park with reason
// "unresolved-secret" (the connector stays alive and answers its admin
// surface), while an unparseable document is lifecycle.md's fail-stop.
var ErrUnresolvedSecret = errors.New("unresolved-secret")

// Path returns the mounted config document path from NEX_WORKLOAD_CONFIG_FILE
// and whether it is set (D-2).
func Path() (string, bool) {
	return os.LookupEnv(configFileEnv)
}

// Load reads the mounted config document (D-2) into dst, a pointer to a
// struct, or returns ErrNoDocument if no document is mounted for this run.
//
// Unknown fields in the document are rejected (json.Decoder's
// DisallowUnknownFields, D-2). For every field absent from the document, Load
// applies the field's `schema:"default=..."` struct-tag value, if any (D-14);
// a field present with an empty/zero JSON value is left as decoded, since
// presence and value are orthogonal under D-14. Load recurses into nested
// struct fields so defaults and the guard below apply at every level.
//
// Load also enforces the D-4 unresolved-secret guard, document-wide and
// schema-blind: after decoding and defaulting, Load walks
// every string-typed value in dst, at every nesting level (nested structs,
// slices, arrays, maps), regardless of any `display:"secret"` tag, and if a
// value still carries the whole-value "secret://" prefix (the entire value,
// not a substring), Load returns an error identifying the field by its
// dotted json-name path. `display:"secret"` (D-13 x-display.secret) marks
// where a reference is expected; it does not scope the guard, because the nexlet's node-side resolver has
// no access to config_schema/x-display at resolution time and must scan
// every field the same way. The prefix is reserved document-wide: a literal
// value that happens to start with "secret://" is prohibited, not passed
// through — there is no escaping mechanism.
func Load(dst any) error {
	path, ok := Path()
	if !ok {
		return ErrNoDocument
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}

	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("config: Load requires a non-nil pointer to struct, got %T", dst)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}

	// A second pass decodes into raw messages so we can tell "absent" from
	// "present with a zero value" per field (D-14) — the first Decode above
	// can't distinguish the two once it has assigned Go zero values either
	// way.
	var present map[string]json.RawMessage
	if err := json.Unmarshal(data, &present); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}

	if err := applyStruct(v.Elem(), present); err != nil {
		return err
	}

	return scanForUnresolvedSecrets(v.Elem(), "")
}

// applyStruct walks v's exported fields, applying schema defaults to fields
// absent from present (D-14), recursing into nested struct fields with their
// own sub-document's present map. The D-4 secret guard is a separate,
// schema-blind pass (scanForUnresolvedSecrets) run once defaults are applied.
func applyStruct(v reflect.Value, present map[string]json.RawMessage) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue // unexported
		}
		name, skip := spec.JSONFieldName(field)
		if skip {
			continue
		}
		fv := v.Field(i)
		raw, isPresent := present[name]

		elemType := field.Type
		isPointer := elemType.Kind() == reflect.Pointer
		if isPointer {
			elemType = elemType.Elem()
		}

		if elemType.Kind() == reflect.Struct {
			target := fv
			if isPointer {
				if fv.IsNil() {
					// Absent pointer-to-struct group: D-14's zero value for
					// a pointer field is nil, so there is nothing to
					// recurse into or default.
					continue
				}
				target = fv.Elem()
			}
			var subPresent map[string]json.RawMessage
			if isPresent {
				// Decode already validated data as well-formed JSON; a
				// non-object value here (e.g. explicit null) simply yields
				// no sub-fields, so every child is treated as absent.
				_ = json.Unmarshal(raw, &subPresent)
			}
			if err := applyStruct(target, subPresent); err != nil {
				return err
			}
			continue
		}

		parsed, err := spec.ParseTags(field)
		if err != nil {
			return fmt.Errorf("config: field %q: %w", name, err)
		}

		if !isPresent && parsed.HasDefault {
			if err := applyDefault(fv, parsed.Default); err != nil {
				return fmt.Errorf("config: field %q: default: %w", name, err)
			}
		}
	}
	return nil
}

// applyDefault sets fv (allocating through one pointer level if needed) to
// raw parsed as fv's scalar kind, mirroring sdk-go/spec's coerceScalar so a
// `schema:"default=..."` tag means the same thing to both the spec generator
// and the loader.
func applyDefault(fv reflect.Value, raw string) error {
	if fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}
		fv = fv.Elem()
	}

	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("%q is not a bool: %w", raw, err)
		}
		fv.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not an integer: %w", raw, err)
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not an unsigned integer: %w", raw, err)
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("%q is not a number: %w", raw, err)
		}
		fv.SetFloat(f)
	default:
		return fmt.Errorf("default not supported for type %s", fv.Kind())
	}
	return nil
}

// scanForUnresolvedSecrets implements the D-4 unresolved-secret guard,
// document-wide and schema-blind: it walks every value
// reachable from v — structs, pointers, slices/arrays, maps — and reports an
// error naming path the first string whose entire value still carries the
// whole-value "secret://" prefix. It deliberately ignores struct tags
// entirely (no ParsedTags/Display lookup): the nexlet's node-side resolver
// has no schema access either, so the connector-side guard mirrors that same
// schema-blind scan rather than trusting x-display to narrow it.
func scanForUnresolvedSecrets(v reflect.Value, path string) error {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return scanForUnresolvedSecrets(v.Elem(), path)

	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue // unexported
			}
			name, skip := spec.JSONFieldName(field)
			if skip {
				continue
			}
			childPath := name
			if path != "" {
				childPath = path + "." + name
			}
			if err := scanForUnresolvedSecrets(v.Field(i), childPath); err != nil {
				return err
			}
		}
		return nil

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := scanForUnresolvedSecrets(v.Index(i), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		for _, k := range v.MapKeys() {
			childPath := fmt.Sprintf("%s[%v]", path, k.Interface())
			if err := scanForUnresolvedSecrets(v.MapIndex(k), childPath); err != nil {
				return err
			}
		}
		return nil

	case reflect.String:
		if strings.HasPrefix(v.String(), secretPrefix) {
			return fmt.Errorf(
				"config: field %q: %w: value still carries a %q reference; "+
					"node-side secret resolution did not happen (D-4)", path, ErrUnresolvedSecret, secretPrefix)
		}
		return nil

	default:
		return nil
	}
}
