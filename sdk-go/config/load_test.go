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

package config_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/connectors-sdk/sdk-go/config"
)

// nestedGroup exercises recursion into a nested struct field (D-14 defaults
// must apply at every level, not just the top one).
type nestedGroup struct {
	Region string `json:"region" schema:"default=us-east-1"`
	Token  string `json:"token"`
}

// testConfig mirrors the shape a real connector config struct takes: plain
// fields, a defaulted field, a secret-tagged field, and a nested group.
type testConfig struct {
	Name     string      `json:"name"`
	Type     string      `json:"type" schema:"default=core"`
	Note     string      `json:"note" schema:"default=fallback"`
	Password string      `json:"password" display:"secret"`
	PlainRef string      `json:"plain_ref"`
	Group    nestedGroup `json:"group"`
}

func writeDoc(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write doc: %v", err)
	}
	return path
}

func setDocEnv(t *testing.T, path string) {
	t.Helper()
	t.Setenv("NEX_WORKLOAD_CONFIG_FILE", path)
}

func unsetDocEnv(t *testing.T) {
	t.Helper()
	// t.Setenv registers a cleanup that restores whatever the variable held
	// (including "unset") before this test touched it; the follow-up
	// os.Unsetenv gives us a guaranteed-unset value for the test body itself.
	t.Setenv("NEX_WORKLOAD_CONFIG_FILE", "placeholder")
	os.Unsetenv("NEX_WORKLOAD_CONFIG_FILE")
}

func TestLoad_NoDocument(t *testing.T) {
	unsetDocEnv(t)

	var cfg testConfig
	err := config.Load(&cfg)
	if !errors.Is(err, config.ErrNoDocument) {
		t.Fatalf("Load() error = %v, want ErrNoDocument", err)
	}
}

func TestPath_Unset(t *testing.T) {
	unsetDocEnv(t)

	if _, ok := config.Path(); ok {
		t.Fatal("Path() ok = true, want false when env unset")
	}
}

func TestLoad_ValidDocument(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{
		"name": "orders",
		"type": "premium",
		"note": "custom note",
		"password": "hunter2",
		"plain_ref": "not-a-secret",
		"group": {"region": "eu-west-1"}
	}`)
	setDocEnv(t, path)

	var cfg testConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := testConfig{
		Name:     "orders",
		Type:     "premium",
		Note:     "custom note",
		Password: "hunter2",
		PlainRef: "not-a-secret",
		Group:    nestedGroup{Region: "eu-west-1"},
	}
	if cfg != want {
		t.Fatalf("Load() = %+v, want %+v", cfg, want)
	}
}

func TestLoad_UnknownField(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{"name": "orders", "bogus_field": "x"}`)
	setDocEnv(t, path)

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want error naming the unknown field")
	}
	if !strings.Contains(err.Error(), "bogus_field") {
		t.Fatalf("Load() error = %v, want it to name bogus_field", err)
	}
}

func TestLoad_DefaultApplied(t *testing.T) {
	dir := t.TempDir()
	// "type" is absent entirely: D-14 says the loader applies its schema
	// default.
	path := writeDoc(t, dir, `{"name": "orders"}`)
	setDocEnv(t, path)

	var cfg testConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Type != "core" {
		t.Fatalf("cfg.Type = %q, want default %q", cfg.Type, "core")
	}
}

func TestLoad_NestedDefaultApplied(t *testing.T) {
	dir := t.TempDir()
	// "group" is absent entirely: recursion must still apply its child's
	// default (region) rather than leaving the nested struct's zero value.
	path := writeDoc(t, dir, `{"name": "orders"}`)
	setDocEnv(t, path)

	var cfg testConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Group.Region != "us-east-1" {
		t.Fatalf("cfg.Group.Region = %q, want default %q", cfg.Group.Region, "us-east-1")
	}
}

func TestLoad_EmptyStringIsAValueNotAbsence(t *testing.T) {
	dir := t.TempDir()
	// "note" is present as "" explicitly: D-14 says this is a value, the
	// default must NOT override it.
	path := writeDoc(t, dir, `{"name": "orders", "note": ""}`)
	setDocEnv(t, path)

	var cfg testConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Note != "" {
		t.Fatalf("cfg.Note = %q, want empty string preserved (not default)", cfg.Note)
	}
}

func TestLoad_UnreadableFile(t *testing.T) {
	dir := t.TempDir()
	setDocEnv(t, filepath.Join(dir, "does-not-exist.json"))

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want wrapped read error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() error = %v, want it to wrap os.ErrNotExist", err)
	}
}

// TestLoad_UnresolvedSecretGuard covers spec/config.md D-4: a field marked
// x-display secret:true whose value still carries a whole-value "secret://"
// prefix means node-side resolution never happened. That is fatal
// misconfiguration, not a value to pass through.
func TestLoad_UnresolvedSecretGuard(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{"name": "orders", "password": "secret://mongo/prod-password"}`)
	setDocEnv(t, path)

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want unresolved-secret error")
	}
	if !strings.Contains(err.Error(), "unresolved-secret") {
		t.Fatalf("Load() error = %v, want it to mention unresolved-secret", err)
	}
	if !strings.Contains(err.Error(), "password") {
		t.Fatalf("Load() error = %v, want it to name the field", err)
	}
	// The guard's error is the one config failure a caller has to tell apart
	// from all the others: spec/config.md requires it be reported as a park
	// with reason "unresolved-secret", while an unparseable document is
	// lifecycle.md's fail-stop. Matching on the message text would make that
	// dispatch depend on prose, so the sentinel is part of the contract.
	if !errors.Is(err, config.ErrUnresolvedSecret) {
		t.Errorf("Load() error = %v, want it to wrap config.ErrUnresolvedSecret", err)
	}
}

// TestLoad_OtherFailuresDoNotMatchTheSecretSentinel is the other half of
// that dispatch: a document that simply fails to parse must not be
// mistakable for D-4's park case.
func TestLoad_OtherFailuresDoNotMatchTheSecretSentinel(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{"unknown_field": 1}`)
	setDocEnv(t, path)

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want an unknown-field error")
	}
	if errors.Is(err, config.ErrUnresolvedSecret) {
		t.Errorf("Load() error = %v, want it NOT to match config.ErrUnresolvedSecret", err)
	}
}

// TestLoad_NonSecretFieldWithWholeValueSecretPrefixIsUnresolved pins down
// D-4: the "secret://" prefix is reserved document-wide
// because the nexlet's node-side resolver is schema-blind (no
// config_schema/x-display access at resolution time) and must scan every
// string field the same way. A field with NO display:"secret" tag is not
// exempt — a surviving whole-value "secret://" prefix there is exactly as
// fatal as in a tagged field.
func TestLoad_NonSecretFieldWithWholeValueSecretPrefixIsUnresolved(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{"name": "orders", "plain_ref": "secret://looks/like/one/and/is"}`)
	setDocEnv(t, path)

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want unresolved-secret error for the untagged field too")
	}
	if !strings.Contains(err.Error(), "unresolved-secret") {
		t.Fatalf("Load() error = %v, want it to mention unresolved-secret", err)
	}
	if !strings.Contains(err.Error(), "plain_ref") {
		t.Fatalf("Load() error = %v, want it to name plain_ref", err)
	}
}

// TestLoad_MidStringSecretPrefixPassesThrough covers the "whole-value only"
// half of D-4: the reservation is on the value's entire content starting
// with "secret://", not on containing the substring anywhere. A value that
// merely mentions the token mid-string is ordinary data.
func TestLoad_MidStringSecretPrefixPassesThrough(t *testing.T) {
	dir := t.TempDir()
	doc := `{"name": "orders", "plain_ref": "see secret://mongo/x for context, not a reference"}`
	path := writeDoc(t, dir, doc)
	setDocEnv(t, path)

	var cfg testConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v, want mid-string occurrence to pass through", err)
	}
	want := "see secret://mongo/x for context, not a reference"
	if cfg.PlainRef != want {
		t.Fatalf("cfg.PlainRef = %q, want %q preserved", cfg.PlainRef, want)
	}
}

// TestLoad_NestedUnresolvedSecretGuard proves the guard walks all nesting
// levels, not just top-level fields, and that the error names the field by
// its dotted path.
func TestLoad_NestedUnresolvedSecretGuard(t *testing.T) {
	dir := t.TempDir()
	path := writeDoc(t, dir, `{"name": "orders", "group": {"token": "secret://nested/ref"}}`)
	setDocEnv(t, path)

	var cfg testConfig
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("Load() error = nil, want unresolved-secret error for the nested field")
	}
	if !strings.Contains(err.Error(), "unresolved-secret") {
		t.Fatalf("Load() error = %v, want it to mention unresolved-secret", err)
	}
	if !strings.Contains(err.Error(), "group.token") {
		t.Fatalf("Load() error = %v, want it to name group.token", err)
	}
}

// dynamicConfig holds document values in the Go types encoding/json decodes
// without a schema: a connector that passes part of its document through
// untyped (an options bag, a driver-specific blob) still gets the guard.
type dynamicConfig struct {
	Name    string          `json:"name"`
	Any     any             `json:"any"`
	Options map[string]any  `json:"options"`
	Items   []any           `json:"items"`
	Raw     json.RawMessage `json:"raw"`
}

// TestLoad_UnresolvedSecretGuardScansUntypedValues pins that the guard reads
// the document, not the struct: a "secret://" value the struct receives as
// any, map[string]any, []any or json.RawMessage is what the node-side
// resolver would have rewritten, so it is reported like a string field, by
// its dotted path in the document.
func TestLoad_UnresolvedSecretGuardScansUntypedValues(t *testing.T) {
	tests := []struct {
		name, doc, wantPath string
	}{
		{"any field", `{"name": "n", "any": "secret://a"}`, `"any"`},
		{"map[string]any field", `{"name": "n", "options": {"uri": "secret://b"}}`, `"options.uri"`},
		{"[]any field", `{"name": "n", "items": ["ok", "secret://c"]}`, `"items[1]"`},
		{"RawMessage object", `{"name": "n", "raw": {"inner": {"pw": "secret://d"}}}`, `"raw.inner.pw"`},
		{
			"RawMessage array",
			`{"name": "n", "raw": {"list": [1, {"k": "secret://e"}]}}`,
			`"raw.list[1].k"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDocEnv(t, writeDoc(t, t.TempDir(), tt.doc))
			var cfg dynamicConfig
			err := config.Load(&cfg)
			if err == nil {
				t.Fatal("Load() error = nil, want unresolved-secret error")
			}
			if !errors.Is(err, config.ErrUnresolvedSecret) {
				t.Errorf("Load() error = %v, want it to wrap config.ErrUnresolvedSecret", err)
			}
			if !strings.Contains(err.Error(), tt.wantPath) {
				t.Errorf("Load() error = %v, want it to name %s", err, tt.wantPath)
			}
		})
	}
}

// typedConfig holds fields whose Go types cannot receive a "secret://"
// string: key material as []byte, a timestamp, a port.
type typedConfig struct {
	Name   string    `json:"name"`
	TLSKey []byte    `json:"tls_key"`
	Since  time.Time `json:"since"`
	Port   int       `json:"port"`
}

// TestLoad_UnresolvedSecretGuardRunsBeforeDecode pins that the guard runs on
// the document before the typed decode, so a leftover reference in a field
// whose type cannot hold the string ([]byte fails base64, time.Time fails
// its parse, int refuses a string) or in a field the struct does not have
// is still reported as unresolved-secret, which the runtime parks on, rather
// than as a decode error, which it exits on (spec/config.md: any field).
func TestLoad_UnresolvedSecretGuardRunsBeforeDecode(t *testing.T) {
	tests := []struct {
		name, doc, wantPath string
	}{
		{"[]byte field", `{"name": "n", "tls_key": "secret://tls-key"}`, `"tls_key"`},
		{"time.Time field", `{"name": "n", "since": "secret://ts"}`, `"since"`},
		{"int field", `{"name": "n", "port": "secret://port"}`, `"port"`},
		{"unknown field", `{"name": "n", "bogus": "secret://x"}`, `"bogus"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDocEnv(t, writeDoc(t, t.TempDir(), tt.doc))
			var cfg typedConfig
			err := config.Load(&cfg)
			if err == nil {
				t.Fatal("Load() error = nil, want unresolved-secret error")
			}
			if !errors.Is(err, config.ErrUnresolvedSecret) {
				t.Errorf("Load() error = %v, want it to wrap config.ErrUnresolvedSecret", err)
			}
			if !strings.Contains(err.Error(), tt.wantPath) {
				t.Errorf("Load() error = %v, want it to name %s", err, tt.wantPath)
			}
		})
	}
}

// TestLoad_UntypedMidStringSecretPrefixPassesThrough is the whole-value half
// for untyped values: a mid-string occurrence inside a RawMessage or a
// map[string]any is ordinary data, and the document loads.
func TestLoad_UntypedMidStringSecretPrefixPassesThrough(t *testing.T) {
	doc := `{"name": "n", "options": {"note": "x secret://y"}, "raw": ["see secret://z"], "any": 7}`
	setDocEnv(t, writeDoc(t, t.TempDir(), doc))
	var cfg dynamicConfig
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v, want mid-string occurrences to pass through", err)
	}
	if got := cfg.Options["note"]; got != "x secret://y" {
		t.Errorf("cfg.Options[note] = %q, want it preserved", got)
	}
}

// flagBoundConfig is a config struct the way connectors bind flags into it:
// flag.StringVar(&cfg.Field, ...) fills the fields before Load runs. LogLevel
// stands for a process-level knob the document can't carry (json:"-").
type flagBoundConfig struct {
	Subject  string       `json:"subject"`
	Greeting string       `json:"greeting" schema:"default=hello"`
	Extra    string       `json:"extra"`
	Group    nestedGroup  `json:"group"`
	Opt      *nestedGroup `json:"opt"`
	At       time.Time    `json:"at"`
	LogLevel string       `json:"-"`
}

// TestLoad_DocumentModeIgnoresFlagValues pins spec/config.md's loader
// precedence: with a config document present, the document is
// authoritative for every field it can represent. A field the document
// leaves out gets its schema default or its zero value (D-14), never the
// value a flag put there before Load ran. A field the document can't
// represent (json:"-") keeps its flag value.
func TestLoad_DocumentModeIgnoresFlagValues(t *testing.T) {
	dir := t.TempDir()
	setDocEnv(t, writeDoc(t, dir, `{"subject": "from-doc"}`))

	cfg := flagBoundConfig{
		Subject:  "from-flag",
		Greeting: "from-flag",
		Extra:    "from-flag",
		Group:    nestedGroup{Region: "from-flag", Token: "from-flag"},
		Opt:      &nestedGroup{Region: "from-flag"},
		At:       time.Unix(1, 0),
		LogLevel: "debug",
	}
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Subject != "from-doc" {
		t.Errorf("Subject = %q, want %q from the document", cfg.Subject, "from-doc")
	}
	if cfg.Greeting != "hello" {
		t.Errorf("Greeting = %q, want the schema default %q", cfg.Greeting, "hello")
	}
	if cfg.Extra != "" {
		t.Errorf("Extra = %q, want the zero value", cfg.Extra)
	}
	if cfg.Group.Region != "us-east-1" || cfg.Group.Token != "" {
		t.Errorf("Group = %+v, want the nested default region and an empty token", cfg.Group)
	}
	if cfg.Opt != nil {
		t.Errorf("Opt = %+v, want nil for an absent pointer group", cfg.Opt)
	}
	if !cfg.At.IsZero() {
		t.Errorf("At = %v, want the zero time", cfg.At)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want the flag value %q kept", cfg.LogLevel, "debug")
	}
}

// presetFlagBound returns a flagBoundConfig filled the way bound flags fill
// one before Load runs.
func presetFlagBound() flagBoundConfig {
	return flagBoundConfig{
		Subject:  "from-flag",
		Greeting: "from-flag",
		Extra:    "from-flag",
		Group:    nestedGroup{Region: "from-flag", Token: "from-flag"},
		Opt:      &nestedGroup{Region: "from-flag"},
		At:       time.Unix(1, 0),
		LogLevel: "debug",
	}
}

// TestLoad_FailedLoadLeavesConfigUntouched pins that Load changes dst only
// when the whole document loads: a document that fails to parse, fails
// validation or still holds an unresolved secret leaves every field, flag
// values included, as it was.
func TestLoad_FailedLoadLeavesConfigUntouched(t *testing.T) {
	for _, tt := range []struct{ name, doc string }{
		{"syntax error", `{"subject":`},
		{"not an object", `[]`},
		{"type mismatch on the first field", `{"subject": 5}`},
		{"type mismatch on a later field", `{"subject": "doc", "extra": 5}`},
		{"unknown field", `{"subject": "doc", "bogus": 1}`},
		{"trailing data", `{"subject": "doc"} {}`},
		{"unresolved secret", `{"subject": "doc", "extra": "secret://x"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setDocEnv(t, writeDoc(t, t.TempDir(), tt.doc))
			cfg, want := presetFlagBound(), presetFlagBound()
			if err := config.Load(&cfg); err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
			if !reflect.DeepEqual(cfg, want) {
				t.Errorf("after a failed Load:\n got %+v\nwant %+v (untouched)", cfg, want)
			}
		})
	}
}

// badDefaultConfig has a default its field's type can't hold, so Load fails
// after decoding, while it applies defaults.
type badDefaultConfig struct {
	Name  string `json:"name"`
	Count int    `json:"count" schema:"default=notanint"`
}

func TestLoad_FailedDefaultLeavesConfigUntouched(t *testing.T) {
	setDocEnv(t, writeDoc(t, t.TempDir(), `{"name": "doc"}`))
	cfg := badDefaultConfig{Name: "from-flag", Count: 7}
	if err := config.Load(&cfg); err == nil {
		t.Fatal("Load() error = nil, want the bad default's error")
	}
	if want := (badDefaultConfig{Name: "from-flag", Count: 7}); cfg != want {
		t.Errorf("after a failed Load: got %+v, want %+v (untouched)", cfg, want)
	}
}

func TestLoad_NoDocumentLeavesConfigUntouched(t *testing.T) {
	unsetDocEnv(t)
	cfg, want := presetFlagBound(), presetFlagBound()
	if err := config.Load(&cfg); !errors.Is(err, config.ErrNoDocument) {
		t.Fatalf("Load() error = %v, want ErrNoDocument", err)
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("flag-only mode changed cfg:\n got %+v\nwant %+v", cfg, want)
	}
}

type label string

// keepGroup is a nested group with a field the document can't represent.
type keepGroup struct {
	Region string `json:"region" schema:"default=us-east-1"`
	Local  string `json:"-"`
}

// everyShape has one field of each shape a config struct can hold, so the
// reset can be checked shape by shape.
type everyShape struct {
	Str      string            `json:"str"`
	Int      int               `json:"int"`
	Bool     bool              `json:"bool"`
	Float    float64           `json:"float"`
	IntPtr   *int              `json:"int_ptr"`
	StrPtrD  *string           `json:"str_ptr_d" schema:"default=dflt"`
	Slice    []string          `json:"slice"`
	Map      map[string]string `json:"map"`
	Array    [2]int            `json:"array"`
	Raw      json.RawMessage   `json:"raw"`
	Dur      time.Duration     `json:"dur"`
	At       time.Time         `json:"at"`
	Named    label             `json:"named"`
	Quoted   int               `json:"quoted,string"`
	Omit     string            `json:"omit,omitempty"`
	Untagged string
	Group    keepGroup  `json:"group"`
	GroupPtr *keepGroup `json:"group_ptr"`
	Local    string     `json:"-"`
	private  string
}

// TestLoad_ResetsEveryFieldShape loads an empty document into a struct whose
// every field was preset the way bound flags preset it. Every field the
// document can represent ends up at its default or its zero value; a field it
// can't represent keeps its value, inside a nested struct value too. A
// pointer group is reset whole, json:"-" fields under it included, because an
// absent pointer group is nil (D-14).
func TestLoad_ResetsEveryFieldShape(t *testing.T) {
	setDocEnv(t, writeDoc(t, t.TempDir(), `{}`))
	one, flag := 1, "from-flag"
	cfg := everyShape{
		Str: flag, Int: 1, Bool: true, Float: 1.5, IntPtr: &one, StrPtrD: &flag,
		Slice: []string{flag}, Map: map[string]string{"k": flag}, Array: [2]int{1, 2},
		Raw: json.RawMessage(`"from-flag"`), Dur: time.Second, At: time.Unix(1, 0),
		Named: label(flag), Quoted: 1, Omit: flag, Untagged: flag,
		Group:    keepGroup{Region: flag, Local: flag},
		GroupPtr: &keepGroup{Region: flag, Local: flag},
		Local:    flag, private: flag,
	}
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	dflt := "dflt"
	want := everyShape{
		StrPtrD: &dflt,
		Group:   keepGroup{Region: "us-east-1", Local: flag},
		Local:   flag, private: flag,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("after Load of {}:\n got %+v\nwant %+v", cfg, want)
	}
}
