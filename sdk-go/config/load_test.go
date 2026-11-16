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
	"errors"
	"os"
	"path/filepath"
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
