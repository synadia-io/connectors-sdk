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

// Package specschema compiles the contract's JSON Schemas (spec/*.schema.json
// in this repo) for conformance tests, so every suite validates against the
// same normative files spec/spec_test.go does. It reads spec/ from the
// directory CONFORMANCE_SPEC_DIR names, or else finds it from its own source
// location, which works in this repo and from a repo that has this one as a
// submodule.
package specschema

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Dir is the spec/ directory the schemas are read from: CONFORMANCE_SPEC_DIR
// when set, else spec/ relative to this source file. The fallback does not
// exist when this package comes from the module cache, because spec/ is a
// separate module, so a suite run that way sets CONFORMANCE_SPEC_DIR.
func Dir() string {
	if dir := os.Getenv("CONFORMANCE_SPEC_DIR"); dir != "" {
		return dir
	}
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "spec")
}

// Compile compiles the schema file relPath under spec/, such as
// "status.schema.json", with format assertions on.
func Compile(t testing.TB, relPath string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	sch, err := c.Compile(filepath.Join(Dir(), relPath))
	if err != nil {
		t.Fatalf("compile %s: %v", relPath, err)
	}
	return sch
}

// CompileInline compiles a schema value already decoded from JSON, such as a
// spec document's embedded config_schema, registered under name.
func CompileInline(t testing.TB, name string, doc any) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(name, doc); err != nil {
		t.Fatalf("add resource %s: %v", name, err)
	}
	sch, err := c.Compile(name)
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return sch
}
