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

package bloblang

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// compile is the test helper every semantics test uses.
func compile(t *testing.T, mapping string) transform.Func {
	t.Helper()
	f, err := New().Compile(cfgJSON(t, mapping))
	if err != nil {
		t.Fatalf("compile %q: %v", mapping, err)
	}
	return f
}

func cfgJSON(t *testing.T, mapping string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]string{"mapping": mapping})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCompileValid(t *testing.T) {
	if compile(t, "root = this") == nil {
		t.Fatal("nil Func")
	}
}

func TestCompileSyntaxErrorHasPosition(t *testing.T) {
	_, err := New().Compile(cfgJSON(t, "root = = this"))
	if err == nil {
		t.Fatal("expected parse error")
	}
	// benthos ParseError text carries line/column ("line 1 char N")
	if !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("error has no position: %v", err)
	}
}

func TestCompileUnknownConfigField(t *testing.T) {
	_, err := New().Compile([]byte(`{"maping": "root = this"}`))
	if err == nil || !strings.Contains(err.Error(), "maping") {
		t.Fatalf("typo'd field must fail loudly, got: %v", err)
	}
}

func TestCompileEmptyMapping(t *testing.T) {
	if _, err := New().Compile(cfgJSON(t, "")); err == nil {
		t.Fatal("empty mapping must fail at compile")
	}
}
