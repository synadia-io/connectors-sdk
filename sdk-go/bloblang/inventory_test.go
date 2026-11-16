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
	"sort"
	"testing"

	blobl "github.com/redpanda-data/benthos/v4/public/bloblang"
)

// TestInventoryReviewed fails when a benthos upgrade adds ANY function or
// method we have not reviewed — the fence against silently reintroduced
// nondeterminism or IO. On failure: review the new names against the
// spec's determinism/purity rules, then either add them to the fixture
// (safe) or to deniedFunctions (unsafe).
func TestInventoryReviewed(t *testing.T) {
	var funcs, methods []string
	env := New().env
	env.WalkFunctions(func(name string, _ *blobl.FunctionView) { funcs = append(funcs, name) })
	env.WalkMethods(func(name string, _ *blobl.MethodView) { methods = append(methods, name) })
	sort.Strings(funcs)
	sort.Strings(methods)
	assertSubsetOfReviewed(t, "function", funcs, reviewedFunctions)
	assertSubsetOfReviewed(t, "method", methods, reviewedMethods)
}

func assertSubsetOfReviewed(t *testing.T, kind string, got, reviewed []string) {
	t.Helper()
	ok := map[string]bool{}
	for _, n := range reviewed {
		ok[n] = true
	}
	for _, n := range got {
		if !ok[n] {
			t.Errorf("unreviewed %s %q — review it, then add to the fixture or to deniedFunctions", kind, n)
		}
	}
}

// reviewedFunctions and reviewedMethods are the full names surviving
// New()'s hardened environment (OnlyPure + deniedFunctions +
// WithoutMethods(deniedMethods) + WithDisabledImports), as of benthos
// v4.77.0. Every name here was checked against internal/bloblang/query
// source for determinism and purity:
//   - hash/encrypt_aes/decrypt_aes/uuid_v5 take their key/iv/namespace as
//     explicit args — deterministic given input, not randomly seeded.
//   - filepath_join/filepath_split are pure string manipulation, no
//     filesystem access despite the name.
//   - json_schema validates against an inline schema string; no network
//     fetch.
//   - range is a pure function of its args; the unbounded-loop wall-clock
//     risk (`range(0, 1e9)`) is a documented residual risk in bloblang.go,
//     not a determinism concern, so it stays allowed.
//   - log/log10 are math.Log, not a logging side effect.
//
// A future benthos upgrade that adds a new name here fails
// TestInventoryReviewed. Review the new name the same way, then add it
// here (safe) or to deniedFunctions/deniedMethods in bloblang.go (unsafe).
var reviewedFunctions = []string{
	"bytes",
	"content",
	"deleted",
	"error",
	"error_source_label",
	"error_source_name",
	"error_source_path",
	"errored",
	"json",
	"meta",
	"metadata",
	"nothing",
	"range",
	"root_meta",
	"throw",
	"var",
}

var reviewedMethods = []string{
	"all",
	"any",
	"append",
	"apply",
	"array",
	"assign",
	"bitwise_and",
	"bitwise_or",
	"bitwise_xor",
	"bool",
	"bytes",
	"capitalize",
	"catch",
	"ceil",
	"collapse",
	"contains",
	"decode",
	"decrypt_aes",
	"encode",
	"encrypt_aes",
	"enumerated",
	"escape_html",
	"escape_url_path",
	"escape_url_query",
	"exists",
	"explode",
	"filepath_join",
	"filepath_split",
	"filter",
	"find",
	"find_all",
	"find_all_by",
	"find_by",
	"flatten",
	"floor",
	"fold",
	"format",
	"format_json",
	"format_yaml",
	"from",
	"from_all",
	"get",
	"has_prefix",
	"has_suffix",
	"hash",
	"index",
	"index_of",
	"join",
	"json_schema",
	"key_values",
	"keys",
	"length",
	"log",
	"log10",
	"lowercase",
	"map",
	"map_each",
	"map_each_key",
	"max",
	"merge",
	"min",
	"not",
	"not_empty",
	"not_null",
	"number",
	"or",
	"parse_csv",
	"parse_json",
	"parse_logfmt",
	"parse_url",
	"parse_yaml",
	"quote",
	"re_find_all",
	"re_find_all_object",
	"re_find_all_submatch",
	"re_find_object",
	"re_match",
	"re_replace",
	"re_replace_all",
	"repeat",
	"replace",
	"replace_all",
	"replace_all_many",
	"replace_many",
	"reverse",
	"round",
	"slice",
	"sort",
	"sort_by",
	"split",
	"string",
	"sum",
	"timestamp",
	"trim",
	"trim_prefix",
	"trim_suffix",
	"type",
	"unescape_html",
	"unescape_url_path",
	"unescape_url_query",
	"unique",
	"unquote",
	"uppercase",
	"uuid_v5",
	"values",
	"without",
}
