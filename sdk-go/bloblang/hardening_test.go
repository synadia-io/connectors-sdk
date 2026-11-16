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

import "testing"

// Everything here must be REJECTED AT PARSE — before any message flows.
func TestHardeningRejectsAtCompile(t *testing.T) {
	for _, m := range []string{
		`root = env("PATH")`,            // impure: stripped by OnlyPure
		`root = file("/etc/hosts")`,     // impure
		`root = hostname()`,             // impure
		`root = counter()`,              // impure
		`root = random_int()`,           // denied: nondeterministic
		`root = uuid_v4()`,              // denied
		`root = uuid_v7()`,              // denied
		`root = ksuid()`,                // denied
		`root = nanoid()`,               // denied
		`root = now()`,                  // denied
		`root = timestamp_unix()`,       // denied
		`root = timestamp_unix_milli()`, // denied
		`root = timestamp_unix_micro()`, // denied
		`root = timestamp_unix_nano()`,  // denied
		`root = batch_index()`,          // denied: unstable semantics
		`root = batch_size()`,           // denied
		`root = tracing_id()`,           // denied: span attached by pipeline, unstable under replay
		`root = tracing_span()`,         // denied
		`import "/tmp/x.blobl"`,         // imports disabled

		// .bloblang(<mapping string>) parses its argument against benthos's
		// own hardcoded global registry (OnlyPure only — none of
		// deniedFunctions applies there), so it must be denied outright
		// rather than relying on the nested mapping's own restrictions.
		`root = this.bloblang("root = now()")`,
		`root = this.bloblang("root = uuid_v4()")`,
		`root = this.bloblang("root = this")`, // even an inert mapping: the method itself is denied
	} {
		if _, err := New().Compile(cfgJSON(t, m)); err == nil {
			t.Errorf("%s: compiled; must be rejected", m)
		}
	}
}
