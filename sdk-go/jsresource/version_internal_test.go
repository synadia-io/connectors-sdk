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

package jsresource

import "testing"

func TestVersionAtLeast(t *testing.T) {
	tests := []struct {
		v    string
		want bool
	}{
		{"2.12.0", true},
		{"2.14.3", true},
		{"3.0.0", true},
		{"v2.12.0-RC.1", true},
		{"2.11.9", false},
		{"1.99.0", false},
		{"", true},        // unknown: ask, and read the effective config back
		{"garbage", true}, // same
	}
	for _, tt := range tests {
		if got := versionAtLeast(tt.v, 2, 12); got != tt.want {
			t.Errorf("versionAtLeast(%q, 2, 12) = %v, want %v", tt.v, got, tt.want)
		}
	}
}
