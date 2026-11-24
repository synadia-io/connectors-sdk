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
	"strconv"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// The error branches of fromService are unreachable through well-formed
// mappings (bloblang cannot produce a channel-valued metadata entry), so
// they are exercised white-box with crafted service.Messages. The contract
// under test is the same either way: a conversion failure surfaces as an
// error, never as a silently wrong message.

func TestSubjectRewriteNonStringErrors(t *testing.T) {
	r := run1(t, "root = this\nmeta nats_subject = 5", msg("a.b", `{}`, nil))
	if r.Err == nil || !strings.Contains(r.Err.Error(), "nats_subject") {
		t.Fatalf("non-string nats_subject must be a per-message error, got %+v", r)
	}
}

// TestSubjectProblemNamesTheCharacter: subjectProblem names the whole
// offending character, even an invalid byte or a multi-byte U+FFFD.
func TestSubjectProblemNamesTheCharacter(t *testing.T) {
	for _, tt := range []struct{ in, char string }{
		{"a.b\xffc", "\xff"},
		{"a.b\ufffdc", "\ufffd"},
		{"é.b\x01", "\x01"},
		{"a.b\x7f", "\x7f"},
		{"a.b*c", "*"},
		{"a.b>c", ">"},
	} {
		if got, want := subjectProblem(tt.in), strconv.Quote(tt.char); !strings.Contains(got, want) {
			t.Errorf("subjectProblem(%q) = %q, want it to name %s", tt.in, got, want)
		}
	}
	for _, in := range []string{".a", "a.", "a..b"} {
		if got := subjectProblem(in); !strings.Contains(got, "empty token") {
			t.Errorf("subjectProblem(%q) = %q, want it to say empty token", in, got)
		}
	}
}

// TestTruncateKeepsWholeCharacters: the start of an over-long subject that
// its error quotes doesn't end in part of a character.
func TestTruncateKeepsWholeCharacters(t *testing.T) {
	for _, tt := range []struct {
		in   string
		n    int
		want string
	}{
		{"abc", 4, "abc"},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"aéb", 2, "a"}, // é is 2 bytes
		{"aéb", 3, "aé"},
		{"a€b", 2, "a"}, // € is 3 bytes
		{"a€b", 3, "a"},
		{"a€b", 4, "a€"},
		{"a\xffb", 2, "a\xff"}, // an invalid byte is cut as one character
	} {
		if got := truncate(tt.in, tt.n); got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
	}
}

func TestFromServiceUnmarshalableHeaderValue(t *testing.T) {
	m := service.NewMessage([]byte(`{}`))
	m.MetaSetMut("K", make(chan int)) // json.Marshal cannot encode a channel
	if _, err := fromService(m, msg("a.b", `{}`, nil)); err == nil {
		t.Fatal("unmarshalable header value must error")
	}
}
