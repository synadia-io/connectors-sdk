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

package subject_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

func TestValidToken(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"conn-1", true},
		{"a_b-c", true},
		{"aéb", true},
		{"a\u0080b", true}, // C1 control: the server accepts it, nothing breaks
		{"a\u0085b", true}, // NEL
		{"a\u00a0b", true}, // no-break space
		{"a\u3000b", true}, // ideographic space
		{"a\u200bb", true}, // zero-width space
		{"", false},
		{".", false},
		{"a.b", false},
		{"a b", false},
		{"a\tb", false},
		{"a\nb", false},
		{"a\rb", false},
		{"a\vb", false},
		{"a\x00b", false},
		{"a\x1bb", false}, // ESC
		{"a\x7fb", false}, // DEL
		{"*", false},
		{">", false},
		{"a*b", false},
		{"a>b", false},
		{"a\xffb", false},   // invalid UTF-8
		{"a\ufffdb", false}, // U+FFFD
	}
	for _, tt := range tests {
		if got := subject.ValidToken(tt.in); got != tt.want {
			t.Errorf("ValidToken(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestValidLiteralAndPattern(t *testing.T) {
	tests := []struct {
		in               string
		literal, pattern bool
	}{
		{"a", true, true},
		{"a.b.c", true, true},
		{"a.*", false, true},
		{"a.>", false, true},
		{"*", false, true},
		{">", false, true},
		{"a.*.c", false, true},
		{"", false, false},
		{"a..b", false, false},
		{".a", false, false},
		{"a.", false, false},
		{"a.>.b", false, false},
		{"a.b*", false, false},
		{"a.*b", false, false},
		{"a. ", false, false},
		{"a.\x01", false, false},
		{"a.\xff", false, false},
	}
	for _, tt := range tests {
		if got := subject.ValidLiteral(tt.in); got != tt.literal {
			t.Errorf("ValidLiteral(%q) = %v, want %v", tt.in, got, tt.literal)
		}
		if got := subject.ValidPattern(tt.in); got != tt.pattern {
			t.Errorf("ValidPattern(%q) = %v, want %v", tt.in, got, tt.pattern)
		}
	}
}

func TestCovers(t *testing.T) {
	tests := []struct {
		pattern, subject string
		want             bool
	}{
		{"a.>", "a", false}, // '>' needs at least one token
		{"a.>", "a.b.c", true},
		{"a.*", "a.b.c", false},
		{"a.*", "a.>", false},
		{"*", ">", false},
		{"a.>", "a.*", true},
		{"a.*.c", "a.>", false},
		{">", ">", true},
		{"a.*", "a.*", true},
		{"a.*", "a.b", true},
		{"a.b", "a.*", false},
		{"a.b", "a.b", true},
		{"a.b*", "a.bc", false}, // invalid pattern
		{"", "", false},         // invalid
		{"a..b", "a..b", false}, // invalid
		{"a.>", "a..b", false},  // valid pattern, invalid subject
		{"a.*", "a.b*", false},  // valid pattern, invalid subject token
		{">", "a.>.b", false},   // '>' not last in subject
		{"a.>.b", "a.c", false}, // invalid pattern, valid subject
	}
	for _, tt := range tests {
		if got := subject.Covers(tt.pattern, tt.subject); got != tt.want {
			t.Errorf("Covers(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.want)
		}
	}
}

func TestOverlap(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"a.>", "a", false},
		{"a.*", "a.>", true},
		{"a.*.c", "a.b.*", true},
		{"a.b.>", "a.*", false},
		{"a.*.>", "a.b", false},
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{">", "a", true},
		{"*", "a.b", false},
		{"a.b*", "a.*", false}, // invalid
		{"", "a", false},       // invalid
	}
	for _, tt := range tests {
		if got := subject.Overlap(tt.a, tt.b); got != tt.want {
			t.Errorf("Overlap(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
		if got := subject.Overlap(tt.b, tt.a); got != tt.want {
			t.Errorf("Overlap(%q, %q) = %v, want %v (symmetry)", tt.b, tt.a, got, tt.want)
		}
	}
}

func TestValidNames(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"ORDERS", true},
		{"mongo_orders-1", true},
		{"CONN-conn-1", true},
		{"", false},
		{"a.b", false},
		{"a*", false},
		{"a>", false},
		{"a b", false},
		{"a/b", false},
		{`a\b`, false},
		{"a\tb", false},
		{"a\nb", false},
		{"a\x00b", false},
		{"a\xffb", false},
		{strings.Repeat("a", 255), true},
		{strings.Repeat("a", 256), false},
	}
	for _, tt := range tests {
		if got := subject.ValidStreamName(tt.in); got != tt.want {
			t.Errorf("ValidStreamName(%q) = %v, want %v", tt.in, got, tt.want)
		}
		if got := subject.ValidConsumerName(tt.in); got != tt.want {
			t.Errorf("ValidConsumerName(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestReserved(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"$JS.API.STREAM.PURGE.X", true},
		{"$JS.>", true},
		{"$SYS.x", true},
		{"$KV.x", true},
		{"$O.x", true},
		{"$", true},
		{"_INBOX.abc", true},
		{"_INBOX", true},
		{"_INBOX.*", true},
		{"_INBOXX.a", false},
		{"_inbox.a", false},
		{"events._INBOX", false},
		{"events.$JS.x", false},
		{"events.a", false},
		{"", false},
		{".$JS", false},
	}
	for _, tt := range tests {
		if got := subject.Reserved(tt.in); got != tt.want {
			t.Errorf("Reserved(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestIsNotice(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"events.error", true},
		{"events.a.oversize", true},
		{"events.*.error", true},
		{"error", true},
		{"oversize", true},
		{"events.errors", false},
		{"events.error.a", false},
		{"events.Error", false},
		{"events.a", false},
		{"", false},
		{"events.", false},
	}
	for _, tt := range tests {
		if got := subject.IsNotice(tt.in); got != tt.want {
			t.Errorf("IsNotice(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestNoticeSubjects(t *testing.T) {
	both := []string{subject.SuffixOversize, subject.SuffixError}
	tests := []struct {
		in       string
		suffixes []string
		want     []string
	}{
		{"mongo.shop.orders", both, []string{"mongo.shop.orders.oversize", "mongo.shop.orders.error"}},
		{"events.s3", []string{subject.SuffixOversize}, []string{"events.s3.oversize"}},
		{"events.*", both, []string{"events.*.oversize", "events.*.error"}},
		{"events.>", both, nil}, // already matches its notices
		{">", both, nil},
		{"events.s3", nil, []string{}},
		{"a..b", both, nil},              // invalid subject
		{"events", []string{"a.b"}, nil}, // invalid suffix
		{"events", []string{""}, nil},    // invalid suffix
	}
	for _, tt := range tests {
		got := subject.NoticeSubjects(tt.in, tt.suffixes...)
		if !slices.Equal(got, tt.want) {
			t.Errorf("NoticeSubjects(%q, %q) = %#v, want %#v", tt.in, tt.suffixes, got, tt.want)
		}
	}
}
