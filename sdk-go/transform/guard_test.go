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

package transform_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

func TestNewGuardRejectsBadPatterns(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{"no patterns", nil},
		{"invalid pattern", []string{"events.>.x"}},
		{"empty pattern", []string{""}},
		{"reserved pattern", []string{"$JS.API.>"}},
		{"inbox pattern", []string{"_INBOX.>"}},
		{"one bad among good", []string{"events.>", "$SYS.>"}},
	}
	for _, tt := range tests {
		if g, err := transform.NewGuard(tt.patterns...); err == nil || g != nil {
			t.Errorf("NewGuard(%q) = %v, %v; want nil, error", tt.patterns, g, err)
		}
	}
	if _, err := transform.NewGuard("events.>", "orders.*"); err != nil {
		t.Errorf("NewGuard(valid patterns) = %v, want nil", err)
	}
}

func guard(t *testing.T, patterns ...string) *transform.Guard {
	t.Helper()
	g, err := transform.NewGuard(patterns...)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func hdr(k, v string) nats.Header {
	h := nats.Header{}
	h.Set(k, v)
	return h
}

func TestGuardCheck(t *testing.T) {
	g := guard(t, "events.>", "orders.*")
	long := "events." + strings.Repeat("x", subject.MaxSubjectLen)
	tests := []struct {
		name string
		m    transform.Message
		want error  // sentinel the error wraps, or nil
		text string // a fragment the error text carries
	}{
		{"covered", transform.Message{Subject: "events.a.b"}, nil, ""},
		{"covered by second pattern", transform.Message{Subject: "orders.eu"}, nil, ""},
		{"nil headers", transform.Message{Subject: "events.a", Header: nil}, nil, ""},
		{"ordinary header", transform.Message{Subject: "events.a", Header: hdr("X-Trace", "1")}, nil, ""},
		{"X-Nats-Thing passes", transform.Message{Subject: "events.a", Header: hdr("X-Nats-Thing", "1")}, nil, ""},
		{"Natsy passes", transform.Message{Subject: "events.a", Header: hdr("Natsy", "1")}, nil, ""},
		{"Nats passes", transform.Message{Subject: "events.a", Header: hdr("Nats", "1")}, nil, ""},
		{"over-long", transform.Message{Subject: long}, transform.ErrUnpublishableSubject, "longer than"},
		{"not literal", transform.Message{Subject: "events.*"}, transform.ErrUnpublishableSubject, "not a literal"},
		{"empty", transform.Message{Subject: ""}, transform.ErrUnpublishableSubject, "not a literal"},
		{"reserved", transform.Message{Subject: "$JS.API.STREAM.PURGE.ORDERS"}, transform.ErrUnpublishableSubject, "server-reserved"},
		{"inbox", transform.Message{Subject: "_INBOX.abc"}, transform.ErrUnpublishableSubject, "server-reserved"},
		{"notice under covering pattern", transform.Message{Subject: "events.a.error"}, transform.ErrUnpublishableSubject, "notice suffix"},
		{"oversize notice", transform.Message{Subject: "events.oversize"}, transform.ErrUnpublishableSubject, "notice suffix"},
		{"not covered", transform.Message{Subject: "other.a"}, transform.ErrUnpublishableSubject, "not covered"},
		{"orders too deep", transform.Message{Subject: "orders.eu.x"}, transform.ErrUnpublishableSubject, "not covered"},
		{"Nats-Rollup", transform.Message{Subject: "events.a", Header: hdr("Nats-Rollup", "all")}, transform.ErrReservedHeader, "Nats-Rollup"},
		{"nats-rollup lowercase", transform.Message{Subject: "events.a", Header: hdr("nats-rollup", "all")}, transform.ErrReservedHeader, "nats-rollup"},
		{"NATS-MSG-ID", transform.Message{Subject: "events.a", Header: hdr("NATS-MSG-ID", "1")}, transform.ErrReservedHeader, "NATS-MSG-ID"},
		{"Nats- alone", transform.Message{Subject: "events.a", Header: hdr("Nats-", "1")}, transform.ErrReservedHeader, "Nats-"},
		{"subject error wins over header", transform.Message{Subject: "other.a", Header: hdr("Nats-Rollup", "all")}, transform.ErrUnpublishableSubject, "not covered"},
	}
	for _, tt := range tests {
		err := g.Check(tt.m)
		if tt.want == nil {
			if err != nil {
				t.Errorf("%s: Check = %v, want nil", tt.name, err)
			}
			continue
		}
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: Check = %v, want it to wrap %v", tt.name, err, tt.want)
			continue
		}
		if !strings.Contains(err.Error(), tt.text) {
			t.Errorf("%s: Check = %q, want it to say %q", tt.name, err, tt.text)
		}
	}
}

// TestGuardOverLongQuotesStart: the over-long error states the length and
// quotes only the subject's start, cut at a character boundary.
// TestGuardWidePatternStillRejectsReserved pins the order of Check's rules: a
// data-subject pattern as wide as ">" covers every reserved subject, so the
// namespace and notice-suffix checks must run before, and independently of,
// coverage. A reorder would reopen the hole this guard closes.
func TestGuardWidePatternStillRejectsReserved(t *testing.T) {
	for _, pattern := range []string{">", "*.>", "*"} {
		g := guard(t, pattern)
		for _, subj := range []string{"$JS.API.STREAM.PURGE.ORDERS", "_INBOX.abc", "events.error", "error"} {
			if subject.Covers(pattern, subj) {
				if err := g.Check(transform.Message{Subject: subj}); !errors.Is(err, transform.ErrUnpublishableSubject) {
					t.Errorf("pattern %q, subject %q: Check = %v, want ErrUnpublishableSubject", pattern, subj, err)
				}
			}
		}
		if err := g.Check(transform.Message{Subject: "events.a"}); err != nil && pattern != "*" {
			t.Errorf("pattern %q, subject events.a: Check = %v, want nil", pattern, err)
		}
	}
}

func TestGuardOverLongQuotesStart(t *testing.T) {
	g := guard(t, "events.>")
	s := "events." + strings.Repeat("é", subject.MaxSubjectLen)
	err := g.Check(transform.Message{Subject: s})
	if !errors.Is(err, transform.ErrUnpublishableSubject) {
		t.Fatalf("Check = %v, want it to wrap ErrUnpublishableSubject", err)
	}
	text := err.Error()
	if len(text) > 256 {
		t.Errorf("error is %d bytes, want it to quote only the subject's start", len(text))
	}
	if !strings.Contains(text, "("+strconv.Itoa(len(s))+" bytes)") {
		t.Errorf("error = %q, want it to state the length %d", text, len(s))
	}
	if strings.Contains(text, "�") || strings.Contains(text, `\x`) {
		t.Errorf("error = %q, want the quote cut at a character boundary", text)
	}
}

func TestGuardNilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Check on a nil *Guard did not panic")
		}
	}()
	var g *transform.Guard
	_ = g.Check(transform.Message{Subject: "events.a"})
}

func TestReservedHeader(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"Nats-Msg-Id", true},
		{"nats-rollup", true},
		{"NATS-TTL", true},
		{"Nats-", true},
		{"Nats", false},
		{"Natsy", false},
		{"X-Nats-Thing", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := transform.ReservedHeader(tt.in); got != tt.want {
			t.Errorf("ReservedHeader(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
