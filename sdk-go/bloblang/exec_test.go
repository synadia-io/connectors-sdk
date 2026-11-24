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
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

func run1(t *testing.T, mapping string, in transform.Message) transform.Result {
	t.Helper()
	res, err := compile(t, mapping)(context.Background(), []transform.Message{in})
	if err != nil {
		t.Fatalf("batch error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("index alignment: %d results for 1 input", len(res))
	}
	return res[0]
}

func msg(subject, payload string, hdr nats.Header) transform.Message {
	return transform.Message{Subject: subject, Header: hdr, Payload: []byte(payload)}
}

func TestPayloadMapping(t *testing.T) {
	r := run1(t, `root.y = this.x`, msg("a.b", `{"x":1}`, nil))
	if r.Err != nil || string(r.Output[0].Payload) != `{"y":1}` {
		t.Fatalf("got %+v", r)
	}
}

func TestMetadataReadQuotedHyphen(t *testing.T) {
	h := nats.Header{}
	h.Set("Mongo-Op", "insert")
	r := run1(t, `root.op = @"Mongo-Op"`, msg("a.b", `{}`, h))
	if r.Err != nil || string(r.Output[0].Payload) != `{"op":"insert"}` {
		t.Fatalf("got %+v", r)
	}
}

func TestMetadataWriteAndDelete(t *testing.T) {
	h := nats.Header{}
	h.Set("Mongo-Id", "secret")
	h.Set("Keep", "yes")
	r := run1(t, "root = this\nmeta \"Mongo-Id\" = deleted()\nmeta New = \"v\"", msg("a.b", `{}`, h))
	out := r.Output[0]
	if out.Header.Get("Mongo-Id") != "" || out.Header.Get("New") != "v" || out.Header.Get("Keep") != "yes" {
		t.Fatalf("headers wrong: %+v", out.Header)
	}
}

func TestSubjectRewriteViaReservedKey(t *testing.T) {
	r := run1(t, "root = this\nmeta nats_subject = \"a.c\"", msg("a.b", `{}`, nil))
	if r.Output[0].Subject != "a.c" {
		t.Fatalf("subject: %q", r.Output[0].Subject)
	}
}

func TestSubjectUntouchedPassesThrough(t *testing.T) {
	r := run1(t, `root = this`, msg("a.b", `{}`, nil))
	if r.Output[0].Subject != "a.b" {
		t.Fatalf("subject: %q", r.Output[0].Subject)
	}
}

// TestSubjectRewriteUnpublishableErrors: a message can't have an empty
// subject, one with whitespace or an empty token, or a wildcard, so the
// mapping must fail for that message instead of producing one that fails
// the batch at publish.
func TestSubjectRewriteUnpublishableErrors(t *testing.T) {
	for _, tt := range []struct{ subject, problem string }{
		{"", "empty"},
		{"a. b", `" "`},
		{"a.b\tc", `"\t"`},
		{"a.*", `"*"`},
		{"a.>", `">"`},
		{"a..b", "empty token"},
	} {
		r := run1(t, "root = this\nmeta nats_subject = "+strconv.Quote(tt.subject), msg("a.b", `{}`, nil))
		if r.Err == nil {
			t.Errorf("nats_subject %q: got %+v, want a per-message error", tt.subject, r.Output)
			continue
		}
		if !errors.Is(r.Err, transform.ErrUnpublishableSubject) {
			t.Errorf("error = %q, want it to wrap transform.ErrUnpublishableSubject", r.Err)
		}
		if want := "nats_subject " + strconv.Quote(tt.subject); !strings.Contains(r.Err.Error(), want) {
			t.Errorf("error = %q, want it to name the subject (%s)", r.Err, want)
		}
		if !strings.Contains(r.Err.Error(), tt.problem) {
			t.Errorf("error = %q, want it to say why (%s)", r.Err, tt.problem)
		}
	}
}

// TestUnpublishableInputSubjectErrors: a mapping that leaves nats_subject
// alone or deletes it gets the input subject, so that is checked too.
func TestUnpublishableInputSubjectErrors(t *testing.T) {
	for _, mapping := range []string{
		"root = this",
		"root = this\nmeta nats_subject = deleted()",
		"root = this\nmeta = deleted()",
	} {
		for _, in := range []string{"", "orders.a*b"} {
			r := run1(t, mapping, msg(in, `{}`, nil))
			if !errors.Is(r.Err, transform.ErrUnpublishableSubject) {
				t.Errorf("mapping %q on subject %q: got %+v, want transform.ErrUnpublishableSubject", mapping, in, r)
			}
		}
	}
}

// TestBadHeaderNameWinsOverBadSubject: with both wrong, the error is the
// header name's every time, whatever order the metadata is walked in, for
// an invalid subject and an over-long one alike.
func TestBadHeaderNameWinsOverBadSubject(t *testing.T) {
	for _, s := range []string{"", longSubject(subject.MaxSubjectLen + 1)} {
		f := compile(t, "root = this\nmeta nats_subject = "+strconv.Quote(s)+"\nmeta \"x/y\" = \"v\"")
		for range 50 {
			res, err := f(context.Background(), []transform.Message{msg("a.b", `{}`, nil)})
			if err != nil {
				t.Fatal(err)
			}
			e := res[0].Err
			if e == nil || errors.Is(e, transform.ErrUnpublishableSubject) || !strings.Contains(e.Error(), `header name "x/y"`) {
				t.Fatalf("subject of %d bytes: error = %v, want the header-name error", len(s), e)
			}
		}
	}
}

// TestSubjectRewriteErrorIsolation: a subject built from a field fails only
// the message whose field makes it unpublishable.
func TestSubjectRewriteErrorIsolation(t *testing.T) {
	f := compile(t, "root = this\nmeta nats_subject = \"orders.\" + this.region")
	res, err := f(context.Background(), []transform.Message{
		msg("a.b", `{"region":"eu"}`, nil),
		msg("a.b", `{"region":""}`, nil), // "orders." has an empty token
		msg("a.b", `{"region":"us"}`, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || !errors.Is(res[1].Err, transform.ErrUnpublishableSubject) || res[2].Err != nil {
		t.Fatalf("error must isolate to message 2: %+v", res)
	}
	if res[0].Output[0].Subject != "orders.eu" || res[2].Output[0].Subject != "orders.us" {
		t.Fatalf("subjects: %q, %q", res[0].Output[0].Subject, res[2].Output[0].Subject)
	}
}

// TestSubjectRewriteValidLiteralsPass: the rule is the spec's literal
// subject, which allows a single token and non-ASCII characters, including
// a no-break space and a C1 control, neither of which it may reject.
func TestSubjectRewriteValidLiteralsPass(t *testing.T) {
	for _, s := range []string{"a", "a.c", "orders.é", "a.b\u00a0c", "a.b\u0085c"} {
		r := run1(t, "root = this\nmeta nats_subject = "+strconv.Quote(s), msg("a.b", `{}`, nil))
		if r.Err != nil || len(r.Output) != 1 || r.Output[0].Subject != s {
			t.Errorf("nats_subject %q: got %+v, want it as the output subject", s, r)
		}
	}
}

// longSubject returns a literal subject of n bytes: "orders." and then 'x's.
func longSubject(n int) string {
	return "orders." + strings.Repeat("x", n-len("orders."))
}

// maxLengthErrorLen bounds the text of an over-long subject's error: it
// quotes only the subject's start, never the subject.
const maxLengthErrorLen = 256

// checkOverLengthError checks that err is the per-message error for an
// output subject of n bytes that starts with prefix: it wraps
// transform.ErrUnpublishableSubject, states the length and the limit,
// quotes the start of the subject, and is short.
func checkOverLengthError(t *testing.T, err error, n int, prefix string) {
	t.Helper()
	if !errors.Is(err, transform.ErrUnpublishableSubject) {
		t.Errorf("subject of %d bytes: error = %v, want it to wrap transform.ErrUnpublishableSubject", n, err)
		return
	}
	text := err.Error()
	if len(text) > maxLengthErrorLen {
		t.Errorf("subject of %d bytes: error is %d bytes, want at most %d: %.300q", n, len(text), maxLengthErrorLen, text)
	}
	for _, want := range []string{
		`nats_subject "` + prefix, // prefix needs no quoting
		"(" + strconv.Itoa(n) + " bytes)",
		strconv.Itoa(subject.MaxSubjectLen) + " bytes",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("subject of %d bytes: error = %.300q, want it to contain %q", n, text, want)
		}
	}
}

// TestSubjectRewriteLength: a subject built from a field can be longer than
// the server takes in a PUB line, and the server answers one by closing the
// connection, so the mapping fails that message instead, even when the
// subject is also invalid, and passes a subject of exactly
// subject.MaxSubjectLen bytes.
func TestSubjectRewriteLength(t *testing.T) {
	f := compile(t, "root = this\nmeta nats_subject = \"orders.\" + this.blob")
	blob := func(n int, c string) transform.Message {
		return msg("a.b", `{"blob":"`+strings.Repeat(c, n-len("orders."))+`"}`, nil)
	}
	res, err := f(context.Background(), []transform.Message{
		blob(subject.MaxSubjectLen, "x"),
		blob(subject.MaxSubjectLen+1, "x"),
		blob(64<<10, "x"),
		blob(64<<10, " "), // over-long and invalid: the length is checked first
	})
	if err != nil {
		t.Fatal(err)
	}
	if r := res[0]; r.Err != nil || len(r.Output) != 1 || r.Output[0].Subject != longSubject(subject.MaxSubjectLen) {
		t.Errorf("subject of exactly MaxSubjectLen bytes: err = %v, want it as the output subject", r.Err)
	}
	checkOverLengthError(t, res[1].Err, subject.MaxSubjectLen+1, "orders.xxxx")
	checkOverLengthError(t, res[2].Err, 64<<10, "orders.xxxx")
	checkOverLengthError(t, res[3].Err, 64<<10, "orders.    ")
}

// TestOverLengthInputSubjectErrors: a mapping that leaves nats_subject
// alone or deletes it gets the input subject, so its length is checked
// too.
func TestOverLengthInputSubjectErrors(t *testing.T) {
	for _, mapping := range []string{
		"root = this",
		"root = this\nmeta nats_subject = deleted()",
		"root = this\nmeta = deleted()",
	} {
		r := run1(t, mapping, msg(longSubject(subject.MaxSubjectLen+1), `{}`, nil))
		checkOverLengthError(t, r.Err, subject.MaxSubjectLen+1, "orders.xxxx")
		in := longSubject(subject.MaxSubjectLen)
		if r := run1(t, mapping, msg(in, `{}`, nil)); r.Err != nil || len(r.Output) != 1 || r.Output[0].Subject != in {
			t.Errorf("mapping %q on a subject of exactly MaxSubjectLen bytes: err = %v, want it as the output subject", mapping, r.Err)
		}
	}
}

func TestReservedKeysNeverBecomeHeaders(t *testing.T) {
	r := run1(t, "root = this\nmeta nats_subject = \"a.c\"\nmeta nats_reply_subject = \"r\"", msg("a.b", `{}`, nil))
	out := r.Output[0]
	if out.Header.Get("nats_subject") != "" || out.Header.Get("nats_reply_subject") != "" {
		t.Fatalf("reserved keys leaked into headers: %+v", out.Header)
	}
}

func TestDeletedDrops(t *testing.T) {
	r := run1(t, `root = deleted()`, msg("a.b", `{}`, nil))
	if r.Err != nil || len(r.Output) != 0 {
		t.Fatalf("expected drop, got %+v", r)
	}
}

func TestEmptyPayloadBindsNull(t *testing.T) {
	// Delete tombstones: this == null, and a null root round-trips to an
	// EMPTY payload (not the 4 bytes "null").
	r := run1(t, `root = this`, msg("a.b", "", nil))
	if r.Err != nil || len(r.Output[0].Payload) != 0 {
		t.Fatalf("null root must serialize to empty payload, got %+v", r)
	}
	r = run1(t, `root.was_null = this == null`, msg("a.b", "", nil))
	if string(r.Output[0].Payload) != `{"was_null":true}` {
		t.Fatalf("this must bind null on empty payload, got %s", r.Output[0].Payload)
	}
}

func TestLazyParseNonJSON(t *testing.T) {
	// Mapping that ignores the payload succeeds on non-JSON bytes: a truly
	// metadata-only mapping never references `this`, so Bloblang never
	// attempts to JSON-parse the payload (verified empirically — `root =
	// this` unconditionally dereferences `this` and therefore unconditionally
	// requires valid JSON, even though it looks like a no-op identity copy;
	// there is no lazy bypass for it in benthos's mapping executor).
	r := run1(t, `meta Tag = "x"`, msg("a.b", "not json", nil))
	if r.Err != nil || string(r.Output[0].Payload) != "not json" {
		t.Fatalf("metadata-only mapping must pass raw bytes through: %+v", r)
	}
	// ...dereferencing this on non-JSON errors per-message.
	r = run1(t, `root.x = this.a`, msg("a.b", "not json", nil))
	if r.Err == nil {
		t.Fatal("structured access on non-JSON must be a per-message error")
	}
}

func TestPerMessageErrorIsolation(t *testing.T) {
	f := compile(t, `root.x = this.a.uppercase()`)
	res, err := f(context.Background(), []transform.Message{
		msg("a.b", `{"a":"ok"}`, nil),
		msg("a.b", `{"a":1}`, nil), // uppercase on number -> error
		msg("a.b", `{"a":"fine"}`, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || res[1].Err == nil || res[2].Err != nil {
		t.Fatalf("error must isolate to message 2: %+v", res)
	}
}

func TestNonStringMetadataJSONEncoded(t *testing.T) {
	r := run1(t, "root = this\nmeta N = 5\nmeta O = {\"k\":\"v\"}", msg("a.b", `{}`, nil))
	out := r.Output[0]
	if out.Header.Get("N") != "5" || out.Header.Get("O") != `{"k":"v"}` {
		t.Fatalf("non-string metadata must JSON-encode: %+v", out.Header)
	}
}

func TestInvalidHeaderNameErrors(t *testing.T) {
	r := run1(t, "root = this\nmeta \"bad name\" = \"v\"", msg("a.b", `{}`, nil))
	if r.Err == nil {
		t.Fatal("nats.go refuses to publish a header name with a space; must be a per-message error")
	}
}

// TestHeaderNameNATSWontSendErrors: nats.go refuses to publish a header
// whose name has one of `"(),/:;<=>?@[\]{}`, so the mapping must fail for
// that message instead of producing one that fails at publish.
func TestHeaderNameNATSWontSendErrors(t *testing.T) {
	r := run1(t, "root = this\nmeta \"x/y\" = \"v\"", msg("a.b", `{}`, nil))
	if r.Err == nil {
		t.Fatal("header name with '/' can't be published; must be a per-message error")
	}
	if !strings.Contains(r.Err.Error(), `"/"`) {
		t.Errorf("error = %q, want it to name the character nats.go won't send", r.Err)
	}
}

func TestMultiValuedHeaderTakesFirst(t *testing.T) {
	h := nats.Header{"K": []string{"first", "second"}}
	r := run1(t, `root.k = @K`, msg("a.b", `{}`, h))
	if string(r.Output[0].Payload) != `{"k":"first"}` {
		t.Fatalf("got %s", r.Output[0].Payload)
	}
}

func TestUntouchedHeadersPropagate(t *testing.T) {
	h := nats.Header{}
	h.Set("Mongo-Cluster-Time", "0000000001.0000000001")
	r := run1(t, `root = this`, msg("a.b", `{}`, h))
	if r.Output[0].Header.Get("Mongo-Cluster-Time") != "0000000001.0000000001" {
		t.Fatalf("headers must flow through untouched mappings: %+v", r.Output[0].Header)
	}
}

// TestSubjectRewriteReservedErrors: a subject the mapping wrote may be
// neither the server's (the JetStream API, system events, an inbox) nor a
// notice, which only the connector publishes.
func TestSubjectRewriteReservedErrors(t *testing.T) {
	for _, tt := range []struct{ subject, problem string }{
		{"$JS.API.STREAM.PURGE.ORDERS", "server-reserved namespace"},
		{"$SYS.REQ.SERVER.PING", "server-reserved namespace"},
		{"_INBOX.abc", "server-reserved namespace"},
		{"x.error", "reserved notice suffix"},
		{"a.b.oversize", "reserved notice suffix"},
		{"error", "reserved notice suffix"},
	} {
		r := run1(t, "root = this\nmeta nats_subject = "+strconv.Quote(tt.subject), msg("a.b", `{}`, nil))
		if r.Err == nil {
			t.Errorf("nats_subject %q: got %+v, want a per-message error", tt.subject, r.Output)
			continue
		}
		if !errors.Is(r.Err, transform.ErrUnpublishableSubject) {
			t.Errorf("error = %q, want it to wrap transform.ErrUnpublishableSubject", r.Err)
		}
		if want := "nats_subject " + strconv.Quote(tt.subject); !strings.Contains(r.Err.Error(), want) {
			t.Errorf("error = %q, want it to name the subject (%s)", r.Err, want)
		}
		if !strings.Contains(r.Err.Error(), tt.problem) {
			t.Errorf("error = %q, want it to say why (%s)", r.Err, tt.problem)
		}
	}
}

// TestInheritedReservedSubjectPassesThrough: the reserved-namespace and
// notice-suffix rules apply to a subject the mapping rewrote, not to one it
// inherited: a connector consuming advisories or notices transforms them
// on their own subjects, and whether it may publish there is its guard's
// decision, not the mapping's. Setting nats_subject to the inherited value
// is not a rewrite.
func TestInheritedReservedSubjectPassesThrough(t *testing.T) {
	for _, in := range []string{"$JS.EVENT.ADVISORY.x", "_INBOX.abc", "events.error", "events.a.oversize"} {
		for _, mapping := range []string{
			"root = this",
			"root = this\nmeta nats_subject = deleted()",
			"root = this\nmeta nats_subject = @nats_subject",
			"root = this\nmeta nats_subject = " + strconv.Quote(in),
		} {
			r := run1(t, mapping, msg(in, `{}`, nil))
			if r.Err != nil || len(r.Output) != 1 || r.Output[0].Subject != in {
				t.Errorf("mapping %q on subject %q: got %+v, want it passed through", mapping, in, r)
			}
		}
	}
}

// TestReservedHeaderNameErrors: a Nats-* header, in any letter case, is the
// server's to act on, so a mapping that sets one fails that message with an
// error wrapping transform.ErrReservedHeader.
func TestReservedHeaderNameErrors(t *testing.T) {
	for _, name := range []string{"Nats-Rollup", "nats-msg-id", "NATS-TTL", "Nats-Expected-Last-Sequence"} {
		r := run1(t, "root = this\nmeta "+strconv.Quote(name)+" = \"all\"", msg("a.b", `{}`, nil))
		if r.Err == nil {
			t.Errorf("header %q: got %+v, want a per-message error", name, r.Output)
			continue
		}
		if !errors.Is(r.Err, transform.ErrReservedHeader) {
			t.Errorf("header %q: error = %q, want it to wrap transform.ErrReservedHeader", name, r.Err)
		}
		if !strings.Contains(r.Err.Error(), strconv.Quote(name)) {
			t.Errorf("header %q: error = %q, want it to name the header", name, r.Err)
		}
	}
}

// TestNatsLikeHeaderNamesPass: only the exact "Nats-" prefix is reserved.
func TestNatsLikeHeaderNamesPass(t *testing.T) {
	for _, name := range []string{"X-Nats", "X-Nats-Thing", "Natsy", "Nats"} {
		r := run1(t, "root = this\nmeta "+strconv.Quote(name)+" = \"1\"", msg("a.b", `{}`, nil))
		if r.Err != nil || r.Output[0].Header.Get(name) != "1" {
			t.Errorf("header %q: got %+v, want it set", name, r)
		}
	}
}

// TestReservedHeaderWinsOverBadSubject: with both wrong, the header error is
// reported, whatever order the metadata is walked in. The input carries a
// Nats-Msg-Id the mapping leaves alone, so the walk also has an inherited
// control header to pass over on its way to the one the mapping set.
func TestReservedHeaderWinsOverBadSubject(t *testing.T) {
	f := compile(t, "root = this\nmeta nats_subject = \"$JS.API.x\"\nmeta \"Nats-Rollup\" = \"all\"")
	h := nats.Header{}
	h.Set("Nats-Msg-Id", "id-1")
	for range 50 {
		res, err := f(context.Background(), []transform.Message{msg("a.b", `{}`, h)})
		if err != nil {
			t.Fatal(err)
		}
		e := res[0].Err
		if !errors.Is(e, transform.ErrReservedHeader) || !strings.Contains(e.Error(), `"Nats-Rollup"`) {
			t.Fatalf("error = %v, want the reserved-header error naming Nats-Rollup", e)
		}
	}
}

// inheritedMsg returns a message on "orders.eu" carrying the server's headers
// a sink reads off a stream: a Nats-Msg-Id (the publisher's dedup id) and a
// Nats-Stream-Source (the sourcing stream's provenance).
func inheritedMsg(payload string) transform.Message {
	h := nats.Header{}
	h.Set("Nats-Msg-Id", "id-1")
	h.Set("Nats-Stream-Source", "ORDERS 7 orders.eu")
	return msg("orders.eu", payload, h)
}

// TestInheritedReservedHeaderPassesThrough: a Nats-* header the message
// arrived with is the server's, not the mapping's, so it flows through to the
// output unchanged, whether the mapping leaves the headers alone, rewrites
// the payload, or sets the header to the value it inherited. A sink consuming
// a stream must be able to transform the messages on it, and every one of
// them may carry a Nats-Msg-Id or a Nats-Stream-Source.
func TestInheritedReservedHeaderPassesThrough(t *testing.T) {
	for _, mapping := range []string{
		"root = this",
		"root.y = this.x",
		"root = this\nmeta \"Nats-Msg-Id\" = @\"Nats-Msg-Id\"",
		"root = this\nmeta \"Nats-Msg-Id\" = \"id-1\"",
	} {
		r := run1(t, mapping, inheritedMsg(`{"x":1}`))
		if r.Err != nil || len(r.Output) != 1 {
			t.Errorf("mapping %q: got %+v, want one output", mapping, r)
			continue
		}
		out := r.Output[0]
		if out.Header.Get("Nats-Msg-Id") != "id-1" || out.Header.Get("Nats-Stream-Source") != "ORDERS 7 orders.eu" {
			t.Errorf("mapping %q: headers = %+v, want the inherited Nats-* headers unchanged", mapping, out.Header)
		}
	}
}

// TestInheritedReservedHeaderRewriteErrors: a mapping may not alter an
// inherited control header: a changed value, a non-string value, or a
// respelling (the server matches the exact spelling, so `nats-msg-id` is a
// new header) is the mapping's own Nats-* header and fails the message.
func TestInheritedReservedHeaderRewriteErrors(t *testing.T) {
	for _, tt := range []struct{ mapping, name string }{
		{"root = this\nmeta \"Nats-Msg-Id\" = \"other\"", "Nats-Msg-Id"},
		{"root = this\nmeta \"Nats-Msg-Id\" = @\"Nats-Msg-Id\" + \"-2\"", "Nats-Msg-Id"},
		{"root = this\nmeta \"Nats-Msg-Id\" = 1", "Nats-Msg-Id"},
		{"root = this\nmeta \"nats-msg-id\" = @\"Nats-Msg-Id\"", "nats-msg-id"},
		{"root = this\nmeta \"Nats-Stream-Source\" = \"OTHER 1 x\"", "Nats-Stream-Source"},
	} {
		r := run1(t, tt.mapping, inheritedMsg(`{}`))
		if r.Err == nil {
			t.Errorf("mapping %q: got %+v, want a per-message error", tt.mapping, r.Output)
			continue
		}
		if !errors.Is(r.Err, transform.ErrReservedHeader) {
			t.Errorf("mapping %q: error = %q, want it to wrap transform.ErrReservedHeader", tt.mapping, r.Err)
		}
		if !strings.Contains(r.Err.Error(), strconv.Quote(tt.name)) {
			t.Errorf("mapping %q: error = %q, want it to name %q", tt.mapping, r.Err, tt.name)
		}
	}
}

// TestInheritedReservedHeaderDeleted: deleting an inherited control header is
// not setting one; the message passes and the output lacks the header while
// the other inherited one stays.
func TestInheritedReservedHeaderDeleted(t *testing.T) {
	r := run1(t, "root = this\nmeta \"Nats-Msg-Id\" = deleted()", inheritedMsg(`{}`))
	if r.Err != nil || len(r.Output) != 1 {
		t.Fatalf("got %+v, want one output", r)
	}
	out := r.Output[0]
	if _, present := out.Header["Nats-Msg-Id"]; present {
		t.Errorf("headers = %+v, want Nats-Msg-Id deleted", out.Header)
	}
	if out.Header.Get("Nats-Stream-Source") != "ORDERS 7 orders.eu" {
		t.Errorf("headers = %+v, want Nats-Stream-Source kept", out.Header)
	}
}

// TestInheritedReservedHeaderNotOnInputErrors: the exemption is for a header
// the message arrived with. A multi-valued input header is bound by its first
// value only, so setting its second value is a change; and a header the input
// has only under another spelling is not inherited under this one.
func TestInheritedReservedHeaderNotOnInputErrors(t *testing.T) {
	h := nats.Header{"Nats-Msg-Id": []string{"first", "second"}}
	r := run1(t, "root = this\nmeta \"Nats-Msg-Id\" = \"second\"", msg("a.b", `{}`, h))
	if !errors.Is(r.Err, transform.ErrReservedHeader) {
		t.Errorf("second value: got %+v, want transform.ErrReservedHeader", r)
	}
	r = run1(t, "root = this\nmeta \"Nats-Msg-Id\" = \"first\"", msg("a.b", `{}`, h))
	if r.Err != nil || r.Output[0].Header.Get("Nats-Msg-Id") != "first" {
		t.Errorf("first value: got %+v, want it passed through", r)
	}
	h = nats.Header{"nats-msg-id": []string{"id-1"}}
	r = run1(t, "root = this\nmeta \"Nats-Msg-Id\" = \"id-1\"", msg("a.b", `{}`, h))
	if !errors.Is(r.Err, transform.ErrReservedHeader) ||
		!strings.Contains(r.Err.Error(), strconv.Quote("Nats-Msg-Id")) {
		t.Errorf("other spelling on input: got %+v, want transform.ErrReservedHeader naming the respelled key", r)
	}
}
