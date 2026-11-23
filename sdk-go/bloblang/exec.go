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
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	blobl "github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/synadia-io/connectors-sdk/sdk-go/natsmsg"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// Reserved metadata keys: exist only as metadata, never as wire headers.
const (
	metaSubject      = "nats_subject"
	metaReplySubject = "nats_reply_subject" // reserved, currently unused
)

func (e *Engine) funcFor(exec *blobl.Executor) transform.Func {
	return func(_ context.Context, batch []transform.Message) ([]transform.Result, error) {
		results := make([]transform.Result, len(batch))
		for i, m := range batch {
			results[i] = mapOne(exec, m)
		}
		return results, nil
	}
}

func mapOne(exec *blobl.Executor, m transform.Message) transform.Result {
	in := toService(m)
	out, err := in.BloblangQuery(exec)
	if err != nil {
		return transform.Result{Err: err}
	}
	if out == nil { // root = deleted()
		return transform.Result{}
	}
	om, err := fromService(out, m.Subject)
	if err != nil {
		return transform.Result{Err: err}
	}
	return transform.Result{Output: []transform.Message{om}}
}

// toService builds the input message: payload copied (the Func contract
// forbids aliasing either way), headers as metadata (first value — benthos's
// own NATS input collapses the same way), subject under the reserved key.
// An empty payload binds `this = null` — a source may publish an empty
// payload (a delete tombstone, say), and a mapping must be able to inspect
// and route it.
func toService(m transform.Message) *service.Message {
	var in *service.Message
	if len(m.Payload) == 0 {
		in = service.NewMessage(nil)
		in.SetStructured(nil)
	} else {
		in = service.NewMessage(append([]byte(nil), m.Payload...))
	}
	for k, vs := range m.Header {
		if len(vs) > 0 {
			in.MetaSetMut(k, vs[0])
		}
	}
	in.MetaSetMut(metaSubject, m.Subject)
	return in
}

// headerNameProblem says why nats.go won't send a header named name, which
// natsmsg.ValidHeaderName has rejected: it is empty, or names the first byte
// that can't be in a header name.
func headerNameProblem(name string) string {
	for i := range len(name) {
		if !natsmsg.ValidHeaderName(name[i : i+1]) {
			return fmt.Sprintf("nats.go won't send a header name with %q in it", name[i:i+1])
		}
	}
	return "nats.go won't send a header with an empty name"
}

// subjectProblem says why s, which subject.ValidLiteral has rejected, can't
// be a message's subject: it is empty, has an empty token, or names the
// first character that can't be in a token.
func subjectProblem(s string) string {
	if s == "" {
		return "a message subject can't be empty"
	}
	for _, tok := range strings.Split(s, ".") {
		if tok == "" {
			return "a message subject can't have an empty token"
		}
		for i := range tok {
			_, n := utf8.DecodeRuneInString(tok[i:])
			if c := tok[i : i+n]; !subject.ValidToken(c) {
				return fmt.Sprintf("a message subject can't have %q in it", c)
			}
		}
	}
	return "not a literal subject"
}

// subjectQuoteLen is how many bytes of an over-long subject its error
// quotes.
const subjectQuoteLen = 64

// truncate returns s cut to at most n bytes, at the start of a character
// where s is valid UTF-8.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// fromService converts a mapped message back. Null root -> empty payload
// (symmetric with binding); nats_subject -> the output subject, or
// origSubject when the mapping doesn't set nats_subject or deletes it, and
// whichever comes out must be a literal subject (subject.ValidLiteral) of
// at most subject.MaxSubjectLen bytes; a subject the mapping rewrote (one
// that differs from origSubject) must also be outside the server's
// namespaces (subject.Reserved) and carry no notice suffix
// (subject.IsNotice); other metadata -> headers with reserved keys
// stripped, non-strings JSON-encoded, names validated and the server's own
// Nats-* names refused (transform.ReservedHeader).
// nats.go refuses to publish to an empty or whitespace subject
// (ErrBadSubject) and a message with an invalid header name
// (ErrBadHeaderMsg); rejecting them here makes the failure this message's
// rather than the whole publish's. nats.go does send an over-long subject,
// and the server answers it by closing the connection, which fails every
// message in flight on it and, since nats.go doesn't reconnect after that
// error, the connection itself.
//
// The connector's transform.Guard checks every output again before it is
// published; this backend's own checks are defence in depth for the rules a
// mapping alone can break, so the failure is this backend's per-message
// error even in a connector that forgets the guard.
func fromService(out *service.Message, origSubject string) (transform.Message, error) {
	var payload []byte
	if v, err := out.AsStructured(); err == nil && v == nil {
		payload = nil
	} else {
		b, berr := out.AsBytes()
		if berr != nil {
			return transform.Message{}, fmt.Errorf("mapping output: %w", berr)
		}
		payload = append([]byte(nil), b...) // AsBytes result is not ours to keep
	}
	subj := origSubject
	hdr := nats.Header{}
	var werr error
	_ = out.MetaWalkMut(func(k string, v any) error {
		switch k {
		case metaSubject:
			s, ok := v.(string)
			if !ok {
				werr = fmt.Errorf("%s must be a string, got %T", metaSubject, v)
				return werr
			}
			subj = s
			return nil
		case metaReplySubject:
			return nil
		}
		if !natsmsg.ValidHeaderName(k) {
			werr = fmt.Errorf("header name %q: %s", k, headerNameProblem(k))
			return werr
		}
		// The server acts on a Nats-* header (Nats-Msg-Id dedups, Nats-Rollup
		// purges, Nats-Expected-* gates the publish), so a mapping may not set
		// one; the connector adds its own after the transform. Like a bad name,
		// this wins over a subject error whatever the map order.
		if transform.ReservedHeader(k) {
			werr = fmt.Errorf("%w: header name %q is reserved for the NATS server",
				transform.ErrReservedHeader, k)
			return werr
		}
		s, ok := v.(string)
		if !ok {
			b, jerr := json.Marshal(v)
			if jerr != nil {
				werr = fmt.Errorf("header %q value: %w", k, jerr)
				return werr
			}
			s = string(b)
		}
		hdr[k] = append(hdr[k], s)
		return nil
	})
	if werr != nil {
		return transform.Message{}, werr
	}
	// Checked after the walk, so a deleted nats_subject's fallback is
	// covered and a bad header name wins whatever the map order. The length
	// first, so an over-long subject is never quoted in full.
	if len(subj) > subject.MaxSubjectLen {
		return transform.Message{}, fmt.Errorf("%w: %s %q... (%d bytes): a message subject can't be longer than %d bytes",
			transform.ErrUnpublishableSubject, metaSubject, truncate(subj, subjectQuoteLen), len(subj), subject.MaxSubjectLen)
	}
	if !subject.ValidLiteral(subj) {
		return transform.Message{}, fmt.Errorf("%w: %s %q: %s",
			transform.ErrUnpublishableSubject, metaSubject, subj, subjectProblem(subj))
	}
	// Only a subject the mapping rewrote is held to the routing rules here. An
	// inherited subject is the connector's own business: a sink consuming
	// `$JS.EVENT.>` advisories or another connector's `.error` notices must
	// still be able to transform them. A rewritten one is mapping output, and
	// mapping output must be a data subject: never the server's (the
	// JetStream API, an inbox) and never a notice, which the connector alone
	// publishes (spec/transformer.md: notices are unforgeable). Rewritten
	// means the output subject differs from the input's: the input's
	// nats_subject metadata is carried through to the output whether or not
	// the mapping touched it, so the walk can't tell an untouched key from
	// one set to the same value, and a mapping that sets the subject to the
	// one it inherited changes nothing.
	if subj != origSubject {
		if subject.Reserved(subj) {
			return transform.Message{}, fmt.Errorf(
				"%w: %s %q: a message subject can't be in a server-reserved namespace",
				transform.ErrUnpublishableSubject, metaSubject, subj)
		}
		if subject.IsNotice(subj) {
			return transform.Message{}, fmt.Errorf(
				"%w: %s %q: a message subject can't carry a reserved notice suffix",
				transform.ErrUnpublishableSubject, metaSubject, subj)
		}
	}
	return transform.Message{Subject: subj, Header: hdr, Payload: payload}, nil
}
