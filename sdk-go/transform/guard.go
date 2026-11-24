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

package transform

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// ErrReservedHeader is wrapped by the per-message error for an output header
// whose name is reserved for the NATS server: any name whose first five
// bytes are "Nats-" in any letter case. The server's control headers
// (Nats-Msg-Id, Nats-Rollup, Nats-Expected-*, Nats-TTL and the rest) are
// matched by their exact spelling, but a client that canonicalises header
// names on read turns "nats-rollup" into the live one, so every spelling is
// refused. A transform may not set one, since the server acts on it; a
// connector adds the ones it means to send after the guard has passed the
// message. Guard.Check refuses every Nats-* header, inherited or not: a
// connector relaying NATS messages to NATS strips the inherited ones (a
// consumed message's Nats-Msg-Id or Nats-Stream-Source, say) before Check,
// or re-adds them deliberately after it. Guard wraps it, so a connector
// classifies that failure with errors.Is. The error's text names the header,
// which may come from message data, so it isn't fit for a wire header.
var ErrReservedHeader = errors.New("reserved header")

// reservedHeaderPrefix starts every header name the server acts on.
const reservedHeaderPrefix = "Nats-"

// ReservedHeader reports whether name is a header name reserved for the NATS
// server: its first five bytes equal "Nats-" in any letter case (see
// ErrReservedHeader for why the case is ignored).
func ReservedHeader(name string) bool {
	return len(name) >= len(reservedHeaderPrefix) &&
		strings.EqualFold(name[:len(reservedHeaderPrefix)], reservedHeaderPrefix)
}

// Guard is a connector's routing guard (spec/transformer.md): the rule that
// decides whether a transform's output message may be published at all.
// Every publish path of a connector calls Check on each output Message
// before publishing it, whatever the backend produced it. A nil error means
// publish; a non-nil error is a per-message failure, which the connector
// reports the way it reports any other transform error: an `.error` notice
// on the PRE-transform subject, carrying a fixed, value-free error class.
// The error's text is for the connector's logs only: it quotes the subject
// or header name, which may come from message data, so it is unfit for a
// wire header.
//
// Check rejects a subject that is over subject.MaxSubjectLen, not a literal,
// in a server-reserved namespace (subject.Reserved), carrying a reserved
// notice suffix (subject.IsNotice), or not covered by any of the
// connector's data-subject patterns; and a header whose name is reserved
// for the server (ReservedHeader), whether the transform set it or the
// message arrived with it: Check sees only the output and cannot tell the
// two apart, and the Bloblang backend's inherited-header exemption ends at
// its output. A connector adds its own Nats-* headers, such as Nats-Msg-Id,
// AFTER Check has passed the message. A connector relaying NATS messages to
// NATS strips the inherited Nats-* headers before Check, or re-adds the ones
// it means to send after it.
//
// NewGuard is the constructor; the zero value is not usable, and Check on a
// nil *Guard panics.
type Guard struct {
	patterns []string
}

// NewGuard returns a Guard that passes subjects covered by one of patterns,
// the connector's data-subject patterns. It fails without a pattern, with a
// pattern that isn't a subject.ValidPattern, and with one that is
// subject.Reserved: a connector must not declare a server-reserved
// namespace as its data.
func NewGuard(patterns ...string) (*Guard, error) {
	if len(patterns) == 0 {
		return nil, errors.New("transform: a guard needs at least one data-subject pattern")
	}
	for _, p := range patterns {
		if !subject.ValidPattern(p) {
			return nil, fmt.Errorf("transform: data-subject pattern %q is not a valid pattern", p)
		}
		if subject.Reserved(p) {
			return nil, fmt.Errorf("transform: data-subject pattern %q is in a server-reserved namespace", p)
		}
	}
	return &Guard{patterns: append([]string(nil), patterns...)}, nil
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

// Check reports whether m may be published: nil when it may, else an error
// wrapping ErrUnpublishableSubject (the subject is over
// subject.MaxSubjectLen, not a literal, in a server-reserved namespace,
// carries a reserved notice suffix, or is covered by none of the guard's
// patterns, checked in that order) or ErrReservedHeader (a header name
// starts with "Nats-" in any letter case). The subject is checked before the
// headers, and its length first, so an over-long subject is never quoted in
// full.
func (g *Guard) Check(m Message) error {
	s := m.Subject
	if len(s) > subject.MaxSubjectLen {
		return fmt.Errorf("%w: %q... (%d bytes) is longer than %d bytes",
			ErrUnpublishableSubject, truncate(s, subjectQuoteLen), len(s), subject.MaxSubjectLen)
	}
	if !subject.ValidLiteral(s) {
		return fmt.Errorf("%w: %q is not a literal subject", ErrUnpublishableSubject, s)
	}
	if subject.Reserved(s) {
		return fmt.Errorf("%w: %q is in a server-reserved namespace", ErrUnpublishableSubject, s)
	}
	if subject.IsNotice(s) {
		return fmt.Errorf("%w: %q carries a reserved notice suffix", ErrUnpublishableSubject, s)
	}
	covered := false
	for _, p := range g.patterns {
		if subject.Covers(p, s) {
			covered = true
			break
		}
	}
	if !covered {
		return fmt.Errorf("%w: %q is not covered by the connector's data subjects",
			ErrUnpublishableSubject, s)
	}
	for k := range m.Header {
		if ReservedHeader(k) {
			return fmt.Errorf("%w: %q is reserved for the server", ErrReservedHeader, k)
		}
	}
	return nil
}
