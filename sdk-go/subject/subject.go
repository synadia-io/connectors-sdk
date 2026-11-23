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

// Package subject holds the NATS subject and name rules every connector
// shares (spec/subjects.md): what one subject token is, what a literal
// subject and a pattern are, how long a subject a connector publishes to
// may be, whether one pattern covers or overlaps another, which stream and
// consumer names are valid, which namespaces belong to the server
// (Reserved), and which subjects carry notices (spec/transformer.md). It
// has no NATS dependency.
//
// The rules are stricter than the server's in three ways (spec/subjects.md):
// they reject what changes where a message goes (a '.' or a wildcard inside
// what should be one token), what the server stores badly (NUL, invalid
// UTF-8, U+FFFD), and a few characters no name needs (the other ASCII
// controls, and '*' or '>' inside a token). Every function returns false (or
// nil) for invalid input instead of guessing.
package subject

import (
	"strings"
	"unicode/utf8"
)

// The reserved notice suffixes (spec/transformer.md): the terminal token a
// connector appends to a data subject to publish a notice about it.
const (
	SuffixOversize = "oversize"
	SuffixError    = "error"
)

// ValidToken reports whether s can be one literal subject token
// (spec/subjects.md): non-empty, valid UTF-8, and no U+FFFD (the
// replacement character, which the server's restart check can't tell from
// invalid bytes), and free of '.', space, tab, the wildcards '*' and '>',
// and ASCII control characters (U+0000 to U+001F, which include CR and LF,
// and U+007F). Every other character is allowed, including non-ASCII
// spaces and C1 control characters: the server accepts them, and they
// change nothing about where a message goes.
func ValidToken(s string) bool {
	if s == "" || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		switch r {
		case '.', ' ', '\t', '*', '>', utf8.RuneError:
			return false
		}
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// MaxSubjectLen is the longest subject, in bytes, that a connector may
// publish to (spec/subjects.md). ValidLiteral and ValidPattern don't check
// it; a caller that publishes to a subject it didn't choose checks it too.
//
// nats-server limits the arguments of a PUB or HPUB line, which are the
// subject, the reply subject, and the header and total sizes, with three
// spaces between them, to max_control_line bytes: 4096 by default for a
// client connection. A longer line gets -ERR 'maximum control line
// exceeded' and a closed connection, and nats.go doesn't reconnect after
// that error: the connection is closed for good, and every other publish
// in flight on it goes unacknowledged.
//
// The SDK can't know the real limit: the server's max_control_line and the
// client's inbox prefix are both configurable. It leaves 4096 - 3072 = 1024
// bytes for the rest of the line. The sizes take at most 10 digits each,
// since max_payload is an int32, which leaves 1024 - 3 - 20 = 1001 bytes for
// the reply. nats.go's reply to a synchronous JetStream publish is
// <inbox prefix>.<22-character nuid>.<8 characters>, 38 bytes with the
// default prefix "_INBOX", and to an asynchronous one
// <inbox prefix>.<6 characters>.<6 characters>, so the limit holds for a
// custom inbox prefix of up to 969 bytes. A server whose max_control_line
// is below 3133 bytes (a MaxSubjectLen subject's line with the default
// prefix and 10-digit sizes), or a longer custom inbox prefix, can still
// close the connection over a subject within this limit.
const MaxSubjectLen = 3072

// ValidLiteral reports whether s is a subject without wildcards: one or more
// '.'-separated tokens, each a ValidToken.
func ValidLiteral(s string) bool {
	for _, t := range strings.Split(s, ".") {
		if !ValidToken(t) {
			return false
		}
	}
	return true
}

// ValidPattern reports whether s is a subject that may contain wildcards:
// one or more '.'-separated tokens, each a ValidToken, '*', or, as the last
// token only, '>'. Every ValidLiteral is a ValidPattern.
func ValidPattern(s string) bool {
	tokens := strings.Split(s, ".")
	for i, t := range tokens {
		switch {
		case t == "*":
		case t == ">":
			if i != len(tokens)-1 {
				return false
			}
		case !ValidToken(t):
			return false
		}
	}
	return true
}

// Covers reports whether pattern matches every subject that subject
// matches. Both arguments are patterns: '*' matches exactly one token, and
// '>' one or more. For a literal subject, that is whether pattern matches
// it. Covers returns false when either argument is not a ValidPattern.
func Covers(pattern, subject string) bool {
	if !ValidPattern(pattern) || !ValidPattern(subject) {
		return false
	}
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, t := range p {
		if i >= len(s) {
			return false
		}
		if t == ">" {
			return true
		}
		if s[i] == ">" {
			return false
		}
		if t != "*" && t != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

// Overlap reports whether some literal subject matches both a and b. It
// returns false when either argument is not a ValidPattern.
func Overlap(a, b string) bool {
	if !ValidPattern(a) || !ValidPattern(b) {
		return false
	}
	x, y := strings.Split(a, "."), strings.Split(b, ".")
	for i := range min(len(x), len(y)) {
		if x[i] == ">" || y[i] == ">" {
			return true
		}
		if x[i] != "*" && y[i] != "*" && x[i] != y[i] {
			return false
		}
	}
	return len(x) == len(y)
}

// inboxPrefix is the first token of nats.go's default request inbox.
const inboxPrefix = "_INBOX"

// Reserved reports whether s is in a namespace that belongs to the server or
// the platform, never to data (spec/subjects.md): its first '.'-separated
// token starts with '$' (the JetStream API and advisories under $JS, system
// events under $SYS, KV and object store internals under $KV and $O, this
// contract's status and admin subjects under $CONNECTOR, and whatever else
// is claimed the same way) or is "_INBOX", the default request inbox
// prefix. A connector never publishes data to such a subject, and a
// transform may not route to one. A custom inbox prefix is not recognised;
// a connector's data-subject patterns (transform.Guard) keep it out of reach.
//
// Only the first token is looked at, so Reserved works on literals and
// patterns alike ("$JS.>" and "$JS.API.STREAM.PURGE.X" are both reserved)
// and says nothing about validity: it is false for "" and for any other
// string whose first token is unreserved, valid or not.
func Reserved(s string) bool {
	first, _, _ := strings.Cut(s, ".")
	return strings.HasPrefix(first, "$") || first == inboxPrefix
}

// IsNotice reports whether s carries a reserved notice suffix
// (spec/transformer.md): its last '.'-separated token is SuffixError or
// SuffixOversize. Those terminal tokens on a data subject are reserved for
// the notices a connector publishes about that subject, so a transform may
// not route a message to one; the connector's own notices are published
// outside the transform. A single-token "error" or "oversize" counts: its
// last token is the suffix. IsNotice says nothing about validity.
func IsNotice(s string) bool {
	last := s
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		last = s[i+1:]
	}
	return last == SuffixError || last == SuffixOversize
}

// maxNameLen is nats-server's JSMaxNameLen, the longest stream or consumer
// name it accepts.
const maxNameLen = 255

// ValidStreamName reports whether s can name a stream. A stream name is a
// token of every JetStream API subject that addresses the stream, and the
// server's directory name for it, so it must be a ValidToken of at most
// 255 bytes, without '/' or '\'.
func ValidStreamName(s string) bool {
	return len(s) <= maxNameLen && ValidToken(s) && !strings.ContainsAny(s, `/\`)
}

// ValidConsumerName reports whether s can name a durable consumer, by the
// same rule as ValidStreamName.
func ValidConsumerName(s string) bool {
	return ValidStreamName(s)
}

// NoticeSubjects returns the subjects that carry notices with the given
// suffixes for data subject s (spec/transformer.md): s + "." + suffix, for
// each suffix. A pattern ending in '>' already matches its own notices, so
// for it NoticeSubjects returns nil, as it does when s is not a
// ValidPattern or a suffix is not a ValidToken. With no suffixes it returns
// an empty slice.
func NoticeSubjects(s string, suffixes ...string) []string {
	if !ValidPattern(s) || s == ">" || strings.HasSuffix(s, ".>") {
		return nil
	}
	out := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		if !ValidToken(suffix) {
			return nil
		}
		out = append(out, s+"."+suffix)
	}
	return out
}
