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

// Package natsmsg holds the NATS message rules connectors share: how many
// bytes a message takes against the server's max_payload, and which header
// names nats.go will send.
//
// Header values need no sanitizing: when nats.go encodes a message, it trims
// a value's leading and trailing whitespace (spaces, tabs, CR and LF) and
// replaces any CR or LF left inside with a space. That changes the value a
// consumer reads ("a\r\nb" arrives as "a  b"), so a connector that wants a
// different replacement makes it itself.
//
// The rules are those of nats.go v1.53.1. The tests check them against
// nats.go itself rather than a copy, so a nats.go upgrade that changes one
// fails them.
package natsmsg

import "github.com/nats-io/nats.go"

// HeaderLen returns the bytes h takes on the wire, which the server counts
// against max_payload: "NATS/1.0\r\n", a "Name: value\r\n" line per value,
// and a closing "\r\n". An empty header is not sent at all, so it is 0, but
// a header with names and no values still costs that 12-byte frame. It is
// exact unless a value has leading or trailing whitespace, which nats.go
// trims; then it counts a little more than is sent, never less.
func HeaderLen(h nats.Header) int {
	if len(h) == 0 {
		return 0
	}
	n := len("NATS/1.0\r\n") + len("\r\n")
	for name, values := range h {
		for _, v := range values {
			n += len(name) + len(": ") + len(v) + len("\r\n")
		}
	}
	return n
}

// Fits reports whether a message with this data and header fits within
// maxPayload, counting what nats.go and the server count: the data and the
// encoded header, not the subject. maxPayload is the connection's limit,
// nc.MaxPayload(). A JetStream stream's max_msg_size, when set, is a
// separate and possibly lower limit that the stream enforces at publish.
//
// A publisher that adds headers after this check, such as an atomic batch
// publisher, must count them too: pass h with them merged in (with values of
// the right length), or subtract HeaderLen of the merged header less
// HeaderLen(h) from maxPayload. Subtracting only the added lines misses the
// 12-byte frame when h is empty.
func Fits(data []byte, h nats.Header, maxPayload int64) bool {
	return int64(len(data))+int64(HeaderLen(h)) <= maxPayload
}

// ValidHeaderName reports whether nats.go will send a header with this name:
// non-empty, printable ASCII (33 to 126), and none of `"(),/:;<=>?@[\]{}`.
// A message with any other name fails to publish with nats.ErrBadHeaderMsg.
// It mirrors nats.go's own check, and TestValidHeaderNameMatchesNATS holds it
// to that check on every single-byte name.
func ValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if c < 33 || c > 126 {
			return false
		}
		switch c {
		case '"', '(', ')', ',', '/', ':', ';', '<', '=', '>', '?', '@', '[', '\\', ']', '{', '}':
			return false
		}
	}
	return true
}
