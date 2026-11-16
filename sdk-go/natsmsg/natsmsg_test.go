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

package natsmsg_test

import (
	"errors"
	"strings"
	"testing"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/natsmsg"
)

// connect starts an in-process server with the given max_payload (0 keeps
// the default) and returns a client.
func connect(t *testing.T, maxPayload int32) *nats.Conn {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	if maxPayload > 0 {
		opts.MaxPayload = maxPayload
	}
	srv := natstest.RunServer(&opts)
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// TestHeaderLenMatchesNATS compares HeaderLen with the header bytes nats.go
// actually encodes, which nats.Msg.Size counts.
func TestHeaderLenMatchesNATS(t *testing.T) {
	for name, h := range map[string]nats.Header{
		"nil":           nil,
		"empty":         {},
		"one value":     {"Mongo-Op": {"insert"}},
		"several":       {"Mongo-Op": {"insert"}, "Mongo-Db": {"shop"}, "Nats-Msg-Id": {"abc-123"}},
		"multi-valued":  {"K": {"a", "bb", "ccc"}},
		"empty value":   {"K": {""}},
		"unicode value": {"K": {"żółć"}},
		// A name with no values sends no line, but the frame still goes.
		"no values":           {"K": {}},
		"nil values":          {"K": nil},
		"two keys, one empty": {"K": {}, "Nats-Msg-Id": {"abc-123"}},
	} {
		msg := &nats.Msg{Subject: "s", Data: []byte("payload"), Header: h}
		want := msg.Size() - len(msg.Subject) - len(msg.Data)
		if got := natsmsg.HeaderLen(h); got != want {
			t.Errorf("%s: HeaderLen = %d, nats.go encodes %d", name, got, want)
		}
	}
}

// TestHeaderLenIsAnUpperBound: nats.go trims a value's leading and trailing
// whitespace and replaces CR and LF with spaces, so HeaderLen may count more
// than is sent, never less.
func TestHeaderLenIsAnUpperBound(t *testing.T) {
	h := nats.Header{"K": {"  padded  ", "a\r\nb"}}
	msg := &nats.Msg{Subject: "s", Header: h}
	if got, sent := natsmsg.HeaderLen(h), msg.Size()-len(msg.Subject); got < sent {
		t.Errorf("HeaderLen = %d, below the %d bytes nats.go sends", got, sent)
	}
}

// TestFitsAtTheServerLimit publishes messages that Fits says fit, and that it
// says don't, at the limit a real server advertised. nats.go refuses an
// over-limit message itself, before sending it, using the same
// len(data)+len(header) the server does; flushing a message that fits makes
// it reach the server, so a server that counted differently would fail this.
func TestFitsAtTheServerLimit(t *testing.T) {
	const max = 1024
	nc := connect(t, max)
	h := nats.Header{"Nats-Msg-Id": {"id-1"}, "Mongo-Op": {"insert"}}
	limit := int(nc.MaxPayload())
	room := limit - natsmsg.HeaderLen(h)

	for _, tt := range []struct {
		size int
		fits bool
	}{
		{room, true},
		{room + 1, false},
	} {
		data := []byte(strings.Repeat("x", tt.size))
		if got := natsmsg.Fits(data, h, nc.MaxPayload()); got != tt.fits {
			t.Errorf("Fits(%d bytes) = %v, want %v", tt.size, got, tt.fits)
		}
		err := nc.PublishMsg(&nats.Msg{Subject: "s", Data: data, Header: h})
		if tt.fits {
			if err == nil {
				err = nc.Flush()
			}
			if err != nil {
				t.Errorf("publishing %d bytes that fit: %v", tt.size, err)
			}
		}
		if !tt.fits && !errors.Is(err, nats.ErrMaxPayload) {
			t.Errorf("publishing %d bytes that don't fit: err = %v, want ErrMaxPayload", tt.size, err)
		}
	}
}

// TestValidHeaderNameMatchesNATS checks every single-byte name, and a few
// longer ones, against whether nats.go will publish a header with that name.
func TestValidHeaderNameMatchesNATS(t *testing.T) {
	nc := connect(t, 0)
	names := []string{"", "Mongo-Id", "X-Long_Header.1", "with space", "a/b", "a:b", "ü"}
	for b := range 256 {
		names = append(names, string([]byte{byte(b)}))
	}
	for _, k := range names {
		err := nc.PublishMsg(&nats.Msg{Subject: "s", Header: nats.Header{k: {"v"}}})
		sent := !errors.Is(err, nats.ErrBadHeaderMsg)
		if got := natsmsg.ValidHeaderName(k); got != sent {
			t.Errorf("ValidHeaderName(%q) = %v, nats.go publishes it: %v (err %v)", k, got, sent, err)
		}
	}
}
