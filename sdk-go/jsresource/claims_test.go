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

package jsresource_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
)

// startClaims boots an in-process JetStream server and connects as a user
// who may publish only to the "events.>" data subjects and to allow, and
// subscribe only to "_INBOX.>".
func startClaims(t *testing.T, allow ...string) jetstream.JetStream {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.Users = []*server.User{{Username: "conn", Password: "conn", Permissions: &server.Permissions{
		Publish:   &server.SubjectPermission{Allow: append([]string{"events.>"}, allow...)},
		Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
	}}}
	return connect(t, runServer(t, &opts), nats.UserInfo("conn", "conn"))
}

// TestClaimSetIsEnough runs a source and a sink under credentials that
// hold exactly the JS API subjects spec/credentials.md (D-10) lists, and
// nothing else. If the SDK starts making another request, this fails, and
// the claim set must grow with it.
func TestClaimSetIsEnough(t *testing.T) {
	ctx := context.Background()
	source := []string{"$JS.API.STREAM.INFO.S", "$JS.API.STREAM.CREATE.S"}

	t.Run("source", func(t *testing.T) {
		js := startClaims(t, source...)
		for range 2 { // create, then find it on the next start
			if _, err := jsresource.EnsureStream(ctx, js, jsresource.StreamSpec{
				Name: "S", Subjects: []string{"events.s3"}, Timeout: time.Second,
			}); err != nil {
				t.Fatalf("EnsureStream: %v", err)
			}
		}
		if _, err := js.Publish(ctx, "events.s3", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	})

	t.Run("source that reconciles", func(t *testing.T) {
		js := startClaims(t, append(source, "$JS.API.STREAM.UPDATE.S")...)
		for _, subj := range []string{"events.a", "events.b"} {
			if _, err := jsresource.EnsureStream(ctx, js, jsresource.StreamSpec{
				Name: "S", Subjects: []string{subj}, AtomicPublish: true,
				Update: jsresource.UpdateAdditive, Timeout: time.Second,
			}); err != nil {
				t.Fatalf("EnsureStream(%s): %v", subj, err)
			}
		}
	})

	t.Run("sink", func(t *testing.T) {
		js := startClaims(t, append(source,
			"$JS.API.CONSUMER.INFO.S.D",
			// D has one filter, so its create request names it.
			"$JS.API.CONSUMER.CREATE.S.D.events.s3",
			"$JS.API.CONSUMER.MSG.NEXT.S.D",
			"$JS.ACK.S.D.>",
			// D2 has two filters, so its create request names no filter.
			"$JS.API.CONSUMER.INFO.S.D2",
			"$JS.API.CONSUMER.CREATE.S.D2",
		)...)
		if _, err := jsresource.EnsureStream(ctx, js, jsresource.StreamSpec{
			Name: "S", Subjects: []string{"events.s3", "events.s4"}, Timeout: time.Second,
		}); err != nil {
			t.Fatalf("EnsureStream: %v", err)
		}
		if _, err := js.Publish(ctx, "events.s3", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		var cons jetstream.Consumer
		for range 2 { // create, then find it on the next start
			res, err := jsresource.EnsureConsumer(ctx, js, jsresource.ConsumerSpec{
				Stream: "S", Durable: "D", Filters: []string{"events.s3"}, Timeout: time.Second,
			})
			if err != nil {
				t.Fatalf("EnsureConsumer: %v", err)
			}
			cons = res.Consumer
		}
		if _, err := jsresource.EnsureConsumer(ctx, js, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D2", Filters: []string{"events.s3", "events.s4"}, Timeout: time.Second,
		}); err != nil {
			t.Fatalf("EnsureConsumer, two filters: %v", err)
		}
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		n := 0
		for msg := range batch.Messages() {
			if err := msg.DoubleAck(ctx); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			n++
		}
		if err := batch.Error(); err != nil || n != 1 {
			t.Fatalf("Fetch got %d messages, err %v; want 1", n, err)
		}
	})
}
