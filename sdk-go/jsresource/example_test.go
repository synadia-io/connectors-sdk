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
	"log"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// An inlet that publishes change events and oversize notices, and appends
// subjects for collections added on redeploy (as mongodb does).
func ExampleEnsureStream() {
	var js jetstream.JetStream // from jetstream.New
	res, err := jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name:          "SHOP",
		Subjects:      []string{"mongo.shop.orders", "mongo.shop.payments"},
		Notices:       []string{subject.SuffixOversize, subject.SuffixError},
		AtomicPublish: true,
		Update:        jsresource.UpdateAdditive,
		Create:        jetstream.StreamConfig{Replicas: 3},
	})
	if err != nil {
		log.Fatal(err) // in a connector: return it from Run, and the harness parks
	}
	if !res.Stream.CachedInfo().Config.AllowAtomicPublish {
		log.Print("atomic publish unavailable: publishing one message at a time")
	}
}

// An outlet that reads one subject through a durable named after the
// connector. A durable it creates starts at the stream's first message (the
// default; set DeliverNew to skip the backlog). It adopts the operator's
// MaxDeliver and refuses a durable that strips payloads.
func ExampleEnsureConsumer() {
	var js jetstream.JetStream // from jetstream.New
	var id identity.Identity   // from identity.DetectIdentity
	durable, err := identity.DurableName(id)
	if err != nil {
		log.Fatal(err)
	}
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream:  "EVENTS",
		Durable: durable,
		Filters: []string{"events.s3"},
		Create:  jetstream.ConsumerConfig{MaxDeliver: 5},
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.Created {
		log.Print("created the durable: it starts at the stream's first message")
	}
	cfg := res.Consumer.CachedInfo().Config
	if cfg.HeadersOnly {
		log.Fatal("the durable delivers headers only; this connector needs payloads")
	}
	if cfg.MaxDeliver == -1 { // the server reports an unlimited MaxDeliver as -1
		log.Print("redelivering without a limit")
	} else {
		log.Printf("redelivering up to %d times", cfg.MaxDeliver)
	}
}
