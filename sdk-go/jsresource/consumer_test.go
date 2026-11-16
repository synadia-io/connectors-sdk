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
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
)

func mustCreateConsumer(t *testing.T, js jetstream.JetStream, stream string, cfg jetstream.ConsumerConfig) {
	t.Helper()
	if _, err := js.CreateConsumer(context.Background(), stream, cfg); err != nil {
		t.Fatalf("CreateConsumer(%s): %v", cfg.Durable, err)
	}
}

func consumerConfig(t *testing.T, js jetstream.JetStream, stream, name string) jetstream.ConsumerConfig {
	t.Helper()
	cons, err := js.Consumer(context.Background(), stream, name)
	if err != nil {
		t.Fatalf("Consumer(%s): %v", name, err)
	}
	return cons.CachedInfo().Config
}

func TestEnsureConsumer_CreatesMissingDurable(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}})
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "EVENTS", Durable: "CONN-s3", Filters: []string{"events.s3"},
		Create: jetstream.ConsumerConfig{MaxDeliver: 5, AckWait: time.Minute},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if !res.Created || res.StreamCreated {
		t.Errorf("Created, StreamCreated = %v, %v, want true, false", res.Created, res.StreamCreated)
	}
	cfg := res.Consumer.CachedInfo().Config
	if cfg.Durable != "CONN-s3" || cfg.FilterSubject != "events.s3" || cfg.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("Durable, FilterSubject, AckPolicy = %q, %q, %s", cfg.Durable, cfg.FilterSubject, cfg.AckPolicy)
	}
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %s, want all by default", cfg.DeliverPolicy)
	}
	if cfg.MaxDeliver != 5 || cfg.AckWait != time.Minute {
		t.Errorf("MaxDeliver, AckWait = %d, %v; want the template's", cfg.MaxDeliver, cfg.AckWait)
	}
}

func TestEnsureConsumer_DeliverPolicyOnCreate(t *testing.T) {
	tests := []struct {
		name string
		c    jsresource.ConsumerSpec
		want jetstream.DeliverPolicy
	}{
		{"default", jsresource.ConsumerSpec{}, jetstream.DeliverAllPolicy},
		{"deliver new", jsresource.ConsumerSpec{DeliverNew: true}, jetstream.DeliverNewPolicy},
		{"explicit deliver all in the template stays all", jsresource.ConsumerSpec{Create: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy}}, jetstream.DeliverAllPolicy},
		{"template", jsresource.ConsumerSpec{Create: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverLastPolicy}}, jetstream.DeliverLastPolicy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
			c := tt.c
			c.Stream, c.Durable = "S", "D"
			res, err := jsresource.EnsureConsumer(context.Background(), js, c)
			if err != nil {
				t.Fatalf("EnsureConsumer: %v", err)
			}
			if got := res.Consumer.CachedInfo().Config.DeliverPolicy; got != tt.want {
				t.Errorf("DeliverPolicy = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestEnsureConsumer_MultipleFilters(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	c := jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.a", "e.b"}}
	res, err := jsresource.EnsureConsumer(context.Background(), js, c)
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if got := res.Consumer.CachedInfo().Config.FilterSubjects; !slices.Equal(got, c.Filters) {
		t.Errorf("FilterSubjects = %q, want %q", got, c.Filters)
	}
	// The same set in another order binds to the same durable.
	c.Filters = []string{"e.b", "e.a"}
	if _, err := jsresource.EnsureConsumer(context.Background(), js, c); err != nil {
		t.Fatalf("EnsureConsumer, filters reordered: %v", err)
	}
}

// TestEnsureConsumer_AcceptsOperatorSettings pins that everything but
// the pull, explicit-ack, and filter checks is the operator's, and is left
// exactly as it is.
func TestEnsureConsumer_AcceptsOperatorSettings(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}})
	op := jetstream.ConsumerConfig{
		Durable: "D", FilterSubject: "events.s3", AckPolicy: jetstream.AckExplicitPolicy,
		HeadersOnly: true, ReplayPolicy: jetstream.ReplayOriginalPolicy, InactiveThreshold: time.Hour,
		MaxDeliver: 7, MaxAckPending: 37, DeliverPolicy: jetstream.DeliverAllPolicy, Description: "operator",
	}
	mustCreateConsumer(t, js, "EVENTS", op)
	before := consumerConfig(t, js, "EVENTS", "D")

	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "EVENTS", Durable: "D", Filters: []string{"events.s3"},
		Create: jetstream.ConsumerConfig{MaxDeliver: 5},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	after := consumerConfig(t, js, "EVENTS", "D")
	if !after.HeadersOnly || after.ReplayPolicy != jetstream.ReplayOriginalPolicy || after.InactiveThreshold != time.Hour ||
		after.MaxDeliver != 7 || after.MaxAckPending != 37 || after.Description != "operator" {
		t.Errorf("consumer changed: before %+v, after %+v", before, after)
	}
	if res.Created {
		t.Error("Created = true for an existing durable")
	}
	if got := res.Consumer.CachedInfo().Config.MaxDeliver; got != 7 {
		t.Errorf("returned MaxDeliver = %d, want the existing durable's 7", got)
	}
}

func TestEnsureConsumer_RejectsIncompatibleDurables(t *testing.T) {
	tests := []struct {
		name    string
		stream  []string
		setup   func(t *testing.T, js jetstream.JetStream)
		filters []string
		want    string
	}{
		{"push", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			if _, err := js.CreatePushConsumer(context.Background(), "S", jetstream.ConsumerConfig{
				Durable: "D", DeliverSubject: "deliver.d", AckPolicy: jetstream.AckExplicitPolicy,
			}); err != nil {
				t.Fatalf("CreatePushConsumer: %v", err)
			}
		}, nil, "push consumer"},
		{"ack none", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", AckPolicy: jetstream.AckNonePolicy})
		}, nil, "ack policy"},
		{"ack all", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", AckPolicy: jetstream.AckAllPolicy})
		}, nil, "ack policy"},
		{"broader filter", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubject: "e.>", AckPolicy: jetstream.AckExplicitPolicy})
		}, []string{"e.x"}, "filters"},
		{"narrower filter", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubject: "e.x", AckPolicy: jetstream.AckExplicitPolicy})
		}, []string{"e.>"}, "filters"},
		{"filtered, want all", []string{"e.>"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubject: "e.x", AckPolicy: jetstream.AckExplicitPolicy})
		}, nil, "filters"},
		{"unfiltered on a shared stream", []string{"e.x", "other"}, func(t *testing.T, js jetstream.JetStream) {
			mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", AckPolicy: jetstream.AckExplicitPolicy})
		}, []string{"e.x"}, "filters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: tt.stream})
			tt.setup(t, js)
			_, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
				Stream: "S", Durable: "D", Filters: tt.filters,
			})
			if !errors.Is(err, jsresource.ErrIncompatible) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("EnsureConsumer error = %v, want ErrIncompatible naming %q", err, tt.want)
			}
		})
	}
}

// TestEnsureConsumer_AcceptsUnfilteredDurableOnItsOwnStream: an unfiltered
// durable on a stream that holds only the requested subjects delivers the
// same messages as a filtered one.
func TestEnsureConsumer_AcceptsUnfilteredDurableOnItsOwnStream(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"events.s3"}})
	mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", AckPolicy: jetstream.AckExplicitPolicy})
	if _, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"events.s3"},
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
}

func TestEnsureConsumer_FilterOutsideStream(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"other.>"}})
	_, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"events.s3"},
	})
	if !errors.Is(err, jsresource.ErrNotCaptured) || !strings.Contains(err.Error(), "events.s3") {
		t.Fatalf("EnsureConsumer error = %v, want ErrNotCaptured naming events.s3", err)
	}
	if _, err := js.Consumer(context.Background(), "S", "D"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Errorf("durable created for a filter the stream can't hold (err %v)", err)
	}
}

// TestEnsureConsumer_ReadsAMirror: a stream that lists no subjects can hold
// any subject, so a filter is not checked against it.
func TestEnsureConsumer_ReadsAMirror(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "ORIGIN", Subjects: []string{"events.>"}})
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "MIRROR", Mirror: &jetstream.StreamSource{Name: "ORIGIN"}})
	if _, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "MIRROR", Durable: "D", Filters: []string{"events.s3"},
	}); err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
}

// TestEnsureConsumer_ReadsAStreamThatSources: a stream that sources another
// holds subjects it doesn't list. A filter is not checked against the listed
// subjects, and an unfiltered durable is not taken to deliver only them.
func TestEnsureConsumer_ReadsAStreamThatSources(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "ORIGIN", Subjects: []string{"events.>"}})
	mustCreateStream(t, js, jetstream.StreamConfig{
		Name: "S", Subjects: []string{"local.>"}, Sources: []*jetstream.StreamSource{{Name: "ORIGIN"}},
	})
	if _, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"events.s3"},
	}); err != nil {
		t.Fatalf("EnsureConsumer, sourced subject: %v", err)
	}

	mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "U", AckPolicy: jetstream.AckExplicitPolicy})
	_, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "U", Filters: []string{"local.>"},
	})
	if !errors.Is(err, jsresource.ErrIncompatible) {
		t.Fatalf("EnsureConsumer, unfiltered durable on a sourcing stream: error = %v, want ErrIncompatible", err)
	}
}

// TestEnsureConsumer_ReadsAStreamWithSubjectTransform: the server stores the
// transformed subject, so the subjects the stream lists say nothing about
// what a filter can match.
func TestEnsureConsumer_ReadsAStreamWithSubjectTransform(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{
		Name: "T", Subjects: []string{"in.>"},
		SubjectTransform: &jetstream.SubjectTransformConfig{Source: "in.>", Destination: "out.>"},
	})
	if _, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "T", Durable: "D", Filters: []string{"out.x"},
	}); err != nil {
		t.Fatalf("EnsureConsumer, transformed subject: %v", err)
	}

	mustCreateConsumer(t, js, "T", jetstream.ConsumerConfig{Durable: "U", AckPolicy: jetstream.AckExplicitPolicy})
	_, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "T", Durable: "U", Filters: []string{"in.>"},
	})
	if !errors.Is(err, jsresource.ErrIncompatible) {
		t.Fatalf("EnsureConsumer, unfiltered durable on a transforming stream: error = %v, want ErrIncompatible", err)
	}
}

// TestEnsureConsumer_CreateStreamFromAMirrorOrSources: a template that
// mirrors or sources another stream gets no subjects (the server rejects
// subjects on a mirror, and the sources already deliver the filter's
// subjects), and the filter is not checked against the empty list.
func TestEnsureConsumer_CreateStreamFromAMirrorOrSources(t *testing.T) {
	for name, tmpl := range map[string]jetstream.StreamConfig{
		"mirror":  {Mirror: &jetstream.StreamSource{Name: "ORIGIN"}},
		"sources": {Sources: []*jetstream.StreamSource{{Name: "ORIGIN"}}},
	} {
		t.Run(name, func(t *testing.T) {
			js := startJetStream(t)
			mustCreateStream(t, js, jetstream.StreamConfig{Name: "ORIGIN", Subjects: []string{"events.>"}})
			res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
				Stream: "COPY", Durable: "D", Filters: []string{"events.s3"}, CreateStream: &tmpl,
			})
			if err != nil {
				t.Fatalf("EnsureConsumer: %v", err)
			}
			if !res.StreamCreated {
				t.Error("StreamCreated = false, want true")
			}
			if got := streamConfig(t, js, "COPY").Subjects; len(got) != 0 {
				t.Errorf("Subjects = %q, want none", got)
			}
		})
	}
}

// TestEnsureConsumer_TemplateFiltersEqualToFiltersAreAccepted: an author
// porting a ConsumerConfig that already names the filters gets them, and the
// durable carries them once (FilterSubject and FilterSubjects together are
// a server error).
func TestEnsureConsumer_TemplateFiltersEqualToFiltersAreAccepted(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"},
		Create: jetstream.ConsumerConfig{FilterSubjects: []string{"e.x"}},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if cfg := res.Consumer.CachedInfo().Config; cfg.FilterSubject != "e.x" || len(cfg.FilterSubjects) != 0 {
		t.Errorf("FilterSubject, FilterSubjects = %q, %q; want e.x once", cfg.FilterSubject, cfg.FilterSubjects)
	}
}

// TestEnsureConsumer_ConcurrentStreamCreate: another instance created the
// stream, with other subjects, after this one's lookup. The filter is
// checked against the subjects of the stream that won.
func TestEnsureConsumer_ConcurrentStreamCreate(t *testing.T) {
	t.Run("winner holds the filter", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
		res, err := jsresource.EnsureConsumer(context.Background(), &hiddenStreamJS{JetStream: js}, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{},
		})
		if err != nil {
			t.Fatalf("EnsureConsumer: %v", err)
		}
		if res.StreamCreated || !res.Created {
			t.Errorf("StreamCreated, Created = %v, %v, want false, true", res.StreamCreated, res.Created)
		}
	})
	t.Run("winner does not hold the filter", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"other"}})
		_, err := jsresource.EnsureConsumer(context.Background(), &hiddenStreamJS{JetStream: js}, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{},
		})
		if !errors.Is(err, jsresource.ErrNotCaptured) {
			t.Fatalf("EnsureConsumer error = %v, want ErrNotCaptured", err)
		}
	})
}

// TestEnsureConsumer_OneFilterInFilterSubjects: an existing durable whose
// single filter is in FilterSubjects (plural) matches one Filters entry.
func TestEnsureConsumer_OneFilterInFilterSubjects(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubjects: []string{"e.x"}})
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"},
	})
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if res.Created {
		t.Error("Created = true for an existing durable")
	}
}

func TestEnsureConsumer_MissingStream(t *testing.T) {
	js := startJetStream(t)
	c := jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"events.s3"}}
	if _, err := jsresource.EnsureConsumer(context.Background(), js, c); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("EnsureConsumer error = %v, want ErrStreamNotFound", err)
	}

	c.CreateStream = &jetstream.StreamConfig{MaxAge: time.Hour}
	res, err := jsresource.EnsureConsumer(context.Background(), js, c)
	if err != nil {
		t.Fatalf("EnsureConsumer with CreateStream: %v", err)
	}
	if !res.StreamCreated || !res.Created {
		t.Errorf("StreamCreated, Created = %v, %v, want true, true", res.StreamCreated, res.Created)
	}
	cfg := streamConfig(t, js, "S")
	if !slices.Equal(cfg.Subjects, []string{"events.s3"}) || cfg.MaxAge != time.Hour {
		t.Errorf("Subjects, MaxAge = %q, %v; want the filters and the template's", cfg.Subjects, cfg.MaxAge)
	}
}

// TestEnsureConsumer_ConcurrentCreateValidatesTheWinner: another instance
// created the durable, with a different config, after this one's lookup.
func TestEnsureConsumer_ConcurrentCreateValidatesTheWinner(t *testing.T) {
	t.Run("winner compatible", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
		mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubject: "e.x", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 7})
		res, err := jsresource.EnsureConsumer(context.Background(), &hiddenConsumerJS{JetStream: js}, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D", Filters: []string{"e.x"}, Create: jetstream.ConsumerConfig{MaxDeliver: 5},
		})
		if err != nil {
			t.Fatalf("EnsureConsumer: %v", err)
		}
		if res.Created {
			t.Error("Created = true, want false: another instance created it")
		}
		if got := res.Consumer.CachedInfo().Config.MaxDeliver; got != 7 {
			t.Errorf("MaxDeliver = %d, want the winner's 7", got)
		}
	})
	t.Run("winner incompatible", func(t *testing.T) {
		js := startJetStream(t)
		mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
		mustCreateConsumer(t, js, "S", jetstream.ConsumerConfig{Durable: "D", FilterSubject: "e.y", AckPolicy: jetstream.AckExplicitPolicy})
		_, err := jsresource.EnsureConsumer(context.Background(), &hiddenConsumerJS{JetStream: js}, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D", Filters: []string{"e.x"},
		})
		if !errors.Is(err, jsresource.ErrIncompatible) {
			t.Fatalf("EnsureConsumer error = %v, want ErrIncompatible", err)
		}
	})
}

func TestEnsureConsumer_RejectsInvalidInput(t *testing.T) {
	// The rows share one server: each is rejected before any request is
	// sent. A row that reached the server would leak state into the next.
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	tests := []struct {
		name string
		c    jsresource.ConsumerSpec
		want string // in the error, so a row can't pass on another rule
	}{
		{"bad stream", jsresource.ConsumerSpec{Stream: "a.b", Durable: "D"}, `stream name "a.b"`},
		{"bad durable", jsresource.ConsumerSpec{Stream: "S", Durable: "a/b"}, `durable name "a/b"`},
		{"bad filter", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e..x"}}, `filter "e..x"`},
		{"overlapping filters", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.*", "e.x"}}, "overlap"},
		{"template name differs", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Create: jetstream.ConsumerConfig{Name: "E"}}, "Create.Name"},
		{"template durable differs", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Create: jetstream.ConsumerConfig{Durable: "E"}}, "Create.Durable"},
		{"stream template without subjects", jsresource.ConsumerSpec{Stream: "S", Durable: "D", CreateStream: &jetstream.StreamConfig{}}, "has no subjects"},
		{"template filter differs", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, Create: jetstream.ConsumerConfig{FilterSubject: "e.y"}}, "differ from ConsumerSpec.Filters"},
		{"template filter without filters", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Create: jetstream.ConsumerConfig{FilterSubject: "e.x"}}, "differ from ConsumerSpec.Filters"},
		{"template ack policy", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Create: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy}}, "AckPolicy"},
		{"stream template doesn't overlap a filter", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"events.s3"}, CreateStream: &jetstream.StreamConfig{Subjects: []string{"other.>"}}}, "don't overlap filter"},
		{"stream template subject invalid", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{Subjects: []string{"e.>", "e..x"}}}, `CreateStream subject "e..x"`},
		{"mirror template with subjects", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{Subjects: []string{"e.>"}, Mirror: &jetstream.StreamSource{Name: "O"}}}, "a mirror can't have subjects"},
		{"push template", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Create: jetstream.ConsumerConfig{DeliverSubject: "x"}}, "DeliverSubject"},
		{"deliver conflict", jsresource.ConsumerSpec{Stream: "S", Durable: "D", DeliverNew: true, Create: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverLastPolicy}}, "DeliverNew conflicts"},
		{"stream template name differs", jsresource.ConsumerSpec{Stream: "S", Durable: "D", CreateStream: &jetstream.StreamConfig{Name: "T", Subjects: []string{"e.>"}}}, "CreateStream.Name"},
		{"stream template subject only a no-ack stream may capture", jsresource.ConsumerSpec{Stream: "S", Durable: "D", CreateStream: &jetstream.StreamConfig{Subjects: []string{">"}}}, "reserved"},
		{"filter a created stream would capture, reserved", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"*.x"}, CreateStream: &jetstream.StreamConfig{}}, "reserved $JS.>"},
		{"no-ack stream template", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{NoAck: true}}, "sets NoAck"},
		{"no-ack stream template on a reserved subject", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{NoAck: true, Subjects: []string{">"}}}, "sets NoAck"},
		{"sealed stream template", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{Sealed: true}}, "sets Sealed"},
		{"counter stream template", jsresource.ConsumerSpec{Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{AllowMsgCounter: true}}, "sets AllowMsgCounter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := jsresource.EnsureConsumer(context.Background(), js, tt.c); !errors.Is(err, jsresource.ErrInvalid) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("EnsureConsumer error = %v, want ErrInvalid saying %q", err, tt.want)
			}
		})
	}
	st, err := js.Stream(context.Background(), "S")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	names := st.ConsumerNames(context.Background())
	for name := range names.Name() {
		t.Errorf("consumer %q exists after rejected input", name)
	}
	if err := names.Err(); err != nil {
		t.Fatalf("ConsumerNames: %v", err)
	}
}

func TestEnsureConsumer_NamesTheDeniedPermission(t *testing.T) {
	admin, limited := startLimited(t, "$JS.API.CONSUMER.CREATE.>")
	mustCreateStream(t, admin, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	_, err := jsresource.EnsureConsumer(context.Background(), limited, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"}, Timeout: time.Second,
	})
	if !errors.Is(err, jsresource.ErrPermission) || !strings.Contains(err.Error(), "$JS.API.CONSUMER.CREATE.S.D.e.x") {
		t.Fatalf("EnsureConsumer error = %v, want ErrPermission naming $JS.API.CONSUMER.CREATE.S.D.e.x", err)
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Errorf("EnsureConsumer error = %v, want it to match ErrConsumerNotFound too", err)
	}

	// The server quotes the denied subject, so a '"' in a filter (a valid
	// token character) arrives escaped.
	_, err = jsresource.EnsureConsumer(context.Background(), limited, jsresource.ConsumerSpec{
		Stream: "S", Durable: "Q", Filters: []string{`e.a"b`}, Timeout: time.Second,
	})
	if !errors.Is(err, jsresource.ErrPermission) {
		t.Fatalf("EnsureConsumer with a quote in the filter: error = %v, want ErrPermission", err)
	}
}

// TestEnsureConsumer_RejectsAnEphemeral: a consumer created with only a Name
// is ephemeral. The server deletes it once it has had no client for a few
// seconds, and the next start would create it again from the spec's start
// position, so it is not a durable to read from.
func TestEnsureConsumer_RejectsAnEphemeral(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}})
	if _, err := js.CreateConsumer(context.Background(), "S", jetstream.ConsumerConfig{Name: "D", FilterSubject: "e.x"}); err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	_, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"},
	})
	if !errors.Is(err, jsresource.ErrIncompatible) || !strings.Contains(err.Error(), "not durable") {
		t.Fatalf("EnsureConsumer error = %v, want ErrIncompatible saying it is not durable", err)
	}
}

// TestEnsureConsumer_ErrorKeepsStreamCreated: the stream was created, and
// then creating the durable failed. The error comes with StreamCreated, so
// the caller can still log the stream it made.
func TestEnsureConsumer_ErrorKeepsStreamCreated(t *testing.T) {
	js := timeoutJS{JetStream: startJetStream(t), t: t, fail: "CreateConsumer"}
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{},
	})
	if !errors.Is(err, context.DeadlineExceeded) || !res.StreamCreated || res.Consumer != nil {
		t.Errorf("EnsureConsumer = %+v, %v; want StreamCreated, no consumer, and the timeout", res, err)
	}
}

// TestEnsureConsumer_ReadsAnExistingNoAckStream: EnsureConsumer won't create
// a no-ack stream, but it reads from one an operator made. The CreateStream
// template is not used, since the stream exists.
func TestEnsureConsumer_ReadsAnExistingNoAckStream(t *testing.T) {
	js := startJetStream(t)
	mustCreateStream(t, js, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.>"}, NoAck: true})
	res, err := jsresource.EnsureConsumer(context.Background(), js, jsresource.ConsumerSpec{
		Stream: "S", Durable: "D", Filters: []string{"e.x"}, CreateStream: &jetstream.StreamConfig{},
	})
	if err != nil || !res.Created || res.StreamCreated {
		t.Fatalf("EnsureConsumer = %+v, %v; want the durable created on the existing stream", res, err)
	}
}
