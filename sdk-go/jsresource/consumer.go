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

package jsresource

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// ConsumerSpec describes the durable pull consumer a connector reads from.
type ConsumerSpec struct {
	// Stream is the stream to read from. It must be a
	// subject.ValidStreamName.
	Stream string

	// Durable is the consumer's name. It must be a
	// subject.ValidConsumerName. identity.DurableName(id) is the default a
	// connector without a configured name should use.
	Durable string

	// Filters are the subjects the durable delivers, as literals or
	// patterns. Empty means every subject in the stream. No two may
	// overlap, and when the stream holds only the subjects it lists (it
	// mirrors, sources and transforms nothing), each filter must overlap
	// one of them.
	Filters []string

	// DeliverNew makes a durable EnsureConsumer creates deliver only
	// messages published after it is created. By default it starts at the
	// stream's first message (DeliverAllPolicy, the zero value, as in
	// nats.go). A Create.DeliverPolicy other than DeliverAllPolicy is used
	// as is and can't be combined with DeliverNew. A WorkQueue stream
	// accepts only DeliverAllPolicy.
	DeliverNew bool

	// Create is the template for a durable EnsureConsumer creates, for
	// settings such as MaxDeliver, AckWait and BackOff. It is not used for
	// an existing durable. EnsureConsumer sets its Durable and Name to
	// Durable and its filters to Filters. A field that would conflict is an
	// ErrInvalid error, not overwritten: its Durable and Name, if set, must
	// equal Durable, its filters, if set, must be the same set as Filters,
	// and its AckPolicy must be AckExplicitPolicy (the zero value).
	Create jetstream.ConsumerConfig

	// CreateStream, when not nil, is the template for creating the stream
	// when it doesn't exist. Its Name is set to Stream. It can't set NoAck,
	// Sealed or AllowMsgCounter, which make a stream a connector can't
	// publish into: a no-ack stream never acknowledges publishes, so a
	// connector publishing into it times out on every message; a counter
	// stream rejects every publish that isn't a counter increment; and a
	// sealed stream takes no publishes at all. Only the template is
	// checked: an existing no-ack stream is still read from. A template
	// that mirrors or sources another stream is used as is; otherwise
	// empty Subjects default to Filters, and listed Subjects must overlap
	// every filter. Either way, the subjects can't be ones the server
	// reserves for no-ack streams (see StreamSpec.Subjects). When nil, a
	// missing stream is an error that matches jetstream.ErrStreamNotFound.
	CreateStream *jetstream.StreamConfig

	// Timeout bounds each JetStream API request. Zero or negative means
	// DefaultTimeout.
	Timeout time.Duration
}

// ConsumerResult is what EnsureConsumer found or did. With an error,
// Consumer is nil, but Created and StreamCreated still report what the call
// created before it failed.
type ConsumerResult struct {
	// Consumer is the durable. Its CachedInfo().Config is its effective
	// configuration.
	Consumer jetstream.Consumer

	// Created reports that this call's create request succeeded. Two
	// instances racing with the same config both see true. A durable that
	// disappeared (an operator deleted it, or the server removed it after
	// its InactiveThreshold) is created again, from the start position in
	// the spec, not where the old one was: log it.
	Created bool

	// StreamCreated reports that this call created the stream, from
	// ConsumerSpec.CreateStream.
	StreamCreated bool
}

// EnsureConsumer returns the durable c describes, creating it from c.Create
// when it doesn't exist. Another instance creating it first is not an
// error: the durable it created is validated like any existing one.
//
// An existing durable is never changed. It must be a durable (not an
// ephemeral consumer of the same name, which the server deletes once it has
// no client for its InactiveThreshold), a pull consumer, with explicit acks,
// whose filters are exactly c.Filters, or ErrIncompatible names what
// differs. An unfiltered durable is also accepted when the
// stream holds only the subjects it lists and c.Filters covers all of
// them, since it then delivers the same messages; that is checked when
// EnsureConsumer runs, so a subject added to the stream later is delivered
// until the next start, when the durable is rejected.
//
// Every other setting is accepted as the operator left it, including its
// DeliverPolicy, PriorityGroups and PauseUntil: read it from
// ConsumerResult.Consumer.CachedInfo().Config,
// adopt what the connector should (MaxDeliver, AckWait), and reject what it
// can't work with (for example, HeadersOnly for a connector that needs
// payloads). The server reports an unlimited MaxDeliver as -1. A durable
// EnsureConsumer creates also takes the stream's ConsumerLimits, such as an
// InactiveThreshold after which the server removes it.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, c ConsumerSpec) (ConsumerResult, error) {
	if err := c.validate(); err != nil {
		return ConsumerResult{}, err
	}
	timeout := timeoutOr(c.Timeout)

	var res ConsumerResult
	// failed keeps the record of what this call created before it failed.
	failed := func(err error) (ConsumerResult, error) {
		return ConsumerResult{Created: res.Created, StreamCreated: res.StreamCreated}, err
	}
	st, err := lookupStream(ctx, js, c.Stream, timeout)
	if errors.Is(err, jetstream.ErrStreamNotFound) && c.CreateStream != nil {
		cfg := *c.CreateStream
		cfg.Name = c.Stream
		if len(cfg.Subjects) == 0 && cfg.Mirror == nil && len(cfg.Sources) == 0 {
			cfg.Subjects = slices.Clone(c.Filters)
		}
		st, res.StreamCreated, err = createStream(ctx, js, cfg, timeout)
	}
	if err != nil {
		return failed(err)
	}
	var streamSubjects []string
	if scfg := st.CachedInfo().Config; holdsOnlyListed(scfg) {
		streamSubjects = scfg.Subjects
	}
	if len(streamSubjects) > 0 {
		for _, f := range c.Filters {
			if !overlapsAny(streamSubjects, f) {
				return failed(fmt.Errorf("%w: stream %q has no subject matching filter %q", ErrNotCaptured, c.Stream, f))
			}
		}
	}

	res.Consumer, err = lookupConsumer(ctx, js, c.Stream, c.Durable, timeout)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		res.Consumer, res.Created, err = createConsumer(ctx, js, c, timeout)
	}
	if err != nil {
		return failed(err)
	}
	if err := checkConsumer(c, res.Consumer.CachedInfo().Config, streamSubjects); err != nil {
		return failed(err)
	}
	return res, nil
}

func (c ConsumerSpec) validate() error {
	if !subject.ValidStreamName(c.Stream) {
		return invalid("stream name %q is not valid (spec/subjects.md)", c.Stream)
	}
	if !subject.ValidConsumerName(c.Durable) {
		return invalid("durable name %q is not valid (spec/subjects.md)", c.Durable)
	}
	for i, f := range c.Filters {
		if !subject.ValidPattern(f) {
			return invalid("filter %q is not a valid subject or pattern (spec/subjects.md)", f)
		}
		for _, g := range c.Filters[:i] {
			if subject.Overlap(f, g) {
				return invalid("filters %q and %q overlap", g, f)
			}
		}
	}
	if tmpl := filtersOf(c.Create); len(tmpl) > 0 && !sameSet(tmpl, c.Filters) {
		return invalid("ConsumerSpec.Create filters %q differ from ConsumerSpec.Filters %q", tmpl, c.Filters)
	}
	if c.Create.AckPolicy != jetstream.AckExplicitPolicy {
		return invalid("ConsumerSpec.Create.AckPolicy is %s; EnsureConsumer creates explicit-ack consumers", c.Create.AckPolicy)
	}
	if c.Create.Name != "" && c.Create.Name != c.Durable {
		return invalid("ConsumerSpec.Create.Name %q differs from ConsumerSpec.Durable %q", c.Create.Name, c.Durable)
	}
	if c.Create.Durable != "" && c.Create.Durable != c.Durable {
		return invalid("ConsumerSpec.Create.Durable %q differs from ConsumerSpec.Durable %q", c.Create.Durable, c.Durable)
	}
	if c.Create.DeliverSubject != "" {
		return invalid("ConsumerSpec.Create.DeliverSubject is set; EnsureConsumer creates pull consumers")
	}
	if c.DeliverNew && c.Create.DeliverPolicy != jetstream.DeliverAllPolicy {
		return invalid("ConsumerSpec.DeliverNew conflicts with ConsumerSpec.Create.DeliverPolicy %s", c.Create.DeliverPolicy)
	}
	if cs := c.CreateStream; cs != nil {
		if cs.Name != "" && cs.Name != c.Stream {
			return invalid("ConsumerSpec.CreateStream.Name %q differs from ConsumerSpec.Stream %q", cs.Name, c.Stream)
		}
		// A counter stream that only sources other counter streams takes no
		// ordinary publishes, but it is refused too: allowing it later
		// breaks no caller, while refusing it later would.
		for _, f := range unpublishable {
			if !f.reader && f.set(cs) {
				return invalid("ConsumerSpec.CreateStream sets %s; EnsureConsumer doesn't create a stream a connector can't publish into", f.name)
			}
		}
		if cs.Mirror != nil && len(cs.Subjects) > 0 {
			return invalid("ConsumerSpec.CreateStream mirrors %q and lists subjects; a mirror can't have subjects", cs.Mirror.Name)
		}
		if len(cs.Subjects) == 0 && len(c.Filters) == 0 && cs.Mirror == nil && len(cs.Sources) == 0 {
			return invalid("ConsumerSpec.CreateStream has no subjects: set its Subjects or ConsumerSpec.Filters")
		}
		for _, p := range cs.Subjects {
			if !subject.ValidPattern(p) {
				return invalid("ConsumerSpec.CreateStream subject %q is not a valid subject or pattern (spec/subjects.md)", p)
			}
			if err := checkCapturable(p); err != nil {
				return err
			}
		}
		if len(cs.Subjects) == 0 && cs.Mirror == nil && len(cs.Sources) == 0 {
			// The stream would be created with the filters as its subjects.
			for _, f := range c.Filters {
				if err := checkCapturable(f); err != nil {
					return err
				}
			}
		}
		if holdsOnlyListed(*cs) && len(cs.Subjects) > 0 {
			for _, f := range c.Filters {
				if !overlapsAny(cs.Subjects, f) {
					return invalid("ConsumerSpec.CreateStream subjects %q don't overlap filter %q", cs.Subjects, f)
				}
			}
		}
	}
	return nil
}

// lookupConsumer returns the durable, or an error that matches
// jetstream.ErrConsumerNotFound when it does not exist.
func lookupConsumer(ctx context.Context, js jetstream.JetStream, stream, durable string, timeout time.Duration) (jetstream.Consumer, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	before := lastErr(js)
	cons, err := js.Consumer(ctx, stream, durable)
	switch {
	case errors.Is(err, jetstream.ErrNotPullConsumer):
		return nil, fmt.Errorf("%w: consumer %q on stream %q is a push consumer; it must be a pull consumer", ErrIncompatible, durable, stream)
	case err != nil:
		return nil, wrapErr(js, err, before, "CONSUMER.INFO."+stream+"."+durable, fmt.Sprintf("consumer %q on stream %q lookup", durable, stream))
	}
	return cons, nil
}

// createConsumer creates the durable from c.Create. When another instance
// created it after the caller's lookup, with a different config, it
// returns that durable and created == false.
func createConsumer(ctx context.Context, js jetstream.JetStream, c ConsumerSpec, timeout time.Duration) (cons jetstream.Consumer, created bool, err error) {
	cfg := c.Create
	cfg.Durable, cfg.Name = c.Durable, c.Durable
	cfg.FilterSubject, cfg.FilterSubjects = "", nil
	api := "CONSUMER.CREATE." + c.Stream + "." + c.Durable
	switch len(c.Filters) {
	case 0:
	case 1:
		cfg.FilterSubject = c.Filters[0]
		api += "." + c.Filters[0]
	default:
		cfg.FilterSubjects = slices.Clone(c.Filters)
	}
	cfg.AckPolicy = jetstream.AckExplicitPolicy
	if c.DeliverNew {
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	before := lastErr(js)
	cons, err = js.CreateConsumer(cctx, c.Stream, cfg)
	cancel()
	if errors.Is(err, jetstream.ErrConsumerExists) {
		cons, err = lookupConsumer(ctx, js, c.Stream, c.Durable, timeout)
		return cons, false, err
	}
	if err != nil {
		err = wrapErr(js, err, before, api, fmt.Sprintf("consumer %q on stream %q create", c.Durable, c.Stream))
		if errors.Is(err, ErrPermission) {
			// The lookup found no durable: say so, so that a mistyped name
			// doesn't read as a request for more permissions.
			err = fmt.Errorf("%w (the durable does not exist: %w)", err, jetstream.ErrConsumerNotFound)
		}
		return nil, false, err
	}
	return cons, true, nil
}

// checkConsumer rejects a durable the connector cannot read from as c
// describes. The push-consumer case is caught by lookupConsumer.
func checkConsumer(c ConsumerSpec, cfg jetstream.ConsumerConfig, streamSubjects []string) error {
	if cfg.Durable != c.Durable {
		return fmt.Errorf("%w: consumer %q on stream %q is not durable: the server deletes it once it has no client for %v (InactiveThreshold)", ErrIncompatible, c.Durable, c.Stream, cfg.InactiveThreshold)
	}
	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		return fmt.Errorf("%w: consumer %q on stream %q uses ack policy %s; it must use explicit acks", ErrIncompatible, c.Durable, c.Stream, cfg.AckPolicy)
	}
	have := filtersOf(cfg)
	if sameSet(have, c.Filters) {
		return nil
	}
	if len(have) == 0 && len(streamSubjects) > 0 && len(uncovered(c.Filters, streamSubjects)) == 0 {
		// Unfiltered, but every subject the stream holds is one the
		// connector asked for.
		return nil
	}
	return fmt.Errorf("%w: consumer %q on stream %q filters %q, want %q", ErrIncompatible, c.Durable, c.Stream, have, c.Filters)
}

// holdsOnlyListed reports whether a stream holds only the subjects it lists:
// one that mirrors or sources others can hold any subject, and one that
// transforms subjects stores the transformed ones.
func holdsOnlyListed(cfg jetstream.StreamConfig) bool {
	return cfg.Mirror == nil && len(cfg.Sources) == 0 && cfg.SubjectTransform == nil
}

// overlapsAny reports whether some pattern in patterns overlaps f.
func overlapsAny(patterns []string, f string) bool {
	return slices.ContainsFunc(patterns, func(p string) bool { return subject.Overlap(p, f) })
}

// filtersOf returns a consumer config's filters, from FilterSubject and
// FilterSubjects.
func filtersOf(cfg jetstream.ConsumerConfig) []string {
	if cfg.FilterSubject != "" {
		return append([]string{cfg.FilterSubject}, cfg.FilterSubjects...)
	}
	return cfg.FilterSubjects
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}
