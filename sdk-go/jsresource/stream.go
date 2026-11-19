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
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
)

// UpdatePolicy says what EnsureStream may change on a stream that already
// exists.
type UpdatePolicy int

const (
	// UpdateNever validates an existing stream and never changes it. A
	// stream that doesn't capture a required subject fails with
	// ErrNotCaptured, naming the subjects an operator must add.
	UpdateNever UpdatePolicy = iota

	// UpdateAdditive appends the required subjects an existing stream
	// doesn't capture and, when StreamSpec.AtomicPublish is set, enables
	// AllowAtomicPublish. It changes nothing else, but a stream update is
	// not compare-and-swap: it sends back the whole configuration read just
	// before it, so a concurrent change to any field in that window is
	// reverted, and a concurrent update can drop the subjects this one
	// added. After each update, EnsureStream reads the stream back and
	// checks its subjects again, for up to three rounds. A round sends a
	// second update, without AllowAtomicPublish, when the server rejects
	// the first.
	UpdateAdditive
)

// maxUpdateAttempts bounds the updates UpdateAdditive sends before it
// gives up with ErrNotCaptured.
const maxUpdateAttempts = 3

// StreamSpec describes the stream a connector publishes into.
type StreamSpec struct {
	// Name is the stream's name. It must be a subject.ValidStreamName.
	Name string

	// Subjects are the data subjects the connector publishes to, as
	// literals or patterns. The stream must capture each of them. They
	// can't be subjects the server lets only a no-ack stream capture: ">"
	// and those overlapping $JS.>, $JSC.>, $NRG.> or $SYS.>, apart from
	// those within $JS.EVENT.> and $SYS.ACCOUNT.> (so not "*.orders",
	// which overlaps $JS.>).
	Subjects []string

	// Notices are the reserved suffixes the connector is configured to
	// publish notices with: subject.SuffixOversize, subject.SuffixError, or
	// both, and nothing else (spec/transformer.md). The stream must also
	// capture subject.NoticeSubjects(s, Notices...) for every s in
	// Subjects.
	Notices []string

	// AtomicPublish asks for AllowAtomicPublish when the server is 2.12 or
	// later: it is set on a stream EnsureStream creates, and enabled on an
	// existing stream under UpdateAdditive. An update that fails only
	// because of it is not an error (StreamResult.AtomicErr says why): read
	// the effective value from
	// StreamResult.Stream.CachedInfo().Config.AllowAtomicPublish. It is
	// part of the create request, though, so a template the server can't
	// combine with it (PersistMode async) fails the create.
	//
	// A credential that may not update the stream (spec/credentials.md) has
	// the update dropped, so it only times out: every start then waits one
	// full Timeout before returning the stream without the capability, for
	// as long as the stream lacks it. Budget startup for that, or leave
	// AtomicPublish off for such a credential.
	AtomicPublish bool

	// Update is the policy for an existing stream. The zero value is
	// UpdateNever.
	Update UpdatePolicy

	// Create is the template for a stream EnsureStream creates, for
	// settings such as retention, limits, storage and Replicas. It is not
	// used for an existing stream. Its Name, if set, must equal Name, and
	// it can't set NoAck, Sealed, Mirror, or AllowMsgCounter. When its
	// Subjects are empty, the stream is created with the required subjects;
	// otherwise they must capture every required subject.
	Create jetstream.StreamConfig

	// Timeout bounds each JetStream API request. Zero or negative means
	// DefaultTimeout.
	Timeout time.Duration
}

// StreamResult is what EnsureStream found or did. With an error, Stream is
// nil, but Created and Updated still report what the call changed before it
// failed.
type StreamResult struct {
	// Stream is the stream. Its CachedInfo().Config is the effective
	// configuration after any create or update.
	Stream jetstream.Stream

	// Created reports that this call's create request succeeded. Two
	// instances racing with the same template both see true.
	Created bool

	// Updated reports that EnsureStream changed an existing stream
	// (UpdateAdditive only).
	Updated bool

	// AtomicErr says why the stream is without AllowAtomicPublish after
	// EnsureStream asked for it for StreamSpec.AtomicPublish, in the create
	// request or an UpdateAdditive update: the update failed, or the server
	// did not keep it (it needs nats-server 2.12 or later). The stream is
	// still usable: log the error and publish without atomic batches. It is
	// nil when EnsureStream didn't ask (an existing stream under
	// UpdateNever, or a server older than 2.12) or the request worked.
	AtomicErr error
}

// EnsureStream returns the stream s describes, creating it from s.Create
// when it doesn't exist. Another instance creating it first is not an
// error: the stream it created is validated like any existing stream.
//
// An existing stream must be one a connector can publish into: it must
// acknowledge publishes (not NoAck), not be sealed, and not be a mirror,
// or EnsureStream fails with ErrIncompatible. It must capture every
// required subject (s.Subjects and their notice subjects): under
// UpdateNever, a stream that doesn't fails with ErrNotCaptured; under
// UpdateAdditive, the missing subjects are appended. A required pattern
// that partly overlaps a subject the stream already has can't be appended
// (the server rejects overlapping subjects in one stream), and the update
// fails.
func EnsureStream(ctx context.Context, js jetstream.JetStream, s StreamSpec) (StreamResult, error) {
	required, err := s.required()
	if err != nil {
		return StreamResult{}, err
	}
	timeout := timeoutOr(s.Timeout)

	res := StreamResult{}
	res.Stream, err = lookupStream(ctx, js, s.Name, timeout)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		cfg := s.Create
		cfg.Name = s.Name
		if len(cfg.Subjects) == 0 {
			cfg.Subjects = required
		}
		// An older server drops the field, or, in strict mode, rejects the
		// create for it.
		cfg.AllowAtomicPublish = cfg.AllowAtomicPublish || (s.AtomicPublish && atomicSupported(js))
		res.Stream, res.Created, err = createStream(ctx, js, cfg, timeout)
		if err == nil && res.Created && cfg.AllowAtomicPublish && !res.Stream.CachedInfo().Config.AllowAtomicPublish {
			res.AtomicErr = droppedAtomic(s.Name)
		}
	}
	if err != nil {
		return StreamResult{}, err
	}

	cfg := res.Stream.CachedInfo().Config
	if err := checkPublishable(cfg); err != nil {
		return StreamResult{}, err
	}
	if s.Update == UpdateAdditive && !res.Created {
		return updateAdditive(ctx, js, s, res, required, timeout)
	}
	if missing := uncovered(cfg.Subjects, required); len(missing) > 0 {
		return StreamResult{}, notCaptured(s.Name, missing)
	}
	return res, nil
}

// unpublishable lists the stream settings that make a stream a connector
// can't publish into. A StreamSpec.Create template can't set any of them. A
// ConsumerSpec.CreateStream template can't set any whose reader field is
// false: a reader may create a mirror, since nothing publishes into one.
var unpublishable = []struct {
	name   string
	set    func(*jetstream.StreamConfig) bool
	reader bool
}{
	{"NoAck", func(c *jetstream.StreamConfig) bool { return c.NoAck }, false},
	{"Sealed", func(c *jetstream.StreamConfig) bool { return c.Sealed }, false},
	{"Mirror", func(c *jetstream.StreamConfig) bool { return c.Mirror != nil }, true},
	{"AllowMsgCounter", func(c *jetstream.StreamConfig) bool { return c.AllowMsgCounter }, false},
}

// required validates s and returns the subjects the stream must capture:
// s.Subjects and their notice subjects, without any that another one
// covers.
func (s StreamSpec) required() ([]string, error) {
	if !subject.ValidStreamName(s.Name) {
		return nil, invalid("stream name %q is not valid (spec/subjects.md)", s.Name)
	}
	if s.Create.Name != "" && s.Create.Name != s.Name {
		return nil, invalid("StreamSpec.Create.Name %q differs from StreamSpec.Name %q", s.Create.Name, s.Name)
	}
	for _, f := range unpublishable {
		if f.set(&s.Create) {
			return nil, invalid("StreamSpec.Create for %q sets %s, which makes a stream a connector can't publish into", s.Name, f.name)
		}
	}
	if s.Update != UpdateNever && s.Update != UpdateAdditive {
		return nil, invalid("unknown update policy %d", s.Update)
	}
	if len(s.Subjects) == 0 {
		return nil, invalid("stream %q: no subjects", s.Name)
	}
	for _, n := range s.Notices {
		if n != subject.SuffixOversize && n != subject.SuffixError {
			return nil, invalid("notice suffix %q is not %q or %q (spec/transformer.md)", n, subject.SuffixOversize, subject.SuffixError)
		}
	}
	var all []string
	for _, subj := range s.Subjects {
		if !subject.ValidPattern(subj) {
			return nil, invalid("subject %q is not a valid subject or pattern (spec/subjects.md)", subj)
		}
		all = append(all, subj)
		all = append(all, subject.NoticeSubjects(subj, s.Notices...)...)
	}
	required := minimal(all)
	for i, a := range required {
		if err := checkCapturable(a); err != nil {
			return nil, err
		}
		for _, b := range required[:i] {
			if subject.Overlap(a, b) {
				return nil, invalid("subjects %q and %q overlap: list one pattern that covers both", b, a)
			}
		}
	}
	for _, p := range s.Create.Subjects {
		if !subject.ValidPattern(p) {
			return nil, invalid("StreamSpec.Create subject %q is not a valid subject or pattern (spec/subjects.md)", p)
		}
		if err := checkCapturable(p); err != nil {
			return nil, err
		}
	}
	if len(s.Create.Subjects) > 0 {
		if missing := uncovered(s.Create.Subjects, required); len(missing) > 0 {
			return nil, invalid("StreamSpec.Create.Subjects for %q do not capture %s", s.Name, strings.Join(missing, ", "))
		}
	}
	return required, nil
}

// checkCapturable rejects a subject that only a no-ack stream may capture,
// which this package never creates: nats-server reserves ">" and
// every subject overlapping $JS.>, $JSC.>, $NRG.> or $SYS.> for no-ack
// streams, apart from those within $JS.EVENT.> and $SYS.ACCOUNT.>.
func checkCapturable(subj string) error {
	reserved := ""
	for _, ns := range []string{"$JS.>", "$JSC.>", "$NRG.>"} {
		if subject.Overlap(subj, ns) && !subject.Covers("$JS.EVENT.>", subj) {
			reserved = ns
			break
		}
	}
	if reserved == "" && subject.Overlap(subj, "$SYS.>") && !subject.Covers("$SYS.ACCOUNT.>", subj) {
		reserved = "$SYS.>"
	}
	if reserved != "" {
		return invalid("subject %q overlaps the server's reserved %s, which only a no-ack stream may capture", subj, reserved)
	}
	return nil
}

// minimal drops every subject that another subject in the list covers,
// keeping the first of exact duplicates, so the list can be one stream's
// subjects.
func minimal(subjects []string) []string {
	var out []string
	for i, s := range subjects {
		redundant := false
		for j, other := range subjects {
			if i != j && subject.Covers(other, s) && (s != other || j < i) {
				redundant = true
				break
			}
		}
		if !redundant {
			out = append(out, s)
		}
	}
	return out
}

// uncovered returns the subjects in required that no pattern in have
// covers.
func uncovered(have, required []string) []string {
	var missing []string
	for _, r := range required {
		if !coveredBy(have, r) {
			missing = append(missing, r)
		}
	}
	return missing
}

func coveredBy(patterns []string, s string) bool {
	return slices.ContainsFunc(patterns, func(p string) bool { return subject.Covers(p, s) })
}

func notCaptured(stream string, missing []string) error {
	return fmt.Errorf("%w: stream %q does not capture %s", ErrNotCaptured, stream, strings.Join(missing, ", "))
}

// checkPublishable rejects a stream a connector cannot publish into.
func checkPublishable(cfg jetstream.StreamConfig) error {
	switch {
	case cfg.NoAck:
		return fmt.Errorf("%w: stream %q does not acknowledge publishes (no_ack)", ErrIncompatible, cfg.Name)
	case cfg.Sealed:
		return fmt.Errorf("%w: stream %q is sealed", ErrIncompatible, cfg.Name)
	case cfg.Mirror != nil:
		return fmt.Errorf("%w: stream %q is a mirror; publish to its origin", ErrIncompatible, cfg.Name)
	case cfg.AllowMsgCounter:
		return fmt.Errorf("%w: stream %q is a counter stream; every publish needs a Nats-Incr header", ErrIncompatible, cfg.Name)
	}
	return nil
}

func updateAdditive(ctx context.Context, js jetstream.JetStream, s StreamSpec, res StreamResult, required []string, timeout time.Duration) (StreamResult, error) {
	// failed keeps the record of an update an earlier round made.
	failed := func(err error) (StreamResult, error) { return StreamResult{Updated: res.Updated}, err }
	atomic := s.AtomicPublish && atomicSupported(js)
	for range maxUpdateAttempts {
		cfg := res.Stream.CachedInfo().Config
		missing := uncovered(cfg.Subjects, required)
		addAtomic := atomic && !cfg.AllowAtomicPublish
		if len(missing) == 0 && !addAtomic {
			return res, nil
		}
		for _, m := range missing {
			for _, e := range cfg.Subjects {
				if subject.Overlap(m, e) {
					return failed(fmt.Errorf("%w: stream %q does not capture %s, and it can't be appended: it overlaps %s, and a stream's subjects can't overlap", ErrNotCaptured, s.Name, m, e))
				}
			}
		}
		next := cfg
		next.Subjects = append(slices.Clone(cfg.Subjects), missing...)
		next.AllowAtomicPublish = cfg.AllowAtomicPublish || addAtomic
		err := updateStream(ctx, js, next, timeout)
		if err != nil && addAtomic {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// The caller's own deadline passed: success here would leave
				// it no time for what it does next.
				return failed(fmt.Errorf("jsresource: stream %q update: %w", s.Name, ctxErr))
			}
			if len(missing) == 0 {
				// Only the capability was missing, and it is best-effort.
				res.AtomicErr = err
				return res, nil
			}
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) {
				// A timeout, typically a request the credential may not
				// make: the server would drop the update without the
				// capability the same way.
				return failed(err)
			}
			// The server rejected the update, possibly for the capability
			// alone: retry with the subjects only, and don't ask again.
			res.AtomicErr = err
			atomic = false
			next.AllowAtomicPublish = false
			err = updateStream(ctx, js, next, timeout)
		}
		if err != nil {
			return failed(err)
		}
		res.Updated = true
		// Read it back: a concurrent update may have replaced this one.
		res.Stream, err = lookupStream(ctx, js, s.Name, timeout)
		if err != nil {
			return failed(err)
		}
		if addAtomic && atomic && !res.Stream.CachedInfo().Config.AllowAtomicPublish {
			// The server dropped it without an error: don't ask again.
			res.AtomicErr = droppedAtomic(s.Name)
			atomic = false
		}
	}
	if missing := uncovered(res.Stream.CachedInfo().Config.Subjects, required); len(missing) > 0 {
		return failed(fmt.Errorf("%w (still missing after %d rounds: a concurrent update keeps replacing them)", notCaptured(s.Name, missing), maxUpdateAttempts))
	}
	return res, nil
}

func updateStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	before := lastErr(js)
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return wrapErr(js, err, before, "STREAM.UPDATE."+cfg.Name, fmt.Sprintf("stream %q update", cfg.Name))
	}
	return nil
}

func droppedAtomic(stream string) error {
	return fmt.Errorf("jsresource: stream %q: the server did not keep AllowAtomicPublish (it needs nats-server 2.12 or later)", stream)
}

// atomicSupported reports whether the connected server stores
// AllowAtomicPublish (2.12 or later). An older server accepts the field
// and drops it, so asking would update the stream on every start. An
// unknown version counts as supported: the effective config is read back
// anyway. This is the server the client is connected to, not necessarily
// the JetStream meta leader; in a mixed-version cluster, an existing stream
// may go without the capability, which costs only throughput.
func atomicSupported(js jetstream.JetStream) bool {
	return versionAtLeast(serverVersion(js), 2, 12)
}

// serverVersion returns the version of the server js is connected to, or ""
// when it isn't known. Tests replace it to play an older server.
var serverVersion = func(js jetstream.JetStream) string {
	if nc := js.Conn(); nc != nil {
		return nc.ConnectedServerVersion()
	}
	return ""
}

// versionAtLeast reports whether version v ("2.14.3", "v2.12.0-RC.1") is
// at least major.minor. A version it can't parse counts as new enough.
func versionAtLeast(v string, major, minor int) bool {
	parts := strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3)
	if len(parts) < 2 {
		return true
	}
	maj, err1 := strconv.Atoi(parts[0])
	mnr, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return true
	}
	return maj > major || maj == major && mnr >= minor
}
