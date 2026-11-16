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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
)

// fakeJS answers Stream, CreateStream and UpdateStream from memory, to play
// servers a real 2.14 server can't: one that drops AllowAtomicPublish
// without an error (before 2.12), one that rejects an update, one that never
// answers an update asking for AllowAtomicPublish, or a slow one. Like
// nats.go, it fails a call whose context is already done. Any other
// JetStream method panics.
type fakeJS struct {
	jetstream.JetStream
	streams    map[string]jetstream.StreamConfig
	updates    []jetstream.StreamConfig
	updateErrs []error // popped per UpdateStream call; nil means success
	dropAtomic bool
	hangAtomic bool          // an update asking for AllowAtomicPublish waits out its context
	delay      time.Duration // every call takes this long, or until its context is done
}

func (f *fakeJS) Conn() *nats.Conn { return nil }

// wait fails at once on a done context, and otherwise takes f.delay.
func (f *fakeJS) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fakeJS) store(cfg jetstream.StreamConfig) jetstream.Stream {
	if f.dropAtomic {
		cfg.AllowAtomicPublish = false
	}
	f.streams[cfg.Name] = cfg
	return fakeStream{info: &jetstream.StreamInfo{Config: cfg}}
}

func (f *fakeJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	cfg, ok := f.streams[name]
	if !ok {
		return nil, jetstream.ErrStreamNotFound
	}
	return fakeStream{info: &jetstream.StreamInfo{Config: cfg}}, nil
}

func (f *fakeJS) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return f.store(cfg), nil
}

func (f *fakeJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.updates = append(f.updates, cfg)
	if f.hangAtomic && cfg.AllowAtomicPublish {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	if len(f.updateErrs) > 0 {
		err := f.updateErrs[0]
		f.updateErrs = f.updateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return f.store(cfg), nil
}

type fakeStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (s fakeStream) CachedInfo() *jetstream.StreamInfo { return s.info }

// rejected is how the server answers an update it refuses.
var rejected = &jetstream.APIError{Code: 400, ErrorCode: 10052, Description: "invalid stream config"}

func ensureAtomic(js jetstream.JetStream) (jsresource.StreamResult, error) {
	return jsresource.EnsureStream(context.Background(), js, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.a", "e.b"}, AtomicPublish: true, Update: jsresource.UpdateAdditive,
	})
}

// TestEnsureStream_AtomicDroppedSilently: a server before 2.12 accepts
// AllowAtomicPublish and drops it. EnsureStream reports it off, says why in
// AtomicErr, and asks once, not once per round.
func TestEnsureStream_AtomicDroppedSilently(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		f := &fakeJS{streams: map[string]jetstream.StreamConfig{}, dropAtomic: true}
		res, err := ensureAtomic(f)
		if err != nil {
			t.Fatalf("EnsureStream: %v", err)
		}
		if res.Stream.CachedInfo().Config.AllowAtomicPublish || len(f.updates) != 0 || res.AtomicErr == nil {
			t.Errorf("AllowAtomicPublish = %v after %d updates, AtomicErr = %v; want false after none, with an AtomicErr",
				res.Stream.CachedInfo().Config.AllowAtomicPublish, len(f.updates), res.AtomicErr)
		}
	})
	t.Run("update", func(t *testing.T) {
		f := &fakeJS{streams: map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a"}}}, dropAtomic: true}
		res, err := ensureAtomic(f)
		if err != nil {
			t.Fatalf("EnsureStream: %v", err)
		}
		if res.Stream.CachedInfo().Config.AllowAtomicPublish || len(f.updates) != 1 || res.AtomicErr == nil {
			t.Errorf("AllowAtomicPublish = %v after %d updates, AtomicErr = %v; want false after 1, with an AtomicErr",
				res.Stream.CachedInfo().Config.AllowAtomicPublish, len(f.updates), res.AtomicErr)
		}
	})
}

// TestEnsureStream_AtomicRejectedRetriesWithoutIt: the server rejects the
// update, possibly because of the capability. The subjects still go in,
// without it, and AtomicErr carries the rejection.
func TestEnsureStream_AtomicRejectedRetriesWithoutIt(t *testing.T) {
	f := &fakeJS{
		streams:    map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a"}}},
		updateErrs: []error{rejected},
	}
	res, err := ensureAtomic(f)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	cfg := res.Stream.CachedInfo().Config
	if !slices.Equal(cfg.Subjects, []string{"e.a", "e.b"}) || cfg.AllowAtomicPublish {
		t.Errorf("Subjects, AllowAtomicPublish = %q, %v; want [e.a e.b], false", cfg.Subjects, cfg.AllowAtomicPublish)
	}
	if len(f.updates) != 2 || !f.updates[0].AllowAtomicPublish || f.updates[1].AllowAtomicPublish {
		t.Errorf("updates = %+v, want one with the capability, then one without", f.updates)
	}
	if !errors.Is(res.AtomicErr, rejected) {
		t.Errorf("AtomicErr = %v, want the rejection", res.AtomicErr)
	}
}

// TestEnsureStream_AtomicBestEffortReportsErr: only the capability is
// missing, and the server rejects enabling it. That is not an error, but
// AtomicErr says why the stream is without it.
func TestEnsureStream_AtomicBestEffortReportsErr(t *testing.T) {
	f := &fakeJS{
		streams:    map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a", "e.b"}}},
		updateErrs: []error{rejected},
	}
	res, err := ensureAtomic(f)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if res.Updated || res.Stream.CachedInfo().Config.AllowAtomicPublish || !errors.Is(res.AtomicErr, rejected) {
		t.Errorf("Updated = %v, AllowAtomicPublish = %v, AtomicErr = %v; want false, false, the rejection",
			res.Updated, res.Stream.CachedInfo().Config.AllowAtomicPublish, res.AtomicErr)
	}
}

// TestEnsureStream_AtomicBestEffortReturnsCtxErr: the best-effort path must
// not report success once the caller's own deadline has passed; the caller
// has no time left for whatever it does next.
func TestEnsureStream_AtomicBestEffortReturnsCtxErr(t *testing.T) {
	f := &fakeJS{streams: map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a", "e.b"}}}, hangAtomic: true}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := jsresource.EnsureStream(ctx, f, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.a", "e.b"}, AtomicPublish: true,
		Update: jsresource.UpdateAdditive, Timeout: 5 * time.Second,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EnsureStream error = %v, want the caller's deadline", err)
	}
}

// TestEnsureStream_UpdateFailureNeverDowngradesAtomic: when the stream
// already has the capability, a failed update is an error, never a retry
// that would switch it off for every other producer.
func TestEnsureStream_UpdateFailureNeverDowngradesAtomic(t *testing.T) {
	f := &fakeJS{
		streams:    map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a"}, AllowAtomicPublish: true}},
		updateErrs: []error{rejected},
	}
	if _, err := ensureAtomic(f); err == nil {
		t.Fatal("EnsureStream error = nil, want the update failure")
	}
	for _, u := range f.updates {
		if !u.AllowAtomicPublish {
			t.Errorf("an update switched AllowAtomicPublish off: %+v", u)
		}
	}
}

// TestEnsureStream_ATimedOutUpdateIsNotRetried: a request the credential may
// not make is dropped by the server, so the update only times out. An update
// without the capability would be dropped the same way, so EnsureStream
// returns after one timeout instead of paying a second.
func TestEnsureStream_ATimedOutUpdateIsNotRetried(t *testing.T) {
	f := &fakeJS{streams: map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a"}}}, hangAtomic: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := jsresource.EnsureStream(ctx, f, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.a", "e.b"}, AtomicPublish: true,
		Update: jsresource.UpdateAdditive, Timeout: 100 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EnsureStream error = %v, want the update's timeout", err)
	}
	if len(f.updates) != 1 {
		t.Errorf("updates = %d, want 1", len(f.updates))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("EnsureStream took %v, want one 100ms request timeout, not the caller's 5s", elapsed)
	}
}

// TestEnsureStream_EachRequestGetsItsOwnTimeout: Timeout bounds each request,
// not the whole call. Three requests of 60ms each (lookup, update, read-back)
// fit three 100ms timeouts, but not one shared 100ms.
func TestEnsureStream_EachRequestGetsItsOwnTimeout(t *testing.T) {
	f := &fakeJS{streams: map[string]jetstream.StreamConfig{"S": {Name: "S", Subjects: []string{"e.a"}}}, delay: 60 * time.Millisecond}
	res, err := jsresource.EnsureStream(context.Background(), f, jsresource.StreamSpec{
		Name: "S", Subjects: []string{"e.a", "e.b"},
		Update: jsresource.UpdateAdditive, Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if got := res.Stream.CachedInfo().Config.Subjects; !slices.Equal(got, []string{"e.a", "e.b"}) {
		t.Errorf("Subjects = %q, want [e.a e.b]", got)
	}
}

// TestEnsureStream_NoAtomicCreateBeforeServer212: a server before 2.12 drops
// AllowAtomicPublish from a create, or in strict mode rejects the create for
// it, so EnsureStream doesn't put it in the create request either.
func TestEnsureStream_NoAtomicCreateBeforeServer212(t *testing.T) {
	defer jsresource.SetServerVersion("2.11.9")()
	f := &fakeJS{streams: map[string]jetstream.StreamConfig{}}
	res, err := ensureAtomic(f)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if f.streams["S"].AllowAtomicPublish || res.AtomicErr != nil {
		t.Errorf("create asked for AllowAtomicPublish = %v, AtomicErr = %v; want neither", f.streams["S"].AllowAtomicPublish, res.AtomicErr)
	}
}
