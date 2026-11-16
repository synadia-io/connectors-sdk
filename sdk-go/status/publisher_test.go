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

package status_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// startTestServer boots an in-process NATS server and a connected client,
// returning both plus a cleanup func. Mirrors conformance/natsenv's
// pattern, scaled down to what Publisher's tests need (no JetStream).
func startTestServer(t *testing.T) (*nats.Conn, func()) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1 // random free port, hermetic across parallel test runs

	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats.Connect: %v", err)
	}

	return nc, func() {
		nc.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
}

func testIdentity() identity.Identity {
	return identity.Identity{
		Namespace:   "test",
		ConnectorID: "conn-1",
		WorkloadID:  "wl-1",
		InstanceID:  "conn-1",
	}
}

// TestNewPublisher_SetStatePublishesImmediately guards D-5's "immediately
// on state change" half: a bare SetState call, with no Run loop started,
// must still land a document on the connector's status subject.
func TestNewPublisher_SetStatePublishesImmediately(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	sub, err := nc.SubscribeSync(identity.StatusSubject(id))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()

	p := status.NewPublisher(nc, id)
	p.SetState(status.Parked, "unresolved-secret")

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}

	var doc status.Document
	if err := json.Unmarshal(msg.Data, &doc); err != nil {
		t.Fatalf("unmarshal published document: %v", err)
	}
	if doc.State != status.Parked {
		t.Errorf("published State = %q, want %q", doc.State, status.Parked)
	}
	if doc.Reason != "unresolved-secret" {
		t.Errorf("published Reason = %q, want %q", doc.Reason, "unresolved-secret")
	}
	if doc.Admin != identity.AdminRoot(id) {
		t.Errorf("published Admin = %q, want %q", doc.Admin, identity.AdminRoot(id))
	}
	if doc.ConnectorID != id.ConnectorID || doc.WorkloadID != id.WorkloadID {
		t.Errorf("published identity = (%q,%q), want (%q,%q)",
			doc.ConnectorID, doc.WorkloadID, id.ConnectorID, id.WorkloadID)
	}
}

// TestPublisher_Run_TickerRepublishes guards D-5's periodic half: with
// WithInterval shortened for the test, Run must land multiple documents on
// the status subject without any SetState call driving them.
func TestPublisher_Run_TickerRepublishes(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	sub, err := nc.SubscribeSync(identity.StatusSubject(id))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()

	p := status.NewPublisher(nc, id, status.WithInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("NextMsg (1st tick): %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("NextMsg (2nd tick): %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}

// TestPublisher_SetState_SinceOnlyResetsOnStateChange guards the D-5/status.md
// "since is the most recent transition" rule: repeating the same state must
// leave Since untouched, while an actual transition must advance it.
func TestPublisher_SetState_SinceOnlyResetsOnStateChange(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	p.SetState(status.Failed, "boom")
	first := p.Snapshot().Since

	time.Sleep(5 * time.Millisecond)
	p.SetState(status.Failed, "boom again")
	second := p.Snapshot().Since
	if !second.Equal(first) {
		t.Errorf("Since changed on a same-state SetState: first=%v second=%v", first, second)
	}

	time.Sleep(5 * time.Millisecond)
	p.SetState(status.Running, "recovered")
	third := p.Snapshot().Since
	if !third.After(first) {
		t.Errorf("Since did not advance on an actual state change: first=%v third=%v", first, third)
	}
}

// TestPublisher_WithErrorHandler_ReceivesPublishErrors guards the sanctioned
// error surface: SetState must never panic or block on a publish failure,
// and must hand the error to WithErrorHandler when one is configured.
func TestPublisher_WithErrorHandler_ReceivesPublishErrors(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	var mu sync.Mutex
	var gotErr error
	p := status.NewPublisher(nc, testIdentity(), status.WithErrorHandler(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		gotErr = err
	}))

	nc.Close() // forces every subsequent Publish call to fail

	p.SetState(status.Failed, "boom")

	mu.Lock()
	defer mu.Unlock()
	if gotErr == nil {
		t.Fatal("WithErrorHandler callback was not invoked after the connection closed")
	}
}

// TestPublisher_Snapshot_RaceFreeUnderConcurrentMutation guards the
// mutex-guarded Document contract under -race: SetState, SetCursor,
// SetBehind, SetBackfill, Add, Snapshot, and a concurrently running Run
// loop must be safe to call/have running concurrently with no data race and
// no panic.
func TestPublisher_Snapshot_RaceFreeUnderConcurrentMutation(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity(), status.WithInterval(time.Millisecond))
	states := []status.State{status.Running, status.Paused, status.Restarting}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				p.Add("published", 1)
				p.SetCursor(status.Cursor{Backfill: &status.Backfill{Copied: int64(i), Done: false}})
				p.SetBehind(time.Duration(i) * time.Millisecond)
				p.SetBackfill(status.Backfill{Copied: int64(i), Done: false})
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				p.SetState(states[i%len(states)], "cycling")
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 500 {
			snap := p.Snapshot()
			if snap.Contract == "" {
				t.Error("Snapshot().Contract is empty")
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	cancel()
	wg.Wait()
}

// TestNilPublisher_IsANoOp guards the "nil *Publisher is a valid no-op sink"
// contract: connector code wired up without a harness-provided Publisher
// must be able to call every exported method on a nil *status.Publisher
// without panicking, with Snapshot returning the zero Document and Run
// returning immediately regardless of ctx.
func TestNilPublisher_IsANoOp(t *testing.T) {
	var p *status.Publisher

	p.SetState(status.Failed, "boom")
	p.SetCursor(status.Cursor{Backfill: &status.Backfill{Copied: 1, Done: true}})
	p.SetBehind(5 * time.Millisecond)
	p.SetBackfill(status.Backfill{Copied: 2, Done: false})
	p.Add("published", 1)

	if got := p.Snapshot(); !reflect.DeepEqual(got, status.Document{}) {
		t.Errorf("Snapshot() on nil Publisher = %+v, want zero Document", got)
	}

	// context.Background() is never done on its own; if Run on a nil
	// Publisher ever started waiting on <-ctx.Done() instead of returning
	// immediately, this would time out rather than return promptly.
	done := make(chan struct{})
	go func() {
		p.Run(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() on nil Publisher did not return immediately")
	}
}

// TestNilPublisher_EveryMethodIsANoOp calls every exported method of
// *Publisher on a nil receiver with zero-value arguments (a cancelled
// context for Run), so a method added later without its nil guard fails
// here instead of panicking in a connector's unit tests.
func TestNilPublisher_EveryMethodIsANoOp(t *testing.T) {
	var p *status.Publisher
	v := reflect.ValueOf(p)
	if v.NumMethod() == 0 {
		t.Fatal("*status.Publisher has no exported methods")
	}
	ctxType := reflect.TypeFor[context.Context]()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := range v.NumMethod() {
		name, m := v.Type().Method(i).Name, v.Method(i)
		args := make([]reflect.Value, m.Type().NumIn())
		for j := range args {
			if in := m.Type().In(j); in == ctxType {
				args[j] = reflect.ValueOf(cancelled)
			} else {
				args[j] = reflect.Zero(in)
			}
		}
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s on a nil *Publisher panicked: %v", name, r)
				}
			}()
			if m.Type().IsVariadic() {
				m.CallSlice(args)
			} else {
				m.Call(args)
			}
		})
	}
}

// TestPublisher_SetBehindThenSetBackfill_BothLandInCursor guards that the
// two partial setters compose: calling them in either order must leave both
// fields set on the Document's Cursor, and SetCursor afterward must still
// replace the whole thing (clearing both).
func TestPublisher_SetBehindThenSetBackfill_BothLandInCursor(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	p.SetBehind(1500 * time.Millisecond)
	p.SetBackfill(status.Backfill{Copied: 42, Done: false})

	snap := p.Snapshot()
	if snap.Cursor == nil || snap.Cursor.BehindMs == nil || *snap.Cursor.BehindMs != 1500 {
		t.Fatalf("Cursor.BehindMs = %+v, want 1500", snap.Cursor)
	}
	if snap.Cursor.Backfill == nil || snap.Cursor.Backfill.Copied != 42 || snap.Cursor.Backfill.Done {
		t.Fatalf("Cursor.Backfill = %+v, want {Copied:42 Done:false}", snap.Cursor.Backfill)
	}

	p.SetCursor(status.Cursor{})
	snap = p.Snapshot()
	if snap.Cursor == nil || snap.Cursor.BehindMs != nil || snap.Cursor.Backfill != nil {
		t.Fatalf("after SetCursor(Cursor{}), Cursor = %+v, want both fields cleared", snap.Cursor)
	}
}

// TestPublisher_SetBehind_ClampsNegativeLag guards the clock-skew backstop:
// behind_ms is never negative on the wire (spec/status.schema.json minimum
// 0), but a connector computing lag from a source clock (e.g.
// time.Since(clusterTime)) can hand SetBehind a negative duration under skew.
// SetBehind must clamp it to 0 rather than storing it verbatim.
func TestPublisher_SetBehind_ClampsNegativeLag(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	p.SetBehind(-5 * time.Second)

	snap := p.Snapshot()
	if snap.Cursor == nil || snap.Cursor.BehindMs == nil {
		t.Fatalf("Cursor.BehindMs = %+v, want a non-nil 0", snap.Cursor)
	}
	if *snap.Cursor.BehindMs != 0 {
		t.Errorf("Cursor.BehindMs = %d, want 0 (clamped)", *snap.Cursor.BehindMs)
	}
}

// TestPublisher_SetBehind_ReportsWholeMilliseconds pins the unit conversion:
// behind_ms is the duration in whole milliseconds, truncated.
func TestPublisher_SetBehind_ReportsWholeMilliseconds(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())
	for _, tt := range []struct {
		d    time.Duration
		want int64
	}{
		{90 * time.Second, 90000},
		{1999 * time.Microsecond, 1},
		{999 * time.Microsecond, 0},
	} {
		p.SetBehind(tt.d)
		snap := p.Snapshot()
		if snap.Cursor == nil || snap.Cursor.BehindMs == nil || *snap.Cursor.BehindMs != tt.want {
			t.Errorf("after SetBehind(%v), Cursor = %+v, want BehindMs %d", tt.d, snap.Cursor, tt.want)
		}
	}
}

// TestPublisher_SetCursor_ClampsNegativeLag mirrors the above for SetCursor:
// a negative BehindMs on the Cursor it is handed must also clamp to 0, and
// the caller's own pointer must be left untouched (SetCursor already deep
// copies; clamping must not mutate through that pointer either).
func TestPublisher_SetCursor_ClampsNegativeLag(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	neg := int64(-5)
	p.SetCursor(status.Cursor{BehindMs: &neg})

	snap := p.Snapshot()
	if snap.Cursor == nil || snap.Cursor.BehindMs == nil {
		t.Fatalf("Cursor.BehindMs = %+v, want a non-nil 0", snap.Cursor)
	}
	if *snap.Cursor.BehindMs != 0 {
		t.Errorf("Cursor.BehindMs = %d, want 0 (clamped)", *snap.Cursor.BehindMs)
	}
	if neg != -5 {
		t.Errorf("caller's BehindMs pointer was mutated: got %d, want -5", neg)
	}
}

// TestPublisher_SetBackfillThenSetBehind_BothLandInCursor mirrors the
// above with the two partial setters called in the opposite order.
func TestPublisher_SetBackfillThenSetBehind_BothLandInCursor(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	p.SetBackfill(status.Backfill{Copied: 7, Done: true})
	p.SetBehind(99 * time.Millisecond)

	snap := p.Snapshot()
	if snap.Cursor == nil || snap.Cursor.BehindMs == nil || *snap.Cursor.BehindMs != 99 {
		t.Fatalf("Cursor.BehindMs = %+v, want 99", snap.Cursor)
	}
	if snap.Cursor.Backfill == nil || snap.Cursor.Backfill.Copied != 7 || !snap.Cursor.Backfill.Done {
		t.Fatalf("Cursor.Backfill = %+v, want {Copied:7 Done:true}", snap.Cursor.Backfill)
	}
}

// TestPublisher_Snapshot_IsIndependentOfLaterSetterCalls guards that a
// Snapshot taken before SetBehind/SetBackfill (seeded via SetCursor) is
// unaffected by setter calls that come after it, and that neither setter
// reaches back into the caller's own memory (SetCursor already deep-copies
// via cloneCursor; this exercises that contract through the two partial
// setters' seed values too).
func TestPublisher_Snapshot_IsIndependentOfLaterSetterCalls(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())

	x := int64(1)
	bf := &status.Backfill{Copied: 1}
	p.SetCursor(status.Cursor{BehindMs: &x, Backfill: bf})

	snap := p.Snapshot()

	p.SetBehind(2 * time.Millisecond)
	p.SetBackfill(status.Backfill{Copied: 3})

	if x != 1 {
		t.Errorf("caller's BehindMs pointer was mutated: got %d, want 1", x)
	}
	if bf.Copied != 1 {
		t.Errorf("caller's Backfill pointer was mutated: got %+v, want Copied=1", bf)
	}
	if snap.Cursor == nil || snap.Cursor.BehindMs == nil {
		t.Fatal("snap.Cursor.BehindMs is nil, want a non-nil 1")
	}
	if *snap.Cursor.BehindMs != 1 {
		t.Errorf("snap.Cursor.BehindMs = %d, want 1 (unaffected by the later SetBehind(2ms))", *snap.Cursor.BehindMs)
	}
	if snap.Cursor.Backfill == nil {
		t.Fatal("snap.Cursor.Backfill is nil, want a non-nil {Copied:1}")
	}
	if snap.Cursor.Backfill.Copied != 1 {
		t.Errorf("snap.Cursor.Backfill.Copied = %d, want 1 (unaffected by the later SetBackfill(3))",
			snap.Cursor.Backfill.Copied)
	}
}

// TestPublisher_SetBehindAndSetBackfill_DoNotPublishImmediately mirrors
// TestNewPublisher_SetStatePublishesImmediately's pattern in reverse: the
// two partial setters must not publish on their own (same contract as
// SetCursor/Add), while a subsequent SetState call must — and the message it
// publishes must carry both cursor halves, so a late/mis-ordered setter
// publish can't accidentally satisfy this check instead.
func TestPublisher_SetBehindAndSetBackfill_DoNotPublishImmediately(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	id := testIdentity()
	sub, err := nc.SubscribeSync(identity.StatusSubject(id))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush (post-subscribe): %v", err)
	}

	p := status.NewPublisher(nc, id)

	p.SetBehind(10 * time.Millisecond)
	p.SetBackfill(status.Backfill{Copied: 1, Done: false})
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush (post-setters): %v", err)
	}

	if n, _, err := sub.Pending(); err != nil {
		t.Fatalf("Pending: %v", err)
	} else if n != 0 {
		t.Fatalf("SetBehind/SetBackfill published %d message(s), want 0", n)
	}

	p.SetState(status.Paused, "")
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg after SetState: %v", err)
	}

	var doc status.Document
	if err := json.Unmarshal(msg.Data, &doc); err != nil {
		t.Fatalf("unmarshal published document: %v", err)
	}
	if doc.State != status.Paused {
		t.Errorf("published State = %q, want %q", doc.State, status.Paused)
	}
	if doc.Cursor == nil || doc.Cursor.BehindMs == nil || *doc.Cursor.BehindMs != 10 {
		t.Errorf("published Cursor.BehindMs = %+v, want 10", doc.Cursor)
	}
	if doc.Cursor == nil || doc.Cursor.Backfill == nil || doc.Cursor.Backfill.Copied != 1 {
		t.Errorf("published Cursor.Backfill = %+v, want {Copied:1}", doc.Cursor)
	}
}

// TestPublisher_Snapshot_CounterAndCursorAreIndependentCopies guards
// "Snapshot(): copy (deep enough that caller mutation can't race)":
// mutating a Snapshot()'s Counters map or Cursor.Backfill after
// the fact must not reach back into the Publisher's internal state.
func TestPublisher_Snapshot_CounterAndCursorAreIndependentCopies(t *testing.T) {
	nc, cleanup := startTestServer(t)
	defer cleanup()

	p := status.NewPublisher(nc, testIdentity())
	p.Add("published", 1)
	p.SetCursor(status.Cursor{Backfill: &status.Backfill{Copied: 1, Done: false}})

	snap := p.Snapshot()
	snap.Counters["published"] = 999
	snap.Cursor.Backfill.Copied = 999

	again := p.Snapshot()
	if again.Counters["published"] != 1 {
		t.Errorf("internal Counters mutated via a Snapshot copy: got %d, want 1", again.Counters["published"])
	}
	if again.Cursor.Backfill.Copied != 1 {
		t.Errorf("internal Cursor mutated via a Snapshot copy: got %d, want 1", again.Cursor.Backfill.Copied)
	}
}
