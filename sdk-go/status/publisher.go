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

package status

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/spec"
)

// defaultInterval is the D-5 periodic republish period.
const defaultInterval = 10 * time.Second

// Option configures a Publisher at construction time. Options are read
// once, during NewPublisher; a Publisher's exported methods are safe for
// concurrent use afterward, but Options themselves are not synchronized and
// must not be applied once a Publisher is shared across goroutines.
type Option func(*Publisher)

// WithInterval overrides the default 10s periodic republish period (D-5).
// Intended for tests that need to observe a tick without waiting 10s.
func WithInterval(d time.Duration) Option {
	return func(p *Publisher) { p.interval = d }
}

// WithErrorHandler registers a callback invoked whenever a publish attempt
// fails (marshal error or NATS publish error). Without one, publish errors
// are silently dropped: a connector's status reporting must never panic or
// block the caller of SetState/Run on a transient publish failure.
func WithErrorHandler(f func(error)) Option {
	return func(p *Publisher) { p.errHandler = f }
}

// Publisher owns a connector's current status Document and republishes it
// to the connector's status subject (D-5): immediately on every state
// change, and periodically (every 10s by default) while Run is active. All
// exported methods are safe for concurrent use.
//
// A nil *Publisher is a valid no-op sink: every exported method is a no-op
// (Snapshot returns the zero Document, which status.schema.json rejects;
// Run returns immediately) rather than panicking. This lets connector code
// call the Publisher unconditionally, with no nil guard at each call site,
// and still run in unit tests and other harness-free contexts where no
// Publisher is wired. The harness itself always constructs and passes a real
// Publisher.
//
// A nil *Publisher is a no-op, but a non-nil Publisher{} literal is not
// usable: construct one with NewPublisher.
type Publisher struct {
	nc      *nats.Conn
	subject string

	interval   time.Duration
	errHandler func(error)

	mu  sync.Mutex
	doc Document
}

// NewPublisher builds a Publisher for id, publishing on nc to id's status
// subject (D-5). The initial Document is Running with Since set to the
// construction time — a connector that starts serving before its first
// explicit SetState call still has a schema-valid status to report.
func NewPublisher(nc *nats.Conn, id identity.Identity, opts ...Option) *Publisher {
	p := &Publisher{
		nc:       nc,
		subject:  identity.StatusSubject(id),
		interval: defaultInterval,
		doc: Document{
			Contract:    spec.Contract,
			State:       Running,
			Since:       time.Now(),
			Admin:       identity.AdminRoot(id),
			ConnectorID: id.ConnectorID,
			WorkloadID:  id.WorkloadID,
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// SetState updates the current state and reason and publishes the result
// immediately (D-5). Since is reset to the current time only when state
// actually differs from the previously recorded one — repeating the same
// state (e.g. a redundant SetState(Running, "") call) leaves Since, and so
// the reported transition time, untouched.
func (p *Publisher) SetState(s State, reason string) {
	if p == nil {
		return
	}

	// Redact before the reason is ever stored, so it is masked everywhere it
	// is subsequently read: the published document (D-5) and the admin status
	// endpoint's verbatim response (D-8), which both draw from p.doc. This is
	// the harness-level backstop for the whole fleet — a connector's own
	// redaction stays as defense in depth.
	reason = RedactReason(reason)

	p.mu.Lock()
	changed := p.doc.State != s
	p.doc.State = s
	p.doc.Reason = reason
	if changed {
		p.doc.Since = time.Now()
	}
	snap := p.snapshotLocked()
	p.mu.Unlock()

	p.publish(snap)
}

// SetCursor replaces the current D-7 lag/backfill report wholesale — both
// BehindMs and Backfill are overwritten, even when c leaves one of them nil.
// Use SetBehind or SetBackfill instead when only one half of the cursor is
// known at the call site and the other must be left as it is. Like those two,
// SetCursor does not publish immediately — it is expected to be called
// frequently (e.g. once per polled batch), and D-5 only requires the periodic
// republish plus immediate publish on state change. c is deep-copied so the
// caller mutating a Cursor/Backfill they already handed to SetCursor can
// never race with a concurrent Snapshot or publish. A negative c.BehindMs
// is stored as 0 (see SetBehind); c itself is never modified.
func (p *Publisher) SetCursor(c Cursor) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.doc.Cursor = cloneCursor(&c)
	clampBehindMs(p.doc.Cursor)
	p.mu.Unlock()
}

// SetBehind sets the D-7 cursor's lag, how far behind the source the
// connector is, creating the Cursor on first use and leaving Backfill as it
// is. It is reported as behind_ms, in whole milliseconds (truncated). Like
// SetCursor it updates only the in-memory document: it does not publish
// immediately (D-5).
//
// A negative d is reported as 0. status.schema.json requires
// behind_ms >= 0, but a connector typically computes lag from a source
// clock (e.g. time.Since(clusterTime)), which can go negative under clock
// skew; this is the backstop for that, in the same spirit as SetState's
// reason redaction.
func (p *Publisher) SetBehind(d time.Duration) {
	if p == nil {
		return
	}
	ms := max(d, 0).Milliseconds()
	p.mu.Lock()
	if p.doc.Cursor == nil {
		p.doc.Cursor = &Cursor{}
	}
	p.doc.Cursor.BehindMs = &ms
	p.mu.Unlock()
}

// SetBackfill sets the D-7 cursor's backfill progress, creating the
// Cursor on first use and leaving BehindMs as it is. Like SetCursor it
// updates only the in-memory document: it does not publish immediately
// (D-5).
//
// b.Copied is stored as given. A negative lag comes from clock skew, which
// SetBehind absorbs; a negative count is a connector bug, so the status
// document shows it (status.schema.json rejects it) rather than hiding it.
//
// Neither SetBehind nor SetBackfill can remove its half of the cursor. To
// drop one, call SetCursor with that half left nil.
func (p *Publisher) SetBackfill(b Backfill) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.doc.Cursor == nil {
		p.doc.Cursor = &Cursor{}
	}
	bf := b
	p.doc.Cursor.Backfill = &bf
	p.mu.Unlock()
}

// Add increments counter by delta, creating the Counters map on first use.
// It does not publish immediately, for the same reason as SetCursor.
func (p *Publisher) Add(counter string, delta int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.doc.Counters == nil {
		p.doc.Counters = make(map[string]int64)
	}
	p.doc.Counters[counter] += delta
	p.mu.Unlock()
}

// Snapshot returns a copy of the current Document, deep enough (Counters
// map and Cursor/Backfill) that the caller mutating the result can never
// race with or corrupt the Publisher's internal state. A nil *Publisher
// returns the zero Document.
func (p *Publisher) Snapshot() Document {
	if p == nil {
		return Document{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

// snapshotLocked is Snapshot's body, callable while p.mu is already held
// (SetState needs the just-updated Document to publish without a second
// lock acquisition).
func (p *Publisher) snapshotLocked() Document {
	d := p.doc
	d.Cursor = cloneCursor(p.doc.Cursor)
	if p.doc.Counters != nil {
		d.Counters = make(map[string]int64, len(p.doc.Counters))
		for k, v := range p.doc.Counters {
			d.Counters[k] = v
		}
	}
	return d
}

// Run periodically republishes the current Document to the status subject
// (D-5) every interval — 10s by default, overridable with WithInterval —
// until ctx is done. It does not publish immediately on entry; a
// connector's startup path is expected to call SetState first, which
// publishes on its own. On a nil *Publisher, Run returns immediately,
// regardless of ctx.
func (p *Publisher) Run(ctx context.Context) {
	if p == nil {
		return
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publish(p.Snapshot())
		}
	}
}

// publish marshals doc and publishes it to the status subject, handing any
// error to the configured WithErrorHandler (if any) rather than panicking
// or returning it — a status-reporting failure must never take down or
// block the connector's own control flow.
func (p *Publisher) publish(doc Document) {
	data, err := json.Marshal(doc)
	if err != nil {
		p.reportErr(fmt.Errorf("status: marshal document: %w", err))
		return
	}
	if err := p.nc.Publish(p.subject, data); err != nil {
		p.reportErr(fmt.Errorf("status: publish to %s: %w", p.subject, err))
	}
}

// reportErr hands err to the configured error handler, if any.
func (p *Publisher) reportErr(err error) {
	if p.errHandler != nil {
		p.errHandler(err)
	}
}

// clampBehindMs clamps c's BehindMs to 0 in place when it is negative,
// enforcing status.schema.json's "behind_ms >= 0" for SetCursor the same way
// SetBehind's own max(d, 0) does for its path. No-op on a nil Cursor or a
// nil/non-negative BehindMs.
func clampBehindMs(c *Cursor) {
	if c == nil || c.BehindMs == nil || *c.BehindMs >= 0 {
		return
	}
	zero := int64(0)
	c.BehindMs = &zero
}

// cloneCursor deep-copies c (including its BehindMs and Backfill pointer
// fields) so neither the Publisher's internal state nor a caller-held
// Cursor can be mutated through the other's pointers.
func cloneCursor(c *Cursor) *Cursor {
	if c == nil {
		return nil
	}
	clone := *c
	if c.BehindMs != nil {
		v := *c.BehindMs
		clone.BehindMs = &v
	}
	if c.Backfill != nil {
		bf := *c.Backfill
		clone.Backfill = &bf
	}
	return &clone
}
