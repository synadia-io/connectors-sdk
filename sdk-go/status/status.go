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

// Package status implements the connector status document (spec/status.md,
// D-1, D-6, D-7) and its Publisher: the mutex-guarded state a connector
// maintains and republishes to its status subject (D-5), and returns
// verbatim from its admin surface's status endpoint (D-8).
package status

import "time"

// State is a connector's D-6 wire state. Values are always lowercase on the
// wire, matching status.schema.json's enum exactly.
type State string

// The D-6 state model. See spec/status.md for the meaning of each value.
const (
	Running    State = "running"
	Paused     State = "paused"
	Parked     State = "parked"
	Failed     State = "failed"
	Restarting State = "restarting"
)

// Backfill reports D-7 backfill progress: a running count and a completion
// flag. There is deliberately no percentage/denominator field — one does
// not exist in general (status.schema.json enforces this structurally via
// additionalProperties:false, not just by convention).
type Backfill struct {
	Copied int64 `json:"copied"`
	Done   bool  `json:"done"`
}

// Cursor reports D-7 lag. BehindMs is the one canonical number for a
// connector currently tailing changes (a source-time delta in
// milliseconds); Backfill is progress for a connector still copying
// historical data. A connector reports whichever applies to its current
// phase: both, either, or neither.
type Cursor struct {
	BehindMs *int64    `json:"behind_ms,omitempty"`
	Backfill *Backfill `json:"backfill,omitempty"`
}

// Document is the status document published to a connector's status
// subject (D-5) and returned verbatim by its admin surface's "status"
// endpoint (D-8). Field-by-field prose lives in spec/status.md; the
// enforced shape is spec/status.schema.json.
//
// All nine keys are always present in the marshaled JSON. Cursor and
// Counters are the only ones ever null; every other field always carries a
// concrete value, per D-14's absence-vs-empty-value distinction (an empty
// Reason is a value, not the field being left out). The one exception is the
// zero Document a nil *Publisher's Snapshot returns; the harness never
// serves it, because it always runs with a real Publisher.
type Document struct {
	Contract string    `json:"contract"`
	State    State     `json:"state"`
	Reason   string    `json:"reason"`
	Since    time.Time `json:"since"`

	// Cursor and Counters marshal as null, never omitted, when unset — a
	// Document always carries all nine status.schema.json keys.
	Cursor   *Cursor          `json:"cursor"`
	Counters map[string]int64 `json:"counters"`

	Admin       string `json:"admin"`
	ConnectorID string `json:"connector_id"`
	WorkloadID  string `json:"workload_id"`
}
