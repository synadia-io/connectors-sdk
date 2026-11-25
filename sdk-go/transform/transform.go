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

// Package transform defines the pluggable per-message transform stage for
// native connectors: one interface, N backends, one blessed default
// (Bloblang, in module sdk-go/bloblang), and the routing guard (Guard) every
// publish path runs over a transform's output before publishing it
// (spec/transformer.md).
package transform

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go"
)

// ErrUnpublishableSubject is wrapped by the per-message error (Result.Err)
// for an output subject that isn't a literal subject or is longer than
// subject.MaxSubjectLen bytes, and by Guard.Check for one that is in a
// server-reserved namespace, carries a reserved notice suffix, or is not one
// of the connector's data subjects. Every backend wraps it, so a connector
// classifies that failure with errors.Is, whatever the backend. The reasons
// are not told apart by sentinel: a connector reports one error class for all
// of them, and the reason is in the error text, for its logs. The error's
// text quotes the subject, or an over-long subject's start, which may come
// from message data, so it isn't fit for a wire header.
var ErrUnpublishableSubject = errors.New("unpublishable subject")

// Message is one NATS message. Header is nats.Header (case-sensitive,
// multi-valued); it may be nil.
type Message struct {
	Subject string
	Header  nats.Header
	Payload []byte
}

// Result is the outcome for ONE input message. The results slice returned
// by Func is index-aligned with the input batch.
type Result struct {
	Output []Message // 0 = drop, 1 = mapped, N = explode
	Err    error     // per-message failure -> the connector's error policy
}

// Func is a compiled transform. Batch-in so a request-reply backend can
// amortize the hop; in-process backends are typically called with singleton
// batches.
//
// Contract:
//   - MUST return exactly len(batch) results; the connector treats a length
//     mismatch as a batch-level systemic error and never indexes into it.
//   - When Result.Err != nil, Result.Output is ignored and should be nil.
//   - 1:N flatten order is Output[0][0], Output[0][1], Output[1][0], ...
//   - Func MUST NOT mutate the input batch, any input Message, its Header
//     map, or its Payload bytes.
//   - Every Message in Result.Output is caller-owned: safe to retain, safe
//     to mutate (the batch publisher injects headers into the Header map),
//     and its Payload MUST NOT alias an input payload or a backend-internal
//     buffer.
//   - Implementations need not honor ctx cancellation (the Bloblang backend
//     cannot); ctx exists for backends that can.
//   - A batch-level error means systemic failure (engine broken, service
//     timeout) and routes to the connector's retry/park path; per-message
//     failures go in Result.Err.
type Func func(ctx context.Context, batch []Message) ([]Result, error)

// Engine is one backend. Compile validates config at startup — fail fast,
// before data flows. Engines needing runtime dependencies (a NATS
// connection, for a request-reply backend) take them at construction, not
// through config. An Engine holding resources also implements io.Closer;
// the connector type-asserts and closes on shutdown.
type Engine interface {
	Compile(config json.RawMessage) (Func, error)
}
