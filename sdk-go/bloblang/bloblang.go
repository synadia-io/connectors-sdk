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

// Package bloblang is the `mapping` transform backend: a connector's
// per-message transform written in Bloblang, the mapping language of Redpanda
// Benthos (module github.com/redpanda-data/benthos/v4). It implements the
// transform.Engine interface for the transform stage that
// spec/transformer.md defines.
//
// Engine.Compile takes the backend config, {"mapping": "<bloblang program>"}
// (Config), compiles the program once at startup and returns a
// transform.Func; a program that does not compile fails the connector before
// data flows, with the line and column of the error.
//
// Inside a mapping, `this` is the message payload parsed as JSON, or null
// when the payload is empty (a delete tombstone). The parse is lazy: a
// payload that is not JSON fails per message only when the mapping reads
// `this`, so a metadata-only mapping passes it through. Metadata holds the
// first value of each NATS header under the header's name, and the reserved
// key `nats_subject` holds the input subject. A mapping sets `root` to
// produce the output payload (`root = deleted()` drops the message, a null
// root gives an empty payload), `meta nats_subject` to publish on another
// subject, and any other `meta` key to set a header; a header value that is
// not a string is JSON-encoded. The key `nats_reply_subject` is reserved and
// stripped. The output subject must be a literal subject within the length
// limit, and header names must be ones NATS can send; otherwise the message
// fails with a per-message error, and a subject error wraps
// transform.ErrUnpublishableSubject.
//
// The environment is pure and deterministic: a mapping sees only its
// message. The impure functions (env, file, hostname, counter and the like)
// are removed, as are the time and random functions (now, the timestamp_*
// family, uuid_*, random_int, ksuid, nanoid), the batch and tracing
// functions, and the dynamic `.bloblang()` method; `import` is disabled.
// Such a program is rejected at compile time.
//
// Bloblang execution has no wall-clock bound and cannot be cancelled: the
// Func ignores its context, and a mapping that never finishes wedges the
// calling goroutine. Mapping config is trusted operator input.
package bloblang

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	blobl "github.com/redpanda-data/benthos/v4/public/bloblang"

	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// deniedFunctions survive OnlyPure (which strips only the functions benthos
// marks impure: env, file, hostname, file_rel, counter) but break
// determinism, which the spec's replay and dedup reasoning depends on, or
// have unstable semantics under singleton batches (batch_index/batch_size).
//
// tracing_id/tracing_span read the OpenTelemetry span attached to the
// message by pipeline tracing middleware — not part of the mapping's
// declared inputs, and not stable under replay: a redelivered message can be
// re-wrapped in a new span with a different trace ID, so a mapping that
// reads it can disagree with its own prior output for the same logical
// message.
var deniedFunctions = []string{
	"random_int", "uuid_v4", "uuid_v7", "ksuid", "nanoid",
	"now", "timestamp_unix", "timestamp_unix_milli",
	"timestamp_unix_micro", "timestamp_unix_nano",
	"batch_index", "batch_size",
	"tracing_id", "tracing_span",
}

// deniedMethods closes a hole OnlyPure and deniedFunctions cannot reach:
// `.bloblang(<mapping string>)` does not run the argument mapping against
// this Environment. Per internal/bloblang/plugins/bloblang.go, it parses
// the argument against benthos's own hardcoded global registry
// (query.AllFunctions.OnlyPure(), query.AllMethods.OnlyPure()) — a
// completely separate context our WithoutFunctions/deniedFunctions never
// touches. Verified: `this.body.bloblang("root = now()")` and
// `this.body.bloblang("root = uuid_v4()")` both compile even though a
// top-level `now()`/`uuid_v4()` is rejected. No Environment option closes
// this from the caller's side, so the method itself must be denied.
var deniedMethods = []string{
	"bloblang",
}

// Config is the `mapping` backend's config: {"mapping": "<bloblang program>"}.
type Config struct {
	Mapping string `json:"mapping"`
}

// Engine compiles Bloblang programs against the hardened environment.
type Engine struct {
	env *blobl.Environment
}

// New returns an Engine whose environment is pure and deterministic, as the
// package documentation describes: impure, time, random, batch and tracing
// functions and the dynamic `.bloblang()` method are unavailable, and
// `import` is disabled. A program that uses any of them fails to compile.
func New() *Engine {
	env := blobl.NewEnvironment().
		OnlyPure().
		WithoutFunctions(deniedFunctions...).
		WithoutMethods(deniedMethods...).
		WithDisabledImports()
	return &Engine{env: env}
}

// Compile implements transform.Engine.
func (e *Engine) Compile(raw json.RawMessage) (transform.Func, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("mapping backend config: %w", err)
	}
	if cfg.Mapping == "" {
		return nil, fmt.Errorf("mapping backend config: \"mapping\" is required")
	}
	exec, err := e.env.Parse(cfg.Mapping)
	if err != nil {
		var pe *blobl.ParseError
		if errors.As(err, &pe) {
			return nil, fmt.Errorf("mapping does not compile at line %d char %d: %w", pe.Line, pe.Column, err)
		}
		return nil, fmt.Errorf("mapping does not compile: %w", err)
	}
	return e.funcFor(exec), nil
}
