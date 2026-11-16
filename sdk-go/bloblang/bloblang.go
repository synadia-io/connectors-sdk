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

// Package bloblang is the default `mapping` transform backend: Redpanda
// Benthos Bloblang (MIT engine, module github.com/redpanda-data/benthos/v4),
// embedded via public/service (public/bloblang alone cannot express message
// metadata — execution-verified 2026-08-13).
//
// Residual risk, by design: Bloblang execution has no wall-clock bound and
// no context cancellation. Named-map recursion is bounded upstream at a
// hardcoded 5000 stacks (WithMaxMapRecursion caps only the top-level
// executor and is inert for named maps — the only construct that can
// recurse — so nothing is set here). `root = range(0, 1e9)` is neither
// recursion nor cancellable: a pathological mapping wedges the calling
// goroutine. Mapping config is trusted operator input.
package bloblang

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	blobl "github.com/redpanda-data/benthos/v4/public/bloblang"

	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// deniedFunctions survive OnlyPure (which strips only env, file, hostname,
// file_rel, counter) but break determinism — the MongoDB dedup-window and
// replay/LWW reasoning require deterministic mappings — or have unstable
// semantics under singleton batches (batch_index/batch_size).
//
// tracing_id/tracing_span read the OpenTelemetry span attached to the
// message by pipeline tracing middleware (internal/bloblang/query/
// functions.go, tracing.GetSpan/tracing.GetTraceID) — not part of the
// mapping's declared inputs, and not stable under replay: a redelivered
// message can be re-wrapped in a new span with a different trace ID, so a
// mapping that reads it can disagree with its own prior output for the
// same logical message.
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

// New builds the hardened environment. NewEnvironment clones the global
// environment (NOT isolated from init()-time registrations of linked
// packages — that is how public/service's io functions arrive); OnlyPure
// strips the impure set; WithoutFunctions strips the nondeterministic
// survivors; WithoutMethods strips the dynamic-mapping escape hatch
// (deniedMethods); imports are disabled (a filesystem-read primitive
// otherwise).
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
