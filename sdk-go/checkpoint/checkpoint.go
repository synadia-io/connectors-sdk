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

// Package checkpoint is the revision-checked store a connector persists its
// durable progress in (spec/checkpoint.md): by default, one JetStream KV
// bucket per connector instance (an override can share one), where every
// write is a compare-and-swap against the revision it was read at. A lost
// CAS means another writer is live right now, or an operator reset the key
// mid-run; either needs an operator.
// It surfaces as ErrConflict, which the runtime harness parks on with reason
// "checkpoint-conflict".
//
// The store is byte-level. What a connector keeps under its keys, and how it
// encodes that, is the connector's own design (spec/checkpoint.md: the
// value's shape is not normative). There is deliberately no Put: the spec
// forbids an unconditional write.
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
)

var (
	// ErrConflict means a Save lost its revision check: another writer
	// created or changed the key since it was loaded, or an operator
	// Delete or Purge of the key mid-run reset it out from under the
	// caller — so the park reason can mean "the key was reset" as well as
	// "another writer". It is park-class (spec/checkpoint.md): return it
	// (wrapped is fine) from Hooks.Run. Don't wrap it in runtime.Park: an
	// explicit park reason replaces checkpoint-conflict, which
	// spec/checkpoint.md requires.
	//
	// Never retry the write, and never swallow the error, with one narrow
	// exception: when the key holds an idempotent marker — an immutable
	// fact such as "event X was delivered", not a progress position — a
	// connector may Load the key after a conflict and, if the stored
	// value is exactly the value it would have written, count a
	// duplicate and continue without writing (so nothing is returned
	// from Run and the harness never sees it). In every other case,
	// return it from Run so the harness parks.
	ErrConflict = errors.New("checkpoint-conflict")

	// ErrScopeMismatch means the bucket is bound to a different
	// Options.Scope, or bound at all, when Options.Scope is empty: it
	// belongs to another configuration.
	ErrScopeMismatch = errors.New("checkpoint: bucket bound to a different scope")

	// ErrExpiringBucket means the bucket has a TTL. Expiring checkpoint
	// records would silently forget progress and re-emit data.
	ErrExpiringBucket = errors.New("checkpoint: bucket expires records (TTL must be 0)")

	// ErrReservedKey means a key starts with "_", the prefix the SDK keeps
	// for its own records.
	ErrReservedKey = errors.New(`checkpoint: keys starting with "_" are reserved`)
)

// DefaultTimeout bounds Open and each Load and Save when Options.Timeout is
// zero or negative.
const DefaultTimeout = 10 * time.Second

// scopeKey is the reserved key holding the bucket's binding (Options.Scope).
const scopeKey = "_scope"

// Options configures Open. The zero value is the spec default.
type Options struct {
	// Bucket overrides the default bucket name, identity.CheckpointBucket(id).
	// spec/checkpoint.md: an override is used as-is, not merged with the pattern.
	// It is also the way to run with only a workload id: Open won't derive
	// the default from one (identity.ErrNoConnectorID).
	Bucket string

	// Scope, when set, binds the bucket to a configuration: Open records it
	// on first use and fails with ErrScopeMismatch if a different scope is
	// already recorded. Use a stable, non-secret value (a hash of the fields
	// that define "the same source and destination"): it appears verbatim in
	// ErrScopeMismatch errors, and so in park reasons and the published
	// status document. The scope is per bucket, not per Store: instances
	// that share one bucket via Options.Bucket must use the same Scope (or
	// none).
	//
	// A bucket that has no scope record yet — including one that already
	// holds checkpoints from before Scope was adopted — is bound on its
	// first scoped Open. A recorded binding is enforced on every Open, not
	// only scoped ones: an unscoped Open of a bound bucket fails with
	// ErrScopeMismatch. The binding is checked when Open runs, so an
	// unscoped Open racing a first scoped Open can still succeed and read
	// the bucket as unbound; the CAS discipline on Save still catches a
	// dual writer either way. An operator rebinds a bucket by deleting
	// its "_scope" key.
	Scope string

	// Timeout bounds Open and each Load and Save. Zero or negative means
	// DefaultTimeout.
	Timeout time.Duration

	// Replicas sets the replica count when Open creates the bucket. It
	// never changes an existing bucket. Zero or negative means 1: that is
	// nats.go's client default, not necessarily the server's.
	Replicas int
}

// Store is an open checkpoint bucket. It holds no per-key state, and is
// safe for concurrent use.
type Store struct {
	kv      jetstream.KeyValue
	bucket  string
	timeout time.Duration
}

// Open resolves and opens the connector's checkpoint bucket, creating it
// (History 1, replica count from Options.Replicas) when it does not exist.
// An existing bucket is used as it is, except that a bucket with a TTL is
// rejected with ErrExpiringBucket. It also rejects a bucket bound to a
// different Options.Scope, or bound at all when Options.Scope is empty,
// with ErrScopeMismatch, and an empty Options.Bucket paired with no
// InstanceID with a plain error, since there is then nothing to derive a
// name from.
//
// The default name must stay the same from one launch to the next, so with
// an empty Options.Bucket, Open returns identity.ErrNoConnectorID, before
// it touches the server, when the InstanceID is the workload id
// (ConnectorID empty, WorkloadID set): a bucket named after it would start
// over on every launch, and two overlapping instances would open different
// buckets, out of reach of each other's compare-and-swap. The "-dev"
// fallback is stable and is used.
func Open(ctx context.Context, js jetstream.JetStream, id identity.Identity, opts Options) (*Store, error) {
	bucket := opts.Bucket
	if bucket == "" {
		switch {
		case id.InstanceID == "":
			return nil, errors.New("checkpoint: identity has no InstanceID and Options.Bucket is unset")
		case id.ConnectorID == "" && id.WorkloadID != "":
			return nil, fmt.Errorf("checkpoint: %w: set NEX_WORKLOAD_CONNECTOR_ID, or name the bucket in Options.Bucket", identity.ErrNoConnectorID)
		}
		bucket = identity.CheckpointBucket(id)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 1, Replicas: max(opts.Replicas, 0)})
		if errors.Is(err, jetstream.ErrBucketExists) {
			// Created concurrently (possibly with another config) between the
			// lookup and the create: use the bucket that now exists.
			kv, err = js.KeyValue(ctx, bucket)
		} else if err != nil {
			return nil, fmt.Errorf("checkpoint: create bucket %q: %w", bucket, err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: bucket %q: %w", bucket, err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: bucket %q status: %w", bucket, err)
	}
	if ttl := status.TTL(); ttl != 0 {
		return nil, fmt.Errorf("%w: bucket %q has TTL %s", ErrExpiringBucket, bucket, ttl)
	}
	s := &Store{kv: kv, bucket: bucket, timeout: timeout}
	if opts.Scope != "" {
		if err := s.bind(ctx, opts.Scope); err != nil {
			return nil, err
		}
	} else {
		if err := s.requireUnbound(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// requireUnbound fails an unscoped Open against a bucket that already has a
// recorded scope: the binding is a property of the bucket, so it is
// enforced regardless of whether the caller passed Options.Scope. It writes
// nothing.
func (s *Store) requireUnbound(ctx context.Context) error {
	entry, err := s.kv.Get(ctx, scopeKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("checkpoint: bucket %q: read scope: %w", s.bucket, err)
	}
	got := string(entry.Value())
	return fmt.Errorf("%w: bucket %q is bound to %q, but Open was called without a scope (pass the bucket's Options.Scope, or delete its %s key to unbind)",
		ErrScopeMismatch, s.bucket, got, scopeKey)
}

// bind records scope in the bucket's reserved scope key on first use, and
// otherwise checks that the recorded scope matches.
func (s *Store) bind(ctx context.Context, scope string) error {
	entry, err := s.kv.Get(ctx, scopeKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_, err = s.kv.Create(ctx, scopeKey, []byte(scope))
		if err == nil {
			return nil
		}
		if !isConflict(err) {
			return fmt.Errorf("checkpoint: bucket %q: bind scope: %w", s.bucket, err)
		}
		// Another instance bound the bucket between the Get and the Create:
		// compare against what it recorded.
		entry, err = s.kv.Get(ctx, scopeKey)
	}
	if err != nil {
		return fmt.Errorf("checkpoint: bucket %q: read scope: %w", s.bucket, err)
	}
	if got := string(entry.Value()); got != scope {
		return fmt.Errorf("%w: bucket %q is bound to %q, not %q (delete its %s key to rebind)",
			ErrScopeMismatch, s.bucket, got, scope, scopeKey)
	}
	return nil
}

// Bucket returns the name of the open bucket.
func (s *Store) Bucket() string { return s.bucket }

// Load returns key's value and the revision it was read at. An absent or
// deleted key returns (nil, 0, nil); 0 is the revision to pass to Save to
// create it. Absence is rev == 0; a stored value may be empty.
func (s *Store) Load(ctx context.Context, key string) ([]byte, uint64, error) {
	if err := checkKey(key); err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	entry, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("checkpoint: bucket %q: load %q: %w", s.bucket, key, err)
	}
	return entry.Value(), entry.Revision(), nil
}

// Save writes value under key as a compare-and-swap against rev, the
// revision Load returned: rev 0 creates the key, any other rev updates it
// only if it is still at that revision. A lost check, meaning another writer
// got there first, returns an error wrapping ErrConflict. Save returns the
// new revision.
//
// The returned conflict error deliberately exposes only ErrConflict in its
// chain: the underlying nats.go error is formatted with %v, not wrapped
// with %w. Match errors.Is(err, checkpoint.ErrConflict), not a nats.go
// sentinel such as jetstream.ErrKeyRevisionMismatch.
//
// Any other error leaves the outcome unknown: the write may still have been
// applied, for example if its ack was lost to a timeout. Load before
// retrying, because a blind retry with the same rev reports ErrConflict
// against your own write if the first attempt landed. Then:
//   - the loaded revision still equals rev (0 for a create): the write has
//     not landed (yet), so retry it with rev;
//   - the stored value is the one you saved: it landed, so continue from
//     the loaded revision. This equality check is a heuristic, not proof:
//     another instance at the same position can store identical bytes, and
//     such a dual writer is still caught at the next CAS, so this only
//     delays detection rather than losing it. A connector that needs
//     certainty puts a writer id or nonce in the value.
//   - anything else: another writer has been here, so return an error
//     wrapping ErrConflict.
//
// If the original write is applied late — after the recovery Load still
// saw rev, but before the retry — the retry's CAS loses to your own write
// and reports ErrConflict. That is a spurious but safe park: don't "fix"
// it by retrying on conflict, which is exactly the unconditional-write
// behavior this store exists to forbid.
func (s *Store) Save(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	if err := checkKey(key); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		newRev uint64
		err    error
	)
	if rev == 0 {
		newRev, err = s.kv.Create(ctx, key, value)
	} else {
		newRev, err = s.kv.Update(ctx, key, value, rev)
	}
	if isConflict(err) {
		return 0, fmt.Errorf("%w: bucket %q: key %q: %v", ErrConflict, s.bucket, key, err)
	}
	if err != nil {
		return 0, fmt.Errorf("checkpoint: bucket %q: save %q: %w", s.bucket, key, err)
	}
	return newRev, nil
}

// isConflict reports whether err is a lost CAS. ErrKeyExists alone is not
// enough: a replicated (R>1) stream reports a CAS conflict as code 10164
// while another write to the key is still in flight on the leader, and
// nats.go maps 10164 to ErrKeyRevisionMismatch for Update and to neither
// sentinel for a Create over a deleted key.
func isConflict(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

// checkKey rejects keys in the SDK's reserved "_" namespace.
func checkKey(key string) error {
	if strings.HasPrefix(key, "_") {
		return fmt.Errorf("%w: %q", ErrReservedKey, key)
	}
	return nil
}
