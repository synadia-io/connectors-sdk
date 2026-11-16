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

package checkpoint_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
)

// testID resolves to the default bucket CONN-CKPT-conn-1 (spec/checkpoint.md, D-9).
var testID = identity.Identity{Namespace: "test", ConnectorID: "conn-1", WorkloadID: "wl-1", InstanceID: "conn-1"}

// startJetStream boots an in-process JetStream server and returns a client
// for it. Hermetic: random port, a per-test store directory.
func startJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

func TestOpen_DefaultBucketFromIdentity(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, want := st.Bucket(), identity.CheckpointBucket(testID); got != want {
		t.Errorf("Bucket() = %q, want %q", got, want)
	}
	status := bucketStatus(t, js, "CONN-CKPT-conn-1")
	if status.History() != 1 {
		t.Errorf("created bucket History = %d, want 1", status.History())
	}
}

func TestOpen_BucketOverrideUsedAsIs(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "my-ckpt"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if st.Bucket() != "my-ckpt" {
		t.Errorf("Bucket() = %q, want %q", st.Bucket(), "my-ckpt")
	}
	if _, err := js.KeyValue(ctx, "my-ckpt"); err != nil {
		t.Fatalf("bucket my-ckpt was not created: %v", err)
	}
}

// bucketKV opens name's KV handle, failing the test on any error.
func bucketKV(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValue {
	t.Helper()
	kv, err := js.KeyValue(context.Background(), name)
	if err != nil {
		t.Fatalf("KeyValue(%q): %v", name, err)
	}
	return kv
}

// bucketStatus fetches name's KV status, failing the test on any error.
func bucketStatus(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValueStatus {
	t.Helper()
	kv := bucketKV(t, js, name)
	status, err := kv.Status(context.Background())
	if err != nil {
		t.Fatalf("Status(%q): %v", name, err)
	}
	return status
}

// staleLookupJS reports the first KeyValue lookup as not found, simulating a
// bucket created by another instance between Open's lookup and its create.
type staleLookupJS struct {
	jetstream.JetStream
	looked atomic.Bool
}

func (s *staleLookupJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	if !s.looked.Swap(true) {
		return nil, jetstream.ErrBucketNotFound
	}
	return s.JetStream.KeyValue(ctx, bucket)
}

func TestOpen_ConcurrentCreateUsesExistingBucket(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "race-ckpt", History: 5}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	if _, err := checkpoint.Open(ctx, &staleLookupJS{JetStream: js}, testID, checkpoint.Options{Bucket: "race-ckpt"}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	status := bucketStatus(t, js, "race-ckpt")
	if status.History() != 5 {
		t.Errorf("existing bucket History = %d after Open, want 5 (unchanged)", status.History())
	}
}

func TestOpen_ConcurrentCreateStillRejectsTTL(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "race-ckpt", TTL: time.Hour}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	st, err := checkpoint.Open(ctx, &staleLookupJS{JetStream: js}, testID, checkpoint.Options{Bucket: "race-ckpt"})
	if !errors.Is(err, checkpoint.ErrExpiringBucket) {
		t.Fatalf("Open: err = %v, want ErrExpiringBucket", err)
	}
	if st != nil {
		t.Errorf("Open: Store = %v, want nil", st)
	}
}

// An operator-created bucket is used as it is: Open must not rewrite its
// configuration: its replicas and history are the operator's.
func TestOpen_UsesExistingBucketUnchanged(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "op-ckpt", History: 5}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "op-ckpt"}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	status := bucketStatus(t, js, "op-ckpt")
	if status.History() != 5 {
		t.Errorf("existing bucket History = %d after Open, want 5 (unchanged)", status.History())
	}
}

// Expiring records would silently forget progress and re-emit data.
func TestOpen_RejectsExpiringBucket(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "ttl-ckpt", TTL: time.Hour}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "ttl-ckpt"})
	if !errors.Is(err, checkpoint.ErrExpiringBucket) {
		t.Fatalf("Open on a TTL bucket: err = %v, want ErrExpiringBucket", err)
	}
	if st != nil {
		t.Errorf("Open on a TTL bucket: Store = %v, want nil", st)
	}
}

// An empty InstanceID with no bucket override has no safe default: opening
// the unqualified shared bucket "CONN-CKPT-" would mix unrelated connectors'
// checkpoints.
func TestOpen_RejectsEmptyInstanceID(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	st, err := checkpoint.Open(ctx, js, identity.Identity{}, checkpoint.Options{})
	if err == nil {
		t.Fatal("Open with empty InstanceID: err = nil, want non-nil")
	}
	if st != nil {
		t.Errorf("Open with empty InstanceID: Store = %v, want nil", st)
	}
	if _, err := js.KeyValue(ctx, "CONN-CKPT-"); !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Errorf("bucket \"CONN-CKPT-\": err = %v, want ErrBucketNotFound (must not be created)", err)
	}
}

// The default bucket keeps its name from one launch to the next only if the
// instance id does (spec/status.md, D-3). The workload id is new on every
// launch, so a bucket named after it would start over each time, and two
// overlapping instances would open different buckets, where neither one's
// CAS can catch the other. The connector id and the "-dev" fallback are
// stable, and a named Options.Bucket doesn't depend on the id at all.
func TestOpen_DefaultBucketNeedsAStableID(t *testing.T) {
	for _, tt := range []struct {
		name, connectorID, workloadID, bucket string
		want                                  string // the bucket opened; "" means ErrNoConnectorID
	}{
		{"connector id", "conn-1", "wl-1", "", "CONN-CKPT-conn-1"},
		{"workload id only", "", "wl-1", "", ""},
		{"workload id only, bucket named", "", "wl-1", "my-ckpt", "my-ckpt"},
		{"neither", "", "", "", "CONN-CKPT-inlet-dev"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("NEX_WORKLOAD_CONNECTOR_ID", tt.connectorID)
			t.Setenv("NEX_WORKLOAD_ID", tt.workloadID)
			js := startJetStream(t)
			open := jetstream.JetStream(js)
			if tt.want == "" {
				// A bucket named after a workload id may already exist,
				// left behind by an earlier SDK: Open must refuse before it
				// looks one up, not only before it would create one.
				open = untouchedJS{JetStream: js, t: t}
			}

			st, err := checkpoint.Open(context.Background(), open, identity.Detect("inlet"), checkpoint.Options{Bucket: tt.bucket})
			if tt.want == "" {
				if !errors.Is(err, identity.ErrNoConnectorID) {
					t.Fatalf("Open: err = %v, want ErrNoConnectorID", err)
				}
				if st != nil {
					t.Errorf("Open: Store = %v, want nil", st)
				}
				if names := bucketNames(t, js); len(names) != 0 {
					t.Errorf("buckets after the refused Open = %q, want none", names)
				}
				return
			}
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if st.Bucket() != tt.want {
				t.Errorf("Bucket() = %q, want %q", st.Bucket(), tt.want)
			}
			if names := bucketNames(t, js); !slices.Equal(names, []string{tt.want}) {
				t.Errorf("buckets after Open = %q, want [%q]", names, tt.want)
			}
		})
	}
}

// untouchedJS fails the test if Open looks up or creates a bucket.
type untouchedJS struct {
	jetstream.JetStream
	t *testing.T
}

func (u untouchedJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) {
	u.t.Error("Open looked up a bucket before refusing")
	return nil, jetstream.ErrBucketNotFound
}

func (u untouchedJS) CreateKeyValue(context.Context, jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	u.t.Error("Open created a bucket before refusing")
	return nil, errors.New("unreachable")
}

// bucketNames lists the server's KV buckets, failing the test on any error.
func bucketNames(t *testing.T, js jetstream.JetStream) []string {
	t.Helper()
	lister := js.KeyValueStoreNames(context.Background())
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Error(); err != nil {
		t.Fatalf("KeyValueStoreNames: %v", err)
	}
	return names
}

// An override is still subject to JetStream's own bucket-name constraints
// (spec/checkpoint.md).
func TestOpen_InvalidOverrideNameFails(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	_, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "bad.name"})
	if !errors.Is(err, jetstream.ErrInvalidBucketName) {
		t.Fatalf("Open with invalid bucket name: err = %v, want ErrInvalidBucketName", err)
	}
}

func TestOpen_CancelledContextFails(t *testing.T) {
	js := startJetStream(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{})
	if err == nil {
		t.Fatal("Open with a cancelled context: err = nil, want non-nil")
	}
	if st != nil {
		t.Errorf("Open with a cancelled context: Store = %v, want nil", st)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Open with a cancelled context: err = %v, want context.Canceled", err)
	}
}

func TestOpen_ScopeBindsThenMatches(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "inlet-v1:abc"}); err != nil {
		t.Fatalf("first Open (binds scope): %v", err)
	}
	kv := bucketKV(t, js, identity.CheckpointBucket(testID))
	entry, err := kv.Get(ctx, "_scope")
	if err != nil {
		t.Fatalf("Get(_scope) after first Open: %v", err)
	}
	if got := string(entry.Value()); got != "inlet-v1:abc" {
		t.Errorf("_scope after first Open = %q, want %q", got, "inlet-v1:abc")
	}

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "inlet-v1:abc"}); err != nil {
		t.Fatalf("second Open with the same scope: %v", err)
	}
	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "inlet-v1:other"})
	if !errors.Is(err, checkpoint.ErrScopeMismatch) {
		t.Fatalf("Open with a different scope: err = %v, want ErrScopeMismatch", err)
	}
	if st != nil {
		t.Errorf("Open with a different scope returned a non-nil Store")
	}
	entry, err = kv.Get(ctx, "_scope")
	if err != nil {
		t.Fatalf("Get(_scope) after mismatching Open: %v", err)
	}
	if got := string(entry.Value()); got != "inlet-v1:abc" {
		t.Errorf("_scope after mismatching Open = %q, want unchanged %q", got, "inlet-v1:abc")
	}
}

// Without a scope, Open must not write a binding record: existing buckets
// (mongodb's) stay byte-for-byte as they were.
func TestOpen_NoScopeWritesNoRecord(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	kv := bucketKV(t, js, identity.CheckpointBucket(testID))
	if _, err := kv.Get(ctx, "_scope"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("Get(_scope) err = %v, want ErrKeyNotFound (no binding record without Options.Scope)", err)
	}
	if status := bucketStatus(t, js, identity.CheckpointBucket(testID)); status.Values() != 0 {
		t.Errorf("bucket Values() = %d, want 0 (Open without a scope writes nothing)", status.Values())
	}
}

// staleScopeKV misses the first Get of "_scope", simulating another
// instance binding the bucket between bind's Get and its Create.
type staleScopeKV struct {
	jetstream.KeyValue
	gets atomic.Int32
}

func (k *staleScopeKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == "_scope" && k.gets.Add(1) == 1 {
		return nil, jetstream.ErrKeyNotFound
	}
	return k.KeyValue.Get(ctx, key)
}

// staleScopeJS hands Open a staleScopeKV for the bucket.
type staleScopeJS struct {
	jetstream.JetStream
	kv *staleScopeKV
}

func (s *staleScopeJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := s.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}
	s.kv = &staleScopeKV{KeyValue: kv}
	return s.kv, nil
}

func TestOpen_ConcurrentBindSameScope(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "a"}); err != nil {
		t.Fatalf("bind scope %q: %v", "a", err)
	}

	w := &staleScopeJS{JetStream: js}
	if _, err := checkpoint.Open(ctx, w, testID, checkpoint.Options{Scope: "a"}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if w.kv == nil {
		t.Fatal("staleScopeJS: KeyValue was never called")
	}
	if got := w.kv.gets.Load(); got != 2 {
		t.Errorf("gets = %d, want 2 (concurrent-bind branch did not run)", got)
	}
}

func TestOpen_ConcurrentBindOtherScope(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "a"}); err != nil {
		t.Fatalf("bind scope %q: %v", "a", err)
	}

	w := &staleScopeJS{JetStream: js}
	st, err := checkpoint.Open(ctx, w, testID, checkpoint.Options{Scope: "b"})
	if !errors.Is(err, checkpoint.ErrScopeMismatch) {
		t.Fatalf("Open: err = %v, want ErrScopeMismatch", err)
	}
	if st != nil {
		t.Errorf("Open: Store = %v, want nil", st)
	}
	if w.kv == nil {
		t.Fatal("staleScopeJS: KeyValue was never called")
	}
	if got := w.kv.gets.Load(); got != 2 {
		t.Errorf("gets = %d, want 2 (concurrent-bind branch did not run)", got)
	}
}

// Pins the remedy ErrScopeMismatch's message promises: deleting "_scope"
// lets an operator rebind the bucket to a new scope.
func TestOpen_RebindAfterScopeDeleted(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "a"}); err != nil {
		t.Fatalf("bind scope %q: %v", "a", err)
	}

	kv := bucketKV(t, js, identity.CheckpointBucket(testID))
	if err := kv.Delete(ctx, "_scope"); err != nil {
		t.Fatalf("Delete(_scope): %v", err)
	}

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "b"}); err != nil {
		t.Fatalf("Open after deleting _scope: %v", err)
	}
	entry, err := kv.Get(ctx, "_scope")
	if err != nil {
		t.Fatalf("Get(_scope) after rebind: %v", err)
	}
	if got := string(entry.Value()); got != "b" {
		t.Errorf("_scope after rebind = %q, want %q", got, "b")
	}
}

func openStore(t *testing.T, js jetstream.JetStream) *checkpoint.Store {
	t.Helper()
	st, err := checkpoint.Open(context.Background(), js, testID, checkpoint.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return st
}

func TestLoad_AbsentKey(t *testing.T) {
	st := openStore(t, startJetStream(t))

	val, rev, err := st.Load(context.Background(), "checkpoint")
	if err != nil || val != nil || rev != 0 {
		t.Fatalf("Load(absent) = (%q, %d, %v), want (nil, 0, nil)", val, rev, err)
	}
}

func TestSave_CreateThenUpdate(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx := context.Background()

	rev1, err := st.Save(ctx, "checkpoint", []byte(`{"v":1}`), 0)
	if err != nil || rev1 == 0 {
		t.Fatalf("Save(create) = (%d, %v), want a revision > 0", rev1, err)
	}
	val, rev, err := st.Load(ctx, "checkpoint")
	if err != nil || string(val) != `{"v":1}` || rev != rev1 {
		t.Fatalf("Load after create = (%q, %d, %v), want (%q, %d, nil)", val, rev, err, `{"v":1}`, rev1)
	}

	rev2, err := st.Save(ctx, "checkpoint", []byte(`{"v":2}`), rev1)
	if err != nil || rev2 <= rev1 {
		t.Fatalf("Save(update) = (%d, %v), want a revision > %d", rev2, err, rev1)
	}
	val, rev, err = st.Load(ctx, "checkpoint")
	if err != nil || string(val) != `{"v":2}` || rev != rev2 {
		t.Fatalf("Load after update = (%q, %d, %v), want (%q, %d, nil)", val, rev, err, `{"v":2}`, rev2)
	}
}

// The another-writer guard (spec/checkpoint.md): two instances load the same
// revision; the second to save must lose, not overwrite.
func TestSave_StaleRevisionConflicts(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx := context.Background()
	rev, err := st.Save(ctx, "checkpoint", []byte("a"), 0)
	if err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	if _, err := st.Save(ctx, "checkpoint", []byte("writer-1"), rev); err != nil {
		t.Fatalf("first writer Save: %v", err)
	}
	_, err = st.Save(ctx, "checkpoint", []byte("writer-2"), rev)
	if !errors.Is(err, checkpoint.ErrConflict) {
		t.Fatalf("second writer Save with stale revision: err = %v, want ErrConflict", err)
	}
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Errorf("second writer Save error chain also matches ErrKeyRevisionMismatch, want only ErrConflict: %v", err)
	}
	if !strings.Contains(err.Error(), st.Bucket()) {
		t.Errorf("second writer Save error = %q, want it to name the bucket %q", err, st.Bucket())
	}
	val, _, err := st.Load(ctx, "checkpoint")
	if err != nil {
		t.Fatalf("Load after conflict: %v", err)
	}
	if string(val) != "writer-1" {
		t.Errorf("value after conflict = %q, want %q (the loser must not overwrite)", val, "writer-1")
	}
}

func TestSave_CreateOnExistingConflicts(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx := context.Background()
	if _, err := st.Save(ctx, "checkpoint", []byte("first"), 0); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := st.Save(ctx, "checkpoint", []byte("second"), 0)
	if !errors.Is(err, checkpoint.ErrConflict) {
		t.Fatalf("second create of the same key: err = %v, want ErrConflict", err)
	}
}

// A deleted key reads as absent and can be created again.
func TestLoad_DeletedKeyIsAbsent(t *testing.T) {
	js := startJetStream(t)
	st := openStore(t, js)
	ctx := context.Background()
	if _, err := st.Save(ctx, "checkpoint", []byte("x"), 0); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := bucketKV(t, js, st.Bucket()).Delete(ctx, "checkpoint"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	val, rev, err := st.Load(ctx, "checkpoint")
	if err != nil || val != nil || rev != 0 {
		t.Fatalf("Load(deleted) = (%q, %d, %v), want (nil, 0, nil)", val, rev, err)
	}
	if _, err := st.Save(ctx, "checkpoint", []byte("y"), 0); err != nil {
		t.Fatalf("Save(create) after delete: %v", err)
	}
}

func TestReservedKeysRejected(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx := context.Background()

	if _, _, err := st.Load(ctx, "_scope"); !errors.Is(err, checkpoint.ErrReservedKey) {
		t.Errorf("Load(_scope): err = %v, want ErrReservedKey", err)
	}
	if _, err := st.Save(ctx, "_mine", []byte("x"), 0); !errors.Is(err, checkpoint.ErrReservedKey) {
		t.Errorf("Save(_mine): err = %v, want ErrReservedKey", err)
	}
	if _, err := st.Save(ctx, "_scope", []byte("x"), 0); !errors.Is(err, checkpoint.ErrReservedKey) {
		t.Errorf("Save(_scope): err = %v, want ErrReservedKey", err)
	}
}

func TestLoad_CancelledContextFails(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := st.Load(ctx, "checkpoint")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Load with a cancelled context: err = %v, want context.Canceled", err)
	}
}

func TestSave_CancelledContextFails(t *testing.T) {
	st := openStore(t, startJetStream(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := st.Save(ctx, "checkpoint", []byte("x"), 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Save with a cancelled context: err = %v, want context.Canceled", err)
	}
	if errors.Is(err, checkpoint.ErrConflict) {
		t.Errorf("Save with a cancelled context: err = %v, must not be classified as ErrConflict", err)
	}
}

// TestKVConflictErrorMapping pins the sentinel mapping isConflict checks
// first, on a single (non-replicated) server: a stale Update matches
// jetstream.ErrKeyRevisionMismatch, and a Create on an existing key matches
// jetstream.ErrKeyExists. If a nats.go upgrade breaks this, re-check
// isConflict (and TestIsConflict) against the new nats.go before assuming
// Save is broken.
func TestKVConflictErrorMapping(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "pin", History: 1})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	rev, err := kv.Create(ctx, "k", []byte("a"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, errCreate := kv.Create(ctx, "k", []byte("b"))
	if _, err := kv.Update(ctx, "k", []byte("c"), rev); err != nil {
		t.Fatalf("Update: %v", err)
	}
	_, errUpdate := kv.Update(ctx, "k", []byte("d"), rev)

	if !errors.Is(errCreate, jetstream.ErrKeyExists) {
		t.Errorf("Create on existing key: err = %v, want errors.Is(ErrKeyExists)", errCreate)
	}
	if !errors.Is(errUpdate, jetstream.ErrKeyRevisionMismatch) {
		t.Errorf("Update with stale revision: err = %v, want errors.Is(ErrKeyRevisionMismatch)", errUpdate)
	}
}

// An unscoped Open must not bypass an existing scope binding: the binding
// is a property of the bucket, not of the caller.
func TestOpen_UnscopedOnBoundBucketFails(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Scope: "inlet-v1:abc"}); err != nil {
		t.Fatalf("first Open (binds scope): %v", err)
	}

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{})
	if !errors.Is(err, checkpoint.ErrScopeMismatch) {
		t.Fatalf("unscoped Open on a bound bucket: err = %v, want ErrScopeMismatch", err)
	}
	if st != nil {
		t.Errorf("unscoped Open on a bound bucket: Store = %v, want nil", st)
	}

	kv := bucketKV(t, js, identity.CheckpointBucket(testID))
	entry, err := kv.Get(ctx, "_scope")
	if err != nil {
		t.Fatalf("Get(_scope) after unscoped Open: %v", err)
	}
	if got := string(entry.Value()); got != "inlet-v1:abc" {
		t.Errorf("_scope after unscoped Open = %q, want unchanged %q", got, "inlet-v1:abc")
	}
}

// errCodeReplicasUnsupported is nats-server's JSStreamReplicasNotSupportedErr
// code, returned when a non-clustered server rejects Replicas > 1. nats.go
// exports no constant for it, so it is pinned here.
const errCodeReplicasUnsupported jetstream.ErrorCode = 10074

// Options.Replicas must reach CreateKeyValue on the create path. On a
// single (non-clustered) server, requesting 3 replicas fails the create,
// proving the value was passed through rather than silently ignored.
func TestOpen_ReplicasPassedOnCreate(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	st, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "r3-ckpt", Replicas: 3})
	if err == nil {
		t.Fatalf("Open with Replicas: 3 on a single server: err = nil, want the create-bucket error")
	}
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != errCodeReplicasUnsupported {
		t.Fatalf("Open with Replicas: 3 on a single server: err = %v, want an APIError with code %d (proving Replicas reached CreateKeyValue)", err, errCodeReplicasUnsupported)
	}
	if st != nil {
		t.Errorf("Open with Replicas: 3 on a single server: Store = %v, want nil", st)
	}
	if _, err := js.KeyValue(ctx, "r3-ckpt"); !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Errorf("bucket %q: err = %v, want ErrBucketNotFound (must not exist after a failed create)", "r3-ckpt", err)
	}
}

// A negative Replicas, like zero, means 1.
func TestOpen_NegativeReplicasUsesDefault(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "neg-r-ckpt", Replicas: -1}); err != nil {
		t.Fatalf("Open with Replicas: -1: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_neg-r-ckpt")
	if err != nil {
		t.Fatalf("Stream(KV_neg-r-ckpt): %v", err)
	}
	if got := stream.CachedInfo().Config.Replicas; got != 1 {
		t.Errorf("KV_neg-r-ckpt Replicas = %d, want 1", got)
	}
}

// Replicas must never change an existing bucket.
func TestOpen_ReplicasIgnoredForExistingBucket(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "op-r1", History: 1}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	if _, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Bucket: "op-r1", Replicas: 3}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_op-r1")
	if err != nil {
		t.Fatalf("Stream(KV_op-r1): %v", err)
	}
	if got := stream.CachedInfo().Config.Replicas; got != 1 {
		t.Errorf("KV_op-r1 Replicas after Open = %d, want unchanged 1", got)
	}
}

// An operator Delete of the key mid-run surfaces as ErrConflict on the next
// Save: the park reason can mean "the key was reset", not only "another
// writer".
func TestSave_AfterOperatorDeleteConflicts(t *testing.T) {
	js := startJetStream(t)
	st := openStore(t, js)
	ctx := context.Background()

	rev, err := st.Save(ctx, "checkpoint", []byte("x"), 0)
	if err != nil {
		t.Fatalf("seed Save: %v", err)
	}
	if err := bucketKV(t, js, st.Bucket()).Delete(ctx, "checkpoint"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = st.Save(ctx, "checkpoint", []byte("y"), rev)
	if !errors.Is(err, checkpoint.ErrConflict) {
		t.Fatalf("Save after operator Delete: err = %v, want ErrConflict", err)
	}
}

// An operator Purge of the key mid-run surfaces as ErrConflict on the next
// Save, the same as Delete.
func TestSave_AfterOperatorPurgeConflicts(t *testing.T) {
	js := startJetStream(t)
	st := openStore(t, js)
	ctx := context.Background()

	rev, err := st.Save(ctx, "checkpoint", []byte("x"), 0)
	if err != nil {
		t.Fatalf("seed Save: %v", err)
	}
	if err := bucketKV(t, js, st.Bucket()).Purge(ctx, "checkpoint"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	_, err = st.Save(ctx, "checkpoint", []byte("y"), rev)
	if !errors.Is(err, checkpoint.ErrConflict) {
		t.Fatalf("Save after operator Purge: err = %v, want ErrConflict", err)
	}
}

// blockingKeyValueJS is a fake jetstream.JetStream whose KeyValue blocks
// until the context is done, simulating a server that never responds.
type blockingKeyValueJS struct {
	jetstream.JetStream
}

func (blockingKeyValueJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Options.Timeout must bound Open even against a server that never
// responds. The parent context has its own generous timeout so a
// regression in Open can't hang the test binary.
func TestOpen_TimeoutBoundsOpen(t *testing.T) {
	parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := checkpoint.Open(parent, blockingKeyValueJS{}, testID, checkpoint.Options{Timeout: 50 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open against a blocking JetStream: err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Open took %s, want well under 2s", elapsed)
	}
}

// errBoom is a synthetic sentinel for a KV read failure that is not
// ErrKeyNotFound.
var errBoom = errors.New("boom")

// failingScopeKV always fails Get("_scope") with errBoom, simulating a KV
// read failure (not merely an absent key).
type failingScopeKV struct {
	jetstream.KeyValue
}

func (failingScopeKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == "_scope" {
		return nil, errBoom
	}
	return nil, jetstream.ErrKeyNotFound
}

// failingScopeJS hands Open a failingScopeKV for the bucket.
type failingScopeJS struct {
	jetstream.JetStream
	kv *failingScopeKV
}

func (s *failingScopeJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := s.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}
	s.kv = &failingScopeKV{KeyValue: kv}
	return s.kv, nil
}

// requireUnbound must fail closed: a read error other than ErrKeyNotFound
// must not be treated as "unbound", and must not be mistaken for a real
// ErrScopeMismatch.
func TestOpen_RequireUnboundFailsClosedOnReadError(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: identity.CheckpointBucket(testID), History: 1}); err != nil {
		t.Fatalf("pre-create bucket: %v", err)
	}

	w := &failingScopeJS{JetStream: js}
	st, err := checkpoint.Open(ctx, w, testID, checkpoint.Options{})
	if err == nil {
		t.Fatal("unscoped Open against a failing scope read: err = nil, want non-nil")
	}
	if errors.Is(err, checkpoint.ErrScopeMismatch) {
		t.Errorf("unscoped Open against a failing scope read: err = %v, must not match ErrScopeMismatch", err)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("unscoped Open against a failing scope read: err = %v, want it to wrap errBoom", err)
	}
	if st != nil {
		t.Errorf("unscoped Open against a failing scope read: Store = %v, want nil", st)
	}
	if w.kv == nil {
		t.Fatal("failingScopeJS: KeyValue was never called")
	}
}
