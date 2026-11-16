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
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
)

// openReplicated boots an in-process three-node JetStream cluster and opens
// a checkpoint store with three replicas on it. It also returns the client.
//
// A replicated bucket reports a lost compare-and-swap as 10071, as a single
// server does, unless another write to the same key is still in flight on
// the stream leader; then it reports 10164. Only a race produces 10164, and
// even then not every loser gets it.
func openReplicated(t *testing.T) (*checkpoint.Store, jetstream.JetStream) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts a three-node cluster")
	}
	// A clustered JetStream node must have its routes configured when it
	// starts, so reserve the three cluster ports first.
	ports := make([]int, 3)
	routes := make([]string, 3)
	for i := range ports {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve a cluster port: %v", err)
		}
		ports[i] = l.Addr().(*net.TCPAddr).Port
		routes[i] = fmt.Sprintf("nats://127.0.0.1:%d", ports[i])
		l.Close()
	}
	var url string
	for i := range 3 {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.ServerName = fmt.Sprintf("ckpt-%d", i)
		opts.JetStream = true
		opts.StoreDir = t.TempDir()
		opts.Cluster.Name = "ckpt"
		opts.Cluster.Host = "127.0.0.1"
		opts.Cluster.Port = ports[i]
		opts.Routes = server.RoutesFromStr(strings.Join(routes, ","))
		srv := natstest.RunServer(&opts)
		if srv == nil {
			t.Fatal("nats-server: failed to start a cluster node")
		}
		t.Cleanup(func() {
			srv.Shutdown()
			srv.WaitForShutdown()
		})
		if i == 0 {
			url = srv.ClientURL()
		}
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// The cluster takes a moment to elect a meta leader and see its peers.
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		store, err := checkpoint.Open(ctx, js, testID, checkpoint.Options{Replicas: 3})
		cancel()
		if err == nil {
			return store, js
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint.Open on the cluster: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestReplicatedBucketConflicts runs the conflict cases on one three-node
// cluster, each on its own key.
func TestReplicatedBucketConflicts(t *testing.T) {
	store, js := openReplicated(t)
	ctx := context.Background()

	// Sequential conflicts get 10071 here, as on a single server: these
	// check that a replicated bucket classifies them the same way. They
	// don't produce 10164.
	t.Run("stale save", func(t *testing.T) {
		rev, err := store.Save(ctx, "stale", []byte("1"), 0)
		if err != nil {
			t.Fatalf("first Save: %v", err)
		}
		if _, err := store.Save(ctx, "stale", []byte("2"), rev); err != nil {
			t.Fatalf("second Save: %v", err)
		}
		if _, err := store.Save(ctx, "stale", []byte("3"), rev); !errors.Is(err, checkpoint.ErrConflict) {
			t.Fatalf("Save at a stale revision: error = %v, want ErrConflict", err)
		}
	})

	t.Run("create over an existing key", func(t *testing.T) {
		if _, err := store.Save(ctx, "create", []byte("1"), 0); err != nil {
			t.Fatalf("first Save: %v", err)
		}
		if _, err := store.Save(ctx, "create", []byte("2"), 0); !errors.Is(err, checkpoint.ErrConflict) {
			t.Fatalf("Save at revision 0 over an existing key: error = %v, want ErrConflict", err)
		}
	})

	// Many writers Save from the same revision at once. Exactly one wins,
	// and every loser sees ErrConflict, not some other error.
	t.Run("concurrent writers", func(t *testing.T) {
		rev, err := store.Save(ctx, "race", []byte("start"), 0)
		if err != nil {
			t.Fatalf("first Save: %v", err)
		}
		oneWins(t, store, "race", rev)
	})

	// A create over a key an operator deleted retries at the tombstone's
	// revision inside nats.go. A loser of that retry that gets 10164 sees the
	// raw API error, which nats.go maps to no named error; a loser that gets
	// 10071 matches ErrKeyExists. This is the only subtest where an unmapped
	// 10164 reaches isConflict end to end.
	t.Run("concurrent creates over a deleted key", func(t *testing.T) {
		if _, err := store.Save(ctx, "deleted", []byte("start"), 0); err != nil {
			t.Fatalf("first Save: %v", err)
		}
		kv, err := js.KeyValue(ctx, store.Bucket())
		if err != nil {
			t.Fatalf("KeyValue: %v", err)
		}
		if err := kv.Delete(ctx, "deleted"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		oneWins(t, store, "deleted", 0)
	})
}

// oneWins has 16 writers Save key at rev at once, and checks that exactly one
// wins and every other one gets ErrConflict.
func oneWins(t *testing.T, store *checkpoint.Store, key string, rev uint64) {
	t.Helper()
	const writers = 16
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = store.Save(context.Background(), key, []byte(fmt.Sprint(i)), rev)
		}()
	}
	wg.Wait()
	won := 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, checkpoint.ErrConflict):
			t.Errorf("writer %d: error = %v, want ErrConflict", i, err)
		}
	}
	if won != 1 {
		t.Errorf("%d writers won, want exactly 1", won)
	}
}
