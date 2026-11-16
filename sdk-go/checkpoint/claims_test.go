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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
)

// startClaims boots an in-process JetStream server with two users: "admin",
// who may do anything, and "conn", who may publish only to allow and
// subscribe only to "_INBOX.>". It returns a client for each.
func startClaims(t *testing.T, allow ...string) (admin, conn jetstream.JetStream) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.Users = []*server.User{
		{Username: "admin", Password: "admin"},
		{Username: "conn", Password: "conn", Permissions: &server.Permissions{
			Publish:   &server.SubjectPermission{Allow: allow},
			Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
		}},
	}
	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return connectAs(t, srv, "admin"), connectAs(t, srv, "conn")
}

func connectAs(t *testing.T, srv *server.Server, user string) jetstream.JetStream {
	t.Helper()
	// The default handler prints the permissions violations a missing claim
	// provokes; LastError still records them.
	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo(user, user),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	if err != nil {
		t.Fatalf("nats.Connect(%s): %v", user, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// TestClaimSetIsEnough runs the Store under credentials that hold exactly
// the checkpoint bucket's subjects spec/credentials.md (D-10) lists, and
// "_INBOX.>" for the replies, and nothing else. If the Store starts making
// another request, this fails, and the claim set must grow with it.
func TestClaimSetIsEnough(t *testing.T) {
	bucket := identity.CheckpointBucket(testID)
	use := []string{
		"$JS.API.STREAM.INFO.KV_" + bucket,
		"$JS.API.DIRECT.GET.KV_" + bucket + ".>",
		"$KV." + bucket + ".>",
	}
	create := append([]string{"$JS.API.INFO", "$JS.API.STREAM.CREATE.KV_" + bucket}, use...)

	t.Run("connector creates the bucket", func(t *testing.T) {
		admin, js := startClaims(t, create...)
		openUnder(t, js, "scope-a") // creates the bucket and binds it
		st := openUnder(t, js, "scope-a")
		useStore(t, admin, js, st)
	})

	// The least-privilege profile: no $JS.API.INFO, no STREAM.CREATE.
	t.Run("operator created the bucket", func(t *testing.T) {
		admin, js := startClaims(t, use...)
		if _, err := admin.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: bucket, History: 1}); err != nil {
			t.Fatalf("pre-create bucket: %v", err)
		}
		openUnder(t, js, "")        // reads the scope key, writes nothing
		openUnder(t, js, "scope-a") // binds the bucket
		// Another instance binds it between this one's read and write: the
		// write loses, and Open reads the scope again.
		w := &staleScopeJS{JetStream: js}
		st := openUnder(t, w, "scope-a")
		if w.kv == nil || w.kv.gets.Load() != 2 {
			t.Fatal("the concurrent-bind branch did not run")
		}
		useStore(t, admin, js, st)
	})

	// Another instance creates the bucket, with another configuration,
	// between this one's lookup and its create: the create is refused, and
	// Open looks the bucket up again.
	t.Run("bucket created concurrently", func(t *testing.T) {
		admin, js := startClaims(t, create...)
		if _, err := admin.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: bucket, History: 5}); err != nil {
			t.Fatalf("pre-create bucket: %v", err)
		}
		openUnder(t, &staleLookupJS{JetStream: js}, "scope-a")
	})
}

// openUnder opens testID's default bucket on js with scope. The short
// timeout turns a denied request, which gets no reply, into a quick failure.
func openUnder(t *testing.T, js jetstream.JetStream, scope string) *checkpoint.Store {
	t.Helper()
	st, err := checkpoint.Open(context.Background(), js, testID, checkpoint.Options{Scope: scope, Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open(scope %q): %v (last connection error: %v)", scope, err, js.Conn().LastError())
	}
	return st
}

// useStore runs every Load and Save path through st, opened on js: a
// create, a compare-and-swap update, the two ways a write loses, the
// recovery Load after a conflict, and a create over a key an operator
// deleted, which reads the delete marker and writes at its revision.
func useStore(t *testing.T, admin, js jetstream.JetStream, st *checkpoint.Store) {
	t.Helper()
	ctx := context.Background()
	fatalf := func(format string, args ...any) {
		t.Helper()
		t.Fatalf(format+" (last connection error: %v)", append(args, js.Conn().LastError())...)
	}
	if val, rev, err := st.Load(ctx, "position"); err != nil || val != nil || rev != 0 {
		fatalf("Load(absent) = (%q, %d, %v), want (nil, 0, nil)", val, rev, err)
	}
	rev1, err := st.Save(ctx, "position", []byte("1"), 0)
	if err != nil {
		fatalf("Save(create): %v", err)
	}
	rev2, err := st.Save(ctx, "position", []byte("2"), rev1)
	if err != nil {
		fatalf("Save(update): %v", err)
	}
	if _, err := st.Save(ctx, "position", []byte("3"), rev1); !errors.Is(err, checkpoint.ErrConflict) {
		fatalf("Save at a stale revision: err = %v, want ErrConflict", err)
	}
	if _, err := st.Save(ctx, "position", []byte("3"), 0); !errors.Is(err, checkpoint.ErrConflict) {
		fatalf("Save(create) over an existing key: err = %v, want ErrConflict", err)
	}
	if val, rev, err := st.Load(ctx, "position"); err != nil || string(val) != "2" || rev != rev2 {
		fatalf("Load after the conflicts = (%q, %d, %v), want (%q, %d, nil)", val, rev, err, "2", rev2)
	}
	if err := bucketKV(t, admin, st.Bucket()).Delete(ctx, "position"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Save(ctx, "position", []byte("4"), 0); err != nil {
		fatalf("Save(create) over a deleted key: %v", err)
	}
}
