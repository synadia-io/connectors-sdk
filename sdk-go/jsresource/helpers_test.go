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

package jsresource_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startJetStream boots an in-process JetStream server and returns a client
// for it. Hermetic: random port, a per-test store directory.
func startJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	return connect(t, runServer(t, &opts))
}

// startLimited boots an in-process JetStream server with two users: "admin",
// who may do anything, and "limited", who may not publish to deny. It
// returns a client for each.
func startLimited(t *testing.T, deny ...string) (admin, limited jetstream.JetStream) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.Users = []*server.User{
		{Username: "admin", Password: "admin"},
		{Username: "limited", Password: "limited", Permissions: &server.Permissions{
			Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: deny},
		}},
	}
	srv := runServer(t, &opts)
	return connect(t, srv, nats.UserInfo("admin", "admin")), connect(t, srv, nats.UserInfo("limited", "limited"))
}

func runServer(t *testing.T, opts *server.Options) *server.Server {
	t.Helper()
	srv := natstest.RunServer(opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv
}

func connect(t *testing.T, srv *server.Server, opts ...nats.Option) jetstream.JetStream {
	t.Helper()
	// The default handler prints async errors such as the permissions
	// violations the tests provoke; LastError still records them.
	opts = append(opts, nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	nc, err := nats.Connect(srv.ClientURL(), opts...)
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

func mustCreateStream(t *testing.T, js jetstream.JetStream, cfg jetstream.StreamConfig) {
	t.Helper()
	if _, err := js.CreateStream(context.Background(), cfg); err != nil {
		t.Fatalf("CreateStream(%s): %v", cfg.Name, err)
	}
}

func streamConfig(t *testing.T, js jetstream.JetStream, name string) jetstream.StreamConfig {
	t.Helper()
	st, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatalf("Stream(%s): %v", name, err)
	}
	return st.CachedInfo().Config
}

// hiddenStreamJS hides streams from its first Stream lookup, as if another
// instance created the stream between this instance's lookup and create.
type hiddenStreamJS struct {
	jetstream.JetStream
	looked atomic.Bool
}

func (h *hiddenStreamJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if h.looked.CompareAndSwap(false, true) {
		return nil, jetstream.ErrStreamNotFound
	}
	return h.JetStream.Stream(ctx, name)
}

// timeoutJS fails one JetStream call, the one named by fail, with a timeout
// without sending anything, like a request lost to a JetStream leader
// election. With deny set, the call first provokes a denial of that subject,
// as if another request's violation arrived during this one.
type timeoutJS struct {
	jetstream.JetStream
	t    *testing.T
	fail string
	deny string
}

func (j timeoutJS) timesOut(call string) bool {
	if call != j.fail {
		return false
	}
	if j.deny != "" {
		provokeDenial(j.t, j.JetStream, j.deny)
	}
	return true
}

func (j timeoutJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if j.timesOut("Stream") {
		return nil, context.DeadlineExceeded
	}
	return j.JetStream.Stream(ctx, name)
}

func (j timeoutJS) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if j.timesOut("CreateStream") {
		return nil, context.DeadlineExceeded
	}
	return j.JetStream.CreateStream(ctx, cfg)
}

func (j timeoutJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if j.timesOut("UpdateStream") {
		return nil, context.DeadlineExceeded
	}
	return j.JetStream.UpdateStream(ctx, cfg)
}

func (j timeoutJS) Consumer(ctx context.Context, stream, name string) (jetstream.Consumer, error) {
	if j.timesOut("Consumer") {
		return nil, context.DeadlineExceeded
	}
	return j.JetStream.Consumer(ctx, stream, name)
}

func (j timeoutJS) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if j.timesOut("CreateConsumer") {
		return nil, context.DeadlineExceeded
	}
	return j.JetStream.CreateConsumer(ctx, stream, cfg)
}

// provokeDenial publishes to subj, which js's credential may not publish to,
// and waits until the connection reports the violation as its last error.
func provokeDenial(t *testing.T, js jetstream.JetStream, subj string) {
	t.Helper()
	nc := js.Conn()
	if err := nc.Publish(subj, nil); err != nil {
		t.Fatalf("Publish(%s): %v", subj, err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if last := nc.LastError(); errors.Is(last, nats.ErrPermissionViolation) && strings.Contains(last.Error(), strconv.Quote(subj)) {
			return
		}
	}
	t.Fatalf("the connection never reported a denial of %s (last error %v)", subj, nc.LastError())
}

// failLaterStreamJS passes its first Stream lookup through and fails every
// later one with err, like a read-back lost after an update landed.
type failLaterStreamJS struct {
	jetstream.JetStream
	err     error
	lookups int
}

func (f *failLaterStreamJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	f.lookups++
	if f.lookups > 1 {
		return nil, f.err
	}
	return f.JetStream.Stream(ctx, name)
}

// hiddenConsumerJS does the same for its first Consumer lookup.
type hiddenConsumerJS struct {
	jetstream.JetStream
	looked atomic.Bool
}

func (h *hiddenConsumerJS) Consumer(ctx context.Context, stream, name string) (jetstream.Consumer, error) {
	if h.looked.CompareAndSwap(false, true) {
		return nil, jetstream.ErrConsumerNotFound
	}
	return h.JetStream.Consumer(ctx, stream, name)
}

// clobberJS runs clobber after each of its first n successful
// UpdateStream calls, like a concurrent writer whose update, built from an
// older read, replaces the one that just landed.
type clobberJS struct {
	jetstream.JetStream
	n       int
	updates int
	clobber func()
}

func (c *clobberJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	st, err := c.JetStream.UpdateStream(ctx, cfg)
	if err == nil {
		c.updates++
		if c.updates <= c.n {
			c.clobber()
		}
	}
	return st, err
}
