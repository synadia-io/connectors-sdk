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

// Package natsenv provides an embedded NATS server for the conformance and
// scenario tiers: JetStream on, plus start/stop/restart so scenarios can
// simulate a server outage and assert client reconnect.
package natsenv

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// Server is a running embedded NATS server bound to a concrete port so it can be
// restarted on the same address.
type Server struct {
	srv      *server.Server
	opts     server.Options
	Port     int
	URL      string
	storeDir string
	log      *logCapture
}

// logCapture is the server.Logger the embedded server writes to: every
// line the server logs at notice, warning, error or fatal level, kept in
// memory so a test can assert on what the server itself reported about a
// client. Debug and trace are off, so those two methods are never called.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) record(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *logCapture) Noticef(format string, v ...any) { l.record(format, v...) }
func (l *logCapture) Warnf(format string, v ...any)   { l.record(format, v...) }
func (l *logCapture) Fatalf(format string, v ...any)  { l.record(format, v...) }
func (l *logCapture) Errorf(format string, v ...any)  { l.record(format, v...) }
func (l *logCapture) Debugf(format string, v ...any)  { l.record(format, v...) }
func (l *logCapture) Tracef(format string, v ...any)  { l.record(format, v...) }

func (l *logCapture) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// Option adjusts the server options Start builds, after the defaults
// (JetStream on, the store dir, the port) are set and before the server runs.
// Restart keeps every option, since it reuses the same server options.
type Option func(*server.Options)

// WithUsers restricts the server to the given users (server.Options.Users):
// a client then authenticates as one of them, as nats://user:pass@host or
// nats.UserInfo, and gets that user's Permissions. A user with nil
// Permissions may publish and subscribe anywhere, so a test can run a
// connector under exactly the claim set spec/credentials.md lists while its
// own observer connects unrestricted. Without this option the server accepts
// any client, with full permissions.
func WithUsers(users ...*server.User) Option {
	return func(o *server.Options) { o.Users = users }
}

// Start launches an embedded NATS server with JetStream enabled. Pass port <= 0
// to bind a random free port (recommended for hermetic tests); the concrete port
// is captured so Restart reuses the same address. options apply on top of the
// defaults, in order.
func Start(port int, options ...Option) (*Server, error) {
	storeDir, err := os.MkdirTemp("", "connectors-e2e-js-")
	if err != nil {
		return nil, fmt.Errorf("create jetstream store dir: %w", err)
	}

	opts := natstest.DefaultTestOptions
	if port <= 0 {
		opts.Port = -1 // random free port
	} else {
		opts.Port = port
	}
	opts.JetStream = true
	opts.StoreDir = storeDir
	for _, opt := range options {
		opt(&opts)
	}

	s := &Server{opts: opts, storeDir: storeDir}
	if err := s.run(); err != nil {
		_ = os.RemoveAll(storeDir)
		return nil, err
	}
	return s, nil
}

// run boots a server from s.opts and records the concrete port + client URL.
func (s *Server) run() error {
	srv := natstest.RunServer(&s.opts)
	if srv == nil {
		return fmt.Errorf("nats server failed to start")
	}
	s.srv = srv
	// Capture the server's own log (the test options run it with logging
	// off). SetLogger takes effect for every log call after it, which covers
	// everything a client does, since no client can connect before run
	// returns. Restart keeps the capture, lines from before the restart
	// included.
	if s.log == nil {
		s.log = &logCapture{}
	}
	srv.SetLogger(s.log, false, false)

	// Pin the concrete port so a later Restart binds the same address (clients
	// reconnect to the URL they were given).
	if tcp, ok := srv.Addr().(*net.TCPAddr); ok {
		s.Port = tcp.Port
		s.opts.Port = tcp.Port
	}
	s.URL = srv.ClientURL()
	return nil
}

// Connect opens a client connection to the server.
func (s *Server) Connect(opts ...nats.Option) (*nats.Conn, error) {
	return nats.Connect(s.URL, opts...)
}

// Restart shuts the server down and brings it back up on the same port,
// simulating a transient outage. JetStream state is preserved (same store dir).
func (s *Server) Restart() error {
	s.srv.Shutdown()
	s.srv.WaitForShutdown()
	return s.run()
}

// Shutdown stops the server and removes its JetStream store dir.
func (s *Server) Shutdown() {
	if s.srv != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
	}
	if s.storeDir != "" {
		_ = os.RemoveAll(s.storeDir)
	}
}

// Violations returns the permissions violations the server has logged so
// far, in order. The server logs one as `Publish Violation - Subject
// "<subject>"` or `Subscription Violation - Subject "<subject>", SID <sid>`,
// prefixed with the offending client's address and id, so a test can tell
// whether a spawned connector touched a subject outside its claim set
// without relying on the connector's client library printing the async
// error it was sent, which a client library in another language need not do.
func (s *Server) Violations() []string {
	var out []string
	for _, line := range s.log.snapshot() {
		if strings.Contains(line, "Violation - Subject") {
			out = append(out, line)
		}
	}
	return out
}

// HasInterest reports whether any client of the global account has a
// subscription matching subject, so a test can wait until a spawned
// connector has subscribed before publishing to it.
func (s *Server) HasInterest(subject string) bool {
	return s.srv.GlobalAccount().SubscriptionInterest(subject)
}
