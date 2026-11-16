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
// simulate a server outage and assert client reconnect. Ported from the
// connectors-onboard e2e harness (core/natsenv), which prototyped it.
package natsenv

import (
	"fmt"
	"net"
	"os"

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
}

// Start launches an embedded NATS server with JetStream enabled. Pass port <= 0
// to bind a random free port (recommended for hermetic tests); the concrete port
// is captured so Restart reuses the same address.
func Start(port int) (*Server, error) {
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

// HasInterest reports whether any client of the global account has a
// subscription matching subject, so a test can wait until a spawned
// connector has subscribed before publishing to it.
func (s *Server) HasInterest(subject string) bool {
	return s.srv.GlobalAccount().SubscriptionInterest(subject)
}
