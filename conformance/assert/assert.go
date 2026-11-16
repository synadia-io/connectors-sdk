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

// Package assert provides reusable NATS assertions for conformance suites,
// so scenarios express intent ("expect N messages on subject") once. Ported
// from the connectors-onboard e2e harness (core/assert).
package assert

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Collector subscribes to a subject and accumulates received message payloads.
type Collector struct {
	sub  *nats.Subscription
	mu   sync.Mutex
	msgs []*nats.Msg
	got  chan struct{}
}

// Collect subscribes to subject on nc and starts accumulating messages. Call
// Close (or Drain) when done.
func Collect(nc *nats.Conn, subject string) (*Collector, error) {
	c := &Collector{got: make(chan struct{}, 1024)}
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		c.mu.Lock()
		c.msgs = append(c.msgs, m)
		c.mu.Unlock()
		select {
		case c.got <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %q: %w", subject, err)
	}
	c.sub = sub
	return c, nil
}

// Count returns how many messages have been collected so far.
func (c *Collector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

// Messages returns a copy of the collected payloads.
func (c *Collector) Messages() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, len(c.msgs))
	for _, m := range c.msgs {
		out = append(out, m.Data)
	}
	return out
}

// Msgs returns the collected messages, headers included — conformance
// contracts are mostly header contracts.
func (c *Collector) Msgs() []*nats.Msg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*nats.Msg, len(c.msgs))
	copy(out, c.msgs)
	return out
}

// WaitForAtLeast blocks until at least n messages have been collected or timeout
// elapses. It returns the count actually seen and an error on timeout.
func (c *Collector) WaitForAtLeast(n int, timeout time.Duration) (int, error) {
	deadline := time.After(timeout)
	for {
		if got := c.Count(); got >= n {
			return got, nil
		}
		select {
		case <-c.got:
		case <-deadline:
			return c.Count(), fmt.Errorf("timeout: got %d/%d messages after %s", c.Count(), n, timeout)
		}
	}
}

// Close drains the underlying subscription.
func (c *Collector) Close() {
	if c.sub != nil {
		_ = c.sub.Drain()
	}
}
