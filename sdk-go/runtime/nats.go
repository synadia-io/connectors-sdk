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

package runtime

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// The nexlet launch contract: the node injects the NATS endpoint and, in
// authenticated deployments, the credentials, as these env vars. A connector
// takes whatever the platform minted — it never hardcodes them. These are
// the same three variables the s3 and mongodb connectors read today; the
// harness owns them now so every connector reads them identically.
const (
	envNatsServers = "NEX_WORKLOAD_NATS_SERVERS" // comma-separated URL(s)
	envNatsNKey    = "NEX_WORKLOAD_NATS_NKEY"    // the nkey user SEED (misnamed by the contract)
	envNatsB64JWT  = "NEX_WORKLOAD_NATS_B64_JWT" // the user JWT, base64-encoded
)

// shutdownBudget is D-16's normative bound: SIGTERM to exit within 10
// seconds, covering the Run hook's unwind and the NATS drain together. It
// is a var only so tests can shorten it; nothing in the production paths
// assigns to it.
var shutdownBudget = 10 * time.Second

// maxReconnects is nats.MaxReconnects's value. -1 means retry forever,
// so a connector rides out an arbitrarily long NATS outage instead of giving
// up after nats.go's 60-attempt default and stranding a process that can
// never reconnect, can never be reached over admin, and — because serve
// blocks on ctx/resume — never exits for a supervisor to restart. It is a
// var only so a test can shorten it to force the ClosedHandler exit path
// without a multi-minute wait; production never assigns to it.
var maxReconnects = -1

// reconnectWait is nats.ReconnectWait's value: the library default, set
// explicitly so a test can shorten it and exhaust maxReconnects fast.
var reconnectWait = nats.DefaultReconnectWait

// drainReserve is the slice of the shutdown budget held back for the NATS
// drain itself. A Run hook that ignores its cancelled context would
// otherwise be able to spend the entire budget, leaving nothing to flush
// in-flight work with — the reserve caps its share at four fifths. It is a
// fraction rather than a fixed duration so the split still holds when a
// test shortens the budget to milliseconds.
func drainReserve(budget time.Duration) time.Duration { return budget / 5 }

// connectFromEnv opens the connector's own NATS connection from the launch
// contract above. name becomes the connection name, so a connection listing
// on the server identifies which connector owns it.
//
// onClosed is invoked if the connection ever reaches the terminal Closed
// state. With MaxReconnects(-1) a reconnect loop never gives up, so the
// only ways here are the harness's own drain/Close at shutdown, or — belt
// and braces — some future nats.go path that closes anyway; onClosed lets
// the harness cancel its context and EXIT for a supervisor restart instead
// of zombieing. The callback must be safe to call from a nats.go goroutine
// and must itself distinguish an intentional shutdown from an unexpected
// close (the harness gates on a shutting-down flag).
func connectFromEnv(name string, onClosed func()) (*nats.Conn, error) {
	servers := os.Getenv(envNatsServers)
	if servers == "" {
		return nil, fmt.Errorf("%s is required", envNatsServers)
	}

	opts := []nats.Option{
		nats.Name(name),
		// Belt and braces with the deadline drain enforces below: this
		// bounds the client's own drain, that one bounds the wall clock.
		nats.DrainTimeout(shutdownBudget),
		// Never abandon the reconnect loop, and if the connection ever
		// does reach Closed, tell the harness so it exits rather than hangs.
		nats.MaxReconnects(maxReconnects),
		nats.ReconnectWait(reconnectWait),
		nats.ClosedHandler(func(*nats.Conn) {
			if onClosed != nil {
				onClosed()
			}
		}),
	}
	creds, err := credsOptionFromEnv()
	if err != nil {
		return nil, err
	}
	if creds != nil {
		opts = append(opts, creds)
	}

	nc, err := nats.Connect(servers, opts...)
	if err != nil {
		// The URL list may carry userinfo (nats://user:pass@host) and the
		// error is logged, so both halves are masked like a status reason:
		// the list, and nats.go's own text, which quotes a URL it could not
		// parse. Nothing matches on this error, so it wraps nothing.
		return nil, fmt.Errorf("connect to %s: %s",
			redactServerList(servers), status.RedactReason(err.Error()))
	}
	return nc, nil
}

// redactServerList masks the userinfo of each URL in a comma-separated
// server list on its own. Redacting the list whole would run from the first
// URL's scheme to the last URL's "@" and swallow every host but the last.
func redactServerList(servers string) string {
	urls := strings.Split(servers, ",")
	for i, u := range urls {
		urls[i] = status.RedactReason(u)
	}
	return strings.Join(urls, ",")
}

// credsOptionFromEnv builds the JWT+nkey auth option from the launch
// contract, or returns nil when no credentials were injected — an
// unauthenticated dev deployment is allowed, a half-configured one is not.
func credsOptionFromEnv() (nats.Option, error) {
	seed := os.Getenv(envNatsNKey)
	jwt, ok, err := decodeCreds(seed, os.Getenv(envNatsB64JWT))
	if err != nil || !ok {
		return nil, err
	}
	return nats.UserJWTAndSeed(jwt, seed), nil
}

// decodeCreds validates and base64-decodes the injected credentials.
// ok=false with a nil error means none were injected, which is allowed; the
// two vars must appear together, since a lone one is a misconfiguration
// rather than a silent fallback to anonymous auth.
func decodeCreds(seed, b64jwt string) (jwt string, ok bool, err error) {
	if seed == "" && b64jwt == "" {
		return "", false, nil
	}
	if seed == "" || b64jwt == "" {
		return "", false, fmt.Errorf("%s and %s must be set together", envNatsNKey, envNatsB64JWT)
	}
	// The JWT arrives base64-encoded (spec/credentials.md). Decode it before
	// handing it to nats — a raw JWT here fails auth in a way that is painful
	// to diagnose.
	raw, err := base64.StdEncoding.DecodeString(b64jwt)
	if err != nil {
		return "", false, fmt.Errorf("decode %s: %w", envNatsB64JWT, err)
	}
	return string(raw), true, nil
}

// drain flushes and closes nc within budget (D-16). It returns an error when
// the budget was already spent or the drain overran it — in both cases the
// connection is closed hard, because exiting with a live connection is worse
// than exiting with an unflushed one.
func drain(nc *nats.Conn, budget time.Duration) error {
	if nc.IsClosed() {
		return nil
	}
	if budget <= 0 {
		nc.Close()
		return fmt.Errorf("drain: the %s shutdown budget was spent before the drain began (D-16); closed the connection instead", shutdownBudget)
	}

	closed := make(chan struct{})
	nc.SetClosedHandler(func(*nats.Conn) { close(closed) })

	if err := nc.Drain(); err != nil {
		nc.Close()
		return fmt.Errorf("drain: %w", err)
	}

	select {
	case <-closed:
		return nil
	case <-time.After(budget):
		nc.Close()
		return fmt.Errorf("drain: did not complete within the %s shutdown budget (D-16)", shutdownBudget)
	}
}

// shutdownClock records when the shutdown signal arrived, so the D-16 budget
// covers everything after it — the Run hook's unwind and the NATS drain —
// rather than restarting the clock once the loop finally returns.
type shutdownClock struct{ at atomic.Int64 }

// mark records the shutdown instant. Only the first call counts.
func (c *shutdownClock) mark() { c.at.CompareAndSwap(0, time.Now().UnixNano()) }

// hookDeadline is when the harness stops waiting for the Run hook: the
// shutdown signal, or now before one, plus the hook's share of total (the
// rest is the NATS drain's, see drainReserve). A nil clock counts from now.
// The clock is marked from its own goroutine once ctx is done, so a call in
// the instant Run sees ctx done may still count from now: microseconds late,
// well inside hookGrace.
func (c *shutdownClock) hookDeadline(total time.Duration) time.Time {
	from := time.Now()
	if c != nil {
		if at := c.at.Load(); at != 0 {
			from = time.Unix(0, at)
		}
	}
	return from.Add(total - drainReserve(total))
}

// hookGrace is how long before hookDeadline Runtime.ShutdownContext's
// deadline falls, so a Run that returns when that context ends is never
// abandoned: a tenth of the hook's share, at most 250ms.
func hookGrace(total time.Duration) time.Duration {
	return min(250*time.Millisecond, (total-drainReserve(total))/10)
}

// remaining reports how much of total is left. An unmarked clock (a
// connector that stopped on its own, with no signal) gets the full budget.
func (c *shutdownClock) remaining(total time.Duration) time.Duration {
	at := c.at.Load()
	if at == 0 {
		return total
	}
	return total - time.Since(time.Unix(0, at))
}
