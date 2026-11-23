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

// Command testconn is a minimal connector fixture built on the sdk-go/
// runtime harness. It exists only to exercise the connector-contract-v1
// surfaces (spec/contract.md, config.md, status.md, admin.md, lifecycle.md)
// for conformance/contract_test.go, not to do real work: it subscribes to
// one configured subject, publishes an incrementing counter for every
// ordinary message, parks on the literal body "poison", and on the body
// "route:<subject>" publishes to <subject> if its routing guard
// (transform.Guard over <configured subject>.out.>) allows it, else an
// `.error` notice.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
	"github.com/synadia-io/connectors-sdk/sdk-go/subject"
	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// version is the D-8 admin service version (must be valid SemVer).
var version = "0.1.0"

// capabilities is the D-12 spec capability list this fixture advertises: it
// implements check, publishes status, and can park/resume/skip.
var capabilities = []string{"check", "status", "park"}

// Config is testconn's config document/flags shape (config.md, D-2/D-13): a
// required field, a field with a schema default, and a field marked
// x-display.secret so the D-4 unresolved-secret guard has something to
// trip over the same way a real connector's credential field would.
type Config struct {
	Subject   string `json:"subject" schema:"required" display:"label=Subject,order=1"`
	Greeting  string `json:"greeting,omitempty" schema:"default=hello" display:"label=Greeting,order=2"`
	APIKey    string `json:"api_key,omitempty" display:"secret,label=API Key,order=3"`
	FailCheck bool   `json:"fail_check,omitempty" display:"label=Fail Check,order=4"`
}

func registerFlags(fs *flag.FlagSet) *Config {
	cfg := &Config{}
	fs.StringVar(&cfg.Subject, "subject", "", "NATS subject to subscribe on")
	fs.StringVar(&cfg.Greeting, "greeting", "hello", "unused demo field with a schema default")
	fs.StringVar(&cfg.APIKey, "api-key", "", "unused demo secret-marked field")
	fs.BoolVar(&cfg.FailCheck, "fail-check", false, "check mode reports failure when set")
	return cfg
}

func main() {
	os.Exit(runMain())
}

func runMain() int {
	cfg := registerFlags(flag.CommandLine)
	flag.Parse()

	return runtime.Main("testconn", version, cfg, capabilities, runtime.Hooks{
		Probe:  func(ctx context.Context) error { return probe(cfg) },
		Run:    func(ctx context.Context, rt *runtime.Runtime) error { return run(ctx, rt, cfg) },
		Resume: resume,
		Skip:   skip,
	})
}

// probe implements the Probe hook (D-11 check mode): fails iff the config
// says to, so contract_test.go can drive both check-mode outcomes.
func probe(cfg *Config) error {
	if cfg.FailCheck {
		return errors.New("check: fail_check is set")
	}
	return nil
}

// rtRef holds the *runtime.Runtime handed to the most recent run call. Run
// is re-entered with the same Runtime (same connection, same status
// publisher) on every resume for the life of the process (sdk-go/runtime's
// harness constructs Runtime once at startup), so resume/skip — called by
// the admin surface only while parked, i.e. never concurrently with a fresh
// run() that would overwrite it mid-use — can reach the status publisher to
// record their own effect.
var rtRef atomic.Pointer[runtime.Runtime]

// msgCount is the running counter published to <subject>.count for every
// ordinary (non-poison) message received, proving the subscribe loop is
// alive across park/resume/skip cycles.
var msgCount atomic.Int64

// credsPoisonReason is the redaction test's trigger: a park reason shaped
// exactly like a real connector's leaked-credential failure (a mongodb
// connection URI's userinfo quoted verbatim in a driver error), so
// conformance/contract_test.go's TestRedactionEndToEnd can assert the
// sdk-go/status harness redacts it in the published reason without needing
// a real credential-bearing dependency — the point of the test is that the
// HARNESS redacts for any connector, not that testconn itself does.
const credsPoisonReason = "connect mongodb://dbuser:secretpw@db.internal:27017: connection refused"

// run implements the Run hook (sdk-go/runtime Hooks.Run): subscribe to
// cfg.Subject, publish an incrementing counter for every ordinary message,
// and park (lifecycle.md, D-15) on one of two special message bodies:
// "poison" (ordinary poison path, reason names the "Key" header so
// admin.md's skip endpoint has something concrete to discard) or
// "poison-creds" (a reason shaped like a leaked credential, see
// credsPoisonReason). A third special body, "flush-then-term", publishes a
// marker message and immediately self-delivers SIGTERM — conformance's
// drain-flush test needs the publish and the signal to race with no network
// round trip involved in the trigger itself, which an externally-sent
// SIGTERM (from the test process, after publishing over NATS to this one)
// cannot guarantee: this hook makes the race deterministic instead. A body
// "route:<subject>" is the routing-guard probe, see route.
func run(ctx context.Context, rt *runtime.Runtime, cfg *Config) error {
	rtRef.Store(rt)

	guard, err := transform.NewGuard(cfg.Subject + ".out.>")
	if err != nil {
		return fmt.Errorf("routing guard: %w", err)
	}

	park := make(chan string, 1)
	sub, err := rt.Conn.Subscribe(cfg.Subject, func(m *nats.Msg) {
		if target, ok := strings.CutPrefix(string(m.Data), "route:"); ok {
			route(rt, cfg, guard, m, target)
			return
		}
		switch string(m.Data) {
		case "poison":
			key := m.Header.Get("Key")
			if key == "" {
				key = "unknown"
			}
			select {
			case park <- "poison: " + key:
			default:
			}
			return
		case "poison-creds":
			select {
			case park <- credsPoisonReason:
			default:
			}
			return
		case "flush-then-term":
			out := nats.NewMsg(cfg.Subject + ".flushed")
			out.Data = []byte("flushed")
			_ = rt.Conn.PublishMsg(out)
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			return
		}
		n := msgCount.Add(1)
		rt.Status.Add("received", 1)
		out := nats.NewMsg(cfg.Subject + ".count")
		out.Data = fmt.Appendf(nil, "%d", n)
		_ = rt.Conn.PublishMsg(out)
	})
	if err != nil {
		return fmt.Errorf("subscribe %q: %w", cfg.Subject, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	select {
	case <-ctx.Done():
		return nil
	case reason := <-park:
		return runtime.Park(reason)
	}
}

// route is the routing-guard probe (spec/transformer.md, "routing guard"):
// it stands in for a transform whose output is the message
// {Subject: target, Header: the probe's headers, Payload: "routed"}, and
// does what every publish path of a connector does with a transform output.
// When guard.Check allows the message, it is published as is. Otherwise the
// connector publishes an `.error` notice on the PRE-transform subject
// (cfg.Subject), with an empty payload and a Transform-Error header holding
// a fixed, value-free class, since the guard's error text quotes the subject
// or header name, which came from message data; the full text goes to the
// log only. The status counter transform_errored counts the notices.
func route(rt *runtime.Runtime, cfg *Config, guard *transform.Guard, in *nats.Msg, target string) {
	var hdr nats.Header
	if len(in.Header) > 0 {
		hdr = nats.Header{}
		for k, vs := range in.Header {
			hdr[k] = append([]string(nil), vs...)
		}
	}
	m := transform.Message{Subject: target, Header: hdr, Payload: []byte("routed")}
	if err := guard.Check(m); err != nil {
		class := "transform"
		switch {
		case errors.Is(err, transform.ErrUnpublishableSubject):
			class = "unpublishable-subject"
		case errors.Is(err, transform.ErrReservedHeader):
			class = "reserved-header"
		}
		log.Printf("testconn: route: %v", err)
		rt.Status.Add("transform_errored", 1)
		notice := nats.NewMsg(cfg.Subject + "." + subject.SuffixError)
		notice.Header.Set("Transform-Error", class)
		_ = rt.Conn.PublishMsg(notice)
		return
	}
	out := nats.NewMsg(m.Subject)
	if m.Header != nil {
		out.Header = m.Header
	}
	out.Data = m.Payload
	_ = rt.Conn.PublishMsg(out)
}

// resume implements the Resume hook: no state to reset, just a counter so a
// caller can tell a plain resume happened at all.
func resume() error {
	if rt := rtRef.Load(); rt != nil {
		rt.Status.Add("resumed", 1)
	}
	return nil
}

// skip implements the Skip hook (admin.md): records the skipped key as its
// own status counter, distinct from a plain resume, then returns nil so the
// harness un-parks exactly as it would for resume.
func skip(key string) error {
	if rt := rtRef.Load(); rt != nil {
		rt.Status.Add("skipped:"+key, 1)
	}
	return nil
}
