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

// Package jsresource prepares the JetStream resources a connector uses: the
// stream it publishes into (EnsureStream) and the durable consumer it reads
// from (EnsureConsumer).
//
// Existing resources belong to the operator. Both functions create what is
// missing, from a template the connector supplies, and validate what
// exists. EnsureConsumer never changes an existing consumer. EnsureStream
// never changes an existing stream unless the connector asks for
// UpdateAdditive, which only appends missing subjects and enables
// AllowAtomicPublish.
//
// Each JetStream API request has its own timeout (DefaultTimeout unless
// set), so a fallback after a request that timed out still has time to
// run. The connector's credential needs the JS API permissions listed in
// spec/credentials.md (D-10); a request the credential may not make fails
// with ErrPermission.
//
// Besides the stream and consumer calls, the package calls js.Conn() for
// the server version and the connection's last async error. A fake
// jetstream.JetStream in tests must implement Conn; returning nil is fine.
package jsresource

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultTimeout bounds each JetStream API request when a Timeout field is
// zero or negative.
const DefaultTimeout = 10 * time.Second

var (
	// ErrNotCaptured means a stream does not capture a subject the
	// connector needs: a data subject or notice subject it publishes to
	// (EnsureStream), or a filter it reads (EnsureConsumer). The error names
	// the subjects.
	ErrNotCaptured = errors.New("jsresource: stream does not capture the required subjects")

	// ErrIncompatible means an existing stream or consumer is configured in
	// a way the connector cannot use, such as a stream that does not
	// acknowledge publishes or a push consumer. The error names the
	// setting.
	ErrIncompatible = errors.New("jsresource: existing resource is incompatible")

	// ErrPermission means the connector's credential may not make a
	// JetStream API request. The server drops such a request and reports
	// the violation asynchronously, so it is detected only when the request
	// times out. The error names the API subject (spec/credentials.md,
	// D-10) and wraps nats.ErrPermissionViolation. Only a denial the
	// connection reports during the request counts: nats.go keeps the last
	// async error until a reconnect, so an older denial is not attributed
	// to a later timeout. Detection is best effort: nats.go keeps only the
	// last async error, so another one reported before the timeout (a
	// second denial, a slow consumer) hides the denial, and the request
	// fails as a plain timeout.
	ErrPermission = errors.New("jsresource: permission denied")

	// ErrInvalid means the StreamSpec or ConsumerSpec itself is wrong: an
	// invalid name, subject or filter, or a template field that conflicts
	// with the spec. Nothing was sent to the server. Retrying won't help;
	// the configuration has to change.
	ErrInvalid = errors.New("jsresource: invalid spec")
)

// invalid returns an ErrInvalid error with a formatted explanation.
func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func timeoutOr(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultTimeout
	}
	return d
}

// lastErr returns the connection's last async error, or nil.
func lastErr(js jetstream.JetStream) error {
	if nc := js.Conn(); nc != nil {
		return nc.LastError()
	}
	return nil
}

// wrapErr adds context to a failed JetStream API request to api, the
// request subject without its "$JS.API." prefix. before is the connection's
// last async error from just before the request. When the request timed out
// and the connection has since reported a new permissions violation naming
// that subject, the result is ErrPermission.
func wrapErr(js jetstream.JetStream, err, before error, api, what string) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		last := lastErr(js)
		// The server quotes the subject (%q), escaping any '"' in it.
		needle := strconv.Quote("API." + api)[1:]
		if isNew(last, before) && errors.Is(last, nats.ErrPermissionViolation) && strings.Contains(last.Error(), needle) {
			return fmt.Errorf("%w: %s: %w", ErrPermission, what, last)
		}
	}
	return fmt.Errorf("jsresource: %s: %w", what, err)
}

// isNew reports whether last is a different error value from before.
// nats.go stores a new value for each async error, so a denial reported
// during a request differs from the one held before it.
func isNew(last, before error) bool {
	if last == nil {
		return false
	}
	if before == nil {
		return true
	}
	if t := reflect.TypeOf(last); t != reflect.TypeOf(before) || !t.Comparable() {
		return true
	}
	return last != before
}

// lookupStream returns the stream, or an error that matches
// jetstream.ErrStreamNotFound when it does not exist.
func lookupStream(ctx context.Context, js jetstream.JetStream, name string, timeout time.Duration) (jetstream.Stream, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	before := lastErr(js)
	st, err := js.Stream(ctx, name)
	if err != nil {
		return nil, wrapErr(js, err, before, "STREAM.INFO."+name, fmt.Sprintf("stream %q lookup", name))
	}
	return st, nil
}

// createStream creates a stream from cfg. When another instance created the
// stream after the caller's lookup, with a different config, it returns
// that stream and created == false.
func createStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, timeout time.Duration) (st jetstream.Stream, created bool, err error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	before := lastErr(js)
	st, err = js.CreateStream(cctx, cfg)
	cancel()
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		st, err = lookupStream(ctx, js, cfg.Name, timeout)
		return st, false, err
	}
	if err != nil {
		err = wrapErr(js, err, before, "STREAM.CREATE."+cfg.Name, fmt.Sprintf("stream %q create", cfg.Name))
		if errors.Is(err, ErrPermission) {
			// The lookup found no stream: say so, so that a mistyped name
			// doesn't read as a request for more permissions.
			err = fmt.Errorf("%w (the stream does not exist: %w)", err, jetstream.ErrStreamNotFound)
		}
		return nil, false, err
	}
	return st, true, nil
}
