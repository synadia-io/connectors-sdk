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

// Package admin implements the D-8 admin control surface every connector
// exposes over NATS: a micro service named "connector-admin" rooted at
// identity.AdminRoot(id), with status/resume/skip endpoints (spec/admin.md).
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// serviceName is the D-8 fixed micro service name every connector's admin
// surface registers under.
const serviceName = "connector-admin"

// DefaultRequestTimeout is the deadline of the ctx a resume or skip request
// hands the Controller when Start is given no WithRequestTimeout. A NATS
// request carries no deadline of its own, so the service sets one, matching
// what the common callers wait for a reply: the nats CLI's default request
// timeout and the conformance suite's are both five seconds, so a Controller
// still working past this deadline answers nobody.
const DefaultRequestTimeout = 5 * time.Second

// Option adjusts the admin service Start registers.
type Option func(*options)

// options is the service configuration the Options build.
type options struct {
	requestTimeout time.Duration
}

// WithRequestTimeout sets the deadline of the ctx a resume or skip request
// hands the Controller, counted from the moment the request arrives. The
// default is DefaultRequestTimeout; a connector whose Resume legitimately
// takes longer raises it, and the caller then has to wait at least as long
// for the reply (nats req --timeout). A non-positive d is ignored and the
// default kept: Start is called once at startup, and refusing to serve the
// admin surface over a bad option would leave the connector unresumable.
func WithRequestTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.requestTimeout = d
		}
	}
}

// applyOptions folds opts into the service configuration, defaults first.
func applyOptions(opts []Option) options {
	o := options{requestTimeout: DefaultRequestTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// requestContext builds the context a resume or skip request hands the
// Controller: o.requestTimeout from now, cancelled when the handler returns.
func (o options) requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), o.requestTimeout)
}

// Controller is the connector-supplied behavior behind the admin surface
// (spec/admin.md). Its methods are called synchronously from micro's
// subscription callbacks, so implementations must be safe for concurrent
// use and must not block indefinitely. The ctx handed to Resume and Skip is
// the request's: it carries the request's deadline (WithRequestTimeout,
// default DefaultRequestTimeout) and is cancelled once the request is
// answered.
//
// An error a method returns, or a panic value it escapes with, becomes the
// request's micro error message, and nats.go records it in the service's
// $SRV.STATS last_error too. Before it reaches the wire the message is
// redacted like a status reason (status.RedactReason: URI userinfo is masked)
// and capped at 1 KiB, so a driver error that quotes a connection string
// cannot leak a credential into the ops plane. An empty message is sent as
// "error", since micro refuses to send an error with no description.
type Controller interface {
	// Status returns the connector's current status document, byte-for-byte
	// the same shape published to the status subject (D-5) — this is what
	// the "status" endpoint responds with verbatim.
	Status() status.Document

	// Resume un-parks the connector (lifecycle.md, D-15). It must return an
	// error if the connector is not currently parked — resume reverses a
	// park, it is not a generic restart (spec/admin.md).
	Resume(ctx context.Context) error

	// Skip skips the poison item identified by key, then behaves like
	// Resume. It must return an error if skip is unsupported by this
	// connector, or if the connector is not currently parked (spec/admin.md).
	Skip(ctx context.Context, key string) error
}

// skipRequest is the skip endpoint's request body (spec/admin.md).
type skipRequest struct {
	Key string `json:"key"`
}

// okResponse is the success body for resume and skip. spec/admin.md does
// not define a response shape for the success path (only the error path is
// specified, via micro's standard error headers); {"ok":true} is this
// package's choice absent a normative one.
type okResponse struct {
	OK bool `json:"ok"`
}

// Start registers and starts the D-8 admin micro service on nc: name
// "connector-admin", the given version (must be a valid SemVer string —
// AddService validates it and Start returns micro's own
// ErrConfigValidation-wrapped error unchanged for an invalid one), rooted at
// identity.AdminRoot(id). Endpoints hang off that root exactly as
// spec/admin.md lays out: <root>.status, <root>.resume, <root>.skip.
//
// opts adjust the service; see Option. Without any, resume and skip hand the
// Controller a ctx due DefaultRequestTimeout from the request's arrival.
//
// Start does not spawn any goroutine of its own; nats.go's micro package
// owns the registered subscriptions' lifecycle for the returned Service's
// lifetime. Callers stop the service with Service.Stop.
func Start(
	nc *nats.Conn, id identity.Identity, version string, c Controller, opts ...Option,
) (micro.Service, error) {
	o := applyOptions(opts)
	svc, err := micro.AddService(nc, micro.Config{
		Name:    serviceName,
		Version: version,
	})
	if err != nil {
		return nil, fmt.Errorf("admin: start %s: %w", serviceName, err)
	}

	root := svc.AddGroup(identity.AdminRoot(id))

	endpoints := []struct {
		name    string
		handler micro.HandlerFunc
	}{
		{"status", func(req micro.Request) { handleStatus(req, c) }},
		{"resume", func(req micro.Request) { handleResume(req, c, o) }},
		{"skip", func(req micro.Request) { handleSkip(req, c, o) }},
	}
	for _, ep := range endpoints {
		if err := root.AddEndpoint(ep.name, ep.handler); err != nil {
			_ = svc.Stop()
			return nil, fmt.Errorf("admin: add %s endpoint: %w", ep.name, err)
		}
	}

	return svc, nil
}

// handleStatus implements the "status" endpoint (spec/admin.md): no request
// body, responds with c.Status() marshaled verbatim.
func handleStatus(req micro.Request, c Controller) {
	defer recoverToError(req, "status")
	if err := req.RespondJSON(c.Status()); err != nil {
		respondError(req, "marshal status document: "+err.Error())
	}
}

// handleResume implements the "resume" endpoint (spec/admin.md): no request
// body, calls c.Resume with the request's context; nil responds okResponse,
// an error responds as a standard micro error carrying the error's message.
func handleResume(req micro.Request, c Controller, o options) {
	defer recoverToError(req, "resume")
	ctx, cancel := o.requestContext()
	defer cancel()
	if err := c.Resume(ctx); err != nil {
		respondError(req, err.Error())
		return
	}
	respondOK(req)
}

// handleSkip implements the "skip" endpoint (spec/admin.md): parses the
// {"key": string} request body, rejecting a malformed body or a missing key
// with a micro error before ever calling the Controller, then calls c.Skip
// with the request's context and the key and responds exactly like
// handleResume.
func handleSkip(req micro.Request, c Controller, o options) {
	defer recoverToError(req, "skip")
	var body skipRequest
	if err := json.Unmarshal(req.Data(), &body); err != nil {
		respondError(req, "malformed skip request body: "+err.Error())
		return
	}
	if body.Key == "" {
		respondError(req, `skip request missing required "key"`)
		return
	}
	ctx, cancel := o.requestContext()
	defer cancel()
	if err := c.Skip(ctx, body.Key); err != nil {
		respondError(req, err.Error())
		return
	}
	respondOK(req)
}

// recoverToError converts a panic escaping a Controller call into a micro
// error response, instead of letting it unwind through micro's subscription
// goroutine and crash the whole process. The park-don't-vanish contract
// (lifecycle.md) requires a connector whose admin behavior panics to stay
// alive and answerable — the same recovery the Run and Probe hooks already
// have, which the admin surface was missing. endpoint names the handler that
// paniced, and the panic value rides along in the operator-visible message.
func recoverToError(req micro.Request, endpoint string) {
	if r := recover(); r != nil {
		respondError(req, fmt.Sprintf("%s: panic: %v", endpoint, r))
	}
}

// respondOK sends the resume/skip success response.
func respondOK(req micro.Request) {
	_ = req.RespondJSON(okResponse{OK: true})
}

// maxErrorMessageBytes caps the message of every admin error response,
// truncationMarker included, so what reaches the wire is never longer than
// this. The message rides in a header and in $SRV.STATS last_error; neither
// is a place for a multi-kilobyte stack dump or a driver's verbatim echo of
// a request body.
const maxErrorMessageBytes = 1024

// truncationMarker ends a message that was cut to fit maxErrorMessageBytes.
const truncationMarker = "...[truncated]"

// respondError sends a standard micro error response (Nats-Service-Error /
// Nats-Service-Error-Code headers, spec/admin.md "Errors") carrying msg. It
// is the single choke point for every error message the admin surface
// emits: msg is redacted with status.RedactReason and capped at
// maxErrorMessageBytes (cut on a rune boundary to leave room for
// truncationMarker) before it reaches the wire, because handlers pass
// Controller errors and panic values through verbatim and those can quote a
// connection string. The code is fixed at "500": spec/admin.md defines no
// taxonomy of admin error codes, only that the standard micro headers carry
// the message.
func respondError(req micro.Request, msg string) {
	_ = req.Error("500", sanitizeErrorMessage(msg), nil)
}

// sanitizeErrorMessage redacts msg like a status reason and caps it at
// maxErrorMessageBytes, marker included: a longer msg is cut on a rune
// boundary to leave room for truncationMarker, which is appended. An empty
// msg becomes "error": micro's Request.Error refuses an empty description,
// and a refused response is a request that times out, which is worse than a
// response that says nothing.
func sanitizeErrorMessage(msg string) string {
	msg = status.RedactReason(msg)
	if msg == "" {
		return "error"
	}
	if len(msg) <= maxErrorMessageBytes {
		return msg
	}
	cut := maxErrorMessageBytes - len(truncationMarker)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + truncationMarker
}
