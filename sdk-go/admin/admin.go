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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/connectors-sdk/sdk-go/identity"
	"github.com/synadia-io/connectors-sdk/sdk-go/status"
)

// serviceName is the D-8 fixed micro service name every connector's admin
// surface registers under.
const serviceName = "connector-admin"

// requestTimeout is the deadline of the ctx a resume or skip request hands
// the Controller. A NATS request carries no deadline of its own, so the
// service sets one, matching what the common callers wait for a reply: the
// nats CLI's default request timeout and the conformance suite's are both
// five seconds, so a Controller still working past this deadline answers
// nobody.
const requestTimeout = 5 * time.Second

// requestContext builds the context a resume or skip request hands the
// Controller: requestTimeout from now, cancelled when the handler returns.
func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), requestTimeout)
}

// Controller is the connector-supplied behavior behind the admin surface
// (spec/admin.md). Its methods are called synchronously from micro's
// subscription callbacks, so implementations must be safe for concurrent
// use and must not block indefinitely. The ctx handed to Resume and Skip is
// the request's: it carries the request's deadline and is cancelled once the
// request is answered.
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
// Start does not spawn any goroutine of its own; nats.go's micro package
// owns the registered subscriptions' lifecycle for the returned Service's
// lifetime. Callers stop the service with Service.Stop.
func Start(nc *nats.Conn, id identity.Identity, version string, c Controller) (micro.Service, error) {
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
		{"resume", func(req micro.Request) { handleResume(req, c) }},
		{"skip", func(req micro.Request) { handleSkip(req, c) }},
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
func handleResume(req micro.Request, c Controller) {
	defer recoverToError(req, "resume")
	ctx, cancel := requestContext()
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
func handleSkip(req micro.Request, c Controller) {
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
	ctx, cancel := requestContext()
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

// respondError sends a standard micro error response (Nats-Service-Error /
// Nats-Service-Error-Code headers, spec/admin.md "Errors") carrying msg. The
// code is fixed at "500": spec/admin.md defines no taxonomy of admin error
// codes, only that the standard micro headers carry the message.
func respondError(req micro.Request, msg string) {
	_ = req.Error("500", msg, nil)
}
