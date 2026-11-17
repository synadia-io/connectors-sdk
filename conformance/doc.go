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

// Package conformance is the contract suite for connectors built on the SDK:
// black-box tests that build a connector binary, drive it over the nex launch
// contract against an embedded NATS server, and check what it publishes
// against the schemas in spec/. The tests live in this package's _test files;
// the module also exports the helpers a connector's own suite builds on:
//
//   - assert: NATS assertions for conformance suites ("expect N messages on
//     subject").
//   - driver: builds and drives a connector binary, watches its status
//     documents and calls its admin endpoints.
//   - natsenv: an embedded NATS server with JetStream and start/stop/restart.
//   - specschema: loads and validates against the spec/ JSON Schemas.
//   - testconn (a separate module, conformance/testconn): the fixture
//     connector the suite is developed against.
//
// CONFORMANCE_SPEC_DIR names the spec/ directory the schemas are read from.
// Without it, specschema finds spec/ relative to its own source, which works
// from a checkout of this repository; a downloaded module has no spec/ next to
// it, so set the variable then. See README.md in this directory.
package conformance
