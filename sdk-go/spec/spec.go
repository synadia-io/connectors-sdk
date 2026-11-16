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

// Package spec builds a connector's spec document (contract.md D-12): the
// contract identifier, a JSON Schema 2020-12 config_schema reflected from a
// Go config struct's `json`, `schema`, and `display` struct tags, and the
// capability list the caller supplies. See gen.go for the generator and the
// tag grammar it understands.
package spec

import "encoding/json"

// Contract is the contract identifier every spec document advertises (D-1).
const Contract = "synadia.connector/v1"

// Spec is a connector's spec document (D-12), printed as JSON by the `spec`
// entrypoint mode (D-11).
type Spec struct {
	Contract     string          `json:"contract"`
	ConfigSchema json.RawMessage `json:"config_schema"`
	Capabilities []string        `json:"capabilities"`
}

// JSON renders the spec document as indented JSON, suitable for printing to
// stdout by the `spec` entrypoint mode (D-11).
func (s *Spec) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
