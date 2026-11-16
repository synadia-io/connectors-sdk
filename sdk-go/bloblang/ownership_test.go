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

package bloblang

import (
	"bytes"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// TestNoInputMutationNoAliasing proves the Func contract: inputs untouched,
// outputs independently mutable. Run under -race in CI (`go test -race`).
func TestNoInputMutationNoAliasing(t *testing.T) {
	payload := []byte(`{"x":"abc"}`)
	orig := append([]byte(nil), payload...)
	h := nats.Header{"K": []string{"v"}}
	in := transform.Message{Subject: "a.b", Header: h, Payload: payload}

	r := run1(t, `root.x = this.x.uppercase()`+"\nmeta K2 = @K", in)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !bytes.Equal(payload, orig) || h.Get("K") != "v" || len(h) != 1 {
		t.Fatal("input mutated")
	}
	// Mutating the output must not touch the input.
	out := r.Output[0]
	for i := range out.Payload {
		out.Payload[i] = 'Z'
	}
	out.Header.Set("Injected", "1") // what the batch publisher does
	if !bytes.Equal(payload, orig) || h.Get("Injected") != "" {
		t.Fatal("output aliases input")
	}
}
