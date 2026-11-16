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

package transform_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/transform"
)

// upper is the reference Engine implementation the contract test drives.
type upper struct{}

func (upper) Compile(_ json.RawMessage) (transform.Func, error) {
	return func(_ context.Context, batch []transform.Message) ([]transform.Result, error) {
		out := make([]transform.Result, len(batch))
		for i, m := range batch {
			p := append([]byte(nil), m.Payload...) // caller-owned copy
			for j, b := range p {
				if 'a' <= b && b <= 'z' {
					p[j] = b - 32
				}
			}
			out[i] = transform.Result{Output: []transform.Message{{Subject: m.Subject, Header: m.Header, Payload: p}}}
		}
		return out, nil
	}, nil
}

func TestFuncContract(t *testing.T) {
	var eng transform.Engine = upper{} // Engine satisfied at compile time
	f, err := eng.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	in := []transform.Message{
		{Subject: "a.b", Payload: []byte("x")},
		{Subject: "a.c", Payload: []byte("y")},
	}
	res, err := f(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(in) {
		t.Fatalf("index alignment: got %d results for %d inputs", len(res), len(in))
	}
	if string(res[0].Output[0].Payload) != "X" || string(in[0].Payload) != "x" {
		t.Fatal("output not derived or input mutated")
	}
}
