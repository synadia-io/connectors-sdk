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

package checkpoint_test

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
)

// A Run hook that resumes from its checkpoint and saves its position after
// each batch. A lost compare-and-swap (checkpoint.ErrConflict) is returned
// from Run as it is, so the harness parks with the reason
// "checkpoint-conflict"; wrapping it in runtime.Park would replace that
// reason.
func Example() {
	hooks := runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			js, err := jetstream.New(rt.Conn)
			if err != nil {
				return runtime.Park(fmt.Sprintf("jetstream: %v", err))
			}
			store, err := checkpoint.Open(ctx, js, rt.ID, checkpoint.Options{})
			if err != nil {
				return runtime.Park(fmt.Sprintf("checkpoint: %v", err))
			}
			// rev is 0 when nothing was saved yet; Save then creates the key.
			position, rev, err := store.Load(ctx, "position")
			if err != nil {
				return err
			}
			for ctx.Err() == nil {
				next, err := copyBatch(ctx, position)
				if err != nil {
					return err
				}
				if rev, err = store.Save(ctx, "position", next, rev); err != nil {
					return err // ErrConflict included: never retry the write
				}
				position = next
			}
			return nil
		},
	}
	_ = hooks
}

// copyBatch stands in for the connector's own work: read from the source
// after position, publish, and return the new position.
func copyBatch(ctx context.Context, position []byte) ([]byte, error) {
	return position, ctx.Err()
}
