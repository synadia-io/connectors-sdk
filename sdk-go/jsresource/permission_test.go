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

package jsresource_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/connectors-sdk/sdk-go/jsresource"
)

// TestATimeoutIsPermissionOnlyForItsOwnFreshDenial: a request that times out
// is ErrPermission only when the connection reported a denial of that
// request's subject during it. On every request path, an older denial of the
// same subject (the connection keeps its last async error until a reconnect)
// leaves a plain timeout, and so does another subject's denial arriving
// during the request.
func TestATimeoutIsPermissionOnlyForItsOwnFreshDenial(t *testing.T) {
	ctx := context.Background()
	stream := func(js jetstream.JetStream) error {
		_, err := jsresource.EnsureStream(ctx, js, jsresource.StreamSpec{
			Name: "S", Subjects: []string{"e.b"}, Update: jsresource.UpdateAdditive, Timeout: time.Second,
		})
		return err
	}
	consumer := func(js jetstream.JetStream) error {
		_, err := jsresource.EnsureConsumer(ctx, js, jsresource.ConsumerSpec{
			Stream: "S", Durable: "D", Filters: []string{"e.a"}, Timeout: time.Second,
		})
		return err
	}
	for _, tt := range []struct {
		name   string
		haveS  bool   // the stream exists, capturing e.a
		fail   string // the call that times out
		denied string // the subject denied before the call, or during it
		during bool
		ensure func(jetstream.JetStream) error
	}{
		{"stream lookup, older denial", true, "Stream", "$JS.API.STREAM.INFO.S", false, stream},
		{"stream create, older denial", false, "CreateStream", "$JS.API.STREAM.CREATE.S", false, stream},
		{"stream update, older denial", true, "UpdateStream", "$JS.API.STREAM.UPDATE.S", false, stream},
		{"consumer lookup, older denial", true, "Consumer", "$JS.API.CONSUMER.INFO.S.D", false, consumer},
		{"consumer create, older denial", true, "CreateConsumer", "$JS.API.CONSUMER.CREATE.S.D.e.a", false, consumer},
		{"another subject denied during the request", true, "Stream", "denied.x", true, stream},
	} {
		t.Run(tt.name, func(t *testing.T) {
			admin, limited := startLimited(t, tt.denied)
			if tt.haveS {
				mustCreateStream(t, admin, jetstream.StreamConfig{Name: "S", Subjects: []string{"e.a"}})
			}
			js := timeoutJS{JetStream: limited, t: t, fail: tt.fail}
			if tt.during {
				js.deny = tt.denied
			} else {
				provokeDenial(t, limited, tt.denied)
			}
			err := tt.ensure(js)
			if errors.Is(err, jsresource.ErrPermission) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want a plain timeout, not ErrPermission", err)
			}
		})
	}
}
