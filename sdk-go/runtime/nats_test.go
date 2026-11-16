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

package runtime

import (
	"encoding/base64"
	"testing"

	natstest "github.com/nats-io/nats-server/v2/test"
)

// TestConnectFromEnv_RetriesForever is the reconnect guard: the connection the
// harness opens must be configured to reconnect forever (MaxReconnect == -1),
// not give up after nats.go's 60-attempt default and strand an unreachable
// process. It boots an in-process server just to reach a real *nats.Conn
// whose Opts can be inspected.
func TestConnectFromEnv_RetriesForever(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	if srv == nil {
		t.Fatal("nats-server: failed to start in-process test server")
	}
	defer func() { srv.Shutdown(); srv.WaitForShutdown() }()

	t.Setenv(envNatsServers, srv.ClientURL())
	t.Setenv(envNatsNKey, "")
	t.Setenv(envNatsB64JWT, "")

	nc, err := connectFromEnv("test", nil)
	if err != nil {
		t.Fatalf("connectFromEnv: %v", err)
	}
	defer nc.Close()

	if nc.Opts.MaxReconnect != -1 {
		t.Errorf("nc.Opts.MaxReconnect = %d, want -1 (retry forever)", nc.Opts.MaxReconnect)
	}
}

// Coverage for connectFromEnv's credential handling, decodeCreds and
// credsOptionFromEnv. Internal (package runtime, not runtime_test) since
// both are unexported.

func TestDecodeCreds(t *testing.T) {
	const seed = "SUAEXAMPLESEED"
	goodJWT := base64.StdEncoding.EncodeToString([]byte("header.payload.sig"))

	tests := []struct {
		name      string
		seed, b64 string
		wantJWT   string
		wantOK    bool
		wantErr   bool
	}{
		{name: "none injected -> unauthenticated", wantOK: false},
		{name: "seed without jwt", seed: seed, wantErr: true},
		{name: "jwt without seed", b64: goodJWT, wantErr: true},
		{name: "valid pair decodes", seed: seed, b64: goodJWT, wantJWT: "header.payload.sig", wantOK: true},
		{name: "malformed base64", seed: seed, b64: "not base64!!", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jwt, ok, err := decodeCreds(tc.seed, tc.b64)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if jwt != tc.wantJWT {
				t.Errorf("jwt = %q, want %q", jwt, tc.wantJWT)
			}
		})
	}
}

// TestCredsOptionFromEnv covers the env->nats.Option wiring connectFromEnv
// actually calls (decodeCreds is the pure core; this is its production
// entry point) — the env-var matrix: neither set, only one set, both set.
func TestCredsOptionFromEnv(t *testing.T) {
	goodJWT := base64.StdEncoding.EncodeToString([]byte("h.p.s"))
	t.Run("none injected -> nil option, no error", func(t *testing.T) {
		t.Setenv(envNatsNKey, "")
		t.Setenv(envNatsB64JWT, "")
		opt, err := credsOptionFromEnv()
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if opt != nil {
			t.Error("option = non-nil, want nil when no creds injected")
		}
	})
	t.Run("only seed -> error", func(t *testing.T) {
		t.Setenv(envNatsNKey, "SUASEED")
		t.Setenv(envNatsB64JWT, "")
		if _, err := credsOptionFromEnv(); err == nil {
			t.Error("err = nil, want error when only the seed is set")
		}
	})
	t.Run("only jwt -> error", func(t *testing.T) {
		t.Setenv(envNatsNKey, "")
		t.Setenv(envNatsB64JWT, goodJWT)
		if _, err := credsOptionFromEnv(); err == nil {
			t.Error("err = nil, want error when only the jwt is set")
		}
	})
	t.Run("both injected -> non-nil option", func(t *testing.T) {
		t.Setenv(envNatsNKey, "SUASEED")
		t.Setenv(envNatsB64JWT, goodJWT)
		opt, err := credsOptionFromEnv()
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if opt == nil {
			t.Error("option = nil, want non-nil when both creds present")
		}
	})
	t.Run("both injected but malformed base64 -> error", func(t *testing.T) {
		t.Setenv(envNatsNKey, "SUASEED")
		t.Setenv(envNatsB64JWT, "not base64!!")
		if _, err := credsOptionFromEnv(); err == nil {
			t.Error("err = nil, want error when the injected jwt does not decode")
		}
	})
}
