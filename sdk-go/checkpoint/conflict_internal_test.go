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

package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// TestIsConflict covers both "wrong last sequence" codes, however nats.go
// wraps them: 10071, and 10164 (a replicated stream racing an in-flight
// write), plus the raw API error a Create over a deleted key returns. Errors
// that are not a lost CAS must not match.
func TestIsConflict(t *testing.T) {
	code := func(c jetstream.ErrorCode) error {
		return &jetstream.APIError{Code: 400, ErrorCode: c, Description: "wrong last sequence"}
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"ErrKeyExists", jetstream.ErrKeyExists, true},
		{"wrapped ErrKeyExists", fmt.Errorf("create: %w", jetstream.ErrKeyExists), true},
		{"ErrKeyRevisionMismatch over 10164", fmt.Errorf("%w: %w", code(jetstream.JSErrCodeStreamWrongLastSequenceConstant), jetstream.ErrKeyRevisionMismatch), true},
		// Bare, with no APIError underneath: the only case that tests the
		// ErrKeyRevisionMismatch branch on its own.
		{"bare ErrKeyRevisionMismatch", jetstream.ErrKeyRevisionMismatch, true},
		{"raw 10071", code(jetstream.JSErrCodeStreamWrongLastSequence), true},
		{"raw 10164 (replicated, Create over a delete marker)", code(jetstream.JSErrCodeStreamWrongLastSequenceConstant), true},
		{"other API error", code(jetstream.JSErrCodeStreamNotFound), false},
		{"timeout", context.DeadlineExceeded, false},
		{"nil", nil, false},
		{"unrelated", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConflict(tc.err); got != tc.want {
				t.Errorf("isConflict(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
