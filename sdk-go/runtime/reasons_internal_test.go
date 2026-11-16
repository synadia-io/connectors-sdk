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
	"testing"

	"github.com/synadia-io/connectors-sdk/sdk-go/checkpoint"
)

// TestCheckpointConflictReasonMatchesSDKError pins that the harness's park
// reason literal and checkpoint.ErrConflict's message agree. spec/checkpoint.md
// fixes the value; if this test fails after a checkpoint package change, the
// spec's fixed reason is what must not move.
func TestCheckpointConflictReasonMatchesSDKError(t *testing.T) {
	if reasonCheckpointConflict != checkpoint.ErrConflict.Error() {
		t.Fatalf("reasonCheckpointConflict = %q, checkpoint.ErrConflict.Error() = %q, want equal",
			reasonCheckpointConflict, checkpoint.ErrConflict.Error())
	}
}
