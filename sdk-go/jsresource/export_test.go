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

package jsresource

import "github.com/nats-io/nats.go/jetstream"

// SetServerVersion makes EnsureStream see the connected server as version v,
// to test the pre-2.12 path against a 2.14 test server. It returns a
// function that restores the real lookup.
func SetServerVersion(v string) (restore func()) {
	old := serverVersion
	serverVersion = func(jetstream.JetStream) string { return v }
	return func() { serverVersion = old }
}
