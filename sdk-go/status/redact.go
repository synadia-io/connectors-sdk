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

package status

import "regexp"

// uriUserinfoRe matches the userinfo component of a URI-shaped token in
// free text: "scheme://user:pass@host", or "scheme://token@host". The scheme
// is followed by two slashes, each optionally backslash-escaped, so a URI
// quoted inside JSON ("mongodb:\/\/u:p@h") is matched too. The userinfo is
// everything non-whitespace up to the LAST "@" of the whitespace-delimited
// token, so a password containing "/" or "@" ("pa/ss", "p@ss") is masked
// whole rather than leaking its tail. The cost is over-redaction: anything
// non-whitespace between the scheme and the token's last "@" goes, so a URI
// with no userinfo whose path or query contains "@" ("https://host/path@x")
// loses everything up to that "@", a comma-separated list of URIs keeps only
// the last host, and in compact JSON ({"uri":"mongodb://u:p@h","email":"a@b"})
// the fields up to the last "@" go with it. All accepted, since a reason that
// is too short beats one that leaks. Out of scope, like every non-URI secret:
// whitespace inside a password, which a valid URI cannot carry, and a URI cut
// off before its "@".
//
// This is wider than the regex the mongodb and s3 connectors redact with
// themselves, which stops at the first "/" or "@"; they should adopt
// RedactReason so the two layers agree.
var uriUserinfoRe = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*:\\?/\\?/)\S+@`)

// RedactReason masks the userinfo of any URI found in a status reason,
// rewriting "scheme://user:pass@host" to "scheme://REDACTED@host". Every
// reason the harness publishes to the retained status stream (D-5), and every
// reason returned from the admin status endpoint (D-8), passes through this
// in Publisher.SetState — so a connector that forgets its own redaction still
// cannot leak a credential embedded in a URI (the common case: a mongodb
// connection URI, an S3 endpoint) into the ops plane.
//
// It is deliberately conservative, and NOT a general secret scanner: it masks
// only whole userinfo inside a URI-shaped substring. A secret that is not URI
// userinfo — a bare token, a query-string parameter, an opaque key — is out
// of scope and remains the connector's own responsibility to redact. The
// "REDACTED" marker matches the per-connector redactors so the wire output is
// uniform. Exported so connectors and conformance can reference the exact
// behavior the harness guarantees.
func RedactReason(reason string) string {
	// ${1}, not $1: Go's $name replacement expansion would greedily read the
	// group reference as a name "1REDACTED" (which expands to empty). Mirrors
	// mongodb.redactURI's note.
	return uriUserinfoRe.ReplaceAllString(reason, "${1}REDACTED@")
}
