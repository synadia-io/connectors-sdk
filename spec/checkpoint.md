# Checkpoint — cross-connector contract

Language-neutral contract for how a connector persists its durable
progress position in a JetStream KV bucket, and for the guard that
makes checkpoint corruption from two instances writing at once structurally
impossible rather than merely unlikely. Runtime identity backing the
bucket name below is defined in `status.md` (D-3).

## Bucket naming (D-9)

Default bucket name:

```
CONN-CKPT-<instanceID>
```

`<instanceID>` is sanitized to the character class `[A-Za-z0-9_-]`
before substitution — a durable connector id or workload id may
contain characters a KV bucket name can't (JetStream bucket names have
their own restricted alphabet); sanitization happens once, at the
naming boundary, not scattered across call sites.

A connector **may** accept an override — a flag or a config document
field — that replaces the default bucket name outright. This exists
for migration and multi-instance-sharing-one-bucket scenarios; when
present, the override is used as-is (still subject to JetStream's own
bucket-name constraints), not merged with the default pattern.

The default name needs a stable `instanceID` (`status.md`, D-3). The
workload id is new on every launch, so a default bucket named after it
starts over each time, and two overlapping instances open different
buckets, where neither one's compare-and-swap can catch the other. A
connector with only a workload id names its bucket explicitly (the
override above) or doesn't start; the Go SDK's `checkpoint.Open` refuses
to derive the default from a workload id (`identity.ErrNoConnectorID`).

The permissions needed to open and use the bucket are in
`credentials.md` (D-10).

## Reserved keys

Keys starting with `_` are reserved for SDK records; connector keys
**MUST NOT** use that prefix. The reference implementation
(`sdk-go/checkpoint`) keeps its optional configuration binding under
`_scope`.

## The another-writer guard (REQUIRED)

Every checkpoint write **MUST** be a compare-and-swap against the
revision it was read at (or a create-if-absent for the first write) —
never an unconditional put. Concretely, in JetStream KV terms: read
returns a value and a revision number; write calls the bucket's
`Update(key, value, expectedRevision)` (or `Create` when no entry
exists yet), never a bare `Put`.

**Why this is required, not a hardening nice-to-have**: two instances
of the same connector can overlap (`credentials.md`, "What this means
for a connector"), and both believe they own the checkpoint. Without a
CAS guard, this is a
silent last-write-wins race: whichever instance's write lands last
wins, with no signal that anything went wrong. With the guard, the
losing writer's `Update` call fails with a revision-mismatch error,
and that failure is a definite signal: another writer exists right
now, or an operator reset the key mid-run.

**What a connector does when the guard fires**: treat the conflict as
a park-class error (`lifecycle.md`, D-15) — halt the loop, report the
machine-readable reason `checkpoint-conflict`, and wait for an
operator. The Go SDK's harness (`sdk-go/runtime`) sets this reason
itself when a Run hook returns an error wrapping the conflict (unless
the error also carries an explicit Park reason), so a connector gets
the right reason even if it never labels the error as a park. Never
fall back to retrying the write, never treat it as a
transient error to paper over: a second live writer means two
instances overlap, and continuing to write compounds the corruption
instead of surfacing it.

There is one narrow exception: for a key holding an idempotent
marker — an immutable fact, such as "event X was delivered", not a
progress position — a connector **may** re-read the key after a
conflict and, if the stored value is exactly the value it would have
written, treat the conflict as a duplicate and continue. In every
other case it parks. The exception never permits rewriting the key:
a marker whose stored value differs parks like any other conflict. A
connector using the exception **should** count the duplicate (e.g.
in its status counters) so an overlapping deployment stays visible.

**Reference implementation**: `sdk-go/checkpoint` is a working instance
of this pattern — a byte-level store with a `Load` that returns the
revision, revision-checked `Create`/`Update` writes, and a distinguished
`ErrConflict` on a lost CAS. A lost CAS can surface as either the
"wrong last sequence" error code 10071, or code 10164 on a replicated
(R>1) stream; an implementation must treat both as a conflict, not just
the one its tests happen to exercise. The store itself is byte-level: what a
connector keeps under its keys, and how it encodes that, is that
connector's own design choice for what it needs to check out and back
in atomically — none of it is normative here. What this contract
requires is the bucket-naming convention, the reserved key prefix, and
the CAS discipline on every write; how a connector structures the
value it's guarding is up to the connector.
