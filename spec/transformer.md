# Transformer — cross-connector contract

Language-neutral contract for the optional per-message transform stage.
Any connector, in any language, that offers a transformer honors this;
backend-specific semantics (the Bloblang hardening profile, message
binding) and connector-specific semantics (choke-point placement,
checkpoint interlock, dedup interplay) live with their implementations.

## Config shape

One optional `transformer` object per connector, one field shape across
runtimes:

```jsonc
"transformer": {                      // optional; absent = stage absent
  "type": "mapping",                  // backend id
  "config": { "mapping": "root.x = this.y" }  // schema per backend
}
```

- Exactly one transformer per connector. Ordered chains, when demanded,
  compose inside this shape as a `chain` backend whose config is a list
  of `{type, config}` — the field stays singular.
- An unknown `type` is a startup failure listing the known backends.
- Backend config is validated (and programs compiled) at startup: a bad
  config fails the connector before data flows, never per-message at
  runtime. Unknown config fields are rejected, not ignored.
- Absent transformer means byte-identical behavior to a connector
  without the stage — including notice shapes.

## Reserved subject suffixes

The terminal tokens `.oversize` and `.error` on a data subject are
reserved for connector-emitted notices:

- `<subject>.oversize` — the source item could not be published at its
  wire size.
- `<subject>.error` — the transform stage could not produce output for
  one message (per-message failure; the connector's position still
  advances).

Notices are always published on the **pre-transform** subject plus the
suffix (a rewritten subject may be the very thing that is invalid). A
transform must not be able to produce a subject carrying a reserved
suffix — notices are unforgeable. A connector binds each suffix it is
configured to publish a notice on into the streams that carry its data
subjects, so the first notice cannot fail with no-responders. A connector
that is not configured to publish a notice need not bind its suffix: one
without a transform stage never publishes `.error`, and one with oversize
notices switched off never publishes `.oversize`.

## Notice shape

A notice is **metadata-only**: empty payload, identity in headers. When
a transformer is configured, every notice (error and oversize alike)
carries the *minimal* header set:

- The connector's source-identity headers **without the raw source id**
  (for MongoDB: `Mongo-Op`, `Mongo-Db`, `Mongo-Coll`,
  `Mongo-Cluster-Time` — no `Mongo-Id`). A transform may exist to
  redact the id; emitting it on the failure path would bypass the
  redaction.
- `<Source>-Id-Hash` (e.g. `Mongo-Id-Hash`) for correlation: SHA-256
  over the connector's canonical encoding of the source id, first 16
  bytes, lowercase hex (32 characters).
- Error notices additionally carry `Transform-Error`: a stable error
  class plus line/column position, never the raw error message —
  transform engines quote payload values in their error text, so a
  connector must reduce it to a value-free class before it reaches a
  header. A 256-byte cap and CR/LF stripping (header framing is CRLF)
  apply as a backstop, not the primary defense. The full error text
  goes to the connector's logs only.

Without a transformer, a connector's pre-existing notice shapes are
unchanged.

## Reserved metadata keys

Inside a transform, the message subject is exposed as metadata, never as
a header:

- `nats_subject` — the message subject; injected on input, and if
  written by the mapping, consumed as the output subject (subject to
  the connector's routing guard) and stripped from the output headers.
  When the output subject, written or inherited, isn't a literal
  subject, or is longer than the subject length limit in `subjects.md`
  (3072 bytes), the backend fails that message with a per-message
  mapping error: every `.`-separated token must satisfy the token rule
  in `subjects.md`. The error states an over-long subject's length and
  quotes only its start. In the Go SDK every backend wraps
  `transform.ErrUnpublishableSubject` in that error, so a connector
  classifies it with `errors.Is` whatever the backend.
- `nats_reply_subject` — reserved for future use; stripped from output.

Reserved keys never appear as wire headers.

## Retention

Suffix notices share the data stream's retention — they are bound into
the same stream as the data subjects. (Kafka/Debezium use a separate
dead-letter topic for independent retention; this contract keeps notices
in-stream so one retention setting covers data and notices.) Operators
sizing retention for audit of gaps must size the data stream's retention.

## Determinism

Transform backends must be deterministic: the same input message always
yields the same output. Connector replay windows (at-least-once
delivery), last-write-wins consumption, and dedup reasoning all assume
a replayed message transforms identically. Backends deny
nondeterministic primitives (clocks, random ids, host identity, IO) by
construction.
