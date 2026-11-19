# spec/ — language-neutral connector contracts

Contracts every connector honors, regardless of implementation
language.

The `D-n` labels are stable decision ids: the heading or sentence that
carries one defines it, and other docs and code cite it.

Current contents:

- **[contract.md](contract.md)** — the umbrella: contract identifier
  `synadia.connector/v1` (D-1), `spec`/`check`/run entrypoint mechanics
  (D-11), the spec document shape (D-12), and the capability vocabulary
  (`check`, `status`, `park`, `transform:mapping`).
- **[manifest.md](manifest.md)** / **manifest.schema.json** — the
  published artifact naming a connector image and its spec: `name`,
  `class`, digest-pinned `image`, `contract`, `spec` (embedded or URI),
  `tier`, `icon`, `docs`. Example: `examples/manifest.mongodb.json`.
- **[config.md](config.md)** / **x-display.schema.json** — how a
  connector's config document is delivered and discovered (D-2), the
  `secret://` reference syntax and the fatal unresolved-secret rule
  (D-4), absence-vs-empty-value optionality (D-14), loader precedence
  of document over flags, and the closed `x-display` presentation-hints
  vocabulary (D-13). Example pair: `examples/spec.mongodb.json` /
  `examples/config.mongodb.json`.
- **[status.md](status.md)** / **status.schema.json** — runtime identity
  (D-3), the status subject and publish cadence (D-5), the state model
  (D-6), and lag reporting with no percentage field (D-7). Example:
  `examples/status.parked.json`.
- **[admin.md](admin.md)** — the `connector-admin` NATS micro service
  (NATS ADR-32): root subject, and the `status`/`resume`/`skip`
  endpoints (D-8).
- **[lifecycle.md](lifecycle.md)** — park semantics (D-15), the reason
  strings the harness itself produces, and drain semantics (D-16).
- **[checkpoint.md](checkpoint.md)** — checkpoint KV bucket naming and
  the required compare-and-swap another-writer guard (D-9).
- **[credentials.md](credentials.md)** — how a connector gets its NATS
  endpoint and credential, the requested claim set (D-10), what its
  JetStream create and update grants allow, and what the claim set means
  for a connector.
- **[subjects.md](subjects.md)** — the token rule for every value a
  connector builds a subject or a JetStream name from: what is rejected,
  why, and test vectors.
- **[transformer.md](transformer.md)** — the optional per-message transform
  stage: `transformer {type, config}` config shape, reserved
  `.oversize`/`.error` subject suffixes, metadata-only notice shape with
  `<Source>-Id-Hash` correlation, reserved metadata keys
  (`nats_subject`/`nats_reply_subject`), retention and determinism notes.

Validated by `spec_test.go` (`go test ./...` from this directory; a
test-only module — see `go.mod` — so schema-validation deps never reach
sdk-go or the connector binaries).
