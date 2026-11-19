# Contract — cross-connector umbrella

Language-neutral contract identifier and the mechanics every connector,
in any language, honors to be recognized as a connector rather than an
arbitrary workload. The individual surfaces (manifest, config document,
status, admin, checkpoint, credentials, transformer) each have their own
normative doc in this directory; this file is the umbrella that ties
their entrypoint mechanics and capability set together.

## Contract identifier

`synadia.connector/v1` (D-1). A manifest's `contract` field and a spec
document's `contract` field both carry this value verbatim. It names one
version of this entire document set — the manifest schema, the config
document envelope, the status schema, the admin surface, and the
entrypoint mechanics below all version together. A connector build that
implements a later contract revision advertises a different value; there
is no independent versioning per surface.

## Entrypoint mechanics

A connector binary's first positional argument selects its mode (D-11).
Flags remain valid in every mode — mode selection is additive, not a
different flag surface:

- `spec` — print the spec document (below) as JSON to stdout, exit 0.
  Takes no config; a connector must be able to describe itself without
  a config document present. This is what "embedded" means in a
  manifest's `spec` field (`manifest.md`): the launcher runs the image
  with `spec` instead of fetching a catalog-hosted document.
- `check` — validate the supplied config and probe the external system
  it names (connectivity, auth, permissions — whatever "is this
  reachable and usable" means for the connector). Prints
  `{"ok": bool, "message": string, "latency_ms": int64}` to stdout;
  `message` may be omitted when it is empty. Exit code is 0 iff `ok` is
  true: a probe failure MUST exit non-zero
  *and* still print that JSON body (`"ok": false` included) before
  exiting — the caller needs both signals, not one in place of the
  other. The contract violation is a **zero** exit paired with
  `"ok": false`, or a bare non-zero exit that prints no JSON body at
  all.
  This is the Airbyte CHECK pattern: run the image as a throwaway job
  against submitted-but-not-yet-deployed config, before committing.
  Check **reads the config document** (`config.md`, D-2) before
  probing — the config is the thing under test, and the probe dials
  the system it names. A document that fails to load is itself a
  failed check: it is reported as `"ok": false` with the loader's
  message, not as a bare non-zero exit.
  Check finishes in bounded time: a probe that never answers is
  reported as `"ok": false`, not left to hang the job. The Go SDK's
  harness (`sdk-go/runtime`) allows 10 seconds for the config load and
  the probe together, then up to 1 second more for a probe to report
  its own error; a caller that times out check jobs should allow at
  least that.
- (no positional argument, or any value other than `spec`/`check`) —
  run. This is the only mode that requires runtime identity (D-3) to
  be resolvable, and the only one that connects to NATS: `spec` and
  `check` both run as throwaway jobs with no status subject, no admin
  surface, and no injected credentials to connect with.

## Spec document

`{"contract": "synadia.connector/v1", "config_schema": <JSON Schema
2020-12>, "capabilities": [...]}` (D-12). `config_schema` is the schema
the connector's config document (`config.md`) must satisfy — the same
document a control plane validates submitted answers against before
deploy. It carries the `x-display` presentation hints
(`x-display.schema.json`) inline on its properties; there is no separate
form-schema artifact.

## Capabilities

`capabilities` is a flat list of strings a connector's spec document
declares. Each names a contract surface the build actually implements,
so a generic client (control plane, CLI, conformance driver) can adapt
instead of guessing from connector identity:

| Capability | Means the connector |
|---|---|
| `check` | implements the `check` entrypoint mode above. |
| `status` | publishes the status document (`status.md`) and answers `status` on its admin surface (`admin.md`). |
| `park` | can enter the `parked` state (`lifecycle.md`, D-15) and answers `resume`/`skip` on its admin surface. |
| `transform:mapping` | offers the transformer stage (`transformer.md`) with the `mapping` backend. |

Within `park`, `resume` is mandatory but `skip` is optional: a connector
with no poison-item concept (e.g. one with no per-item retry ladder to
short-circuit) may answer every `skip` call with an "unsupported" error
(`admin.md`) — the harness default when the connector supplies no `Skip`
hook — without that being a conformance failure of the `park` capability
itself.

A capability list is a floor, not a ceiling: a connector may implement
none of these and still be a connector (it need only meet the
entrypoint mechanics and — for a source or sink that has one — the
manifest contract). Declaring a capability without behaving accordingly
is a conformance failure; the list exists so callers can rely on it
without probing behavior first.

## Where each surface is defined

- **Manifest** — `manifest.md`, `manifest.schema.json`: what gets
  published and how a launcher finds the image and its spec.
- **Config document** — `config.md`: how config reaches the running
  process (D-2), secret references (D-4), optionality (D-14).
- **x-display vocabulary** — `x-display.schema.json`: the closed hint
  set (D-13) a `config_schema` uses to describe form rendering.
- **Status** — `status.md`, `status.schema.json`: the state model
  (D-6), lag reporting (D-7), publish cadence (D-5).
- **Admin** — `admin.md`: the NATS micro surface (D-8).
- **Lifecycle** — `lifecycle.md`: park (D-15) and drain (D-16).
- **Checkpoint** — `checkpoint.md`: KV bucket naming and the
  another-writer guard (D-9).
- **Credentials** — `credentials.md`: the requested claim set (D-10).
- **Transformer** — `transformer.md`: the optional per-message
  transform stage.
