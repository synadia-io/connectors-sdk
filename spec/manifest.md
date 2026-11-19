# Manifest — cross-connector contract

Language-neutral contract for the published artifact that names a
connector image and tells a launcher (control plane, `nex`, a CLI) how
to find its config spec. The published artifact is a manifest, not a
template that renders a deployment. Schema: `manifest.schema.json` (JSON Schema
2020-12); example: `examples/manifest.mongodb.json`.

## Field reference

| Field | Type | Required | Constraint |
|---|---|---|---|
| `name` | string | yes | `^[a-z0-9][a-z0-9-]*$` |
| `class` | string | yes | `source` or `sink` |
| `image` | string | yes | must end `@sha256:<64 hex>` |
| `contract` | string | yes | exactly `synadia.connector/v1` |
| `spec` | string | yes | `"embedded"` or a URI |
| `tier` | integer | no | 1–3 |
| `icon` | string | no | — |
| `docs` | string (URI) | no | — |

`additionalProperties: false` — the manifest is a closed, ten-line
format by design; a launcher must not need to guess at unknown
fields, and a typo in a field name fails loudly instead of being
silently ignored.

### `name`

The connector's identifier. Lowercase alphanumeric with internal
hyphens, starting with a letter or digit — safe to use unmodified in
subject names, bucket names, and file paths (it is also the fallback
component of the runtime identity in `credentials.md` / D-3, when no
durable connector id has been injected).

### `class`

`source` (produces messages) or `sink` (consumes messages). No third
value: a connector that is both is modeled as two manifests sharing an
image.

### `image`

**Digest-pinned, always.** The pattern requires a trailing
`@sha256:<64 lowercase hex>` — a bare tag (`:latest`, `:v1.2.0`) fails
schema validation, full stop. This is load-bearing, not cosmetic: a
launcher may reuse a locally cached image for a tag instead of pulling
it, so redeploying a manifest that references a mutable tag can run a
stale image without any error.
Digest pinning makes "which image is this manifest" a pure function of
the manifest's bytes, which is also what makes the catalog's
compatibility checks and provenance signing meaningful across
manifest versions.

### `contract`

Must equal `synadia.connector/v1` (D-1) — see `contract.md`. A manifest
naming a different value is describing a different, incompatible
runtime contract; there is no minor-version leniency.

### `spec`

Either the literal string `"embedded"` — the launcher runs the image
with the `spec` entrypoint (D-11) to obtain the config spec at deploy
time — or a URI pointing at a catalog-hosted, pre-extracted spec
document, so forms render without pulling images. The schema tells the two apart by scheme prefix
(`scheme://...`) rather than the JSON Schema `format` keyword, because
`format` is optional-assertion by the JSON Schema spec itself — some
validators check it, some only annotate with it — and this distinction
is semantically load-bearing, not documentation, so it cannot depend on
which validator happens to enforce `format`.

### `tier`

Integer 1 through 3, support/maturity tier — lower is more mature and
more supported. Optional; a manifest with no `tier` makes no claim in
either direction (see `config.md` / D-14 on optionality generally:
absence is not the same as `tier: 3`).

### `icon` / `docs`

Purely presentational — an icon reference and a documentation link for
catalog/UI rendering. Neither is validated against the artifact's
actual behavior; both are optional and have no default.

## Example

`examples/manifest.mongodb.json`:

```json
{
  "name": "mongodb",
  "class": "source",
  "image": "registry.example.com/connectors/mongodb@sha256:0000000000000000000000000000000000000000000000000000000000000000",
  "contract": "synadia.connector/v1",
  "spec": "embedded",
  "tier": 1,
  "icon": "https://example.com/connectors/mongodb.svg",
  "docs": "https://example.com/docs/connectors/mongodb"
}
```

The registry, digest and URLs above are placeholders; a real manifest
carries the image's actual digest.
