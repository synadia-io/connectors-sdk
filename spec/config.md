# Config document — cross-connector contract

Language-neutral contract for how validated config reaches a running
connector process, and for the vocabulary a connector's spec document
(`contract.md` D-12) uses to describe how that config should be
presented in a form. Config is data: a document, not rendered
command-line flags. Vocabulary schema:
`x-display.schema.json`. Example pair: `examples/spec.mongodb.json` /
`examples/config.mongodb.json`.

## Delivery and discovery (D-2)

The config document is a single JSON object, UTF-8 encoded, mounted
into the container read-only. There is **no envelope** — the mounted
document *is* the config object; a connector does not unwrap an
`{"answers": {...}}` or `{"config": {...}}` shell to reach its fields.
Numeric values must round-trip through JSON exactly as written:
integers beyond 2^53 are not safely representable as JSON numbers, so
a field whose values can exceed that bound must be encoded as a
string, not a JSON number.

The binary finds the document through one environment variable:

- `NEX_WORKLOAD_CONFIG_FILE` — absolute path to the mounted document.

If this variable is unset, the connector is **not** in config-document
mode at all — it falls back to its own flags entirely for that run.
This is a mode switch, not a per-field fallback: a connector never
merges "some fields from the document, the rest from flags" in a
single run. See "Loader precedence" below for what that means when
both are present.

**Unknown fields are rejected.** The document is validated against the
connector's own `config_schema` with `additionalProperties: false`
enforced at every level the schema declares it (Go's
`json.Decoder.DisallowUnknownFields()` is the reference behavior; other
languages use their JSON Schema validator's equivalent strict mode). A
config document carrying a field the schema doesn't know about is a
startup failure, not a warning — see `TestExampleSpecConfigPair` in
`spec_test.go` for the enforced case.

## Loader precedence: document over flags

When `NEX_WORKLOAD_CONFIG_FILE` is set, the config document is
authoritative for every field it is capable of representing — the
connector must not additionally consult flags to override or fill in
document fields for that run. Flags remain available in every
entrypoint mode (D-11) for the fields config-document mode doesn't
cover (e.g. `--config-file` itself, log level, or a genuinely
process-level knob outside the schema) and for connectors run without
a mounted document at all. A connector's `Load` (`sdk-go/config`)
implements exactly this:
document present -> parse and validate it exclusively; document absent
-> fall back to flags.

## Secret references (D-4)

A config document field never carries a secret value directly. Instead
it carries a reference:

```
secret://<id>
```

`<id>` may contain `/` (e.g. `secret://mongo/prod-cluster-password`).

**Resolution is whole-value only.** A field participates in `secret://`
resolution iff **its entire value**, start to end, is the reference —
there is no mid-string or partial resolution.

**The prefix is reserved document-wide, and resolution is
schema-blind**. Node-side
resolution (the nexlet, see below) runs before the config document is
matched against the connector's `config_schema` — at that point in the
pipeline the resolver has no access to `config_schema` or its
`x-display` hints at all, so it cannot check "is this the kind of
field that's allowed to hold a reference" before deciding whether to
resolve a value. It resolves **every** string field whose entire value
starts with `secret://`, full stop, regardless of what that field's
schema says. The connector-side guard mirrors this on purpose: it too
scans every string value, at every nesting level, with no schema
lookup, for the same reason — the two checks only agree if they use
the same (lack of) scope.

Because the prefix is reserved everywhere, **a literal value that
happens to start with `secret://` is prohibited, not passed through as
data** — there is no escaping mechanism for a field that would
otherwise legitimately want that literal text as its value. A schema
author who needs a field to hold arbitrary user text should treat this
as a real constraint on the value space, the same way a field can't
hold arbitrary binary in a JSON string.

`"x-display": {"secret": true}` (see `mongo_password` in
`examples/spec.mongodb.json`) marks a field whose value is a
`secret://` reference; a rendering client masks it. It does not
narrow which fields resolution or the guard inspect: that scope is
document-wide, whatever the hint says.

`x-display.secret_parts` is a **different, non-resolving** mechanism —
see "Composite values and `secret_parts`" below. A field's schema must
not carry both `secret` and `secret_parts` (`x-display.schema.json`
enforces this with a root-level `not`/`required` constraint); the two
hints answer different questions and combining them on one field is a
contradiction, not a richer declaration.

Resolution happens **node-side**, in the nexlet, before the config
document is ever mounted into the connector's container — through
nex's `models.SecretStore.GetSecret(namespace, id)`, applied to every
string field in the document (schema-blind, per above). By the time a
connector reads its config document, no field anywhere should still
hold a `secret://` value — resolved fields hold the real value in
full, replacing the reference outright, and every other field never
legitimately contained one in the first place.

**Unresolved secret is fatal, not degraded-mode.** If a connector
observes a value in **any** field — not only one marked
`x-display.secret` — that still starts with `secret://`, resolution
did not happen — the node/environment is misconfigured. This is a
startup-only-recoverable condition: the connector MUST treat it as a
fatal misconfiguration, entering `parked` with `reason:
"unresolved-secret"` (`status.md`, `lifecycle.md`) rather than
attempting to connect with a token that is obviously not a real
credential. The connector's own guard must scan the whole document the
same schema-blind way the resolver does — checking only
`x-display.secret` fields would miss exactly the failure mode this
guard exists to catch (a field whose schema doesn't even carry the
hint, but whose value is a leftover, never-resolved reference because
the platform-side request that should have triggered resolution was
itself misconfigured).

### Composite values and `secret_parts`

`x-display.secret_parts` is a **display-masking hint, not a resolution
mechanism**. It names which URL component(s) (`userinfo`, `query`,
`fragment`) of a composite value, such as a connection URI, a
rendering client should mask from view. Nothing about the underlying
value changes, and no `secret://` substitution happens inside it: the
field is validated and delivered byte for byte, same as any other
string field. A credential belongs in a `secret://` reference, not in
the literal value.

A field that needs an actual resolvable credential *inside* a
composite value does not use `secret_parts` for that — it decomposes
the credential into its own leaf field marked `secret: true`, and the
connector assembles the effective value at runtime.
`examples/spec.mongodb.json` / `examples/config.mongodb.json`
illustrate this decomposition pattern **in the abstract**: `mongo_uri`
(marked `secret_parts: ["userinfo"]`, so a UI masks that part of the
URI) alongside a separate
`mongo_password` field (marked `secret: true`, the actual credential
path) —

```
mongo_uri:      mongodb://cluster0.example.net:27017/?authSource=admin
mongo_password: secret://mongo/prod-cluster-password
```

What the contract guarantees for this pattern is only that a
`secret`-marked field's value, by the time the connector reads it, is
the real credential in full — never a `secret://` prefix, and never
spliced into another field's string. Exactly how a connector recombines
decomposed fields into the value its client library wants is
connector-specific and outside this contract's scope.

That two-field split illustrates D-4's composite rule; it is not any
real connector's schema. A connector whose connection config is a
single URI field (also `secret_parts: ["userinfo"]`, for the same
display masking) takes a managed credential as a whole-value
`secret://` reference in that field instead. The node-side resolver
resolves it under the schema-blind rule (above) like any other field,
and the connector reads back the complete URI, credential included.
A document that sets a field the connector's schema doesn't have, such
as `mongo_password` against that connector, is rejected ("Unknown
fields are rejected," above).

## Optionality (D-14)

An optional field absent from the config document is **not** the same
as an empty value:

- Absent: the connector applies the field's zero value, or its
  schema's `default` if the schema declares one. Applying `default` is
  the loader's job, not something a connector author special-cases
  field by field.
- Present as `""` (empty string), `0`, or `false`: that **is** the
  configured value. It must be honored as such, not treated as
  "unset".

A config document keeps the two apart: presence and value are
orthogonal.

`examples/spec.mongodb.json` shows both patterns: `transformer_type`
defaults to `"none"` (absence means "no transformer"), while
`transformer_mapping` defaults to `""` and is only meaningful — and
only rendered, via its `visible_when` hint — once `transformer_type`
is explicitly `"mapping"`.

## The x-display vocabulary (D-13)

A `config_schema` property may carry presentation hints for
form-rendering clients under its own `"x-display"` key. The vocabulary
is closed and machine-validated: see `x-display.schema.json` for the
full hint set (`label`, `group`, `order`, `placeholder`, `hidden`,
`secret`, `secret_parts`, `visible_when`, `cp-type`, `code`) and
`TestXDisplaySchema` in `spec_test.go` for the enforcement. Two rules
worth calling out here because they affect config-document semantics,
not just rendering:

- `hidden` hides a field from a rendered form; it does not exempt the
  field from config-document validation, required-ness, or
  optionality rules above.
- `visible_when` is a rendering condition, not a validation
  shortcut — schema authors never hand-write the equivalent
  `allOf`/`if`/`then`. A generator derives conditional validation from
  `visible_when` when a schema actually needs conditionally-required
  fields; the raw `config_schema` in a spec document may leave a
  `visible_when`-gated field simply optional if the connector code
  itself refuses to start on the invalid combination.
- `secret` and `secret_parts` are mutually exclusive on a single field
  — `x-display.schema.json` rejects a hint object carrying both — for
  the reason covered in full under "Secret references" above: they are
  different mechanisms (whole-value resolution vs. display masking of
  a literal value), not two strengths of the same thing.
