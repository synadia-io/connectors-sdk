# Status — cross-connector contract

Language-neutral contract for a connector's self-reported state: what
it publishes, where, how often, and the wire shape of the document
itself. Schema: `status.schema.json`. Example:
`examples/status.parked.json`.

## Runtime identity (D-3)

Every naming decision below — the status subject, the admin root
subject (`admin.md`), the checkpoint bucket (`checkpoint.md`), and the
default durable consumer name, `CONN-<instanceID>` — is a pure
function of one identity, injected by the nexlet:

- `NEX_WORKLOAD_CONNECTOR_ID` — the durable connector id, injected from
  the start-request metadata key `connector_id`.
- `NEX_WORKLOAD_NAMESPACE` — the namespace the connector runs in.

If `NEX_WORKLOAD_CONNECTOR_ID` is unset, a connector falls back, in
order: the workload id env var if the nexlet provides one, else the
manifest's `name` field (`manifest.md`) with `-dev` appended. This
fallback chain guarantees an `instanceID` is always resolvable, with
no error path — a connector run outside any launcher (a developer's
laptop) still gets a stable, if synthetic, identity.

**`instanceID`** — the value used everywhere a subject or bucket name
needs one — is the connector id when present, else the workload id
(after the fallback above). `status.parked.json`'s `connector_id` and
`workload_id` fields report both halves of this resolution
individually; `instanceID` itself is not a separate wire field, it's
what gets substituted into subject and bucket templates.

**Durable state needs a stable `instanceID`.** The workload id is new
on every launch, so a durable consumer or checkpoint bucket named after
it is a new one each time the connector starts: the durable replays
the stream from its start, the old one is left behind, and the
checkpoint starts over. The connector id and the `-dev` fallback are
stable. A connector with only a workload id names its durable consumer
and checkpoint bucket explicitly; the Go SDK's `identity.DurableName`
and `checkpoint.Open` refuse to derive them from a workload id
(`identity.ErrNoConnectorID`).

## Status subject (D-5)

```
$CONNECTOR.<namespace>.status.<instanceID>
```

Published on the connector's own NATS connection (not through the
admin micro service) every 10 seconds, and immediately on any state
change — a park, a resume, a failure — so an observer never waits out
the full interval to see a transition.

Retention is an ops-stream concern: an account-level JetStream stream
subscribing `$CONNECTOR.<namespace>.status.>` gives operators and the
control plane a durable, replayable feed of every connector's status
history without polling. Creating that stream is the operator's job,
out of this repo's scope, but recorded here because it's the reason
the subject hierarchy is namespace-scoped with a wildcard-friendly
shape rather than one subject per connector with no common prefix.

## State model (D-6)

Wire values, always lowercase:

| State | Meaning |
|---|---|
| `running` | Normal operation. |
| `paused` | Stopped by explicit external request, definition and durables intact (Kafka Connect PAUSE semantics: a pause of the connector resource, not to be confused with `parked`). |
| `parked` | Halted itself on a poison input or persistent downstream failure; alive, reporting, waiting for an operator (`lifecycle.md`, D-15). |
| `failed` | Unrecoverable — the fail-stop path for errors that happen before the admin surface exists (`lifecycle.md`). |
| `restarting` | Mid-restart, before the loop is back to `running`. |

`parked` is the differentiator this state model adds over the bare
Kafka Connect shape: it distinguishes "an operator hasn't looked at
this yet" from every other kind of stopped.

## Lag (D-7)

One canonical lag number, reported inside `cursor`:

- **CDC connectors**: `cursor.behind_ms` — an `int64` source-time
  delta in milliseconds (a connector that already stamps a source
  timestamp on every event, e.g. mongodb's cluster time, computes this
  directly; nothing new to instrument). A delta that clock skew makes negative is
  reported as `0`, never omitted and never negative.
- **Backfill connectors**: `cursor.backfill.copied` (`int64` count) and
  `cursor.backfill.done` (`bool`).

**There is deliberately no percentage field.** A "percent complete"
number needs a denominator, and no denominator exists in general — a
collection count query is a real cost on a large collection, and the
count can change while backfill is in flight. `status.schema.json`
enforces this structurally (`additionalProperties: false` on the
`backfill` object), not just by convention: a `pct` or `percent` key
under `backfill` fails schema validation outright (see
`TestStatusSchema`'s `backfill_percentage_field_does_not_exist`
subtest in `spec_test.go`).

A connector reports whichever of `behind_ms` / `backfill` apply to its
current phase — both, either, or neither (a connector that has
finished backfill and has no CDC concept, or a sink, may report a null
`cursor` entirely).

## Field reference

See `status.schema.json` for the enforced shape; in prose:

| Field | Type | Notes |
|---|---|---|
| `contract` | string | Exactly `synadia.connector/v1` (D-1). |
| `state` | string | One of the D-6 enum. |
| `reason` | string | Machine-readable; `""` when the state needs no explanation. |
| `since` | string (date-time) | Timestamp of the most recent state transition. |
| `cursor` | object or null | D-7 shape; null if the connector has no cursor concept. |
| `counters` | object or null | Free-form named `int64` counters (e.g. `published`, `errors`). |
| `admin` | string | This connector's admin root subject (`admin.md`, D-8). |
| `connector_id` | string | Runtime identity (D-3). |
| `workload_id` | string | Runtime identity (D-3). |

All nine keys are always present in the document — `cursor` and
`counters` are the only ones ever `null`; everything else is always a
concrete value (D-14: a status document has no optional fields, only
fields whose value can be a zero value).

`reason` is published to the status subject (`$CONNECTOR.<namespace>.status.<instanceID>`,
world-readable to anything with a subscription and retained in the
account's ops stream) and returned by the admin surface's `status`
endpoint — both broader audiences than the operator diagnosing the
failure. An implementation MUST NOT let a `reason` string carry
credentials or other secret material: an error built from a connection
string, DSN, or similar (a driver's own error text often quotes the
offending value verbatim, including any userinfo component) MUST have
that component redacted before it becomes part of `reason`.

## Where this document surfaces twice

The exact same document shape is what a connector's admin surface
(`admin.md`) returns from its `status` endpoint — there is one status
document per connector instance, published proactively to a subject
and available on demand via request-reply. A client never needs to
know both to get the current picture; either one is authoritative and
they never disagree, because a connector maintains one status value
and both paths read it.
