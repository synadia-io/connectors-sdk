# Admin — cross-connector contract

Language-neutral contract for the request-reply control surface every
connector exposes over NATS: a NATS micro service (NATS ADR-32). Runtime
identity backing the subject below is defined in `status.md` (D-3).

## Service (D-8)

A [NATS micro](https://github.com/nats-io/nats.go/tree/main/micro)
service:

- **Name**: `connector-admin`.
- **Version**: the connector's own version string (the same one a
  status document's provenance would report, e.g. from build info).
- **Root subject**: `$CONNECTOR.<namespace>.admin.<instanceID>` — same
  `<namespace>` and `<instanceID>` as the status subject (`status.md`,
  D-5), so an operator who has one has the other by substitution alone.

## Endpoints

All three hang off the root subject as `<root>.<endpoint>`:

### `status`

Returns the connector's current status document — byte-for-byte the
same shape published to the status subject (`status.schema.json`,
`status.md`). No request body. This is what makes the status subject
optional for a consumer that only needs the current value on demand
rather than a live feed: request-reply and publish-subscribe are two
paths to the identical value.

### `resume`

Un-parks the connector (`lifecycle.md`, D-15): the loop re-enters, the
status transitions out of `parked`. No request body. **Error if the
connector is not currently parked** — `resume` is not a generic
restart, it specifically reverses a park.

### `skip`

Skips the poison item that caused a park, then behaves like `resume`.
Request body: `{"key": string}` — the connector-defined key
identifying the item to skip (e.g. a resume-token position, a message
id; the key's shape is connector-specific and not standardized here).
**Error if unsupported by this connector, or if not currently
parked** — skip is meaningless outside a park, and not every connector
has a well-defined notion of "skip this one item" (an unsupported
connector answers every `skip` call with an error, it does not omit
the endpoint).

## Errors

Every endpoint uses NATS micro's standard error headers
(`Nats-Service-Error`, `Nats-Service-Error-Code`) rather than an
ad hoc error shape in the response body — a generic micro client
already knows how to surface these without connector-specific
handling. The error message is redacted the same way a status
`reason` is (`status.md`) and may be truncated.

## Relationship to lifecycle

`resume` and `skip` only make sense in terms of the park state machine
defined in `lifecycle.md` (D-15): both are errors outside `parked`,
and `resume` is the only way out of `parked` other than an operator
directly editing durable state (not a supported path). Read
`lifecycle.md` alongside this file for the state transitions these
two endpoints trigger.
