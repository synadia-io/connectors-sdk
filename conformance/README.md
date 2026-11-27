# conformance/ — black-box connector suite

Runs a connector *binary* (any language) against a real embedded NATS server, driving it over the nex launch contract (the `NEX_WORKLOAD_*` environment) and asserting the contracts in `../spec/`.

## Layout

- `contract_test.go` — the contract suite: the 15 tests every SDK's harness must pass (spec and check modes, the config document, the unresolved-secret guard, status, park/resume/skip, redaction, the SIGTERM drain, crash and restart, the routing guard, and the claim set: the whole suite's surface under a credential holding exactly the subjects `spec/credentials.md` lists). It runs against `testconn/` by default.
- `testconn/` — the fixture connector on sdk-go: it subscribes to one subject, counts messages, and parks on special bodies, so the suite can exercise each contract surface.
- `driver/` — builds a connector directory once per run, spawns the binary with the launch environment, waits on output markers, and asserts the lifecycle contract (`StopGraceful`: SIGTERM, drain, exit 0).
- `natsenv/` — embedded NATS server with JetStream; start, stop and restart, so a test can simulate a server outage.
- `assert/` — subscribe-and-collect NATS assertions, headers included.
- `specschema/` — compiles `../spec/*.schema.json` for validation.

`driver`, `natsenv`, `assert` and `specschema` are importable, so a connector repo can write its own conformance suites on them (`github.com/synadia-io/connectors-sdk/conformance/...`).

## Running

```bash
go test ./...                                    # the contract suite against testconn/ (sdk-go)
CONFORMANCE_TESTCONN_BIN=/path/to/fixture go test -run 'Test' .   # against another SDK's prebuilt fixture
```

`CONFORMANCE_TESTCONN_BIN` replaces the fixture with a prebuilt binary; that is how another SDK's fixture is held to the same suite. The fixture must meet the contract in "Fixture contract" below.

`CONFORMANCE_SPEC_DIR` names the `spec/` directory the schemas are read from. Without it, `specschema` finds `spec/` relative to its own source, which works from a checkout of this repo or a submodule of it. When `conformance` is used as a downloaded module, `spec/` is not next to it (it is a separate module), so set `CONFORMANCE_SPEC_DIR`:

```bash
CONFORMANCE_SPEC_DIR=/path/to/connectors-sdk/spec go test ./...
```

## Fixture contract

Every SDK provides a fixture connector built on its harness, and the suite drives it as a black box. `testconn/` is the sdk-go fixture. A fixture must behave as follows.

Invocation:

1. A single executable. The first argument `spec` or `check` selects that mode; no argument is run mode. Identity and the NATS URL come from the `NEX_WORKLOAD_*` environment the driver sets.
2. Config is accepted as a config document (`NEX_WORKLOAD_CONFIG_FILE`) and as flags. The suite passes one flag, `-subject=<subject>`. The document fields are `subject` (string, required), `greeting` (string, schema default `hello`), `api_key` (string, marked `x-display.secret`) and `fail_check` (bool). An unknown field is rejected, and the error names it.

`spec` mode:

1. `contract` is `synadia.connector/v1`.
2. `capabilities` includes `check`, `status` and `park`.
3. `config_schema` has at least one field marked `x-display.secret` and at least one field with a `default`, and `{"subject": "<subject>"}` validates against it.

`check` mode:

1. With `fail_check: true`, the result has `ok: false` and the exit code is 1. Otherwise `ok: true` and exit 0.
2. With an unknown field in the config document, the result has `ok: false` and its message names the field.

Run mode subscribes (core NATS) on `subject` and handles each message by its body:

| Body | Fixture action |
|---|---|
| `poison` | Park with reason `poison: <Key header>`, or `poison: unknown` when the message has no `Key` header. |
| `poison-creds` | Park with reason `connect mongodb://dbuser:secretpw@db.internal:27017: connection refused`, unredacted. The suite asserts the harness redacts it. |
| `flush-then-term` | Publish `flushed` to `<subject>.flushed`, then send SIGTERM to its own process. |
| `route:<target>` | Run the routing guard (`spec/transformer.md`) over the message `{subject: <target>, headers: the probe's headers, payload: routed}` with `<subject>.out.>` as the only data-subject pattern. Allowed: publish that message to `<target>`. Refused: publish an empty-payload notice to `<subject>.error` whose `Transform-Error` header is `unpublishable-subject` (subject over the length limit, not a literal, in a reserved namespace, carrying a reserved notice suffix, or not under `<subject>.out.>`), `reserved-header` (a header name starting `Nats-` in any letter case) or `transform` (anything else), and add 1 to the status counter `transform_errored`. |
| anything else | Add 1 to the status counter `received`, and publish the running message count, in decimal, to `<subject>.count`. |

Admin hooks:

1. Resume adds 1 to the status counter `resumed`. The suite does not assert this counter today.
2. Skip with key K adds 1 to the status counter `skipped:K`.
3. While parked, the fixture holds no subscription on `subject` (lifecycle.md: a parked connector consumes no messages). The suite checks this after every park, before it sends resume or skip.
4. After resume or skip the fixture is running again and has subscribed again.

The fixture has no code of its own for the rest: the SDK harness publishes status, serves the admin endpoints, redacts reasons, parks with `unresolved-secret` when a config value still starts with `secret://`, and drains on SIGTERM. Those are what the suite holds each harness to.

## What belongs here

What is true for *every* connector: the contract surfaces in `../spec/`. Connector-specific behavior stays in each connector's own tests and conformance suites.
