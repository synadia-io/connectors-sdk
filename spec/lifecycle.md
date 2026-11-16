# Lifecycle — cross-connector contract

Language-neutral contract for the two ways a connector's main loop
stops running: park (an internal, recoverable halt) and drain (an
external, terminal shutdown). Related:
`status.md` (D-6 state model), `admin.md` (`resume`/`skip`
endpoints), `config.md` (a concrete park trigger, D-4's
`unresolved-secret`).

## Park (D-15)

Park is **in-process, not an exit**. On a poison input or persistent
downstream failure, a connector:

1. Sets its status to `parked` with a machine-readable `reason`
   (`status.schema.json`'s `reason` field; see
   `examples/status.parked.json`, which uses the reason
   `unresolved-secret` — the exact fatal-misconfiguration case
   `config.md`'s "Secret references" section mandates parking for).
2. Halts its main loop — no more messages consumed or produced.
3. **Stays alive**, continuing to serve its status subject (`status.md`)
   and admin surface (`admin.md`) — a parked connector is not a crashed
   one; the whole point is that an operator can inspect and act on it.
4. Waits for an operator to call `resume` (re-enter the loop) or
   `skip` (discard the specific poison item, then resume) on the
   admin surface. `skip` is only valid while parked (`admin.md`).

**Fail-stop is reserved for one narrow window**: unrecoverable errors
that happen *before the admin surface exists* — bad flags, an
unparseable config document. At that point there is no admin surface
for an operator to reach, so there is nothing park's "stay alive and
answer requests" property would buy; exiting non-zero and letting the
supervisor's restart policy (and its backoff) take over is the only
option. Once the admin surface is up, every subsequent failure that
would otherwise crash the process parks instead — a crash loop under
an auto-restarting supervisor is explicitly rejected as a failure
mode this contract exists to prevent.

This window is the only producer of the `failed` state (`status.md`,
D-6). A fail-stop late enough in startup to already hold a status
publisher — the NATS connection is up but the admin service failed to
register, say — publishes `failed` on its way out, with reason
`startup-error: ` followed by the error's message, as a courtesy to
whoever is watching the subject; one earlier than that publishes
nothing, because there is nothing to publish with. Neither case is
recoverable in-process.

### Errors that are not parks

The rule above is unconditional, and it covers the failures a connector
never labelled: **every** error that ends the main loop after the admin
surface is up parks, not only the ones the connector deliberately
raised as a park. A connector that returns a bare error — or panics —
where it meant to park still gets the park, because the alternative is
the crash loop. The two paths differ only in the `reason` string: a
deliberate park names its own machine-readable reason, while an
unlabelled error is reported as `runtime-error: ` followed by the
error's message — except an error wrapping the SDK's checkpoint
conflict (`checkpoint.md`), which gets that contract's fixed reason
instead — so an operator can tell "this connector halted on a
condition it recognizes" from "this connector fell over". `resume`
re-enters the loop identically in both cases.

Exiting non-zero for a post-startup failure is therefore a contract
violation, not a stylistic choice: under a supervisor with an
on-failure restart policy it produces exactly the restart loop D-15
rejects, and it destroys the admin surface an operator would have used
to diagnose the failure.

### Reason strings the harness itself produces

A connector's own park reasons are its own vocabulary. These five come
from the shared harness rather than from connector code, so a client
can rely on them across every connector:

| Reason | State | Meaning |
|---|---|---|
| `unresolved-secret` | `parked` | A config value still carries a `secret://` prefix (`config.md`, D-4). |
| `config-invalid` | `parked` | The config document failed to load for any other reason, at a point where the admin surface already exists — reachable when a `resume` re-reads a document that has since become unparseable. (The same failure *before* the admin surface exists is a fail-stop, above.) |
| `checkpoint-conflict` | `parked` | The Run hook returned an error wrapping the SDK's checkpoint conflict (`checkpoint.md`); an explicit `Park` reason on the same error takes precedence over this one. |
| `runtime-error: <message>` | `parked` | The main loop ended with an error the connector did not label as a park. |
| `startup-error: <message>` | `failed` | A fail-stop late enough in startup to have a status publisher. |

A `resume` out of a config park re-reads the document rather than
re-entering the loop with config that never applied; if it is still
broken, the connector parks again with whichever of the two config
reasons now applies.

**In the Go SDK**, the harness (`sdk-go/runtime`) implements this for
every connector wired through `runtime.Main`. A Run hook parks by
returning `runtime.Park(reason)`, any other error parks with
`runtime-error`, and the admin surface (`sdk-go/admin`) serves
`resume` and `skip` (`admin.md`), so a connector implements neither
itself.

## Drain (D-16)

On `SIGTERM`, a connector: stops accepting new work, flushes or acks
whatever is in flight, closes its NATS connection, and exits `0` —
**within 10 seconds** of receiving the signal.

A draining connector publishes **no final status document**. There is
no terminal state in the D-6 model to publish (`status.md`), and a
synthetic last document — `paused`, say — would sit in the retained
ops stream describing a process that no longer exists. The last
document on the subject is the true one the connector was in when it
stopped: `running` for a healthy drain, `parked` with its reason for a
connector that was parked when the signal arrived. **A consumer must
not treat the last retained status document as live state**: a status
document describes the connector at publish time, and only the
workload's own lifecycle (the launcher's view of whether the process is
running) says whether that description is still current. A park
requested after the shutdown signal has arrived is not begun — it
publishes nothing — consistent with "no final status document" above.

**Checking it.** A conformance check sends `SIGTERM` and asserts that
the process exits `0` within the 10 seconds themselves, not within a
looser outer window: a connector that takes 12 seconds to drain fails.

**In the Go SDK**, the harness owns the NATS connection of every
connector wired through `runtime.Main`, so the bound is enforced in one
place. It dials with `nats.DrainTimeout` set to the 10-second budget
(`sdk-go/runtime/nats.go`), and splits that budget, counted from the
signal, between the Run hook's unwind and the `nc.Drain()` that follows
(`sdk-go/runtime/runtime.go`): a slow unwind eats into the same 10
seconds instead of getting a fresh budget. A connector on the harness
doesn't call `nc.Drain()` itself, and isn't subject to nats.go's
30-second default drain timeout (`nats.DefaultDrainTimeout`).

## What park and drain don't share

Park and drain are independent axes, not two ends of one spectrum: a
parked connector is not draining (it keeps serving), and a draining
connector is not necessarily parked (a healthy `running` connector
drains on SIGTERM just the same as a `parked` one — SIGTERM's meaning
doesn't depend on current state). A connector can, in principle,
receive SIGTERM while parked; the drain contract above still applies
unchanged — stop intake (already stopped), flush/ack in-flight
(nothing in flight, so this is a no-op), close NATS, exit 0.
