# Credentials — cross-connector contract

Language-neutral record of how a connector receives its NATS
connection and credential, the permission claims it needs to operate
under this contract, and what those claims mean for how it is built.

## Runtime identity env vars (D-3)

The `<namespace>`/`<instanceID>` tokens in the claim set below come from
three nexlet-injected environment variables: `NEX_WORKLOAD_CONNECTOR_ID`
(durable connector id) and `NEX_WORKLOAD_NAMESPACE` (namespace; defaults to
`"default"` when unset — an empty namespace would make the status/admin
subjects below malformed) are described in `status.md`'s D-3 section;
`NEX_WORKLOAD_ID` is the third — the workload instance id, and the fallback
identity source when `NEX_WORKLOAD_CONNECTOR_ID` is absent. See `status.md`
(D-3) for the full instanceID resolution order.

## NATS connection

In run mode (`contract.md`, D-11), the platform hands a connector its NATS
endpoint and credential in three environment variables:

| Variable | Value |
|---|---|
| `NEX_WORKLOAD_NATS_SERVERS` | Required. One or more server URLs, comma-separated |
| `NEX_WORKLOAD_NATS_B64_JWT` | The user JWT, base64-encoded (standard alphabet, padded); decode it before use |
| `NEX_WORKLOAD_NATS_NKEY` | The user's nkey seed, despite the name |

The JWT and the seed come together or not at all. With neither, the
deployment is unauthenticated. One without the other is a
misconfiguration, and a connector must not fall back to connecting
anonymously. A connector uses the endpoint and credential it is given and
never hardcodes either. `sdk-go/runtime` connects this way for every
connector.

## Requested claim set (D-10)

The permission set a connector's minted NATS credential should carry,
for the platform that mints it to grant:

| Direction | Subjects |
|---|---|
| pub | The data subjects a source publishes to (connector-specific), and the notice subjects it is configured to publish (`<subject>.oversize`, `<subject>.error`; `transformer.md`) |
| sub | The data subjects a sink subscribes to over core NATS (connector-specific; a sink that reads a durable needs the JS API rows below instead) |
| pub | `$CONNECTOR.<namespace>.status.<instanceID>` (`status.md`, D-5) |
| sub | `$CONNECTOR.<namespace>.admin.<instanceID>.>` (`admin.md`, D-8) |
| sub | `$SRV.PING`, `$SRV.INFO` and `$SRV.STATS`, each also with `.connector-admin` and `.connector-admin.*` appended (NATS micro discovery of the admin service; the last token is a per-start service id) |
| resp | Permission to reply to the requests it receives (`resp` in a user JWT, `allow_responses` in server config). The admin service answers through it; without it, every admin request times out |
| sub | `_INBOX.>` (request-reply replies) |
| JS API | The connector's checkpoint bucket, `<bucket>` (`CONN-CKPT-<instanceID>` with the id sanitized, or an override; `checkpoint.md`, D-9): `$JS.API.STREAM.INFO.KV_<bucket>` and `$JS.API.DIRECT.GET.KV_<bucket>.>`; plus, if it may create the bucket, `$JS.API.STREAM.CREATE.KV_<bucket>` and `$JS.API.INFO`, since nats.go reads the account's JetStream info before it creates a bucket |
| pub | `$KV.<bucket>.>` (writes to the checkpoint bucket) |
| JS API | Each stream it publishes into or reads from: `$JS.API.STREAM.INFO.<stream>`; plus `$JS.API.STREAM.CREATE.<stream>` if it may create the stream, and `$JS.API.STREAM.UPDATE.<stream>` if it may change an existing one (see below for what these allow) |
| JS API | Each durable it reads from: `$JS.API.CONSUMER.INFO.<stream>.<durable>` and `$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>`; plus, if it may create the durable, `$JS.API.CONSUMER.CREATE.<stream>.<durable>.<filter>` for a durable with one filter, or `$JS.API.CONSUMER.CREATE.<stream>.<durable>` for one with none or several |
| pub | `$JS.ACK.<stream>.<durable>.>` (acks for each durable it reads from; see below for the v2 ack format) |

**What the create and update grants allow.** NATS authorizes a
JetStream API request by its subject, not its body, so a create or
update grant on a stream, durable or bucket lets the holder give that
resource any configuration, which reaches well beyond the resource
itself. Grant them only where the connector has to create or change
the resource; otherwise use the least-privilege profile below.

The least-privilege profile is an operator who creates the stream, the
durable and the checkpoint bucket ahead of time. The connector then
needs only `STREAM.INFO`, `CONSUMER.INFO`, `CONSUMER.MSG.NEXT`, the
bucket's `DIRECT.GET`, and its ack and `$KV` subjects, besides its
data, status, admin and discovery subjects and the response
permission. The operator creates the bucket with
direct gets allowed, as `nats kv add` and nats.go do; without them,
the connector reads through `$JS.API.STREAM.MSG.GET.KV_<bucket>`
instead of `DIRECT.GET`.

**Ack subjects.** The ack row matches the v1 ack format. nats-server
2.14 has a `js_ack_fc_v2` feature flag, off by default and planned to
become the default, that puts the domain and an account hash first:
`$JS.ACK.<domain>.<account hash>.<stream>.<durable>.>`. Where it may
be on, grant `$JS.ACK.*.*.<stream>.<durable>.>` too. A denied ack is
dropped while the client's ack call still reports success, so every
message is redelivered, without limit under the default MaxDeliver.

**JetStream domains.** A connector that uses a JetStream domain sends
its API requests to `$JS.<domain>.API.…`, and its checkpoint writes to
`$JS.<domain>.API.$KV.<bucket>.…`. A server in that domain maps both
back to `$JS.API.…` and `$KV.…` before it checks permissions, so for a
connector connected to one, the rows above apply as written. Where the
domain is reached through another server, as across a leaf node,
replace `$JS.API.` in the rows above with that prefix, and grant
`$JS.<domain>.API.$KV.<bucket>.>` in place of `$KV.<bucket>.>`.

## What this means for a connector

- **Stay inside the claim set.** A connector touches only the subjects,
  streams, durables and bucket listed above, so it keeps working when
  its credential is scoped to exactly that.
- **Work with resources an operator created.** Under the least-privilege
  profile a connector can't create or change its stream, durable or
  bucket, so it validates what is there instead. `sdk-go/jsresource`
  creates only what is missing and validates what exists; it changes
  an existing stream only when the connector opts into `UpdateAdditive`
  (appending missing subjects and, when the connector asks for atomic
  publish, enabling it), which needs the `STREAM.UPDATE` grant.
- **Don't check the credential.** A connector can't read its own
  permission set, so it doesn't test it or refuse to start over it.
- **Tolerate a second instance.** Two instances of a connector can
  overlap. The checkpoint guard (`checkpoint.md`) detects that and
  parks. Make writes to an external system safe to repeat.
