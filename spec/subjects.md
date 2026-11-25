# Subjects — cross-connector contract

Language-neutral rule for the values a connector builds NATS subjects and
JetStream names from: its runtime identity (`status.md`, D-3) and its own
configuration, such as a collection or topic name. Each such value must be
exactly **one literal subject token**. The Go SDK implements this rule in
`sdk-go/subject`.

## The token rule

A value is a valid token when it is:

- non-empty;
- valid UTF-8, without U+FFFD (the replacement character);
- free of `.` (the token separator), space, tab, the wildcards `*` and `>`,
  and ASCII control characters (U+0000 to U+001F, which include CR and LF,
  and U+007F).

Every other character is allowed, including non-ASCII letters, non-ASCII
spaces, and C1 control characters (U+0080 to U+009F).

A value that breaks the rule is **rejected, never rewritten**. Rewriting
`.` to `_` would silently move the connector onto subjects another
connector may own. An identity value that breaks it is a fail-stop at
startup (`lifecycle.md`, D-15). A configuration value that breaks it fails
`check`.

## Why this rule

The server accepts almost any byte in a subject: it rejects only empty
tokens, whitespace, and a `>` that is not the last token. The rule rejects
values the server accepts but that change where messages go, or that the
server stores badly:

| Value | The server | Why it is rejected |
|---|---|---|
| `a.b` | accepts: two tokens | The connector lands on another subject. A connector with id `a` subscribes to `$CONNECTOR.<namespace>.admin.a.>`, which also receives the admin requests meant for `a.b`. |
| `*`, `>` | accepts: a wildcard | A subscription built from it matches every sibling connector. |
| NUL, invalid UTF-8, U+FFFD | accepts | A file-backed stream that stores such a subject rebuilds its index on every restart (nats-server 2.14.3). The server's check cannot tell U+FFFD from invalid bytes. |
| CR, LF, tab, space | rejects (as do the clients) | They break the client protocol line. |
| other ASCII controls, `a*b`, `a>b` | accepts | No name needs them, and `a*b` reads as a wildcard. |

The rule deliberately accepts what the server accepts and breaks nothing,
such as non-ASCII spaces and C1 control characters: an implementation must
not reject them.

## Test vectors

| Value | Valid token |
|---|---|
| `conn-1`, `a_b-c`, `aéb` | yes |
| `a` U+00A0 `b` (no-break space), `a` U+0085 `b` (NEL), `a` U+0080 `b` | yes |
| empty, `.`, `a.b`, `a b` | no |
| `a` + tab, LF, CR, VT, NUL, ESC or DEL + `b` | no |
| `*`, `>`, `a*b`, `a>b` | no |
| `a` 0xFF `b` (invalid UTF-8) | no |
| `a` U+FFFD `b` (the replacement character) | no |

## Subjects and names

- A **literal subject** is one or more `.`-separated valid tokens.
- A **pattern** is one or more `.`-separated tokens, each a valid token,
  `*`, or `>` as the last token only. Every literal subject is a pattern.
- A **stream or consumer name** is a valid token of at most 255 bytes that
  contains neither `/` nor `\`. It is a token of every JetStream API
  subject that addresses the stream or consumer, the server stores it as a
  directory name, and the server rejects longer names.

## Reserved namespaces

A subject whose first token starts with `$` or is `_INBOX` belongs to the
server or the platform, never to data: `$JS` is the JetStream API and its
advisories, `$SYS` the system account's events and requests, `$KV` and `$O`
the key-value and object store internals, `$CONNECTOR` this contract's
status and admin subjects (`status.md`, `admin.md`), and `_INBOX` the
request inboxes clients listen on. A publish there is a command, not data:
`$JS.API.STREAM.PURGE.<stream>` purges a stream, and `_INBOX.<id>` answers
someone else's request.

A connector never publishes data to a reserved subject, never declares one
as a data subject, and a transform may not route a message to one: an
output subject in a reserved namespace fails that message
(`transformer.md`, "Routing guard"). Only the first token is looked at, so
the rule applies to patterns as it does to literals, and `events.$JS` or
`events._INBOX` is not reserved. `_INBOX` is the default inbox prefix; a
client with a custom prefix is not recognised here, and its inboxes are
kept out of reach by the guard's other rule, that an output subject must
be one of the connector's data subjects. The Go SDK implements the rule in
`subject.Reserved`.

| Subject | Reserved |
|---|---|
| `$JS.API.STREAM.PURGE.ORDERS`, `$SYS.REQ.SERVER.PING`, `$KV.x`, `$JS.>` | yes |
| `_INBOX.abc`, `_INBOX` | yes |
| `_INBOXX.a`, `_inbox.a`, `events._INBOX`, `events.$JS.x` | no |

## Subject length

A subject a connector publishes to is at most **3072 bytes** long. A
subject the connector didn't choose, such as one a transform mapping
builds from message data, must be checked against this limit before it is
published; a longer one fails that message.

The server limits the arguments of a client's `PUB` or `HPUB` line (the
subject, the reply subject, and the header and total sizes, with three
spaces between them) to `max_control_line` bytes, 4096 by default. It
answers a longer line with `-ERR 'maximum control line exceeded'` and
closes the connection, which fails every publish in flight on it, and
nats.go doesn't reconnect after that error. nats.go sends a long subject
without checking its length.

A connector can't know the real limit: `max_control_line` is configured
on the server, and the reply subject's length depends on the client's
inbox prefix. The limit leaves 4096 - 3072 = 1024 bytes for the rest of
the line. Each size takes at most 10 digits, since `max_payload` is a
32-bit integer, which leaves 1001 bytes for the reply. A nats.go
JetStream publish's reply is the inbox prefix plus 32 bytes (38 bytes with
the default prefix `_INBOX`), so the limit holds for a custom inbox
prefix of up to 969 bytes.

The limit doesn't cover a server whose `max_control_line` is below 3133
bytes (the line a 3072-byte subject makes with the default inbox prefix
and 10-digit sizes), or an inbox prefix longer than 969 bytes: either can
still close the connection over a subject within the limit.

| Subject length | Publishable |
|---|---|
| 3072 bytes | yes |
| 3073 bytes | no |
