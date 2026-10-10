# mat adapter contract

Baton's core knows nothing about mat. The mat-specific mapping lives in
`src/mat/`, behind the optional `mat` Cargo feature. Core modules never import
it, and a build without the feature has no `baton mat` command. This document is
the contract between the two sides, so mat and baton can change independently.

```
cargo build --release --features mat
```

## Generic core primitives the adapter uses

The adapter adds no delivery logic of its own. It maps mat state onto these core
primitives, which are also available to any non-mat caller through `baton send`:

| Primitive | Core surface | Contract |
|---|---|---|
| Session manifest | `baton send --session <manifest> --to <name>` | `baton.session/v1`: `{"schema", "mailbox_root", "participants": [...]}`. Recipient `<name>` resolves to `<mailbox_root>/<name>/inbox`. See [Mailbox § Session manifest](mailbox.md#session-manifest-batonsessionv1). |
| Liveness gate | `--require-live` | A one-shot `serve.lock` probe on the resolved inbox immediately before enqueue. No live serve means a non-zero exit with nothing written. |
| Structured origin | `--origin <value>` | Optional opaque `origin` on the `baton.message/v1` envelope. The body is unchanged. Agents receive it as `BATON_ORIGIN` (body mode) or on each envelope (`batch-json`). See [Protocol](protocol.md#a2a-message-envelope-batonmessagev1). |
| Send result | stdout | The message id, printed only after the atomic enqueue succeeds. |

## Entry point: `baton mat send`

```
baton mat send --state <baton.state> --to <role> --from <role> (--body <text> | --body-file <path>)
```

On success, stdout is exactly one line: the message id. Any failure exits
non-zero and prints a diagnostic on stderr. Steps, in order:

1. **Read state.** Read the duo `baton.state` (see below).
2. **Resolve the recipient.** Build an in-memory session manifest over
   `mailbox_root` with participants `dev` and `reviewer`, then resolve `--to`.
3. **Build the envelope.** Kind `request`, `from`/`to` set to the canonical
   roles, and `origin` mapped from `--from` (see below). The body is taken
   verbatim; any `[relay]` framing is the caller's.
4. **Deliver.** Send through the core with `require_live`. If no live serve is
   found, nothing is enqueued, nothing is recorded, and nothing is printed.
5. **Record `queued`.** If `queue_root` is set, write the `queued` transition
   from the returned message id. The mailbox is not rescanned.
6. **Print the message id**, exactly once.

If step 5 fails after the envelope has been enqueued, the command exits
non-zero with a diagnostic naming the enqueued message id. The id is not
printed on stdout.

### State keys read

`baton.state` is flat `key=value` lines; the first line for a key wins.

| Key | Required | Use |
|---|---|---|
| `session_name` | yes | Stamped on the transition record; names the recovery ledger. |
| `mailbox_root` | yes | Role `<r>` is served at `<mailbox_root>/<r>/inbox`. |
| `queue_root` | no | Transition records go under `<queue_root>/transitions/`. If it is empty or absent, the ledger is skipped (mat's queue-disabled case). |

Paths must be native paths that baton can open; on Windows that means
`C:/…` paths, not MSYS `/c/…` paths.

### Roles and origin

| Token | Canonical role | Valid as | Origin when sending |
|---|---|---|---|
| `dev`, `developer` | `dev` | `--to`, `--from` | `peer` |
| `reviewer`, `review` | `reviewer` | `--to`, `--from` | `peer` |
| `operator` | `operator` | `--from` | `operator` |
| `system` | `system` | `--from` | `system` |

A self-send (`--from` equal to `--to`) is refused.

### `queued` transition record

The record path is `<queue_root>/transitions/<safe(message_id)>.json`, where
`safe` replaces every character outside `[A-Za-z0-9._-]` with `_`. The record is
one compact JSON line followed by a newline:

```json
{"schema":"baton.duo.transition/v1","message_id":"…","session_name":"…","role":"reviewer","from":"dev","to":"reviewer","body_digest":"<sha256 hex of the envelope body>","state":"queued","recovery_epoch":1,"recovery_reason":"","transitions":[{"state":"queued","recovery_epoch":1,"timestamp_ns":1760000000000000000}]}
```

- `role` is the recipient role.
- `recovery_epoch` comes from the `recovery_epoch=` line of
  `<duo-recovery>/<safe(session_name)>.ledger`. `<duo-recovery>` is the
  grandparent of `queue_root`, which follows mat's layout
  `<duo-recovery>/<session>/queue`. A missing ledger, or a value that is not a
  positive integer, reads as `1`.
- The write is create-if-absent. A fully written temp file is hard-linked into
  place, so a reader never sees a partial record. If the worker has already
  written a record (for example, it claimed the message first), that record is
  left untouched. This matches mat's rule that `queued` over any existing state
  is a no-op, so no cross-process lock is needed.

### Left to mat

The owed-ledger update in `baton.state`, the `events.log` line, the
duplicate-send guard, and team/caucus sessions stay on the mat side.
