# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

The current blessed release is `v0.9.1`.

Baton's default build is **harness-only**: it ships the agent-to-agent
substrate — the `baton.message/v1` envelope, the file mailbox (`baton serve` /
`send` / `status`), the external-agent seam (`serve --agent-cmd`, where a
full-tooled agent CLI owns its own provider), the N-party `converse-ring`
driver, trail recording and replay (`baton log`), role identity
(`baton roles`), and the host-owned supervisor (`baton service` / `baton
task`). It carries **no provider client and needs no API key**: every reply is
produced by an external agent process of your choosing.

The in-process provider path — the Claude-compatible Messages client behind
`baton ask`, `baton session`, `baton exchange`, and provider-backed `baton
converse`/`serve` — is kept behind an opt-in `local` Cargo feature as a legacy
escape hatch and is scheduled for removal; use
[`SHUKE-LABS/leg`](https://github.com/SHUKE-LABS/leg) (a standalone CLI
carrying those verbs) or an external agent instead.

## Documentation

Everything past this page is reference. Start with the map, then read the one
page for what you are doing.

**Concepts**

- [docs/architecture.md](docs/architecture.md) — read this first: what Baton is,
  the **two participant paths** (external-agent wrapper vs. Baton-owned Messages
  client), the module layout, and the CLI-verb → A2A-model map.
- [docs/protocol.md](docs/protocol.md) — read this when you serialize against
  Baton: the `baton.message/v1` envelope, the `baton.exchange/v1` event schema and
  its nesting, the trail JSONL / replay / merge semantics, and the `baton log`
  verbs that read them.

**Using the CLI**

- [docs/configuration.md](docs/configuration.md) — read this when you configure a
  process: the environment-variable table, role homes (`roles/<name>/`), per-role
  session recording, and the provider transport those settings drive.
- [docs/conversations.md](docs/conversations.md) — read this when you drive a
  conversation: `ask`, `session` (and `--resume`), `exchange`, `converse`, and
  `converse-ring`.
- [docs/mailbox.md](docs/mailbox.md) — read this when agents talk asynchronously:
  `serve`, `send`, `status`, `mailbox prune`, the delivery/at-least-once contract,
  and the routing registry.
- [docs/external-agent.md](docs/external-agent.md) — read this when a mailbox seat
  should be a full-tooled agent CLI rather than one provider call
  (`serve --agent-cmd`).
- [docs/service.md](docs/service.md) — read this when a `serve` session must
  outlive the process that launched it: `baton service` ownership, control
  surface, lifecycle, and systemd/launchd setup.

**Project**

- [docs/versioning.md](docs/versioning.md) — read this before cutting or pinning a
  release: the automated release calculation and the no-retagging baseline.
- [docs/development.md](docs/development.md) — read this before opening a PR: the
  CI gates to run locally.

Run `baton --help` (or `baton -h`) for the current command synopsis. Run
`baton --version` (or `baton -V`) to print the installed crate version. These
global flags need no Baton configuration or provider credentials.

## Install

For a registry-native install, use the npm package. The root package contains
the command shim and selects the matching native package from npm's registry;
it never downloads a binary from GitHub at install time:

```bash
npm install --global @shukelabs/baton
baton --version
```

The supported npm platforms are Linux x64 and arm64, macOS x64 and arm64, and
Windows x64. Unsupported operating-system or CPU combinations fail with a
clear platform-not-supported message rather than selecting a wrong-architecture
binary.

A global install also places the native binary at `~/.local/bin/baton` (this
runs automatically as part of `npm install -g`; it is skipped for a
non-global install). Re-run it manually after an upgrade, or whenever the
lifecycle script didn't run (`--ignore-scripts`, or a package manager that
blocks install scripts by default, such as pnpm and yarn):

```bash
baton install               # installs to ~/.local/bin
baton install --prefix /usr/local/bin
```

Running the shim without a native binary in place prints a one-time hint to
re-run `baton install`. Supervised or service deployments (systemd, launchd,
or a process manager like `mat`) should point at the installed native binary
directly rather than the shim — it avoids the extra shim process and its
orphaned-child risk on shutdown.

The primary install path is a prebuilt, checksummed archive from the current
blessed release. Release assets use this constructible pattern:

```text
https://github.com/shukebeta/baton/releases/download/v<version>/baton-<version>-<target>.<archive>
```

Supported targets are `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`x86_64-apple-darwin`, `aarch64-apple-darwin`, and
`x86_64-pc-windows-msvc`. Set `<version>` to the release shown in the Status
section and choose `.tar.gz` for Unix targets or `.zip` for Windows. Each
archive contains only `baton` (or `baton.exe`) at its root.

For example, from a shell with `curl`, `sha256sum`, and the appropriate archive
tool available:

```bash
version="<version>"
target="<target>"
archive="baton-${version}-${target}.tar.gz" # use .zip for Windows
base_url="https://github.com/shukebeta/baton/releases/download/v${version}"

curl --fail --location --remote-name "${base_url}/${archive}"
curl --fail --location --remote-name "${base_url}/SHA256SUMS"
grep -F "  ${archive}" SHA256SUMS | sha256sum --check -
tar -xzf "${archive}" # use unzip for the Windows .zip
```

Put the extracted executable on your `PATH`. On macOS, use
`shasum -a 256` in place of `sha256sum` when checking the selected checksum.

If a Rust toolchain (≥ 1.89) is available, the from-source alternative is:

```bash
cargo install --git https://github.com/shukebeta/baton --tag v0.9.1 --locked
```

This puts `baton` on your PATH. The `--locked` flag is **required**: without it
`cargo install --git` ignores the tracked `Cargo.lock` and resolves fresh
dependency versions, losing the reproducibility the lockfile exists to
guarantee. `--tag <tag>` pins the build to a blessed commit;
`cargo install --git … --rev <sha> --locked` pins just as immutably if you prefer
a raw SHA — the tag is the human-memorable name and GitHub releases anchor over
it.

Consumers stay frozen by pinning a tag, and upgrade by re-pinning a newer tag
deliberately. Pinning is the churn-control mechanism. [`CHANGELOG.md`](CHANGELOG.md)
records what each tag bump includes — read it before re-pinning.

Re-pinning and reinstalling over an installed `baton` while `baton service run`
is still live is safe on Unix — see
[Upgrading the `baton` binary under a live service](docs/service.md#upgrading-the-baton-binary-under-a-live-service-unix)
in `docs/service.md`.

The automated release calculation and its no-retagging baseline are documented
in [docs/versioning.md](docs/versioning.md).

**Historical v0.1.0 baseline.** At the initial release, neither the Rust
library API nor the CLI flag surface was promised stable; the CLI was only the
*intended* integration surface, and pinning a tag was how a consumer insulated
itself from change. That baseline shipped no crates.io publish, no prebuilt or
cross-platform binaries (no homebrew / apt), and no supported library-
dependency recipe — baton compiled as lib+bin, but crate consumption was
unsupported at v0.1.0 because the module layout was intentionally thin and
would be reworked.

## Quickstart

To see the whole A2A loop end-to-end — with no API key, no network, and no
provider at all — run:

```bash
./scripts/quickstart.sh
```

Every participant is a tiny shell "agent" wired in over `--agent-cmd`, exactly
like a real external agent CLI (`claude -p`, `codex exec`, `leg exchange`, ...)
would be. The script drives both A2A surfaces:

1. **`baton converse-ring`** — a governed two-party conversation between two
   independent `serve --agent-cmd` peers (the mailbox/external-agent
   equivalent of `baton converse`), driven to the turn-cap.
2. **`baton serve --agent-cmd` + `baton send --await`** — an asynchronous
   mailbox round-trip: the script waits for `serve` to report readiness, then
   `serve` answers a request dropped into an inbox and `send` consumes the
   correlated reply.

The resulting JSONL trails are written under `target/quickstart/`
(`converse-trail.jsonl` and `serve-send-reply.jsonl`); the script prints each
path and exits 0. It needs only a Rust toolchain — nothing leaves your machine
and no credential is read.

### Swap in a real agent

The stub agents prove **plumbing and reproducibility**: that the commands wire
together and terminate deterministically. To make it a real demonstration,
point `--agent-cmd` at your own agent CLI and give each peer its own identity
(see [docs/external-agent.md](docs/external-agent.md)):

```bash
# In one shell: a long-lived responder backed by a real agent.
baton serve --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox \
  --agent-cmd claude \
  --agent-arg -p --agent-arg --dangerously-skip-permissions
# In another: post a request and read the correlated reply.
baton send --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox \
  --await --body "Ping over the mailbox."
```

The agent owns its own model, credentials, and MCP config; baton owns only the
mailbox plumbing, which is identical either way. For the provider-backed chat
verbs (`ask`, `session`, `exchange`), use
[`SHUKE-LABS/leg`](https://github.com/SHUKE-LABS/leg) or build baton from
source with `--features local` (legacy; scheduled for removal).
