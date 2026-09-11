#!/usr/bin/env bash
#
# baton quickstart — run the whole A2A loop end-to-end, reproducibly, with no
# API key, no external network, and no provider credential of any kind.
#
# This is the harness-only default build's quickstart: every participant is a
# tiny shell "agent" wired in over `--agent-cmd`, exactly like a real external
# agent CLI (Claude Code, Codex, `leg`, ...) would be. It drives both A2A
# surfaces:
#   1. `baton converse-ring` — a governed two-party conversation between two
#      independent `serve --agent-cmd` peers (the mailbox/external-agent
#      equivalent of `baton converse`; see docs/external-agent.md).
#   2. `baton serve --agent-cmd` + `baton send --await` — an async mailbox
#      round-trip.
#
# To drive a real provider-backed agent instead of these canned stubs, point
# `--agent-cmd` at your own agent CLI (see docs/external-agent.md) — the
# mailbox plumbing this script exercises is unchanged either way.
#
# Overrides (used by the CI test so it need not rebuild):
#   BATON_BIN        path to the baton binary       (default target/debug/baton)
#   QUICKSTART_OUT   durable dir for the trails     (default target/quickstart)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BATON_BIN="${BATON_BIN:-$TARGET_DIR/debug/baton}"
OUT_DIR="${QUICKSTART_OUT:-$TARGET_DIR/quickstart}"

# Build whatever is missing. A CI test pre-builds and overrides the path, so
# this is a no-op there; a developer running the script cold gets a build.
if [[ ! -x "$BATON_BIN" ]]; then
  cargo build --quiet
fi

mkdir -p "$OUT_DIR"
WORK="$(mktemp -d)"

declare -a SERVE_PIDS=()
cleanup() {
  for pid in "${SERVE_PIDS[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

unset ANTHROPIC_API_KEY ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN \
  CLAUDE_CODE_OAUTH_TOKEN BATON_MODEL BATON_TOKEN_BUDGET BATON_EVENT_LOG || true
export BATON_MAX_TURNS="3"

# Writes a one-line "agent" at `path`: it drains the request envelope baton
# feeds it on stdin, then prints `reply` as its answer.
write_agent_stub() {
  local path="$1" reply="$2"
  cat >"$path" <<SCRIPT
#!/bin/sh
set -eu
cat > /dev/null
printf '%s' '${reply}'
SCRIPT
  chmod +x "$path"
}

# Blocks until `serve`'s readiness line lands in `stdout_file`, or the process
# it belongs to (`pid`) exits first.
wait_serve_ready() {
  local stdout_file="$1" pid="$2"
  for _ in $(seq 1 50); do
    if grep -Fxq "baton serve: ready" "$stdout_file"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

# --- 1. converse-ring: two agents, one governed conversation ----------------
RING_DIR="$WORK/ring"
INTERVIEWER="$RING_DIR/interviewer"
CANDIDATE="$RING_DIR/candidate"
mkdir -p "$INTERVIEWER/inbox" "$INTERVIEWER/outbox" "$CANDIDATE/inbox" "$CANDIDATE/outbox"

INTERVIEWER_STUB="$RING_DIR/interviewer-agent"
CANDIDATE_STUB="$RING_DIR/candidate-agent"
write_agent_stub "$INTERVIEWER_STUB" "Nice to meet you — tell me about yourself."
write_agent_stub "$CANDIDATE_STUB" "I am the candidate, ready to answer."

REGISTRY="$RING_DIR/registry.json"
cat >"$REGISTRY" <<JSON
{
  "participants": {
    "interviewer": { "inbox": "$INTERVIEWER/inbox", "outbox": "$INTERVIEWER/outbox" },
    "candidate": { "inbox": "$CANDIDATE/inbox", "outbox": "$CANDIDATE/outbox" }
  }
}
JSON

INTERVIEWER_STDOUT="$RING_DIR/interviewer-serve.stdout"
"$BATON_BIN" serve --agent-cmd "$INTERVIEWER_STUB" \
  --inbox "$INTERVIEWER/inbox" --outbox "$INTERVIEWER/outbox" \
  >"$INTERVIEWER_STDOUT" 2>"$RING_DIR/interviewer-serve.stderr" &
SERVE_PIDS+=("$!")
if ! wait_serve_ready "$INTERVIEWER_STDOUT" "$!"; then
  echo "quickstart: interviewer serve did not become ready" >&2
  cat "$RING_DIR/interviewer-serve.stderr" >&2 || true
  exit 1
fi

CANDIDATE_STDOUT="$RING_DIR/candidate-serve.stdout"
"$BATON_BIN" serve --agent-cmd "$CANDIDATE_STUB" \
  --inbox "$CANDIDATE/inbox" --outbox "$CANDIDATE/outbox" \
  >"$CANDIDATE_STDOUT" 2>"$RING_DIR/candidate-serve.stderr" &
SERVE_PIDS+=("$!")
if ! wait_serve_ready "$CANDIDATE_STDOUT" "$!"; then
  echo "quickstart: candidate serve did not become ready" >&2
  cat "$RING_DIR/candidate-serve.stderr" >&2 || true
  exit 1
fi
echo "quickstart: interviewer + candidate serve ready"

CONVERSE_TRAIL="$OUT_DIR/converse-trail.jsonl"
"$BATON_BIN" converse-ring \
  --registry "$REGISTRY" \
  --roster "interviewer,candidate" \
  --seed "Introduce yourself in one sentence." \
  --await-ms 10000 \
  --out "$CONVERSE_TRAIL"
echo "quickstart: converse trail -> $CONVERSE_TRAIL"

"$BATON_BIN" serve --stop --inbox "$INTERVIEWER/inbox"
"$BATON_BIN" serve --stop --inbox "$CANDIDATE/inbox"
for pid in "${SERVE_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
SERVE_PIDS=()

# --- 2. serve + send: an async mailbox round-trip ---------------------------
INBOX="$WORK/mailbox/inbox"
OUTBOX="$WORK/mailbox/outbox"
mkdir -p "$INBOX" "$OUTBOX"

AGENT_STUB="$WORK/mailbox-agent"
write_agent_stub "$AGENT_STUB" "Pong from the mailbox agent."

SERVE_STDOUT="$WORK/serve.stdout"
SERVE_STDERR="$WORK/serve.stderr"
"$BATON_BIN" serve --agent-cmd "$AGENT_STUB" --inbox "$INBOX" --outbox "$OUTBOX" \
  >"$SERVE_STDOUT" 2>"$SERVE_STDERR" &
SERVE_PID=$!
SERVE_PIDS+=("$SERVE_PID")

# `baton serve` emits this exact, flushed line only after participant setup,
# mailbox lock acquisition, stale-stop handling, and stale-claim reclamation.
# Waiting for it makes the first send independent of process scheduling.
if ! wait_serve_ready "$SERVE_STDOUT" "$SERVE_PID"; then
  echo "quickstart: serve did not become ready" >&2
  if [[ -s "$SERVE_STDERR" ]]; then
    cat "$SERVE_STDERR" >&2
  fi
  exit 1
fi
echo "quickstart: serve ready"

REPLY_TRAIL="$OUT_DIR/serve-send-reply.jsonl"
# --await prints the correlated reply envelope (one JSON line) to stdout.
"$BATON_BIN" send --inbox "$INBOX" --outbox "$OUTBOX" --await \
  --body "Ping over the mailbox." >"$REPLY_TRAIL"
echo "quickstart: serve+send reply -> $REPLY_TRAIL"

# Cooperative graceful stop, then reap the daemon.
"$BATON_BIN" serve --stop --inbox "$INBOX"
wait "$SERVE_PID" 2>/dev/null || true
SERVE_PIDS=()

echo "quickstart: done"
