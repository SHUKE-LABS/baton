//! The participant seam: an envelope-in / envelope-out boundary.
//!
//! [`Participant`] is the A2A analog of [`crate::transport::Transport`]. Where a
//! `Transport` hides *which provider* answers a call, a `Participant` hides
//! *which participant* answers a `baton.message/v1` envelope — in-process here,
//! subprocess (M3b) or mailbox (M4) later. The boundary is envelope-only: a
//! participant holds no state shared with any other, so the M3c driver can hold
//! one abstractly and reach it the same way regardless of how it is realised.
//!
//! [`LocalParticipant`] is the first implementation: an in-process, LLM-backed
//! participant that is a system prompt + a [`Transport`]. It carries the same
//! request-envelope → response-envelope transformation the `baton exchange`
//! verb performs, so the two share one source of truth (the verb delegates
//! here); the CLI layers the `BATON_EVENT_LOG` side trail on top.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::error::{BatonError, Result};
#[cfg(feature = "local")]
use crate::events::ExchangeMeta;
use crate::events::now_ms;
#[cfg(feature = "local")]
use crate::log::{Exchange, Outcome, RequestRecord};
use crate::mailbox;
#[cfg(feature = "local")]
use crate::message::WrappedExchange;
use crate::message::{MessageEnvelope, MessageKind};
#[cfg(feature = "local")]
use crate::model::Prompt;
#[cfg(feature = "local")]
use crate::transport::Transport;

/// Answers a `baton.message/v1` request envelope with a response envelope.
///
/// Infallible by contract: a provider (or delivery) failure is a *delivered*
/// `kind: "error"` response, never a propagated `Err` — matching the
/// `baton exchange` delivered-error contract. Implementations share no mutable
/// state with one another; the envelope is the entire boundary.
pub trait Participant {
    /// Consumes a `request` envelope and returns the correlated response.
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope;

    /// Answers a batch of one-or-more claimed requests, returning exactly one
    /// response per request in the same order.
    ///
    /// The default forwards each request through [`respond`](Self::respond)
    /// independently — today's per-message behavior, unchanged for every
    /// participant. [`ExternalAgentParticipant`] overrides this to run a
    /// single child invocation for the whole batch and fan its one reply out
    /// to every member (`baton serve --agent-batch-max`).
    fn respond_batch(&self, requests: &[MessageEnvelope]) -> Vec<MessageEnvelope> {
        requests.iter().map(|request| self.respond(request)).collect()
    }
}

/// An in-process, LLM-backed participant: a system prompt + a [`Transport`].
///
/// The system prompt already lives in the transport's config (applied by the
/// Claude client), so a participant reply is exactly one provider exchange. The
/// response envelope preserves `conversation_id`, links `in_reply_to` to the
/// request, swaps addressing (`from`/`to`), and nests the `baton.exchange/v1`
/// record for the call it ran so the call — and its token usage — is observable
/// in-band. [`ExchangeMeta`] supplies the `model`/`base_url` stamped on that
/// nested record.
#[cfg(feature = "local")]
pub struct LocalParticipant<T: Transport> {
    transport: T,
    meta: ExchangeMeta,
}

#[cfg(feature = "local")]
impl<T: Transport> LocalParticipant<T> {
    /// Builds a participant over `transport`, stamping `meta` (`model` /
    /// `base_url`) onto the nested `baton.exchange/v1` record of each reply.
    pub fn new(transport: T, meta: ExchangeMeta) -> Self {
        Self { transport, meta }
    }
}

#[cfg(feature = "local")]
impl<T: Transport> Participant for LocalParticipant<T> {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        let request_ts = now_ms();
        let start = Instant::now();
        let result = self.transport.send(&Prompt::new(request.body.as_str()));
        let duration_ms = start.elapsed().as_millis() as u64;
        let outcome_ts = now_ms();

        let request_record = RequestRecord {
            ts_ms: request_ts,
            model: self.meta.model.clone(),
            base_url: self.meta.base_url.clone(),
            prompt: request.body.clone(),
            session_id: None,
            turn_index: None,
        };

        let (kind, body, outcome) = match result {
            Ok(reply) => {
                let outcome = Outcome::Ok {
                    ts_ms: outcome_ts,
                    duration_ms,
                    reply: reply.text.clone(),
                    input_tokens: reply.usage.input_tokens,
                    output_tokens: reply.usage.output_tokens,
                    stop_reason: reply.stop_reason.clone(),
                };
                (MessageKind::Response, reply.text, outcome)
            }
            Err(err) => {
                let outcome = Outcome::Error {
                    ts_ms: outcome_ts,
                    duration_ms,
                    kind: err.kind().to_string(),
                    message: err.to_string(),
                };
                (MessageKind::Error, err.to_string(), outcome)
            }
        };

        // Addressing swaps: the reply is from the request's recipient, to its
        // sender.
        let mut response = MessageEnvelope::new(
            fresh_message_id(&request.conversation_id, outcome_ts),
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            body,
            outcome_ts,
        );
        response.in_reply_to = Some(request.message_id.clone());
        response.exchange = Some(WrappedExchange::new(Exchange {
            request: request_record,
            outcome,
        }));
        response
    }
}

/// A subprocess-backed participant: each reply is one `baton exchange` child.
///
/// Where [`LocalParticipant`] answers in-process, this impl reaches a *separate
/// OS process* — the honest "two independent agents, no shared state" boundary.
/// One [`respond`](Participant::respond) call spawns the program, writes the
/// request envelope to its stdin, reads one response envelope from its stdout,
/// and reaps it. The child is configured through its own environment (its own
/// `BATON_MODEL` / `BATON_SYSTEM_PROMPT` / credential vars), so it is a
/// genuinely independent Baton agent driven over the same envelope boundary.
///
/// The trait stays infallible. The delivered-error boundary (aligned with the
/// `baton exchange` verb) lives entirely in envelope terms:
///
/// - A child that **exits 0 with a well-formed envelope** is returned
///   *unchanged* — including a provider-failure `kind: "error"` envelope with
///   its nested `baton.exchange/v1` record, since that is exactly what the verb
///   emits on a delivered provider error.
/// - A child that **exits non-zero**, emits a **malformed or absent** envelope,
///   or **exceeds [`read_timeout`](Self::read_timeout)** is reconciled into a
///   *synthesized* delivered `kind: "error"` envelope with **no** nested record
///   — the parent observed no provider call it can vouch for (mirroring how
///   [`testing::ScriptedParticipant`] nests nothing when it ran no call).
#[cfg(feature = "local")]
pub struct SubprocessParticipant {
    program: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, String)>,
    read_timeout: Duration,
}

#[cfg(feature = "local")]
impl SubprocessParticipant {
    /// Builds a participant that spawns `program` with `args`, layering `envs`
    /// over the inherited environment, and waits at most `read_timeout` for the
    /// child's response envelope.
    ///
    /// `envs` are applied *on top of* the parent environment, so credentials
    /// flow through while `BATON_MODEL` / `BATON_SYSTEM_PROMPT` can differ — the
    /// layering that makes the child an independent agent rather than a clone.
    /// `read_timeout` must sit *above* the child's own `BATON_TIMEOUT_SECS` (the
    /// child's provider deadline); a shorter parent deadline would kill a
    /// slow-but-alive child and discard a real delivered error.
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        read_timeout: Duration,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            envs: envs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            read_timeout,
        }
    }

    /// Builds a participant that spawns *this* `baton` binary
    /// ([`std::env::current_exe`]) with the `exchange` verb — the production
    /// wiring. `envs` / `read_timeout` are as in [`new`](Self::new).
    pub fn for_current_exe(
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        read_timeout: Duration,
    ) -> Result<Self> {
        let program = std::env::current_exe().map_err(|err| {
            BatonError::Io(format!("could not resolve the current executable: {err}"))
        })?;
        Ok(Self::new(program, ["exchange"], envs, read_timeout))
    }

    /// Runs one child exchange, returning the parsed response envelope or an
    /// `Err` describing the machinery failure (non-zero exit, malformed/absent
    /// envelope, or read timeout). The infallible [`Participant::respond`]
    /// reconciles that `Err` into a delivered error envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<MessageEnvelope> {
        let payload = serde_json::to_string(request).map_err(|err| {
            BatonError::Io(format!("could not serialize request envelope: {err}"))
        })?;

        let (stdout, _stderr) = capture_child_output(
            &self.program,
            &self.args,
            &self.envs,
            &[],
            None,
            payload.as_bytes(),
            Some(self.read_timeout),
        )?;

        if stdout.trim().is_empty() {
            return Err(BatonError::Decode(
                "child participant produced no response envelope".to_string(),
            ));
        }
        serde_json::from_str(&stdout).map_err(|err| {
            BatonError::Decode(format!(
                "child participant produced a malformed response envelope: {err}"
            ))
        })
    }
}

#[cfg(feature = "local")]
impl Participant for SubprocessParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(response) => response,
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }
}

/// A mailbox-backed participant: each reply is one round-trip over a file-mailbox.
///
/// Where [`SubprocessParticipant`] reaches an independent agent over pipes, this
/// impl reaches one over the *file-mailbox* (M4): a peer `baton serve` daemon.
/// One [`respond`](Participant::respond) call delivers the request into the
/// peer's inbox via the lock-free atomic path ([`mailbox::deliver_to`]) and then
/// polls the outbox for the correlated reply ([`mailbox::try_claim_response`],
/// keyed by the request id) until it appears or [`await_timeout`](Self::await_timeout)
/// elapses. It holds no lock — the peer daemon owns the single-instance lock —
/// so the driver is a *governed client* of a `serve` service, not a co-owner of
/// its mailbox.
///
/// The trait stays infallible; the delivered-error boundary is the same one
/// [`SubprocessParticipant`] draws, in envelope terms:
///
/// - A **peer-delivered reply** (whatever the outbox holds, correlated to the
///   request) is returned *unchanged* — including a peer `kind: "error"` whose
///   nested `baton.exchange/v1` record carries the peer's provider-call outcome,
///   since that is a delivered response the peer vouches for.
/// - A **machinery/transport failure** — delivery failed, no reply arrived
///   before the deadline, or the reply did not correlate — is reconciled into a
///   *synthesized* delivered `kind: "error"` envelope with **no** nested record
///   ([`synthesize_error_response`]): the driver obtained no peer provider-call
///   it can vouch for, mirroring how [`SubprocessParticipant`] synthesizes a
///   machinery failure.
///
/// This is what lets the `converse` trail distinguish "the peer answered with an
/// error" from "the driver stopped waiting": both are `kind: "error"`, but only
/// the former nests a `baton.exchange/v1` record. That predicate rests on the
/// peer nesting a record on every delivered reply — which holds for a `baton
/// serve` peer, whose in-process [`LocalParticipant`] always nests one. A future
/// peer that could deliver a recordless error would blur the two; the synthesized
/// timeout body naming the await-timeout is the tie-breaker for that case.
pub struct MailboxParticipant {
    /// Root of the peer's mailbox; the request is delivered to `<inbox>/pending/`.
    inbox: PathBuf,
    /// Directory the correlated reply is awaited from (the peer's outbox).
    outbox: PathBuf,
    /// Maximum time to await the correlated reply before synthesizing a timeout.
    await_timeout: Duration,
    /// Interval between outbox polls while awaiting the reply.
    poll_interval: Duration,
}

impl MailboxParticipant {
    /// Builds a participant that delivers requests to `<inbox>/pending/` and
    /// awaits their correlated replies from `outbox`, polling every
    /// `poll_interval` for at most `await_timeout` before synthesizing a
    /// transport-timeout error.
    ///
    /// `await_timeout` should be *generous* relative to a single `send --await`:
    /// each reply is a full provider turn run by the peer daemon, so a short
    /// deadline would synthesize a timeout while the peer is still answering.
    pub fn new(
        inbox: impl Into<PathBuf>,
        outbox: impl Into<PathBuf>,
        await_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            inbox: inbox.into(),
            outbox: outbox.into(),
            await_timeout,
            poll_interval,
        }
    }

    /// Delivers `request` and awaits its correlated reply, returning it, or an
    /// `Err` describing the machinery failure (delivery failed, await timed out,
    /// or the reply did not correlate). The infallible [`Participant::respond`]
    /// reconciles that `Err` into a synthesized delivered error envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<MessageEnvelope> {
        mailbox::deliver_to(&self.inbox, request)?;

        let deadline = Instant::now() + self.await_timeout;
        let reply = loop {
            if let Some(reply) = mailbox::try_claim_response(&self.outbox, &request.message_id)? {
                break reply;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(BatonError::Transport(format!(
                    "await timed out after {}ms without a correlated reply to {:?}",
                    self.await_timeout.as_millis(),
                    request.message_id
                )));
            }
            thread::sleep(self.poll_interval.min(remaining));
        };

        // The reply is keyed by the request id, but a mis-correlated envelope
        // filed under that key is a protocol error, not a reply to return.
        if reply.in_reply_to.as_deref() != Some(request.message_id.as_str()) {
            return Err(BatonError::Transport(format!(
                "reply {:?} has in_reply_to {:?}, expected {:?}",
                reply.message_id, reply.in_reply_to, request.message_id
            )));
        }
        Ok(reply)
    }
}

impl Participant for MailboxParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(response) => response,
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }
}

/// An external-agent-backed participant: each reply is one **headless run of a
/// full-tooled native agent CLI** (one that edits files and runs git/bash/MCP),
/// driven entirely through the mailbox with no tmux and no live TUI.
///
/// Where [`SubprocessParticipant`] reaches an independent *Baton* agent that
/// emits a complete `baton.message/v1` envelope on stdout, this impl reaches a
/// generic agent CLI that emits **free text** — its final result — which this
/// participant then **wraps** into a `kind: "response"` envelope (conversation
/// preserved, addressing swapped, `in_reply_to` linked). The agent is run with a
/// **git worktree as cwd** ([`cwd`](Self::cwd)); the request body is written to
/// its stdin, and its final stdout is captured as the reply body.
///
/// Cross-message state is the agent's own responsibility: it reconstructs
/// context across rounds from **durable artifacts** (the git branch/worktree it
/// shares run-to-run, the issue thread, prior mailbox history), not from an
/// in-memory session — headless-per-message is the model. This participant
/// guarantees only the substrate: the same `cwd` on every call, the request
/// delivered on stdin, and the final output captured.
///
/// The trait stays infallible; the delivered-error boundary is drawn in envelope
/// terms, mirroring the sibling impls:
///
/// - A run that **exits 0 with a non-empty extracted result** yields a
///   `kind: "response"` whose body is that result, with **no** nested
///   `baton.exchange/v1` record — an agent run is not a single provider call
///   Baton can vouch for, so it nests nothing (as [`testing::ScriptedParticipant`]
///   does when it runs no call). The result is isolated from the raw stdout by an
///   [`OutputAdapter`]: `Raw` takes the whole stdout, `Json` takes the final JSON
///   line's result field, so a streaming backend's tool/step chatter never leaks
///   into the reply body.
/// - A **machinery failure** — the agent could not be spawned, exited non-zero,
///   produced empty output, exceeded [`read_timeout`](Self::read_timeout), or (in
///   [`OutputAdapter::Json`] mode) emitted a final line the adapter could not
///   extract a string result from — is reconciled into a *synthesized* delivered
///   `kind: "error"` envelope ([`synthesize_error_response`]), its body naming the
///   failure.
pub struct ExternalAgentParticipant {
    /// The native agent CLI to run headless (e.g. `claude`).
    program: PathBuf,
    /// Fixed arguments passed on every run (headless/role flags), before stdin.
    args: Vec<String>,
    /// Environment layered over the inherited environment (the agent carries its
    /// own credentials / MCP config through here).
    envs: Vec<(String, String)>,
    /// Working directory for every run — the git worktree the agent acts in and
    /// reconstructs context from across rounds.
    cwd: PathBuf,
    /// How the reply body is isolated from the agent's raw stdout (whole stdout
    /// vs. the final JSON line's result field).
    output: OutputAdapter,
    /// Maximum time to await the agent's final output before synthesizing an
    /// error. Should be *generous*: a headless agent run is many tool calls, not
    /// one provider turn. `None` is the explicit no-deadline mode: the wait has
    /// no deadline and the turn ends only when the child closes stdout / exits
    /// (stop/teardown still terminates the whole process tree).
    read_timeout: Option<Duration>,
    /// When set, non-empty stderr from successful turns is persisted as
    /// `<stderr_dir>/<message-id>.stderr`. `None` is a strict no-op.
    stderr_dir: Option<PathBuf>,
    /// The serving mailbox root (`--inbox`), stamped as `BATON_INBOX` on every
    /// turn. `None` omits the variable.
    inbox: Option<PathBuf>,
    /// The serving mailbox's outbox (`--outbox`), stamped as `BATON_OUTBOX` on
    /// every turn. `None` omits the variable.
    outbox: Option<PathBuf>,
    /// The `--role` name, stamped as `BATON_ROLE` on every turn. `None` both
    /// omits the variable *and* strips any same-named value the child would
    /// otherwise inherit from this process's environment, so a role-less serve
    /// never leaks a stale `BATON_ROLE` (#361).
    role: Option<String>,
    /// How [`Participant::respond_batch`] shapes stdin for the requests it
    /// answers in one child invocation (`baton serve --agent-input`).
    /// Defaults to [`AgentInputMode::Body`], today's shape.
    input_mode: AgentInputMode,
}

/// Selects the stdin shape [`ExternalAgentParticipant::respond_batch`] feeds
/// the child (`baton serve --agent-input`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentInputMode {
    /// The raw request body on stdin — today's shape (`#68`). Only ever used
    /// with a single-request batch: the CLI parser requires `BatchJson`
    /// whenever `--agent-batch-max` allows more than one.
    Body,
    /// A single JSON object `{"batch": [<request envelope>, ...]}` on stdin,
    /// one element per claimed member in claim order (oldest `ts_ms` first).
    BatchJson,
}

/// Isolates the agent's final *result* from its raw stdout.
///
/// A non-streaming backend (e.g. `claude -p`) prints only its final answer, so
/// the whole stdout *is* the result ([`Raw`](Self::Raw)). A streaming backend
/// (codex/copilot) interleaves tool/step chatter into stdout; run under its
/// `--output-format json`/`stream-json` convention its terminal line is a JSON
/// object carrying the result, which [`Json`](Self::Json) extracts by key so the
/// chatter above it is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputAdapter {
    /// The whole stdout is the reply body (the #68 default; correct when the
    /// backend prints only its final answer).
    Raw,
    /// The reply body is the string value at `result_key` in the final non-empty
    /// stdout line parsed as a JSON object. Anything else — no non-empty line, a
    /// line that is not a JSON object, an absent key, or a present-but-non-string
    /// value — is a machinery failure the caller reconciles into a delivered
    /// error, never a stringified-JSON surprise.
    Json { result_key: String },
}

impl OutputAdapter {
    /// Extracts the reply body from `stdout`, or an `Err` describing why no result
    /// could be isolated (only reachable in [`Json`](Self::Json) mode).
    fn extract(&self, stdout: &str) -> Result<String> {
        match self {
            OutputAdapter::Raw => Ok(stdout.to_string()),
            OutputAdapter::Json { result_key } => {
                let last = stdout.lines().rev().find(|line| !line.trim().is_empty());
                let Some(line) = last else {
                    return Err(BatonError::Decode(
                        "external agent produced no output line to extract a JSON result from"
                            .to_string(),
                    ));
                };
                let value: serde_json::Value =
                    serde_json::from_str(line.trim()).map_err(|err| {
                        BatonError::Decode(format!(
                            "external agent's final output line is not a JSON object: {err}"
                        ))
                    })?;
                match value.get(result_key) {
                    Some(serde_json::Value::String(s)) => Ok(s.clone()),
                    Some(_) => Err(BatonError::Decode(format!(
                        "external agent's JSON result field {result_key:?} is not a string"
                    ))),
                    None => Err(BatonError::Decode(format!(
                        "external agent's JSON output has no {result_key:?} result field"
                    ))),
                }
            }
        }
    }
}

impl ExternalAgentParticipant {
    /// Builds a participant that runs `program` with `args` (layering `envs` over
    /// the inherited environment) in `cwd`, feeding each request body on stdin,
    /// awaiting the agent's final stdout for at most `read_timeout` (`None` waits
    /// indefinitely — the no-deadline mode), and isolating the reply body from
    /// that stdout with `output`.
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
        envs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        cwd: impl Into<PathBuf>,
        output: OutputAdapter,
        read_timeout: Option<Duration>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            envs: envs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            cwd: cwd.into(),
            output,
            read_timeout,
            stderr_dir: None,
            inbox: None,
            outbox: None,
            role: None,
            input_mode: AgentInputMode::Body,
        }
    }

    /// Sets the directory where non-empty stderr from successful turns is
    /// persisted as `<dir>/<message-id>.stderr`. The directory is created on
    /// first write.
    pub fn with_stderr_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.stderr_dir = Some(dir.into());
        self
    }

    /// Sets the serving mailbox root stamped as `BATON_INBOX` on every turn.
    pub fn with_inbox(mut self, inbox: impl Into<PathBuf>) -> Self {
        self.inbox = Some(inbox.into());
        self
    }

    /// Sets the serving mailbox's outbox stamped as `BATON_OUTBOX` on every
    /// turn.
    pub fn with_outbox(mut self, outbox: impl Into<PathBuf>) -> Self {
        self.outbox = Some(outbox.into());
        self
    }

    /// Sets the `--role` name stamped as `BATON_ROLE` on every turn. Without
    /// this, `BATON_ROLE` is both omitted and actively stripped from any
    /// inherited value (#361).
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Sets the stdin shape [`Participant::respond_batch`] uses
    /// (`baton serve --agent-input`). Defaults to [`AgentInputMode::Body`].
    pub fn with_input_mode(mut self, mode: AgentInputMode) -> Self {
        self.input_mode = mode;
        self
    }

    /// Runs one headless agent turn, returning the reply body (the agent's final
    /// result, isolated from raw stdout by the [`OutputAdapter`]) or an `Err`
    /// describing the machinery failure (spawn failure, non-zero exit, empty
    /// output, unextractable result, or read timeout). The infallible
    /// [`Participant::respond`] reconciles that `Err` into a delivered error
    /// envelope.
    fn try_respond(&self, request: &MessageEnvelope) -> Result<String> {
        let mut envs = self.envs.clone();
        envs.extend(self.turn_envs(request));
        // `BATON_ROLE` must be *absent* without `--role` (#361), even if the
        // serving process itself inherited one — so a role-less turn strips
        // it explicitly rather than merely not setting it.
        let env_removals: &[&str] = if self.role.is_none() {
            &["BATON_ROLE"]
        } else {
            &[]
        };

        let (stdout, stderr) = capture_child_output(
            &self.program,
            &self.args,
            &envs,
            env_removals,
            Some(&self.cwd),
            request.body.as_bytes(),
            self.read_timeout,
        )?;

        self.persist_stderr(&request.message_id, &stderr);

        if stdout.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced no output".to_string(),
            ));
        }
        let body = self.output.extract(&stdout)?;
        if body.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced an empty result".to_string(),
            ));
        }
        Ok(body)
    }

    /// Runs one headless agent turn over a whole claimed batch, returning the
    /// single reply body every member fans out to, or an `Err` describing the
    /// machinery failure (the same surface as [`try_respond`](Self::try_respond),
    /// generalized to the batch's stdin shape and env layer).
    ///
    /// `requests` is never empty — [`drain_mailbox`](crate::cli) only calls
    /// this after claiming at least one message.
    fn try_respond_batch(&self, requests: &[MessageEnvelope]) -> Result<String> {
        let stdin = self.batch_stdin(requests)?;
        let mut envs = self.envs.clone();
        envs.extend(self.turn_envs_batch(requests));
        let env_removals: &[&str] = if self.role.is_none() {
            &["BATON_ROLE"]
        } else {
            &[]
        };

        let (stdout, stderr) = capture_child_output(
            &self.program,
            &self.args,
            &envs,
            env_removals,
            Some(&self.cwd),
            &stdin,
            self.read_timeout,
        )?;

        // Stamped under the *last* (newest) member's id, mirroring the
        // `BATON_*` env layer's "stamped from the last member" rule.
        let last = requests.last().expect("non-empty batch");
        self.persist_stderr(&last.message_id, &stderr);

        if stdout.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced no output".to_string(),
            ));
        }
        let body = self.output.extract(&stdout)?;
        if body.trim().is_empty() {
            return Err(BatonError::Decode(
                "external agent produced an empty result".to_string(),
            ));
        }
        Ok(body)
    }

    /// Builds the child's stdin for a batch, per [`AgentInputMode`]. Under
    /// [`AgentInputMode::Body`] this is the sole request's raw body — the CLI
    /// parser only ever allows that mode with a single-request batch. Under
    /// [`AgentInputMode::BatchJson`] it is `{"batch": [...]}`, one full
    /// request envelope per member, in `requests`' order.
    fn batch_stdin(&self, requests: &[MessageEnvelope]) -> Result<Vec<u8>> {
        match self.input_mode {
            AgentInputMode::Body => Ok(requests[0].body.clone().into_bytes()),
            AgentInputMode::BatchJson => {
                #[derive(Serialize)]
                struct BatchPayload<'a> {
                    batch: &'a [MessageEnvelope],
                }
                serde_json::to_vec(&BatchPayload { batch: requests }).map_err(|err| {
                    BatonError::Decode(format!("could not encode batch-json stdin: {err}"))
                })
            }
        }
    }

    /// Best-effort persist of agent stderr. A failed write must not turn a
    /// successful agent turn into an error.
    fn persist_stderr(&self, message_id: &str, stderr: &str) {
        if stderr.trim().is_empty() {
            return;
        }
        let Some(dir) = &self.stderr_dir else { return };
        if std::fs::create_dir_all(dir).is_err() {
            return;
        }
        let safe_id: String = message_id
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let path = dir.join(format!("{safe_id}.stderr"));
        let _ = std::fs::write(&path, stderr.as_bytes());
    }

    /// Builds the `BATON_*` environment layer for one turn, from `request`'s
    /// addressing/correlation fields plus this participant's mailbox/role
    /// configuration (#361). Applied *after* `self.envs`, so a turn value
    /// always overrides a same-named fixed or inherited one.
    fn turn_envs(&self, request: &MessageEnvelope) -> Vec<(String, String)> {
        let mut envs = vec![
            ("BATON_MESSAGE_ID".to_string(), request.message_id.clone()),
            (
                "BATON_CONVERSATION_ID".to_string(),
                request.conversation_id.clone(),
            ),
            ("BATON_FROM".to_string(), request.from.clone()),
            ("BATON_TO".to_string(), request.to.clone()),
            (
                "BATON_KIND".to_string(),
                request.kind.as_wire_str().to_string(),
            ),
            (
                "BATON_IN_REPLY_TO".to_string(),
                request.in_reply_to.clone().unwrap_or_default(),
            ),
            ("BATON_TS_MS".to_string(), request.ts_ms.to_string()),
        ];
        if let Some(inbox) = &self.inbox {
            envs.push((
                "BATON_INBOX".to_string(),
                inbox.to_string_lossy().into_owned(),
            ));
        }
        if let Some(outbox) = &self.outbox {
            envs.push((
                "BATON_OUTBOX".to_string(),
                outbox.to_string_lossy().into_owned(),
            ));
        }
        if let Some(role) = &self.role {
            envs.push(("BATON_ROLE".to_string(), role.clone()));
        }
        envs
    }

    /// Builds the `BATON_*` environment layer for a batch turn: today's
    /// [`turn_envs`](Self::turn_envs) from the **last** (newest) member, plus
    /// `BATON_BATCH_SIZE` — `1` for a single-request batch, so an unbatched
    /// `serve --agent-cmd` run is unchanged except for that one addition.
    fn turn_envs_batch(&self, requests: &[MessageEnvelope]) -> Vec<(String, String)> {
        let last = requests.last().expect("non-empty batch");
        let mut envs = self.turn_envs(last);
        envs.push(("BATON_BATCH_SIZE".to_string(), requests.len().to_string()));
        envs
    }
}

impl Participant for ExternalAgentParticipant {
    fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
        match self.try_respond(request) {
            Ok(body) => {
                let ts_ms = now_ms();
                let mut response = MessageEnvelope::new(
                    fresh_message_id(&request.conversation_id, ts_ms),
                    request.conversation_id.clone(),
                    request.to.clone(),
                    request.from.clone(),
                    MessageKind::Response,
                    body,
                    ts_ms,
                );
                response.in_reply_to = Some(request.message_id.clone());
                response
            }
            Err(err) => synthesize_error_response(request, &err.to_string()),
        }
    }

    /// Runs one child invocation for the whole batch (`try_respond_batch`),
    /// then fans its single body — or, on a machinery failure, one synthesized
    /// error — out to every member, each correlated to its own
    /// `conversation_id`/`from`/`to`/`message_id`.
    fn respond_batch(&self, requests: &[MessageEnvelope]) -> Vec<MessageEnvelope> {
        match self.try_respond_batch(requests) {
            Ok(body) => requests
                .iter()
                .map(|request| {
                    let ts_ms = now_ms();
                    let mut response = MessageEnvelope::new(
                        fresh_message_id(&request.conversation_id, ts_ms),
                        request.conversation_id.clone(),
                        request.to.clone(),
                        request.from.clone(),
                        MessageKind::Response,
                        body.clone(),
                        ts_ms,
                    );
                    response.in_reply_to = Some(request.message_id.clone());
                    response
                })
                .collect(),
            Err(err) => {
                let message = err.to_string();
                requests
                    .iter()
                    .map(|request| synthesize_error_response(request, &message))
                    .collect()
            }
        }
    }
}

/// Maximum bytes retained from a child's stdout. Beyond this the prefix is
/// discarded and the retained tail is prefixed with a marker.
const MAX_STDOUT_BYTES: usize = 8 * 1024 * 1024;
const STDOUT_TRUNCATION_MARKER: &str =
    "[truncated at 8 MiB: output prefix dropped; retained tail begins below]\n";

/// Maximum bytes retained from a child's stderr. Beyond this the buffer is
/// truncated and a marker appended so the reader knows output was lost.
const MAX_STDERR_BYTES: usize = 1024 * 1024;

/// Appends `token` to the raw `cmd /D /S /C` tail while preserving the token
/// through both parsers involved in a Windows `.cmd` launch. MSVC-style
/// quoting is needed by the child CRT, while cmd metacharacters must be
/// caret-escaped for the cmd parser; in particular, `%` expands even inside
/// ordinary double quotes. The tail is assembled as one raw string with an
/// outer quote pair because `arg()` cannot produce that shape.
#[cfg(windows)]
fn append_windows_arg(line: &mut String, token: &str) {
    if token.is_empty() {
        line.push_str("\"\"");
        return;
    }

    // Ordinary tokens can keep the exact compact representation used before
    // the cmd-layer protection was needed. This also keeps the MSVC quoting
    // cases easy to compare with Command::arg() semantics.
    if !token.chars().any(is_windows_cmd_metachar) {
        append_windows_msvc_arg(line, token);
        return;
    }

    // Keep every protected token in a quoted CRT argument. The quote pair is
    // itself caret-escaped so cmd treats it as a literal quote in the command
    // line passed to the child; this is distinct from a normal cmd quote and
    // lets every metacharacter use the same caret escape. This is necessary
    // for `%`, whose expansion is not disabled by ordinary double quotes.
    line.push('^');
    line.push('"');
    let mut backslashes = 0usize;
    for ch in token.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                // The caret keeps cmd from interpreting this quote while the
                // MSVC backslash run makes it a literal quote for the child.
                for _ in 0..(backslashes * 2 + 1) {
                    line.push('\\');
                }
                line.push('^');
                line.push('"');
                backslashes = 0;
            }
            ch if is_windows_cmd_metachar(ch) => {
                // The token quote is cmd-opaque, so escape the metacharacter
                // explicitly; the caret is consumed before the child sees
                // the command line.
                for _ in 0..backslashes {
                    line.push('\\');
                }
                line.push('^');
                if ch == '^' {
                    line.push('^');
                } else {
                    line.push(ch);
                }
                backslashes = 0;
            }
            other => {
                for _ in 0..backslashes {
                    line.push('\\');
                }
                backslashes = 0;
                line.push(other);
            }
        }
    }
    for _ in 0..(backslashes * 2) {
        line.push('\\');
    }
    line.push('^');
    line.push('"');
}

#[cfg(windows)]
fn is_windows_cmd_metachar(ch: char) -> bool {
    matches!(ch, '&' | '|' | '<' | '>' | '^' | '%' | '(' | ')' | '!')
}

/// Appends a token using the MSVC command-line quoting convention. This is
/// kept separate from [`append_windows_arg`] so cmd-layer escapes never leak
/// into the ordinary path/space/quote cases.
#[cfg(windows)]
fn append_windows_msvc_arg(line: &mut String, token: &str) {
    if !token.contains([' ', '\t', '"']) {
        line.push_str(token);
        return;
    }
    line.push('"');
    let mut backslashes = 0usize;
    for ch in token.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                for _ in 0..(backslashes * 2 + 1) {
                    line.push('\\');
                }
                line.push('"');
                backslashes = 0;
            }
            other => {
                for _ in 0..backslashes {
                    line.push('\\');
                }
                backslashes = 0;
                line.push(other);
            }
        }
    }
    for _ in 0..(backslashes * 2) {
        line.push('\\');
    }
    line.push('"');
}

/// Spawns `program` with `args`/`envs` (optionally in `cwd`), writes `payload`
/// to its stdin, and returns `(stdout, stderr)` captured to EOF — the shared
/// process machinery behind [`SubprocessParticipant`] and
/// [`ExternalAgentParticipant`].
///
/// `env_removals` removes each named variable from the child's environment
/// entirely, applied *after* `envs`, so the removal wins over both the
/// inherited environment and any same-named entry in `envs` — a caller can
/// thus guarantee a variable's absence (e.g. a role-less `BATON_ROLE`, #361),
/// which `envs` alone cannot express (it only sets/overrides).
///
/// Both stdout and stderr are drained on their own threads, started *before*
/// the stdin write, so a child that emits before consuming all its input — or
/// that writes more than a pipe buffer to stderr — cannot deadlock against a
/// full pipe. Stdout is capped at [`MAX_STDOUT_BYTES`] with tail retention;
/// stderr is capped at [`MAX_STDERR_BYTES`].
///
/// A child that holds stdout open past `read_timeout` is killed and reaped;
/// `None` (`read_timeout`) waits for stdout EOF with no deadline — termination
/// then comes only from the caller's own stop/teardown of the parent process
/// tree. Returns `Ok((stdout, stderr))` only when the child exits 0 (either
/// string may be empty — the caller decides what empty means); a spawn failure,
/// a non-zero exit (stderr folded into the message), a timeout, or an I/O error
/// is an `Err`.
fn capture_child_output(
    program: &Path,
    args: &[String],
    envs: &[(String, String)],
    env_removals: &[&str],
    cwd: Option<&Path>,
    payload: &[u8],
    read_timeout: Option<Duration>,
) -> Result<(String, String)> {
    // `CreateProcessW` does not consult `PATHEXT`, so `Command::new` cannot
    // resolve an installed Windows CLI's `.cmd` shim by its command name.
    // `cmd /C` performs that resolution for both participant implementations.
    //
    // The program+args tail is assembled here as one raw argument wrapped in
    // an extra outer quote pair. `append_windows_arg` applies the MSVC CRT
    // quoting and cmd metacharacter escaping needed by each token; `/S` strips
    // exactly that outer pair, so the token quoting survives.
    // Letting `arg()` build the tail instead exposes the same `/S` strip to
    // the program's own quotes, which splits any program path containing a
    // space at its first space (`C:\Users\Jane Doe\...` — the integration
    // tests hit this via `CARGO_BIN_EXE_baton`).
    #[cfg(windows)]
    let mut command = {
        use std::os::windows::process::CommandExt;
        let mut line = String::new();
        for token in
            std::iter::once(program.to_string_lossy().into_owned()).chain(args.iter().cloned())
        {
            if !line.is_empty() {
                line.push(' ');
            }
            append_windows_arg(&mut line, &token);
        }
        let mut command = Command::new("cmd");
        command
            .args(["/D", "/S", "/C"])
            .raw_arg(format!("\"{line}\""));
        command
    };
    #[cfg(not(windows))]
    let mut command = Command::new(program);
    #[cfg(not(windows))]
    command.args(args);
    command
        .envs(envs.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Removals apply *after* `envs` so the key cannot be resurrected — by a
    // same-named entry in the fixed layer either — and the caller's absence
    // guarantee wins over every earlier layer (#361).
    for key in env_removals {
        command.env_remove(key);
    }
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let mut child = {
        // Under `cargo test` this crate's flock-assertion tests share an
        // address space with this fork, and a child forked while another
        // thread holds a lock inherits that descriptor — keeping the lock
        // held until `execve` closes it. Serializing just the spawn (the
        // whole fork-to-exec window, which `spawn` does not return before)
        // removes that same-process artifact; see [`crate::test_support`].
        // Compiled out entirely for the shipped binary.
        #[cfg(test)]
        let _fork_guard = crate::test_support::serialize_forks_and_locks();
        command.spawn().map_err(|err| {
            BatonError::Io(format!("could not spawn child process {program:?}: {err}"))
        })?
    };

    // Drain stdout and stderr on their own threads, started *before* writing
    // stdin, so a child that emits before consuming all its input — or that
    // writes more than a pipe buffer to stderr — cannot deadlock.
    let mut stdout_pipe = child.stdout.take().expect("child stdout is piped");
    let (stdout_tx, stdout_rx) = mpsc::channel();
    thread::spawn(move || {
        let result = (|| {
            let mut buf = VecDeque::with_capacity(MAX_STDOUT_BYTES);
            let mut chunk = [0u8; 8192];
            let mut truncated = false;
            loop {
                match stdout_pipe.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        if n >= MAX_STDOUT_BYTES {
                            buf.clear();
                            buf.extend(&chunk[n - MAX_STDOUT_BYTES..n]);
                            truncated = true;
                        } else {
                            let overflow =
                                buf.len().saturating_add(n).saturating_sub(MAX_STDOUT_BYTES);
                            if overflow > 0 {
                                truncated = true;
                                for _ in 0..overflow {
                                    let _ = buf.pop_front();
                                }
                            }
                            buf.extend(&chunk[..n]);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }

            let tail: Vec<u8> = buf.into_iter().collect();
            if truncated {
                let mut output = Vec::with_capacity(STDOUT_TRUNCATION_MARKER.len() + tail.len());
                output.extend_from_slice(STDOUT_TRUNCATION_MARKER.as_bytes());
                output.extend_from_slice(&tail);
                Ok(output)
            } else {
                Ok(tail)
            }
        })();
        let _ = stdout_tx.send(result);
    });

    let mut stderr_pipe = child.stderr.take().expect("child stderr is piped");
    let (stderr_tx, stderr_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = vec![0u8; 0];
        let mut chunk = [0u8; 8192];
        let mut truncated = false;
        loop {
            match stderr_pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    let remaining = MAX_STDERR_BYTES.saturating_sub(buf.len());
                    if remaining >= n {
                        buf.extend_from_slice(&chunk[..n]);
                    } else {
                        if remaining > 0 {
                            buf.extend_from_slice(&chunk[..remaining]);
                        }
                        truncated = true;
                    }
                }
                Err(_) => break,
            }
        }
        let mut s = String::from_utf8_lossy(&buf).into_owned();
        if truncated {
            s.push_str("\n[truncated at 1 MiB]");
        }
        let _ = stderr_tx.send(s);
    });

    // Collect stderr best-effort. The child is reaped before this runs on the
    // success path, so the pipe is at EOF; the timeout is a safety net against
    // a leaked descriptor from a descendant process.
    let collect_stderr = || {
        stderr_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_default()
    };

    // An I/O error means the child may still be running (for example, it can
    // close stdin before the payload is written). Always reap it before
    // returning, and retain any diagnostic it emitted on stderr.
    let kill_and_reap = |child: &mut std::process::Child| {
        let _ = child.kill();
        let _ = child.wait();
    };
    let stderr_detail = |stderr: &str| {
        if stderr.trim().is_empty() {
            String::new()
        } else {
            format!(": {}", stderr.trim())
        }
    };

    // Write the payload, then drop stdin so the child sees EOF.
    let write_result = {
        let mut stdin = child.stdin.take().expect("child stdin is piped");
        stdin.write_all(payload)
    };
    if let Err(err) = write_result {
        kill_and_reap(&mut child);
        let stderr = collect_stderr();
        return Err(BatonError::Io(format!(
            "could not write to child stdin: {err}{}",
            stderr_detail(&stderr)
        )));
    }

    // Await the child's final stdout. `read_timeout` bounds the wait and a
    // breach kills the child; `None` is the explicit no-deadline mode — the
    // wait ends only when the child closes stdout, and termination comes from
    // the caller's own stop/teardown of the parent process tree.
    enum WaitOutcome {
        Done(std::io::Result<Vec<u8>>),
        TimedOut(Duration),
        Disconnected,
    }
    let outcome = match read_timeout {
        Some(timeout) => match stdout_rx.recv_timeout(timeout) {
            Ok(result) => WaitOutcome::Done(result),
            Err(mpsc::RecvTimeoutError::Timeout) => WaitOutcome::TimedOut(timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => WaitOutcome::Disconnected,
        },
        None => match stdout_rx.recv() {
            Ok(result) => WaitOutcome::Done(result),
            Err(_) => WaitOutcome::Disconnected,
        },
    };
    match outcome {
        WaitOutcome::Done(read_result) => {
            let stdout = match read_result {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(err) => {
                    kill_and_reap(&mut child);
                    let stderr = collect_stderr();
                    return Err(BatonError::Io(format!(
                        "could not read child stdout: {err}{}",
                        stderr_detail(&stderr)
                    )));
                }
            };
            let status = child
                .wait()
                .map_err(|err| BatonError::Io(format!("could not reap child process: {err}")))?;
            if !status.success() {
                let stderr = collect_stderr();
                return Err(BatonError::Transport(format!(
                    "child process exited with {status}{}",
                    stderr_detail(&stderr)
                )));
            }
            Ok((stdout, collect_stderr()))
        }
        WaitOutcome::TimedOut(timeout) => {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = collect_stderr();
            Err(BatonError::Transport(format!(
                "child process exceeded the {timeout:?} read timeout{}",
                stderr_detail(&stderr)
            )))
        }
        WaitOutcome::Disconnected => {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = collect_stderr();
            Err(BatonError::Transport(format!(
                "child process stdout reader terminated unexpectedly{}",
                stderr_detail(&stderr)
            )))
        }
    }
}

/// Builds a delivered `kind: "error"` envelope for a machinery failure,
/// correlated to `request` (conversation preserved, addressing swapped,
/// `in_reply_to` linked) with **no** nested `baton.exchange/v1` record — the
/// parent ran no provider call of its own to record.
fn synthesize_error_response(request: &MessageEnvelope, message: &str) -> MessageEnvelope {
    let ts_ms = now_ms();
    let mut response = MessageEnvelope::new(
        fresh_message_id(&request.conversation_id, ts_ms),
        request.conversation_id.clone(),
        request.to.clone(),
        request.from.clone(),
        MessageKind::Error,
        message.to_string(),
        ts_ms,
    );
    response.in_reply_to = Some(request.message_id.clone());
    response
}

/// A process-lifetime counter making every synthesized response id distinct.
///
/// Millisecond timestamps do not separate emissions: a `baton serve` daemon
/// draining a mailbox answers requests in a tight loop, and a fast failure (an
/// unspawnable `--agent-cmd`, say) resolves in microseconds, so several replies
/// in one conversation can share a `ts_ms`.
static RESPONSE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Builds a fresh `message_id` for a response without adding a dependency.
///
/// Derived from the conversation id, the response timestamp, and a draw from
/// [`RESPONSE_SEQ`]. The counter — not the timestamp — carries uniqueness: each
/// call takes a value no other call in this process takes, so two replies
/// emitted in the same conversation within the same millisecond still differ.
/// That matters because the id is the mailbox filename key
/// ([`crate::mailbox`]'s `safe_key`, for which `-` and digits are safe) and the
/// identity `baton log merge` collapses duplicates by: a collision would
/// overwrite a pending reply and erase a turn from the merged transcript.
/// `baton.message/v1` places no format constraint on the id beyond uniqueness.
fn fresh_message_id(conversation_id: &str, ts_ms: u64) -> String {
    let seq = RESPONSE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{conversation_id}-r-{ts_ms}-{seq}")
}

/// Test-only participant doubles, reusable across the crate's unit tests.
///
/// Lives here (not in a `#[cfg(test)] mod tests`) so a future driver module's
/// unit tests can reach [`ScriptedParticipant`] as
/// `crate::participant::testing::ScriptedParticipant`. Compiled only under
/// `cargo test`, so nothing ships in the release binary.
#[cfg(test)]
pub mod testing {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use super::Participant;
    use crate::log::{Exchange, Outcome, RequestRecord};
    use crate::message::{MessageEnvelope, MessageKind, WrappedExchange};

    /// Builds a reply correlated to `request` with a deterministic id/timestamp
    /// (so tests need no wall clock): preserved `conversation_id`, `in_reply_to`
    /// set, and addressing swapped — the reply is from the request's recipient,
    /// to its sender. Shared by every fake here so they correlate identically to
    /// [`super::LocalParticipant`]. `pub(crate)` so other modules' tests (e.g.
    /// `cli`'s `drain_mailbox` batch tests) can build the same correlated shape
    /// for their own fakes.
    pub(crate) fn correlated_reply(
        request: &MessageEnvelope,
        kind: MessageKind,
        body: impl Into<String>,
    ) -> MessageEnvelope {
        let mut response = MessageEnvelope::new(
            format!("{}-r-{}", request.conversation_id, request.message_id),
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            body,
            request.ts_ms + 1,
        );
        response.in_reply_to = Some(request.message_id.clone());
        response
    }

    /// A [`Participant`] that replies from a scripted queue with no network.
    ///
    /// Each `respond` pops the next scripted body and wraps it in a
    /// `kind: "response"` envelope correlated to the request. Unlike
    /// [`super::LocalParticipant`] it nests no `baton.exchange/v1` record — it
    /// runs no provider call. An exhausted queue yields a `kind: "error"`
    /// envelope so a driver test sees a well-formed reply rather than a panic.
    pub struct ScriptedParticipant {
        replies: RefCell<VecDeque<String>>,
    }

    impl ScriptedParticipant {
        /// Builds a participant that answers with `replies`, in order.
        pub fn new(replies: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self {
                replies: RefCell::new(replies.into_iter().map(Into::into).collect()),
            }
        }
    }

    impl Participant for ScriptedParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            match self.replies.borrow_mut().pop_front() {
                Some(body) => correlated_reply(request, MessageKind::Response, body),
                None => correlated_reply(request, MessageKind::Error, "no scripted reply"),
            }
        }
    }

    /// A [`Participant`] that always replies with the same body and never stops
    /// on its own — the shape a turn-cap guarantee test needs (only the cap can
    /// end it). Optionally carries nested token usage so a token-budget test can
    /// accumulate a running total.
    pub struct LoopingParticipant {
        body: String,
        usage: Option<(u64, u64)>,
    }

    impl LoopingParticipant {
        /// A looping participant whose replies nest no usage (contribute zero to
        /// a token budget).
        pub fn new(body: impl Into<String>) -> Self {
            Self {
                body: body.into(),
                usage: None,
            }
        }

        /// A looping participant whose replies nest `(input, output)` token
        /// usage on a `response_ok` record.
        pub fn with_usage(body: impl Into<String>, input: u64, output: u64) -> Self {
            Self {
                body: body.into(),
                usage: Some((input, output)),
            }
        }
    }

    impl Participant for LoopingParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            let mut reply = correlated_reply(request, MessageKind::Response, self.body.clone());
            if let Some((input, output)) = self.usage {
                reply.exchange = Some(WrappedExchange::new(Exchange {
                    request: RequestRecord {
                        ts_ms: request.ts_ms,
                        model: "fake-model".to_string(),
                        base_url: "fake-base-url".to_string(),
                        prompt: request.body.clone(),
                        session_id: None,
                        turn_index: None,
                    },
                    outcome: Outcome::Ok {
                        ts_ms: request.ts_ms + 1,
                        duration_ms: 0,
                        reply: self.body.clone(),
                        input_tokens: Some(input),
                        output_tokens: Some(output),
                        stop_reason: None,
                    },
                }));
            }
            reply
        }
    }

    /// A [`Participant`] that always emits a `kind: "done"` reply — the
    /// unilateral-completion terminal condition, unreachable from today's real
    /// participants (which emit only `response`/`error`).
    pub struct DoneParticipant;

    impl Participant for DoneParticipant {
        fn respond(&self, request: &MessageEnvelope) -> MessageEnvelope {
            correlated_reply(request, MessageKind::Done, "done")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::ScriptedParticipant;
    use super::*;
    #[cfg(feature = "local")]
    use crate::config::{BatonConfig, Credential, DEFAULT_MAX_TOKENS};
    use crate::log::{Exchange, Outcome, RequestRecord};
    use crate::message::WrappedExchange;
    #[cfg(feature = "local")]
    use crate::transport::claude::ClaudeClient;
    #[cfg(feature = "local")]
    use crate::transport::http::{HttpClient, HttpResponse};
    use std::time::Duration;

    /// A fake [`HttpClient`] returning a canned status + body, so a
    /// [`ClaudeClient`] can be driven without a network — mirroring the fake in
    /// `transport::claude`'s own tests.
    #[cfg(feature = "local")]
    struct FakeHttp {
        status: u16,
        body: String,
    }

    #[cfg(feature = "local")]
    impl HttpClient for FakeHttp {
        fn post_json(
            &self,
            _url: &str,
            _headers: &[(&str, &str)],
            _body: &str,
        ) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }

    #[cfg(feature = "local")]
    fn test_meta() -> ExchangeMeta {
        ExchangeMeta {
            model: "claude-test-model".to_string(),
            base_url: "https://api.anthropic.com".to_string(),
        }
    }

    #[cfg(feature = "local")]
    fn test_config() -> BatonConfig {
        BatonConfig {
            credential: Credential::ApiKey("secret-key".to_string()),
            base_url: "https://api.anthropic.com".to_string(),
            model: "claude-test-model".to_string(),
            timeout: Duration::from_secs(60),
            max_tokens: DEFAULT_MAX_TOKENS,
            system_prompt: None,
        }
    }

    fn request_envelope() -> MessageEnvelope {
        MessageEnvelope::new(
            "m-req-1",
            "conv-42",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "what is 2+2?",
            1_700_000_000_000,
        )
    }

    /// A `ClaudeClient`-backed participant (as production uses) turns a request
    /// envelope into a `kind: "response"` reply correlated to the request, with
    /// the provider call nested in-band.
    #[cfg(feature = "local")]
    #[test]
    fn local_participant_builds_response_envelope_correlated_to_request() {
        let body = r#"{"content": [{"type": "text", "text": "four"}]}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 200,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());
        let request = request_envelope();

        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "four");
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps: reply is from the request's recipient, to its sender.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert_ne!(response.message_id, request.message_id);

        let wrapped = response
            .exchange
            .as_ref()
            .expect("wrapped exchange present");
        assert_eq!(wrapped.schema, crate::events::SCHEMA);
        match &wrapped.exchange.outcome {
            Outcome::Ok { reply, .. } => assert_eq!(reply, "four"),
            other => panic!("expected Ok outcome, got {other:?}"),
        }
        assert_eq!(wrapped.exchange.request.prompt, "what is 2+2?");
        assert_eq!(wrapped.exchange.request.model, "claude-test-model");
    }

    /// Reported token usage rides along on the nested `baton.exchange/v1` record.
    #[cfg(feature = "local")]
    #[test]
    fn local_participant_wraps_reported_token_usage() {
        let body = r#"{"content": [{"type": "text", "text": "hi"}], "usage": {"input_tokens": 7, "output_tokens": 11}}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 200,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());

        let response = participant.respond(&request_envelope());

        match &response.exchange.expect("wrapped").exchange.outcome {
            Outcome::Ok {
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, Some(7));
                assert_eq!(*output_tokens, Some(11));
            }
            other => panic!("expected Ok outcome, got {other:?}"),
        }
    }

    /// The provider terminal reason remains available in the nested exchange
    /// so a conversation driver can warn even when this participant is remote.
    #[cfg(feature = "local")]
    #[test]
    fn local_participant_wraps_stop_reason() {
        let body =
            r#"{"content": [{"type": "text", "text": "unfinished"}], "stop_reason": "max_tokens"}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 200,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());

        let response = participant.respond(&request_envelope());

        match &response.exchange.expect("wrapped").exchange.outcome {
            Outcome::Ok { stop_reason, .. } => {
                assert_eq!(stop_reason.as_deref(), Some("max_tokens"));
            }
            other => panic!("expected Ok outcome, got {other:?}"),
        }
    }

    /// A provider failure is a *delivered* `kind: "error"` envelope, never a
    /// propagated error — and the nested outcome carries the machine kind.
    #[cfg(feature = "local")]
    #[test]
    fn local_participant_delivers_error_envelope_on_provider_failure() {
        let body = r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#;
        let client = ClaudeClient::with_http(
            test_config(),
            FakeHttp {
                status: 401,
                body: body.to_string(),
            },
        );
        let participant = LocalParticipant::new(client, test_meta());

        let response = participant.respond(&request_envelope());

        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        assert_eq!(response.conversation_id, "conv-42");
        assert!(
            response.body.contains("invalid x-api-key"),
            "error body carries the failure description: {}",
            response.body
        );
        match &response
            .exchange
            .expect("wrapped failed exchange")
            .exchange
            .outcome
        {
            Outcome::Error { kind, .. } => assert_eq!(kind, "auth"),
            other => panic!("expected Error outcome, got {other:?}"),
        }
    }

    /// The scripted fake answers a driver's requests in order, correlated to each
    /// request, with no provider call (no nested exchange) — the shape M3c's
    /// driver tests consume.
    #[test]
    fn scripted_participant_answers_in_order_correlated_to_each_request() {
        let participant = ScriptedParticipant::new(["first", "second"]);

        let req1 = request_envelope();
        let resp1 = participant.respond(&req1);
        assert_eq!(resp1.kind, MessageKind::Response);
        assert_eq!(resp1.body, "first");
        assert_eq!(resp1.in_reply_to.as_deref(), Some("m-req-1"));
        assert_eq!(resp1.from, "agent-b");
        assert_eq!(resp1.to, "agent-a");
        assert!(
            resp1.exchange.is_none(),
            "scripted fake runs no provider call"
        );

        let mut req2 = request_envelope();
        req2.message_id = "m-req-2".to_string();
        let resp2 = participant.respond(&req2);
        assert_eq!(resp2.body, "second");
        assert_eq!(resp2.in_reply_to.as_deref(), Some("m-req-2"));

        // Queue exhausted → a well-formed delivered error, not a panic.
        let resp3 = participant.respond(&request_envelope());
        assert_eq!(resp3.kind, MessageKind::Error);
    }

    // -- SubprocessParticipant --------------------------------------------
    //
    // These drive the impl against `sh -c` stub programs — no live provider,
    // no `baton` binary — so each delivered-response / machinery-failure path
    // is exercised deterministically. `cat >/dev/null` in each stub consumes
    // the request from stdin so the child never dies on a broken pipe.

    /// Builds a subprocess participant that runs `script` under `sh -c`, passing
    /// `STUB_OUT` through as an env override the script can echo.
    #[cfg(feature = "local")]
    fn stub(script: &str, stub_out: &str, read_timeout: Duration) -> SubprocessParticipant {
        SubprocessParticipant::new("sh", ["-c", script], [("STUB_OUT", stub_out)], read_timeout)
    }

    /// A child that exits 0 emitting a well-formed envelope has that envelope
    /// returned unchanged.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_returns_child_envelope_unchanged_on_success() {
        let mut child_reply = MessageEnvelope::new(
            "child-resp-1",
            "conv-42",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "four",
            1_700_000_000_001,
        );
        child_reply.in_reply_to = Some("m-req-1".to_string());
        let json = serde_json::to_string(&child_reply).expect("serializes");

        let participant = stub(
            "cat >/dev/null; printf %s \"$STUB_OUT\"",
            &json,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());

        // Returned verbatim — the child, not the parent, owns correlation here.
        assert_eq!(response, child_reply);
    }

    /// A child that exits 0 with a `kind: "error"` envelope (a delivered
    /// provider failure) is passed through unchanged, nested record and all —
    /// it is a delivered response, not a machinery failure.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_passes_through_delivered_error_envelope() {
        let mut child_error = MessageEnvelope::new(
            "child-err-1",
            "conv-42",
            "agent-b",
            "agent-a",
            MessageKind::Error,
            "invalid x-api-key",
            1_700_000_000_002,
        );
        child_error.in_reply_to = Some("m-req-1".to_string());
        child_error.exchange = Some(WrappedExchange::new(Exchange {
            request: RequestRecord {
                ts_ms: 1_700_000_000_000,
                model: "claude-test-model".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                prompt: "what is 2+2?".to_string(),
                session_id: None,
                turn_index: None,
            },
            outcome: Outcome::Error {
                ts_ms: 1_700_000_000_002,
                duration_ms: 2,
                kind: "auth".to_string(),
                message: "invalid x-api-key".to_string(),
            },
        }));
        let json = serde_json::to_string(&child_error).expect("serializes");

        let participant = stub(
            "cat >/dev/null; printf %s \"$STUB_OUT\"",
            &json,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());

        // Unchanged: still an error envelope carrying the child's nested record.
        assert_eq!(response, child_error);
        assert_eq!(response.kind, MessageKind::Error);
        assert!(response.exchange.is_some(), "nested record preserved");
    }

    /// Asserts a synthesized machinery-failure envelope: a `kind: "error"`
    /// correlated to the request, with **no** nested provider record.
    fn assert_synthesized_error(response: &MessageEnvelope) {
        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps, just like a delivered reply.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "a machinery failure nests no provider record"
        );
    }

    /// A child that exits non-zero yields a synthesized delivered error.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_synthesizes_error_on_nonzero_exit() {
        let participant = stub(
            "cat >/dev/null; echo boom >&2; exit 3",
            "",
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_envelope());
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("boom") || response.body.contains("exit"),
            "body describes the child failure: {}",
            response.body
        );
    }

    /// A child that exits 0 but emits non-JSON yields a synthesized error.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_synthesizes_error_on_malformed_stdout() {
        let participant = stub(
            "cat >/dev/null; printf 'not an envelope'",
            "",
            Duration::from_secs(5),
        );
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    /// A child that exits 0 with empty stdout yields a synthesized error.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_synthesizes_error_on_absent_envelope() {
        let participant = stub("cat >/dev/null", "", Duration::from_secs(5));
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    /// A child that holds stdout open past the read timeout is killed and
    /// yields a synthesized error, without hanging the parent.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_synthesizes_error_on_read_timeout() {
        // `sleep 30` keeps stdout open; the 150ms parent deadline fires first.
        let participant = stub("cat >/dev/null; sleep 30", "", Duration::from_millis(150));
        let response = participant.respond(&request_envelope());
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("timeout"),
            "body names the timeout: {}",
            response.body
        );
    }

    /// A program that cannot be spawned at all yields a synthesized error, not
    /// a panic.
    #[cfg(feature = "local")]
    #[test]
    fn subprocess_synthesizes_error_when_program_missing() {
        let participant = SubprocessParticipant::new(
            "baton-no-such-program-xyz",
            std::iter::empty::<String>(),
            std::iter::empty::<(String, String)>(),
            Duration::from_secs(5),
        );
        assert_synthesized_error(&participant.respond(&request_envelope()));
    }

    // -- ExternalAgentParticipant -----------------------------------------
    //
    // These drive the impl against `sh -c` stub programs in a tempdir cwd — no
    // real agent, no network, no API key. Each stub `cat`s its stdin (the
    // request body) so it never dies on a broken pipe, then acts in cwd and/or
    // prints its free-text "final result". This proves the machinery: stdin
    // delivery, the cwd side effect, the free-text→envelope wrap, the two-round
    // continuity substrate (shared cwd), and every synthesized-error path. The
    // real cross-context proof (an agent reconstructing from a git branch + the
    // issue thread) is `scripts/external-agent-proof.sh`, run manually.

    /// Builds an external-agent participant running `script` under `sh -c` in
    /// `cwd`, with no env overrides and the `Raw` output adapter (whole stdout).
    fn external_agent(
        script: &str,
        cwd: &std::path::Path,
        read_timeout: Duration,
    ) -> ExternalAgentParticipant {
        external_agent_with_output(script, cwd, OutputAdapter::Raw, read_timeout)
    }

    /// The no-deadline counterpart of [`external_agent`]: the same `sh -c`
    /// stub shape, but the read wait has no deadline at all.
    fn external_agent_unbounded(script: &str, cwd: &std::path::Path) -> ExternalAgentParticipant {
        ExternalAgentParticipant::new(
            "sh",
            ["-c", script],
            std::iter::empty::<(String, String)>(),
            cwd,
            OutputAdapter::Raw,
            None,
        )
    }

    /// Builds an external-agent participant running `script` under `sh -c` in
    /// `cwd`, with the chosen `output` adapter — for exercising streaming-result
    /// extraction against a stub that emits chatter + a final result.
    fn external_agent_with_output(
        script: &str,
        cwd: &std::path::Path,
        output: OutputAdapter,
        read_timeout: Duration,
    ) -> ExternalAgentParticipant {
        ExternalAgentParticipant::new(
            "sh",
            ["-c", script],
            std::iter::empty::<(String, String)>(),
            cwd,
            output,
            Some(read_timeout),
        )
    }

    /// A request envelope with a chosen id/body (agent-a → agent-b), so a
    /// two-round test can address distinct payloads.
    fn request_with_body(id: &str, body: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            id,
            "conv-42",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            body,
            1_700_000_000_000,
        )
    }

    /// A headless run that exits 0 with free-text stdout has that text wrapped
    /// into a `kind: "response"` correlated to the request (no nested record),
    /// the request body arrives on the agent's stdin, and the agent's cwd side
    /// effect lands in the worktree.
    #[test]
    fn external_agent_wraps_stdout_and_produces_cwd_side_effect() {
        let dir = TempDir::new("ext-ok");
        // The stub records its stdin to a file in cwd (proving stdin delivery +
        // an observable side effect), then prints its free-text result.
        let participant = external_agent(
            "cat > round1.txt; printf 'edited round1.txt and committed'",
            &dir.path,
            Duration::from_secs(5),
        );
        let request = request_with_body("m-req-1", "please edit round1.txt");

        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "edited round1.txt and committed");
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps: reply is from the request's recipient, to its sender.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "an agent run is not one provider call, so it nests no record"
        );
        // The request body was delivered on the agent's stdin...
        let stdin_seen = std::fs::read_to_string(dir.path.join("round1.txt")).expect("side effect");
        assert_eq!(stdin_seen, "please edit round1.txt");
    }

    /// A successful external-agent turn may contain non-UTF-8 bytes; those
    /// bytes are lossily decoded and delivered instead of becoming a machinery
    /// error.
    #[test]
    fn external_agent_delivers_lossy_stdout_on_invalid_utf8() {
        let dir = TempDir::new("ext-invalid-utf8");
        let participant = external_agent(
            "cat >/dev/null; printf '\\377'",
            &dir.path,
            Duration::from_secs(5),
        );

        let response = participant.respond(&request_with_body("m-req-1", "go"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "\u{fffd}");
    }

    /// On Windows, an installed agent CLI commonly has a `.cmd` shim. `cmd`
    /// resolves that shim by command name from PATH; the shim still receives
    /// the request on stdin and runs in the participant's configured cwd.
    #[cfg(windows)]
    #[test]
    fn external_agent_resolves_cmd_shim_by_command_name() {
        let dir = TempDir::new("ext-cmd-shim");
        let shim = dir.path.join("baton-test-agent.cmd");
        std::fs::write(
            &shim,
            "@echo off\r\nmore > request.txt\r\n> argument.txt <nul set /p =%~1\r\necho shim response\r\n",
        )
        .expect("write cmd shim");
        let path = format!(
            "{};{}",
            dir.path.display(),
            std::env::var("PATH").expect("PATH is set")
        );
        let participant = ExternalAgentParticipant::new(
            "baton-test-agent",
            ["agent argument"],
            [("PATH", path)],
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        );

        let response = participant.respond(&request_with_body("m-req-1", "shim request"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body.trim(), "shim response");
        assert_eq!(
            std::fs::read_to_string(dir.path.join("request.txt"))
                .expect("stdin side effect")
                .trim_end(),
            "shim request"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path.join("argument.txt")).expect("argument side effect"),
            "agent argument"
        );
    }

    /// Cmd metacharacters in an agent argument remain one intact CRT argv
    /// entry. The environment-variable-shaped percent sequence specifically
    /// proves that quoting alone did not merely hide a cmd expansion.
    #[cfg(windows)]
    #[test]
    fn external_agent_cmd_shim_preserves_metacharacters() {
        let dir = TempDir::new("ext-cmd-metacharacters");
        let shim = dir.path.join("baton-test-agent.cmd");
        std::fs::write(
            &shim,
            "@echo off\r\nmore >nul\r\nset \"BATON_ARG=%~1\"\r\nset BATON_ARG\r\n",
        )
        .expect("write cmd shim");
        let path = format!(
            "{};{}",
            dir.path.display(),
            std::env::var("PATH").expect("PATH is set")
        );
        let argument = "meta & | ^ %BATON_EXPAND_ME% < > ( )";
        let participant = ExternalAgentParticipant::new(
            "baton-test-agent",
            [argument],
            [
                ("PATH", path),
                ("BATON_EXPAND_ME", "must-not-expand".to_string()),
            ],
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        );

        let response = participant.respond(&request_with_body("m-req-1", "metachar request"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body.trim(), format!("BATON_ARG={argument}"));
    }

    /// A program *path* containing spaces must reach the child intact. The
    /// cmd tail is one raw argument in an outer quote pair, so `/S` strips
    /// only that pair and the path keeps its own quotes; an `arg()`-built
    /// tail lost them and split the path at its first space.
    #[cfg(windows)]
    #[test]
    fn external_agent_program_path_with_spaces_runs() {
        let dir = TempDir::new("ext-space-path");
        let nested = dir.path.join("space dir");
        std::fs::create_dir_all(&nested).expect("create nested dir");
        let shim = nested.join("baton-test-agent.cmd");
        std::fs::write(
            &shim,
            "@echo off\r\nmore > request.txt\r\n> argument.txt <nul set /p =%~1\r\necho space path response\r\n",
        )
        .expect("write cmd shim");
        let participant = ExternalAgentParticipant::new(
            &shim,
            ["argument with space"],
            std::iter::empty::<(String, String)>(),
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        );

        let response = participant.respond(&request_with_body("m-req-1", "space request"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body.trim(), "space path response");
        assert_eq!(
            std::fs::read_to_string(dir.path.join("request.txt"))
                .expect("stdin side effect")
                .trim_end(),
            "space request"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path.join("argument.txt")).expect("argument side effect"),
            "argument with space"
        );
    }

    /// The raw command tail must use the MSVC doubling rule so the child's
    /// CRT reconstructs backslashes before embedded and closing quotes.
    #[cfg(windows)]
    #[test]
    fn append_windows_arg_matches_msvc_quoting() {
        let cases = [
            ("plain", "plain"),
            ("with space", r#""with space""#),
            (r#"C:\My Dir\"#, r#""C:\My Dir\\""#),
            (r#"a\"b"#, r#""a\\\"b""#),
        ];

        for (token, expected) in cases {
            let mut line = String::new();
            append_windows_arg(&mut line, token);
            assert_eq!(line, expected, "token {token:?}");
        }
    }

    /// Two sequential runs over the *same cwd*: the second run observes the
    /// first's durable artifact — the continuity substrate a headless-per-message
    /// agent relies on to reconstruct context across rounds.
    #[test]
    fn external_agent_two_rounds_share_cwd_for_continuity() {
        let dir = TempDir::new("ext-continuity");

        // Round 1 appends its payload to a durable ledger in the worktree.
        let round1 = external_agent(
            "cat >> ledger.txt; printf 'r1 done'",
            &dir.path,
            Duration::from_secs(5),
        );
        let resp1 = round1.respond(&request_with_body("m-req-1", "ROUND-ONE-PAYLOAD"));
        assert_eq!(resp1.body, "r1 done");

        // Round 2 (fresh headless process, same cwd) reads the ledger back —
        // seeing round 1's artifact proves cross-round continuity via durable
        // state, not an in-memory session.
        let round2 = external_agent(
            "cat >> ledger.txt; cat ledger.txt",
            &dir.path,
            Duration::from_secs(5),
        );
        let resp2 = round2.respond(&request_with_body("m-req-2", "ROUND-TWO-PAYLOAD"));

        assert_eq!(resp2.kind, MessageKind::Response);
        assert_eq!(resp2.in_reply_to.as_deref(), Some("m-req-2"));
        assert!(
            resp2.body.contains("ROUND-ONE-PAYLOAD"),
            "round 2 reconstructed round 1's durable artifact: {}",
            resp2.body
        );
        assert!(resp2.body.contains("ROUND-TWO-PAYLOAD"));
    }

    // -- BATON_* turn environment (#361) ------------------------------------

    /// Serializes tests that temporarily mutate the *process* environment
    /// (`std::env::set_var`/`remove_var`), so parallel `cargo test` threads
    /// never observe another test's transient value.
    static ENV_MUTATION_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A stub agent script that echoes every `BATON_*` turn variable on stdout,
    /// one `KEY=[value]` line each, brackets making an empty value visible.
    /// `ROLE_SET` carries the `${BATON_ROLE+x}` expansion — `x` when the
    /// variable is present (even set-but-empty), empty when genuinely absent —
    /// so the role-less case can assert removal, not just an empty value.
    const ECHO_BATON_ENV_SCRIPT: &str = "cat >/dev/null; \
        printf 'MESSAGE_ID=[%s]\\n' \"$BATON_MESSAGE_ID\"; \
        printf 'CONVERSATION_ID=[%s]\\n' \"$BATON_CONVERSATION_ID\"; \
        printf 'FROM=[%s]\\n' \"$BATON_FROM\"; \
        printf 'TO=[%s]\\n' \"$BATON_TO\"; \
        printf 'KIND=[%s]\\n' \"$BATON_KIND\"; \
        printf 'IN_REPLY_TO=[%s]\\n' \"$BATON_IN_REPLY_TO\"; \
        printf 'TS_MS=[%s]\\n' \"$BATON_TS_MS\"; \
        printf 'INBOX=[%s]\\n' \"$BATON_INBOX\"; \
        printf 'OUTBOX=[%s]\\n' \"$BATON_OUTBOX\"; \
        printf 'ROLE=[%s]\\n' \"$BATON_ROLE\"; \
        printf 'ROLE_SET=[%s]\\n' \"${BATON_ROLE+x}\"";

    /// A `request` envelope (via [`external_agent`] + `--role`) receives every
    /// `BATON_*` variable from the claimed envelope plus the configured mailbox
    /// addressing and role.
    #[test]
    fn external_agent_stamps_baton_env_for_request_with_role() {
        let dir = TempDir::new("ext-baton-env-request");
        let participant = external_agent(ECHO_BATON_ENV_SCRIPT, &dir.path, Duration::from_secs(5))
            .with_inbox("/mailbox/in")
            .with_outbox("/mailbox/out")
            .with_role("scribe");

        let response = participant.respond(&request_with_body("m-req-1", "go"));

        assert_eq!(
            response.body,
            "MESSAGE_ID=[m-req-1]\n\
             CONVERSATION_ID=[conv-42]\n\
             FROM=[agent-a]\n\
             TO=[agent-b]\n\
             KIND=[request]\n\
             IN_REPLY_TO=[]\n\
             TS_MS=[1700000000000]\n\
             INBOX=[/mailbox/in]\n\
             OUTBOX=[/mailbox/out]\n\
             ROLE=[scribe]\n\
             ROLE_SET=[x]\n"
        );
    }

    /// A `notify` envelope with a non-null `in_reply_to`, no `--role`, and
    /// `BATON_ROLE` poisoned on *both* layers a child could inherit it from —
    /// the participant's fixed env layer and the serving process's own
    /// environment: the turn's values still win (the stale inherited
    /// `BATON_FROM` is overridden) and `BATON_ROLE` ends up genuinely absent
    /// (stripped from both layers), not merely unset by omission.
    #[test]
    fn external_agent_notify_overrides_inherited_from_and_strips_role_without_role() {
        let _guard = ENV_MUTATION_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prior_from = std::env::var("BATON_FROM").ok();
        let prior_role = std::env::var("BATON_ROLE").ok();
        // Safety: serialized by `ENV_MUTATION_LOCK` above, restored below on
        // every exit path (including the panic from a failed assertion).
        unsafe {
            std::env::set_var("BATON_FROM", "stale-inherited-agent");
            std::env::set_var("BATON_ROLE", "leaked-inherited-role");
        }

        let dir = TempDir::new("ext-baton-env-notify");
        let participant = ExternalAgentParticipant::new(
            "sh",
            ["-c", ECHO_BATON_ENV_SCRIPT],
            [("BATON_ROLE", "fixed-layer-role")],
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        )
        .with_inbox("/mailbox/in")
        .with_outbox("/mailbox/out");
        let mut request = request_with_body("m-req-2", "milestone reached");
        request.kind = MessageKind::Notify;
        request.in_reply_to = Some("m-req-1".to_string());

        let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            participant.respond(&request)
        }));

        // Safety: restores exactly what was observed before this test mutated
        // the process environment, regardless of the assertion outcome below.
        unsafe {
            match prior_from {
                Some(v) => std::env::set_var("BATON_FROM", v),
                None => std::env::remove_var("BATON_FROM"),
            }
            match prior_role {
                Some(v) => std::env::set_var("BATON_ROLE", v),
                None => std::env::remove_var("BATON_ROLE"),
            }
        }
        let response = response.unwrap_or_else(|payload| std::panic::resume_unwind(payload));

        assert_eq!(
            response.body,
            "MESSAGE_ID=[m-req-2]\n\
             CONVERSATION_ID=[conv-42]\n\
             FROM=[agent-a]\n\
             TO=[agent-b]\n\
             KIND=[notify]\n\
             IN_REPLY_TO=[m-req-1]\n\
             TS_MS=[1700000000000]\n\
             INBOX=[/mailbox/in]\n\
             OUTBOX=[/mailbox/out]\n\
             ROLE=[]\n\
             ROLE_SET=[]\n"
        );
    }

    /// An agent that exits non-zero yields a synthesized delivered error naming
    /// the failure.
    #[test]
    fn external_agent_synthesizes_error_on_nonzero_exit() {
        let dir = TempDir::new("ext-nonzero");
        let participant = external_agent(
            "cat >/dev/null; echo boom >&2; exit 3",
            &dir.path,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("boom") || response.body.contains("exit"),
            "body describes the agent failure: {}",
            response.body
        );
    }

    /// An agent that exits 0 with empty stdout yields a synthesized error —
    /// there is no final result to deliver.
    #[test]
    fn external_agent_synthesizes_error_on_empty_output() {
        let dir = TempDir::new("ext-empty");
        let participant = external_agent("cat >/dev/null", &dir.path, Duration::from_secs(5));
        assert_synthesized_error(&participant.respond(&request_with_body("m-req-1", "go")));
    }

    /// An agent that holds stdout open past the read timeout is killed and
    /// yields a synthesized error naming the timeout, without hanging the parent.
    #[test]
    fn external_agent_synthesizes_error_on_read_timeout() {
        let dir = TempDir::new("ext-timeout");
        let participant = external_agent(
            "cat >/dev/null; sleep 30",
            &dir.path,
            Duration::from_millis(150),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("timeout"),
            "body names the timeout: {}",
            response.body
        );
    }

    /// Issue #355 regression: in the explicit no-deadline mode the read wait
    /// has no deadline, so an agent that outlives the bounded kill point
    /// (150 ms, as in the timeout test above) still completes and delivers its
    /// reply instead of being killed into a synthesized timeout error. The
    /// bounded counterpart of the same stub proves the sleep really does
    /// outlast the short bound, so only the mode difference explains success.
    #[test]
    fn external_agent_no_deadline_completes_a_turn_past_the_bounded_kill_point() {
        let script = "cat >/dev/null; sleep 1; printf 'late but done'";

        let bounded = {
            let dir = TempDir::new("ext-no-deadline-bounded");
            external_agent(script, &dir.path, Duration::from_millis(150))
                .respond(&request_with_body("m-req-1", "go"))
        };
        assert_synthesized_error(&bounded);
        assert!(
            bounded.body.contains("timeout"),
            "bounded mode still kills the same stub: {}",
            bounded.body
        );

        let dir = TempDir::new("ext-no-deadline");
        let participant = external_agent_unbounded(script, &dir.path);
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "late but done");
    }

    /// A **streaming** backend that interleaves tool/step chatter on stdout and
    /// prints its final answer as a terminal JSON line yields a reply whose body
    /// is **only** that answer — the chatter is excluded by the `Json` adapter.
    #[test]
    fn external_agent_json_adapter_excludes_streaming_chatter() {
        let dir = TempDir::new("ext-json");
        // The stub mimics a streaming agent: tool/step chatter lines, then the
        // final result as a JSON object on the last line (the `--output-format
        // json` convention). Only the `result` field must reach the reply body.
        let participant = external_agent_with_output(
            "cat >/dev/null; \
             printf '[tool] reading files\\n'; \
             printf '[step] editing notes.md\\n'; \
             printf '{\"type\":\"result\",\"result\":\"edited notes.md and committed\"}\\n'",
            &dir.path,
            OutputAdapter::Json {
                result_key: "result".to_string(),
            },
            Duration::from_secs(5),
        );

        let response = participant.respond(&request_with_body("m-req-1", "please edit notes.md"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "edited notes.md and committed");
        assert!(
            !response.body.contains("[tool]") && !response.body.contains("[step]"),
            "streaming chatter must be excluded from the reply body: {}",
            response.body
        );
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        assert!(
            response.exchange.is_none(),
            "an agent run is not one provider call, so it nests no record"
        );
    }

    /// In `Json` mode a final line whose result field is missing or non-string is
    /// a machinery failure — a synthesized delivered error, never a stringified
    /// JSON body.
    #[test]
    fn external_agent_json_adapter_synthesizes_error_on_unextractable_result() {
        let dir = TempDir::new("ext-json-bad");
        // The result field is a nested object, not a string — must not be
        // stringified into the body.
        let participant = external_agent_with_output(
            "cat >/dev/null; printf '{\"result\":{\"nested\":true}}\\n'",
            &dir.path,
            OutputAdapter::Json {
                result_key: "result".to_string(),
            },
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_synthesized_error(&response);
        assert!(
            response.body.contains("not a string"),
            "body names the extraction failure: {}",
            response.body
        );
    }

    // -- stderr capture ----------------------------------------------------

    /// A successful turn that writes to stderr has that output returned from
    /// `capture_child_output` alongside stdout.
    #[test]
    fn capture_child_output_returns_stderr_on_success() {
        let (stdout, stderr) = capture_child_output(
            Path::new("sh"),
            &[
                "-c".to_string(),
                "cat >/dev/null; echo OUT; echo ERR >&2".to_string(),
            ],
            &[],
            &[],
            None,
            b"",
            Some(Duration::from_secs(5)),
        )
        .expect("exits 0");
        assert_eq!(stdout.trim(), "OUT");
        assert_eq!(stderr.trim(), "ERR");
    }

    /// A failed turn still folds stderr into the transport error.
    #[test]
    fn capture_child_output_folds_stderr_on_failure() {
        let err = capture_child_output(
            Path::new("sh"),
            &[
                "-c".to_string(),
                "cat >/dev/null; echo BOOM >&2; exit 1".to_string(),
            ],
            &[],
            &[],
            None,
            b"",
            Some(Duration::from_secs(5)),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("BOOM"),
            "transport error includes stderr: {err}"
        );
    }

    /// A timed-out turn also folds buffered stderr into the transport error.
    #[test]
    fn capture_child_output_folds_stderr_on_timeout() {
        let err = capture_child_output(
            Path::new("sh"),
            &[
                "-c".to_string(),
                "cat >/dev/null; echo TIMEOUT-DIAG >&2; sleep 1 2>/dev/null &".to_string(),
            ],
            &[],
            &[],
            None,
            b"",
            Some(Duration::from_millis(150)),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("read timeout"),
            "failure is reported as a timeout: {err}"
        );
        assert!(
            err.to_string().contains("TIMEOUT-DIAG"),
            "timeout error includes stderr: {err}"
        );
    }

    /// Stderr exceeding `MAX_STDERR_BYTES` is truncated with a marker.
    #[test]
    fn capture_child_output_truncates_large_stderr() {
        let script =
            "cat >/dev/null; dd if=/dev/zero bs=1024 count=1100 status=none | tr '\\0' 'X' >&2"
                .to_string();
        let (_, stderr) = capture_child_output(
            Path::new("sh"),
            &["-c".to_string(), script],
            &[],
            &[],
            None,
            b"",
            Some(Duration::from_secs(10)),
        )
        .expect("exits 0");
        assert!(
            stderr.ends_with("[truncated at 1 MiB]"),
            "expected truncation marker, got tail: ...{}",
            &stderr[stderr.len().saturating_sub(40)..]
        );
        assert!(
            stderr.len() <= MAX_STDERR_BYTES + 30,
            "stderr should be near the cap, got {} bytes",
            stderr.len()
        );
    }

    /// Stdout exceeding `MAX_STDOUT_BYTES` retains only its tail and prefixes
    /// it with a marker identifying the discarded output prefix.
    #[test]
    fn capture_child_output_truncates_stdout_to_tail() {
        let script = format!(
            "cat >/dev/null; printf 'DROPPED-HEAD'; dd if=/dev/zero bs=1024 count={} status=none | tr '\\0' 'X'; printf '\\nTAIL\\n'",
            MAX_STDOUT_BYTES / 1024 + 1,
        );
        let (stdout, _) = capture_child_output(
            Path::new("sh"),
            &["-c".to_string(), script],
            &[],
            &[],
            None,
            b"",
            Some(Duration::from_secs(10)),
        )
        .expect("exits 0");
        assert!(
            stdout.starts_with(STDOUT_TRUNCATION_MARKER),
            "expected stdout truncation marker, got prefix: {}",
            &stdout[..stdout.len().min(100)]
        );
        assert!(
            !stdout.contains("DROPPED-HEAD"),
            "discarded stdout prefix must not be retained"
        );
        assert!(
            stdout.ends_with("TAIL\n"),
            "retained stdout tail is missing"
        );
        assert!(
            stdout.len() <= MAX_STDOUT_BYTES + STDOUT_TRUNCATION_MARKER.len(),
            "stdout should be bounded by the cap plus marker, got {} bytes",
            stdout.len()
        );
    }

    /// The JSON adapter still finds a final result line in the retained tail.
    #[test]
    fn external_agent_json_adapter_extracts_result_from_truncated_stdout() {
        let dir = TempDir::new("ext-json-truncated");
        let script = format!(
            "cat >/dev/null; printf 'DROPPED-HEAD'; dd if=/dev/zero bs=1024 count={} status=none | tr '\\0' 'X'; printf '\\n{{\"type\":\"result\",\"result\":\"tail result\"}}\\n'",
            MAX_STDOUT_BYTES / 1024 + 1,
        );
        let participant = external_agent_with_output(
            &script,
            &dir.path,
            OutputAdapter::Json {
                result_key: "result".to_string(),
            },
            Duration::from_secs(10),
        );

        let response = participant.respond(&request_with_body("m-req-1", "go"));

        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "tail result");
    }

    /// `ExternalAgentParticipant` with `stderr_dir` persists non-empty stderr
    /// from a successful turn to disk.
    #[test]
    fn external_agent_persists_stderr_on_success() {
        let dir = TempDir::new("ext-stderr");
        let stderr_dir = dir.path.join("agent-stderr");
        let participant = ExternalAgentParticipant::new(
            "sh",
            ["-c", "cat >/dev/null; echo diag >&2; printf 'ok'"],
            std::iter::empty::<(String, String)>(),
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        )
        .with_stderr_dir(&stderr_dir);

        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_eq!(response.kind, MessageKind::Response);
        assert_eq!(response.body, "ok");

        let persisted =
            std::fs::read_to_string(stderr_dir.join("m-req-1.stderr")).expect("stderr file exists");
        assert_eq!(persisted.trim(), "diag");
    }

    /// `ExternalAgentParticipant` without `stderr_dir` does not create any files.
    #[test]
    fn external_agent_no_stderr_dir_is_noop() {
        let dir = TempDir::new("ext-no-stderr-dir");
        let participant = external_agent(
            "cat >/dev/null; echo diag >&2; printf 'ok'",
            &dir.path,
            Duration::from_secs(5),
        );
        let response = participant.respond(&request_with_body("m-req-1", "go"));
        assert_eq!(response.kind, MessageKind::Response);
        assert!(!dir.path.join("agent-stderr").exists());
    }

    // -- ExternalAgentParticipant::respond_batch ---------------------------
    //
    // `respond()` above stays untouched by the batching change (proven by the
    // tests above still passing unmodified); these drive `respond_batch`
    // directly, the seam `drain_mailbox`'s `--agent-batch-max` calls into.

    /// A length-1 batch under the default `Body` mode produces the same reply
    /// shape as `respond()` on the same request: raw body delivered on stdin,
    /// one correlated `kind: "response"` envelope with the addressing swapped
    /// the same way.
    #[test]
    fn external_agent_respond_batch_body_mode_single_matches_respond_shape() {
        let dir = TempDir::new("ext-batch-body-single");
        let script = "cat > seen.txt; printf 'edited seen.txt'";
        let via_respond = external_agent(script, &dir.path, Duration::from_secs(5));
        let request = request_with_body("m-req-1", "please edit seen.txt");
        let direct = via_respond.respond(&request);

        let dir2 = TempDir::new("ext-batch-body-single-2");
        let via_batch = external_agent(script, &dir2.path, Duration::from_secs(5));
        let responses = via_batch.respond_batch(std::slice::from_ref(&request));

        assert_eq!(responses.len(), 1);
        let batched = &responses[0];
        assert_eq!(batched.kind, direct.kind);
        assert_eq!(batched.body, direct.body);
        assert_eq!(batched.conversation_id, direct.conversation_id);
        assert_eq!(batched.from, direct.from);
        assert_eq!(batched.to, direct.to);
        assert_eq!(batched.in_reply_to, direct.in_reply_to);
        // The raw body was delivered on stdin in both paths.
        let stdin_seen = std::fs::read_to_string(dir2.path.join("seen.txt")).expect("side effect");
        assert_eq!(stdin_seen, "please edit seen.txt");
    }

    /// Env script extended to also print `BATON_BATCH_SIZE`, for asserting its
    /// value directly (the base [`ECHO_BATON_ENV_SCRIPT`] predates batching).
    const ECHO_BATON_ENV_AND_BATCH_SIZE_SCRIPT: &str = "cat >/dev/null; \
        printf 'MESSAGE_ID=[%s]\\n' \"$BATON_MESSAGE_ID\"; \
        printf 'BATCH_SIZE=[%s]\\n' \"$BATON_BATCH_SIZE\"";

    /// `Body` mode at batch length 1 stamps `BATON_BATCH_SIZE=1` alongside the
    /// unchanged `BATON_*` set, and reads the sole request's raw body — the
    /// default-flag compatibility criterion (`--agent-batch-max` omitted).
    #[test]
    fn external_agent_respond_batch_body_mode_stamps_batch_size_one() {
        let dir = TempDir::new("ext-batch-body-size-one");
        let participant = external_agent(
            ECHO_BATON_ENV_AND_BATCH_SIZE_SCRIPT,
            &dir.path,
            Duration::from_secs(5),
        );
        let request = request_with_body("m-req-1", "go");

        let responses = participant.respond_batch(std::slice::from_ref(&request));

        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].body,
            "MESSAGE_ID=[m-req-1]\nBATCH_SIZE=[1]\n"
        );
    }

    /// Under `BatchJson`, stdin is one JSON object `{"batch": [...]}` carrying
    /// every claimed envelope in slice order — the shape `--agent-input
    /// batch-json` promises.
    #[test]
    fn external_agent_respond_batch_json_mode_stdin_shape_and_order() {
        let dir = TempDir::new("ext-batch-json-stdin");
        // The stub records raw stdin to a file so the test can decode it,
        // then answers with one free-text result for the whole batch.
        let participant = ExternalAgentParticipant::new(
            "sh",
            ["-c", "cat > stdin.json; printf 'handled'"],
            std::iter::empty::<(String, String)>(),
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        )
        .with_input_mode(AgentInputMode::BatchJson);
        let requests = vec![
            request_with_body("m-req-1", "first"),
            request_with_body("m-req-2", "second"),
            request_with_body("m-req-3", "third"),
        ];

        let responses = participant.respond_batch(&requests);

        assert_eq!(responses.len(), 3);
        for (request, response) in requests.iter().zip(&responses) {
            assert_eq!(response.body, "handled");
            assert_eq!(response.in_reply_to.as_deref(), Some(request.message_id.as_str()));
        }

        let stdin_seen = std::fs::read_to_string(dir.path.join("stdin.json")).expect("stdin file");
        let decoded: serde_json::Value =
            serde_json::from_str(&stdin_seen).expect("stdin is valid JSON");
        let batch = decoded
            .get("batch")
            .and_then(|v| v.as_array())
            .expect("stdin has a `batch` array");
        assert_eq!(batch.len(), 3);
        let ids: Vec<&str> = batch
            .iter()
            .map(|entry| entry.get("message_id").and_then(|v| v.as_str()).unwrap())
            .collect();
        assert_eq!(ids, vec!["m-req-1", "m-req-2", "m-req-3"]);
    }

    /// `respond_batch`'s `BATON_*` env (and `BATON_BATCH_SIZE`) is sourced from
    /// the **last** (newest) member, not the first — mirroring how a single
    /// invocation can only carry one envelope's worth of addressing.
    #[test]
    fn external_agent_respond_batch_env_sourced_from_last_member() {
        let dir = TempDir::new("ext-batch-env-last");
        let participant = ExternalAgentParticipant::new(
            "sh",
            ["-c", ECHO_BATON_ENV_AND_BATCH_SIZE_SCRIPT],
            std::iter::empty::<(String, String)>(),
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        )
        .with_input_mode(AgentInputMode::BatchJson);
        let requests = vec![
            request_with_body("m-req-1", "first"),
            request_with_body("m-req-2", "second"),
        ];

        let responses = participant.respond_batch(&requests);

        assert_eq!(responses.len(), 2);
        // The one child invocation saw the *last* member's message id, plus
        // the full batch size — not the first member's id.
        assert_eq!(responses[0].body, "MESSAGE_ID=[m-req-2]\nBATCH_SIZE=[2]\n");
        assert_eq!(responses[1].body, responses[0].body);
    }

    /// A machinery failure (non-zero exit) during a batched invocation fans a
    /// synthesized error out to **every** claimed member, not just one — a
    /// batch never silently drops a message on failure.
    #[test]
    fn external_agent_respond_batch_fans_out_synthesized_error_to_every_member() {
        let dir = TempDir::new("ext-batch-error-fanout");
        let participant = ExternalAgentParticipant::new(
            "sh",
            ["-c", "cat >/dev/null; exit 1"],
            std::iter::empty::<(String, String)>(),
            &dir.path,
            OutputAdapter::Raw,
            Some(Duration::from_secs(5)),
        )
        .with_input_mode(AgentInputMode::BatchJson);
        let requests = vec![
            request_with_body("m-req-1", "first"),
            request_with_body("m-req-2", "second"),
            request_with_body("m-req-3", "third"),
        ];

        let responses = participant.respond_batch(&requests);

        assert_eq!(responses.len(), 3);
        for (request, response) in requests.iter().zip(&responses) {
            assert_eq!(response.kind, MessageKind::Error);
            assert_eq!(response.conversation_id, "conv-42");
            assert_eq!(response.in_reply_to.as_deref(), Some(request.message_id.as_str()));
            assert_eq!(response.from, "agent-b");
            assert_eq!(response.to, "agent-a");
            assert!(response.exchange.is_none(), "no nested record on a machinery failure");
        }
    }

    // -- MailboxParticipant -----------------------------------------------
    //
    // These drive the impl against a tempdir mailbox — no live `serve`, no
    // network. A reply is seeded into the outbox exactly as `serve`'s
    // `deliver_response` would key it (by the request id), so the deliver +
    // await round-trip is exercised deterministically.

    use std::path::PathBuf;

    /// A unique self-cleaning temp directory, mirroring the idiom in
    /// `mailbox`'s own tests.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "baton-mailbox-participant-{}-{}-{tag}",
                std::process::id(),
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Seeds `reply` into `outbox` keyed by `request_id`, as `serve`'s
    /// `deliver_response` would, so `try_claim_response` finds it.
    fn seed_reply(outbox: &std::path::Path, request_id: &str, reply: &MessageEnvelope) {
        std::fs::create_dir_all(outbox).expect("create outbox");
        let json = serde_json::to_string(reply).expect("serialize reply");
        std::fs::write(outbox.join(format!("{request_id}.json")), json).expect("seed reply");
    }

    /// Builds a peer reply correlated to `request` (addressing swapped,
    /// `in_reply_to` linked), optionally nesting a provider-call record — the
    /// shape a `baton serve` peer's `LocalParticipant` delivers.
    fn peer_reply(request: &MessageEnvelope, kind: MessageKind, nested: bool) -> MessageEnvelope {
        let mut reply = MessageEnvelope::new(
            "peer-reply-id",
            request.conversation_id.clone(),
            request.to.clone(),
            request.from.clone(),
            kind,
            "pong",
            request.ts_ms + 1,
        );
        reply.in_reply_to = Some(request.message_id.clone());
        if nested {
            reply.exchange = Some(WrappedExchange::new(Exchange {
                request: RequestRecord {
                    ts_ms: request.ts_ms,
                    model: "peer-model".to_string(),
                    base_url: "https://peer".to_string(),
                    prompt: request.body.clone(),
                    session_id: None,
                    turn_index: None,
                },
                outcome: Outcome::Ok {
                    ts_ms: request.ts_ms + 1,
                    duration_ms: 1,
                    reply: "pong".to_string(),
                    input_tokens: Some(3),
                    output_tokens: Some(5),
                    stop_reason: None,
                },
            }));
        }
        reply
    }

    /// A seeded, correlated reply is delivered unchanged, and the request lands
    /// in the peer's `pending/` — the deliver + await round-trip.
    #[test]
    fn mailbox_returns_correlated_reply_and_delivers_request() {
        let dir = TempDir::new("ok");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let reply = peer_reply(&request, MessageKind::Response, true);
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        // Returned verbatim — the peer, not the driver, owns correlation.
        assert_eq!(response, reply);
        // The request was delivered to the peer's inbox.
        assert!(
            inbox.join("pending").join("m-req-1.json").exists(),
            "request delivered to <inbox>/pending/"
        );
    }

    /// A peer-delivered `kind: "error"` (carrying the peer's nested record) is
    /// passed through unchanged — a delivered response, not a machinery failure.
    #[test]
    fn mailbox_passes_through_peer_delivered_error() {
        let dir = TempDir::new("peer-err");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let reply = peer_reply(&request, MessageKind::Error, true);
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert!(
            response.exchange.is_some(),
            "a peer-delivered error nests the peer's record"
        );
    }

    /// No reply before the deadline yields a synthesized `kind: "error"` with no
    /// nested record and a body naming the await-timeout — the "driver stopped
    /// waiting" terminal, distinct from a peer-delivered error.
    #[test]
    fn mailbox_synthesizes_timeout_error_when_no_reply() {
        let dir = TempDir::new("timeout");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(10),
            Duration::from_millis(2),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert_eq!(response.conversation_id, "conv-42");
        assert_eq!(response.in_reply_to.as_deref(), Some("m-req-1"));
        // Addressing swaps, like a delivered reply.
        assert_eq!(response.from, "agent-b");
        assert_eq!(response.to, "agent-a");
        assert!(
            response.exchange.is_none(),
            "a machinery/transport failure nests no record"
        );
        assert!(
            response.body.contains("timed out"),
            "body names the await-timeout: {}",
            response.body
        );
        // The request is left in the peer's inbox for a later drain.
        assert!(inbox.join("pending").join("m-req-1.json").exists());
    }

    /// A reply filed under the request key but answering a *different* request
    /// is rejected as a machinery failure, never returned as the correlated
    /// reply.
    #[test]
    fn mailbox_synthesizes_error_on_mis_correlated_reply() {
        let dir = TempDir::new("mismatch");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        let request = request_envelope();
        let mut reply = peer_reply(&request, MessageKind::Response, true);
        reply.in_reply_to = Some("some-other-id".to_string());
        seed_reply(&outbox, &request.message_id, &reply);

        let participant = MailboxParticipant::new(
            &inbox,
            &outbox,
            Duration::from_millis(500),
            Duration::from_millis(1),
        );
        let response = participant.respond(&request);

        assert_eq!(response.kind, MessageKind::Error);
        assert!(
            response.exchange.is_none(),
            "a mis-correlated reply is a machinery failure, nesting no record"
        );
    }

    /// Two replies synthesized in one conversation within the same millisecond
    /// must stay distinct all the way through: distinct ids, two surviving
    /// `pending/` files, and two surviving turns in the merged transcript.
    ///
    /// Deterministic by construction — `fresh_message_id` takes `ts_ms` as an
    /// argument and reads no clock, so a single pinned `TS_MS` reproduces the
    /// collision exactly, with no sleep or wall-clock coincidence.
    #[test]
    fn same_millisecond_responses_stay_distinct_end_to_end() {
        const TS_MS: u64 = 1_787_882_604_553;

        // The production construction from `respond`: fresh id, addressing
        // swapped, one shared timestamp.
        let reply = |body: &str| {
            MessageEnvelope::new(
                fresh_message_id("c", TS_MS),
                "c".to_string(),
                "agent".to_string(),
                "caller".to_string(),
                MessageKind::Error,
                body.to_string(),
                TS_MS,
            )
        };
        let first = reply("first");
        let second = reply("second");

        assert_eq!(
            (first.ts_ms, second.ts_ms),
            (TS_MS, TS_MS),
            "the pair shares a millisecond — that is the condition under test"
        );
        assert_ne!(
            first.message_id, second.message_id,
            "same-millisecond replies must not share a message_id"
        );

        // Mailbox leg: the id is the `pending/` filename key, so a collision
        // would silently overwrite the first reply.
        let dir = TempDir::new("same-ms");
        mailbox::deliver_to(&dir.path, &first).expect("deliver first");
        mailbox::deliver_to(&dir.path, &second).expect("deliver second");

        let mut bodies: Vec<String> = std::fs::read_dir(dir.path.join("pending"))
            .expect("read pending")
            .map(|entry| entry.expect("dir entry").path())
            .map(|path| std::fs::read_to_string(path).expect("read envelope"))
            .map(|json| {
                serde_json::from_str::<MessageEnvelope>(&json)
                    .expect("envelope round-trips")
                    .body
            })
            .collect();
        bodies.sort();
        assert_eq!(
            bodies,
            vec!["first".to_string(), "second".to_string()],
            "both replies survive delivery — neither overwrote the other"
        );

        // Merge leg: `merge_conversation` collapses by `message_id`, so a
        // collision would drop one turn from the unified transcript.
        let merged = crate::log::merge_conversation(vec![first.clone(), second.clone()], "c");
        let mut merged_ids: Vec<&str> = merged.iter().map(|e| e.message_id.as_str()).collect();
        merged_ids.sort_unstable();
        let mut expected_ids = vec![first.message_id.as_str(), second.message_id.as_str()];
        expected_ids.sort_unstable();
        assert_eq!(merged_ids, expected_ids, "both turns survive the merge");
        let mut merged_bodies: Vec<&str> = merged.iter().map(|e| e.body.as_str()).collect();
        merged_bodies.sort_unstable();
        assert_eq!(merged_bodies, vec!["first", "second"]);
    }
}
