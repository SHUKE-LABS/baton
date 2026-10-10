//! mat adapter: maps a mat duo session onto baton's generic send primitives.
//!
//! Everything here knows mat's conventions — the flat `key=value`
//! `baton.state` file, the duo role names and their aliases, mat's
//! relay-origin vocabulary, and the duo queue ledger's `queued` transition
//! record. Core baton never imports this module; it is compiled only with the
//! `mat` Cargo feature and reached through `baton mat …` from `main.rs`.
//!
//! The one entry point, `baton mat send`, resolves the recipient through an
//! in-memory [`SessionManifest`], delivers with [`mailbox::send_to`] under
//! `require_live`, stamps the structured origin, records `queued` from the
//! returned message id (no mailbox rescan), and only then prints that id.
//! The contract is specified in `docs/mat-adapter.md`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{BatonError, Result};
use crate::mailbox;
use crate::message::{MessageEnvelope, MessageKind};
use crate::session::SessionManifest;

/// Usage text for `baton mat`.
pub const USAGE: &str = concat!(
    "baton mat send --state <baton.state> --to <role> --from <role> (--body <text> | --body-file <path>)\n",
    "    Deliver one relay message into a mat duo session: requires a live serve on the recipient,\n",
    "    stamps the mat origin, records the duo `queued` transition, then prints the message id.\n",
    "    Roles: dev (alias developer), reviewer (alias review); --from also accepts operator and system.\n",
);

/// The duo recipients a mat `baton.state` session serves.
const DUO_ROLES: [&str; 2] = ["dev", "reviewer"];

/// Schema of mat's durable duo queue transition record.
const TRANSITION_SCHEMA: &str = "baton.duo.transition/v1";

/// Entry point for `baton mat <args>` (`args` excludes `mat` itself).
pub fn run(args: &[String]) -> Result<()> {
    let stdout = std::io::stdout();
    match args.first().map(String::as_str) {
        Some("send") => send(parse_send(&args[1..])?, stdout.lock()),
        Some("--help" | "-h") | None => {
            print!("{USAGE}");
            Ok(())
        }
        Some(other) => Err(usage(&format!("unknown mat subcommand {other:?}"))),
    }
}

/// Parsed `baton mat send` arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendArgs {
    /// Path of the duo session's `baton.state`.
    pub state: PathBuf,
    /// Recipient role, canonicalized (`dev` / `reviewer`).
    pub to: String,
    /// Sender role, canonicalized (`dev` / `reviewer` / `operator` / `system`).
    pub from: String,
    /// Message body source.
    pub body: Body,
}

/// Where the message body comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// `--body <text>`.
    Text(String),
    /// `--body-file <path>`, read at send time.
    File(PathBuf),
}

fn usage(detail: &str) -> BatonError {
    BatonError::Usage(format!("{detail}\nrun 'baton mat --help' for usage."))
}

/// Parses `baton mat send` arguments, canonicalizing role aliases.
pub fn parse_send(args: &[String]) -> Result<SendArgs> {
    let mut state = None;
    let mut to = None;
    let mut from = None;
    let mut body = None;
    let mut body_file = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut take = |flag: &str| {
            iter.next()
                .cloned()
                .ok_or_else(|| usage(&format!("{flag} requires a value")))
        };
        match arg.as_str() {
            "--state" => state = Some(take("--state")?),
            "--to" => to = Some(take("--to")?),
            "--from" => from = Some(take("--from")?),
            "--body" => body = Some(take("--body")?),
            "--body-file" => body_file = Some(take("--body-file")?),
            other => return Err(usage(&format!("unexpected argument {other:?}"))),
        }
    }
    let state = state
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| usage("--state <baton.state> is required"))?;
    let to = canonical_role(&to.ok_or_else(|| usage("--to <role> is required"))?)?;
    if !DUO_ROLES.contains(&to) {
        return Err(usage(&format!("--to must be dev or reviewer, got {to:?}")));
    }
    let from = canonical_role(&from.ok_or_else(|| usage("--from <role> is required"))?)?;
    if from == to {
        return Err(usage(&format!("refusing self-send to {to:?}")));
    }
    let body = match (body, body_file) {
        (Some(text), None) if !text.trim().is_empty() => Body::Text(text),
        (Some(_), None) => return Err(usage("--body must not be empty")),
        (None, Some(path)) if !path.trim().is_empty() => Body::File(PathBuf::from(path)),
        (None, Some(_)) => return Err(usage("--body-file <path> must not be empty")),
        (None, None) => return Err(usage("pass --body <text> or --body-file <path>")),
        (Some(_), Some(_)) => return Err(usage("--body and --body-file are mutually exclusive")),
    };
    Ok(SendArgs {
        state: PathBuf::from(state),
        to: to.to_string(),
        from: from.to_string(),
        body,
    })
}

/// Maps a mat role token (or alias) to its canonical name.
fn canonical_role(token: &str) -> Result<&'static str> {
    match token {
        "dev" | "developer" => Ok("dev"),
        "reviewer" | "review" => Ok("reviewer"),
        "operator" => Ok("operator"),
        "system" => Ok("system"),
        other => Err(usage(&format!("unknown mat role {other:?}"))),
    }
}

/// mat's relay-origin for a canonical sender: a duo peer is `peer`; the
/// operator and system speak as themselves.
fn origin_for(from: &str) -> &'static str {
    match from {
        "operator" => "operator",
        "system" => "system",
        _ => "peer",
    }
}

/// The `baton.state` keys the adapter reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuoState {
    /// `session_name`.
    pub session_name: String,
    /// `mailbox_root`: role `<r>` is served at `<mailbox_root>/<r>/inbox`.
    pub mailbox_root: PathBuf,
    /// `queue_root`; empty/absent disables the queue ledger, as in mat.
    pub queue_root: Option<PathBuf>,
}

impl DuoState {
    /// Reads a flat `key=value` state file; the first line for a key wins.
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).map_err(|err| {
            BatonError::Config(format!(
                "mat state {} could not be read: {err}",
                path.display()
            ))
        })?;
        let get = |key: &str| {
            raw.lines()
                .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
                .map(|v| v.trim_end_matches('\r'))
                .filter(|v| !v.is_empty())
        };
        let required = |key: &str| {
            get(key).ok_or_else(|| {
                BatonError::Config(format!("mat state {} has no {key}", path.display()))
            })
        };
        Ok(Self {
            session_name: required("session_name")?.to_string(),
            mailbox_root: PathBuf::from(required("mailbox_root")?),
            queue_root: get("queue_root").map(PathBuf::from),
        })
    }
}

/// Runs `baton mat send`: resolve, require-live send, record `queued`, then
/// print the message id exactly once to `out`.
pub fn send(args: SendArgs, mut out: impl Write) -> Result<()> {
    let state = DuoState::from_path(&args.state)?;
    let manifest = SessionManifest::new(
        state.mailbox_root.clone(),
        DUO_ROLES.iter().map(|r| r.to_string()).collect(),
    )?;
    let inbox = manifest.resolve(&args.to)?.inbox;
    let body = match &args.body {
        Body::Text(text) => text.clone(),
        Body::File(path) => {
            let body = fs::read_to_string(path).map_err(|err| {
                BatonError::Io(format!(
                    "failed to read --body-file {}: {err}",
                    path.display()
                ))
            })?;
            if body.trim().is_empty() {
                return Err(usage(&format!(
                    "--body-file {} must not be empty",
                    path.display()
                )));
            }
            body
        }
    };

    let ts_ms = now_ns() / 1_000_000;
    let conversation_id = format!("conv-{ts_ms}");
    let mut envelope = MessageEnvelope::new(
        format!("{conversation_id}-{ts_ms}-{}", std::process::id()),
        conversation_id,
        args.from.clone(),
        args.to.clone(),
        MessageKind::Request,
        body,
        ts_ms as u64,
    );
    envelope.origin = Some(origin_for(&args.from).to_string());

    let message_id = mailbox::send_to(&inbox, &envelope, true)?;
    if let Some(queue_root) = &state.queue_root {
        record_queued(queue_root, &state.session_name, &envelope).map_err(|err| {
            BatonError::Io(format!(
                "message {message_id} was enqueued but its queued transition could not be recorded: {err}"
            ))
        })?;
    }
    writeln!(out, "{message_id}")
        .map_err(|err| BatonError::Io(format!("could not write message id: {err}")))
}

/// mat's filename-safe token: every byte outside `[A-Za-z0-9._-]` becomes `_`.
fn safe_token(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// The session's recovery epoch: `recovery_epoch` in
/// `<duo-recovery>/<session>.ledger`, where `queue_root` is
/// `<duo-recovery>/<session>/queue`. Absent or non-positive reads as 1.
fn recovery_epoch(queue_root: &Path, session_name: &str) -> u64 {
    let Some(recovery_dir) = queue_root.parent().and_then(Path::parent) else {
        return 1;
    };
    let ledger = recovery_dir.join(format!("{}.ledger", safe_token(session_name)));
    fs::read_to_string(ledger)
        .ok()
        .and_then(|raw| {
            raw.lines()
                .find_map(|line| line.strip_prefix("recovery_epoch="))
                .map(|v| v.trim_end_matches('\r').to_string())
        })
        .filter(|v| !v.starts_with('0') && !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1)
}

#[derive(Serialize)]
struct TransitionStep {
    state: &'static str,
    recovery_epoch: u64,
    timestamp_ns: u128,
}

/// mat's `baton.duo.transition/v1` record, in mat's field order.
#[derive(Serialize)]
struct TransitionRecord<'a> {
    schema: &'static str,
    message_id: &'a str,
    session_name: &'a str,
    role: &'a str,
    from: &'a str,
    to: &'a str,
    body_digest: String,
    state: &'static str,
    recovery_epoch: u64,
    recovery_reason: &'static str,
    transitions: [TransitionStep; 1],
}

/// Writes the `queued` record at `<queue_root>/transitions/<id>.json` if no
/// record exists yet; returns whether it was created.
///
/// `queued` is always a message's first state, so an existing record means the
/// worker already claimed it — and mat treats `queued` over any existing state
/// as a no-op. The write is create-if-absent: a fully written temp file is
/// hard-linked into place, which fails rather than replaces when the record
/// exists, so a reader never sees a partial record and a worker's record is
/// never overwritten.
fn record_queued(
    queue_root: &Path,
    session_name: &str,
    envelope: &MessageEnvelope,
) -> Result<bool> {
    let dir = queue_root.join("transitions");
    let io = |what: &str, err: std::io::Error| BatonError::Io(format!("{what}: {err}"));
    fs::create_dir_all(&dir).map_err(|e| io(&format!("could not create {}", dir.display()), e))?;
    let path = dir.join(format!("{}.json", safe_token(&envelope.message_id)));
    if path.exists() {
        return Ok(false);
    }
    let epoch = recovery_epoch(queue_root, session_name);
    let record = TransitionRecord {
        schema: TRANSITION_SCHEMA,
        message_id: &envelope.message_id,
        session_name,
        role: &envelope.to,
        from: &envelope.from,
        to: &envelope.to,
        body_digest: format!("{:x}", Sha256::digest(envelope.body.as_bytes())),
        state: "queued",
        recovery_epoch: epoch,
        recovery_reason: "",
        transitions: [TransitionStep {
            state: "queued",
            recovery_epoch: epoch,
            timestamp_ns: now_ns(),
        }],
    };
    let mut line = serde_json::to_string(&record)
        .map_err(|err| BatonError::Io(format!("could not encode transition: {err}")))?;
    line.push('\n');

    let tmp = dir.join(format!(
        ".{}.tmp.{}.{}",
        safe_token(&envelope.message_id),
        std::process::id(),
        now_ns()
    ));
    fs::write(&tmp, &line).map_err(|e| io(&format!("could not write {}", tmp.display()), e))?;
    let linked = fs::hard_link(&tmp, &path);
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(io(&format!("could not publish {}", path.display()), err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mailbox::Mailbox;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!("baton-mat-{}-{tag}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    /// A mat-shaped duo layout under `dir`: `<state>/duo-recovery/s1/queue`,
    /// its mailbox, and a `baton.state` naming them. Returns the state path.
    fn duo_layout(dir: &Path, epoch: Option<&str>) -> PathBuf {
        let recovery = dir.join("duo-recovery");
        let queue = recovery.join("s_1").join("queue");
        fs::create_dir_all(queue.join("mailbox")).unwrap();
        if let Some(epoch) = epoch {
            fs::write(
                recovery.join("s_1.ledger"),
                format!("recovery_epoch={epoch}\n"),
            )
            .unwrap();
        }
        let state = dir.join("baton.state");
        fs::write(
            &state,
            format!(
                "session_name=s 1\nqueue_root={}\nmailbox_root={}\nmailbox_root=/ignored\n",
                queue.display(),
                queue.join("mailbox").display()
            ),
        )
        .unwrap();
        state
    }

    fn pending(state: &Path, role: &str) -> Vec<PathBuf> {
        let dir = DuoState::from_path(state)
            .unwrap()
            .mailbox_root
            .join(role)
            .join("inbox")
            .join("pending");
        fs::read_dir(dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default()
    }

    #[test]
    fn parses_aliases_and_rejects_bad_roles() {
        let args = parse_send(&argv(&[
            "--state",
            "s",
            "--to",
            "review",
            "--from",
            "developer",
            "--body",
            "hi",
        ]))
        .expect("parses");
        assert_eq!((args.to.as_str(), args.from.as_str()), ("reviewer", "dev"));
        for bad in [
            &[
                "--state", "s", "--to", "operator", "--from", "dev", "--body", "hi",
            ][..],
            &[
                "--state", "s", "--to", "ghost", "--from", "dev", "--body", "hi",
            ][..],
            &[
                "--state",
                "s",
                "--to",
                "dev",
                "--from",
                "developer",
                "--body",
                "hi",
            ][..],
            &[
                "--state", "s", "--to", "dev", "--from", "operator", "--body", " ",
            ][..],
            &["--state", "s", "--to", "dev", "--from", "operator"][..],
            &["--to", "dev", "--from", "operator", "--body", "hi"][..],
        ] {
            assert!(
                matches!(parse_send(&argv(bad)), Err(BatonError::Usage(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn origin_maps_peers_operator_and_system() {
        assert_eq!(origin_for("dev"), "peer");
        assert_eq!(origin_for("reviewer"), "peer");
        assert_eq!(origin_for("operator"), "operator");
        assert_eq!(origin_for("system"), "system");
    }

    #[test]
    fn state_reads_first_value_and_requires_keys() {
        let dir = TempDir::new("state");
        let state = duo_layout(&dir.path, None);
        let parsed = DuoState::from_path(&state).unwrap();
        assert_eq!(parsed.session_name, "s 1");
        assert!(
            parsed.mailbox_root.ends_with("mailbox"),
            "first mailbox_root wins"
        );
        fs::write(&state, "session_name=s\n").unwrap();
        assert!(matches!(
            DuoState::from_path(&state),
            Err(BatonError::Config(_))
        ));
    }

    /// The end-to-end adapter contract: a live recipient gets one envelope with
    /// the mat origin and an unchanged body; the `queued` record carries mat's
    /// fields; stdout is exactly one line, the same id.
    #[test]
    fn send_enqueues_records_queued_then_prints_id_once() {
        let dir = TempDir::new("send");
        let state = duo_layout(&dir.path, Some("3"));
        let inbox = DuoState::from_path(&state)
            .unwrap()
            .mailbox_root
            .join("reviewer")
            .join("inbox");
        fs::create_dir_all(&inbox).unwrap();
        let _serve = Mailbox::open(&inbox).expect("live serve on reviewer");

        let mut out = Vec::new();
        let args = parse_send(&argv(&[
            "--state",
            state.to_str().unwrap(),
            "--to",
            "reviewer",
            "--from",
            "dev",
            "--body",
            "[relay] /tmp/plan.md",
        ]))
        .unwrap();
        send(args, &mut out).expect("sends");

        let stdout = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = stdout.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one stdout line: {stdout:?}");
        let id = lines[0];

        let files = pending(&state, "reviewer");
        assert_eq!(files.len(), 1);
        let envelope: MessageEnvelope =
            serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap();
        assert_eq!(envelope.message_id, id);
        assert_eq!(envelope.origin.as_deref(), Some("peer"));
        assert_eq!(envelope.body, "[relay] /tmp/plan.md");
        assert_eq!(
            (envelope.from.as_str(), envelope.to.as_str()),
            ("dev", "reviewer")
        );

        let queue = DuoState::from_path(&state).unwrap().queue_root.unwrap();
        let raw = fs::read_to_string(
            queue
                .join("transitions")
                .join(format!("{}.json", safe_token(id))),
        )
        .unwrap();
        assert!(
            raw.ends_with('\n') && raw.lines().count() == 1,
            "one compact line: {raw:?}"
        );
        let record: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(record["schema"], TRANSITION_SCHEMA);
        assert_eq!(record["message_id"], id);
        assert_eq!(record["session_name"], "s 1");
        assert_eq!(record["role"], "reviewer");
        assert_eq!(record["from"], "dev");
        assert_eq!(record["to"], "reviewer");
        assert_eq!(
            record["body_digest"],
            format!("{:x}", Sha256::digest(b"[relay] /tmp/plan.md"))
        );
        assert_eq!(record["state"], "queued");
        assert_eq!(record["recovery_epoch"], 3);
        assert_eq!(record["recovery_reason"], "");
        assert_eq!(record["transitions"][0]["state"], "queued");
        assert_eq!(record["transitions"][0]["recovery_epoch"], 3);
        assert!(record["transitions"][0]["timestamp_ns"].as_u64().is_some());
    }

    /// No live serve: nothing enqueued, no ledger record, nothing printed.
    #[test]
    fn dead_recipient_writes_nothing() {
        let dir = TempDir::new("dead");
        let state = duo_layout(&dir.path, None);
        let mut out = Vec::new();
        let args = parse_send(&argv(&[
            "--state",
            state.to_str().unwrap(),
            "--to",
            "dev",
            "--from",
            "operator",
            "--body",
            "hi",
        ]))
        .unwrap();
        assert!(send(args, &mut out).is_err());
        assert!(out.is_empty());
        assert!(pending(&state, "dev").is_empty());
        let transitions = DuoState::from_path(&state)
            .unwrap()
            .queue_root
            .unwrap()
            .join("transitions");
        assert_eq!(
            fs::read_dir(transitions).map(|rd| rd.count()).unwrap_or(0),
            0
        );
    }

    /// A record the worker already wrote is left byte-identical, and the
    /// epoch falls back to 1 without a ledger.
    #[test]
    fn existing_record_is_untouched_and_epoch_defaults_to_one() {
        let dir = TempDir::new("existing");
        let queue = dir.path.join("duo-recovery").join("s").join("queue");
        let envelope =
            MessageEnvelope::new("m 1", "c", "dev", "reviewer", MessageKind::Request, "b", 1);
        assert!(record_queued(&queue, "s", &envelope).unwrap());
        let path = queue.join("transitions").join("m_1.json");
        let first: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(first["recovery_epoch"], 1);

        fs::write(&path, "{\"state\":\"claimed\"}\n").unwrap();
        assert!(!record_queued(&queue, "s", &envelope).unwrap());
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "{\"state\":\"claimed\"}\n"
        );
        let leftovers = fs::read_dir(queue.join("transitions")).unwrap().count();
        assert_eq!(leftovers, 1, "no temp file left behind");
    }

    #[test]
    fn no_queue_root_skips_the_ledger() {
        let dir = TempDir::new("noqueue");
        let mailbox_root = dir.path.join("mb");
        let inbox = mailbox_root.join("dev").join("inbox");
        fs::create_dir_all(&inbox).unwrap();
        let state = dir.path.join("baton.state");
        fs::write(
            &state,
            format!(
                "session_name=s\nmailbox_root={}\nqueue_root=\n",
                mailbox_root.display()
            ),
        )
        .unwrap();
        let _serve = Mailbox::open(&inbox).unwrap();
        let mut out = Vec::new();
        let args = parse_send(&argv(&[
            "--state",
            state.to_str().unwrap(),
            "--to",
            "dev",
            "--from",
            "system",
            "--body",
            "hi",
        ]))
        .unwrap();
        send(args, &mut out).expect("sends");
        assert_eq!(String::from_utf8(out).unwrap().lines().count(), 1);
        assert!(!dir.path.join("transitions").exists());
    }
}
