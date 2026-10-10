//! `baton send --session` / `--require-live` / `--origin` / `--body-file`
//! through the real binary (#383).

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use baton::message::MessageEnvelope;

/// A unique self-cleaning temp directory, keyed by pid + tag.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("baton-sendsess-{}-{tag}", std::process::id()));
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

/// Runs `baton <args>` with a cleared environment.
fn baton(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(args)
        .env_clear()
        .output()
        .expect("run baton")
}

fn s(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

/// A `baton.session/v1` manifest over `<dir>/mb` with `dev` and `reviewer`;
/// returns the manifest path. The reviewer inbox exists so a lock can be held.
fn manifest(dir: &Path) -> PathBuf {
    std::fs::create_dir_all(reviewer_inbox(dir)).expect("create inbox");
    let path = dir.join("session.json");
    let json = serde_json::json!({
        "schema": "baton.session/v1",
        "mailbox_root": dir.join("mb"),
        "participants": ["dev", "reviewer"],
    });
    std::fs::write(&path, json.to_string()).expect("write manifest");
    path
}

fn reviewer_inbox(dir: &Path) -> PathBuf {
    dir.join("mb").join("reviewer").join("inbox")
}

/// A registry routing `reviewer` to the same inbox the manifest resolves.
fn registry(dir: &Path) -> PathBuf {
    let path = dir.join("registry.json");
    let json = serde_json::json!({
        "participants": {
            "reviewer": {
                "inbox": reviewer_inbox(dir),
                "outbox": dir.join("mb").join("reviewer").join("outbox"),
            }
        }
    });
    std::fs::write(&path, json.to_string()).expect("write registry");
    path
}

fn pending(inbox: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(inbox.join("pending"))
        .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
        .unwrap_or_default()
}

/// Holds `<inbox>/serve.lock` the way a live `baton serve` does.
fn hold_serve_lock(inbox: &Path) -> File {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(inbox.join("serve.lock"))
        .expect("open serve.lock");
    lock.try_lock().expect("hold serve.lock");
    lock
}

#[test]
fn session_body_file_delivers_and_stdout_id_matches_the_pending_envelope() {
    let dir = TempDir::new("deliver");
    let manifest = manifest(&dir.path);
    let body_path = dir.path.join("body.md");
    std::fs::write(&body_path, "[relay] /tmp/plan.md\nsecond line").expect("write body");

    let out = baton(&[
        "send",
        "--session",
        s(&manifest),
        "--to",
        "reviewer",
        "--from",
        "dev",
        "--body-file",
        s(&body_path),
        "--origin",
        "peer",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8(out.stdout).expect("utf-8");
    let files = pending(&reviewer_inbox(&dir.path));
    assert_eq!(files.len(), 1, "exactly one envelope enqueued");
    let envelope: MessageEnvelope =
        serde_json::from_str(&std::fs::read_to_string(&files[0]).unwrap()).expect("envelope");
    assert_eq!(
        stdout.trim_end(),
        envelope.message_id,
        "stdout id is the enqueued id"
    );
    assert_eq!(
        envelope.body, "[relay] /tmp/plan.md\nsecond line",
        "body unchanged"
    );
    assert_eq!(envelope.origin.as_deref(), Some("peer"));
    assert_eq!(
        (envelope.from.as_str(), envelope.to.as_str()),
        ("dev", "reviewer")
    );
}

#[test]
fn session_failures_exit_non_zero_and_leave_pending_empty() {
    let dir = TempDir::new("failures");
    let good = manifest(&dir.path);
    let malformed = dir.path.join("malformed.json");
    std::fs::write(&malformed, "{ not json").unwrap();
    let missing = dir.path.join("absent.json");
    let inbox = reviewer_inbox(&dir.path);

    for (why, args) in [
        (
            "missing manifest",
            vec!["--session", s(&missing), "--to", "reviewer"],
        ),
        (
            "malformed manifest",
            vec!["--session", s(&malformed), "--to", "reviewer"],
        ),
        (
            "unknown recipient",
            vec!["--session", s(&good), "--to", "ghost"],
        ),
        (
            "empty origin",
            vec!["--session", s(&good), "--to", "reviewer", "--origin", ""],
        ),
        (
            "blank origin",
            vec!["--session", s(&good), "--to", "reviewer", "--origin", "  "],
        ),
        (
            "inbox + session",
            vec![
                "--session",
                s(&good),
                "--to",
                "reviewer",
                "--inbox",
                s(&inbox),
            ],
        ),
    ] {
        let mut argv = vec!["send", "--body", "hi"];
        argv.extend(args);
        let out = baton(&argv);
        assert!(!out.status.success(), "{why} must fail");
        assert!(pending(&inbox).is_empty(), "{why} must not enqueue");
    }

    let registry = registry(&dir.path);
    let out = baton(&[
        "send",
        "--session",
        s(&good),
        "--registry",
        s(&registry),
        "--to",
        "reviewer",
        "--body",
        "hi",
    ]);
    assert!(!out.status.success(), "registry + session must fail");
    assert!(pending(&inbox).is_empty());
}

/// `--require-live` gates every destination form on the resolved inbox's
/// `serve.lock`: unheld refuses with nothing written, held delivers.
#[test]
fn require_live_gates_session_registry_and_inbox_sends() {
    let dir = TempDir::new("live");
    let manifest = manifest(&dir.path);
    let registry = registry(&dir.path);
    let inbox = reviewer_inbox(&dir.path);

    let forms: [Vec<&str>; 3] = [
        vec!["--session", s(&manifest), "--to", "reviewer"],
        vec!["--registry", s(&registry), "--to", "reviewer"],
        vec!["--inbox", s(&inbox)],
    ];
    for form in &forms {
        let mut argv = vec!["send", "--require-live", "--body", "hi"];
        argv.extend(form.iter().copied());
        let out = baton(&argv);
        assert!(!out.status.success(), "{form:?} without a serve must fail");
        assert!(pending(&inbox).is_empty(), "{form:?} must not enqueue");
    }

    let _serve = hold_serve_lock(&inbox);
    for (n, form) in forms.iter().enumerate() {
        let mut argv = vec!["send", "--require-live", "--body", "hi"];
        argv.extend(form.iter().copied());
        let out = baton(&argv);
        assert!(
            out.status.success(),
            "{form:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            pending(&inbox).len(),
            n + 1,
            "{form:?} enqueues one envelope"
        );
    }
}
