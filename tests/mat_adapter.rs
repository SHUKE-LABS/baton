//! `baton mat send` through the real binary (#383); built only with `mat`.
#![cfg(feature = "mat")]

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Command;

use baton::message::MessageEnvelope;

#[test]
fn mat_send_enqueues_records_queued_and_prints_one_id() {
    let dir = std::env::temp_dir().join(format!("baton-matcli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let queue = dir.join("duo-recovery").join("s").join("queue");
    let inbox = queue.join("mailbox").join("dev").join("inbox");
    std::fs::create_dir_all(&inbox).expect("create inbox");
    let state = dir.join("baton.state");
    std::fs::write(
        &state,
        format!(
            "session_name=s\nqueue_root={}\nmailbox_root={}\n",
            queue.display(),
            queue.join("mailbox").display()
        ),
    )
    .expect("write state");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(inbox.join("serve.lock"))
        .expect("open serve.lock");
    lock.try_lock().expect("hold serve.lock");

    let out = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "mat",
            "send",
            "--state",
            state.to_str().unwrap(),
            "--to",
            "dev",
            "--from",
            "operator",
            "--body",
            "go",
        ])
        .env_clear()
        .output()
        .expect("run baton mat send");
    drop(lock);

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let pending: Vec<PathBuf> = std::fs::read_dir(inbox.join("pending"))
        .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
        .unwrap_or_default();
    let envelopes: Vec<String> = pending
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .collect();
    let record = std::fs::read_to_string(
        queue
            .join("transitions")
            .join(format!("{}.json", stdout.trim_end())),
    );
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stdout.lines().count(),
        1,
        "exactly one stdout line: {stdout:?}"
    );
    assert_eq!(envelopes.len(), 1);
    let envelope: MessageEnvelope = serde_json::from_str(&envelopes[0]).expect("envelope");
    assert_eq!(envelope.message_id, stdout.trim_end());
    assert_eq!(envelope.origin.as_deref(), Some("operator"));
    let record: serde_json::Value =
        serde_json::from_str(&record.expect("queued record")).expect("json");
    assert_eq!(record["state"], "queued");
    assert_eq!(record["message_id"], envelope.message_id);
}
