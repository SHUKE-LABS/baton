//! The per-serve worker outcome summary (#391).
//!
//! A client driving a role through a serve needs to know whether the worker's
//! recent invocations succeeded without re-parsing the outbox archive (#375):
//! one malformed old file poisons the rebuild, the cost grows with the
//! archive, and on Windows the file list overflows the command-line limit.
//! The serve therefore keeps one small, fixed-size summary file and rewrites
//! it as each invocation finishes; a client reads exactly that file and never
//! touches the outbox.
//!
//! **The invocation is the unit.** One [`Participant::respond_batch`] call is
//! one invocation — for `serve --agent-batch-max`, a batch of N claimed
//! messages fans one child run's body (or one synthesized error) back to
//! every member, so the summary moves exactly once per batch, never once per
//! member. The summary is updated **before** the per-member reply loop, so a
//! claimed `response`/`error` member (which answers nothing) still counts its
//! invocation exactly once.
//!
//! An invocation **failed** iff every member of its batch came back
//! [`MessageKind::Error`]; anything else (including the empty batch, which
//! cannot reach the record path today) counts as a success. A mixed batch is
//! not producible: external agents answer all-or-errors-all in one child run,
//! and every other participant answers one message per call.
//!
//! **Multiple error bodies in one invocation.** A batch fan-out failure
//! synthesizes one identical error body per member. The **first** member's
//! body supplies `last_error_hash` and `last_error_diagnostic`, and the whole
//! invocation counts as one error occurrence: `identical_error_count` moves
//! by at most one per invocation, whatever the batch size.
//!
//! `identical_error_count` is the number of consecutive failed invocations
//! whose error body hashes to the same digest as the current
//! `last_error_hash` — the "same credential is still missing" signal a client
//! uses to stop automatic wakes. A different body (or a success, which also
//! clears every error field) restarts the count.
//!
//! The file lives at `<inbox>/serve-outcome.json`, written with
//! [`mailbox::atomic_write`] so a reader always sees the old or the new
//! summary, never a torn one. Reading it is a fixed one-file read — the outbox
//! is never scanned. A serve **reset** (a fresh startup) rewrites the file in
//! the zeroed state, so a restart starts a fresh streak. Write failures are
//! observability: the drain warns on stderr and carries on, never aborting.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BatonError, Result};
use crate::mailbox;
use crate::message::{MessageEnvelope, MessageKind};

/// The summary file's name, directly inside the serve's inbox.
pub(crate) const OUTCOME_FILE: &str = "serve-outcome.json";

/// [`ServeOutcomeSummary::schema`].
pub(crate) const OUTCOME_SCHEMA: &str = "baton.serve-outcome/v1";

/// The longest `last_error_diagnostic`, in bytes, truncated on a char
/// boundary — the diagnostic is a bounded hint, not a full body copy.
const MAX_DIAGNOSTIC_BYTES: usize = 2048;

/// The last recorded invocation's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ServeOutcome {
    /// Every member of the invocation's batch was answered.
    Response,
    /// Every member of the invocation's batch came back an error.
    Error,
}

/// The per-serve worker outcome summary: a fixed-size, schema-tagged JSON
/// document a client reads to learn worker health without scanning the outbox
/// archive.
///
/// All fields are `#[serde(default)]` so a reader built from a schema
/// superset/singleton still decodes: a summary written by a newer baton
/// (extra fields) is ignored-field tolerant, and an older file missing a
/// newer field decodes to the zero value rather than failing the read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct ServeOutcomeSummary {
    /// [`OUTCOME_SCHEMA`].
    pub(crate) schema: String,
    /// The last invocation's outcome; `None` only in the zeroed (freshly
    /// reset, nothing recorded) state.
    pub(crate) last_outcome: Option<ServeOutcome>,
    /// The last invocation's completion timestamp, epoch milliseconds.
    pub(crate) last_ts_ms: Option<u64>,
    /// Consecutive failed invocations; reset to 0 by a success.
    pub(crate) error_streak: u64,
    /// `sha256:<hex>` of the first member's error body of the last failed
    /// invocation; `None` when there is no error to report.
    pub(crate) last_error_hash: Option<String>,
    /// Consecutive failed invocations whose error body hashes to the current
    /// `last_error_hash`; 1 after a body change, 0 after a success.
    pub(crate) identical_error_count: u64,
    /// The first member's error body of the last failed invocation, truncated
    /// to [`MAX_DIAGNOSTIC_BYTES`] bytes on a char boundary.
    pub(crate) last_error_diagnostic: Option<String>,
    /// When this file was last written (record or reset), epoch milliseconds.
    pub(crate) updated_at_ms: u64,
}

impl Default for ServeOutcomeSummary {
    fn default() -> Self {
        Self::zeroed(0)
    }
}

impl ServeOutcomeSummary {
    /// The fresh-start state a serve writes on startup: nothing recorded, the
    /// write stamped `updated_at_ms`.
    pub(crate) fn zeroed(updated_at_ms: u64) -> Self {
        Self {
            schema: OUTCOME_SCHEMA.to_string(),
            last_outcome: None,
            last_ts_ms: None,
            error_streak: 0,
            last_error_hash: None,
            identical_error_count: 0,
            last_error_diagnostic: None,
            updated_at_ms,
        }
    }

    /// Folds one finished invocation into the summary: `responses` is the
    /// batch exactly as [`Participant::respond_batch`] returned it, and
    /// `ts_ms` is the invocation's completion timestamp.
    ///
    /// On a success the streak goes to 0 and every error field is cleared —
    /// a healthy worker has nothing to report. On a failure the first
    /// member's error body supplies the hash and diagnostic (see the module
    /// docs for the multi-body rule), and `identical_error_count` either
    /// continues the run of identical bodies or restarts at 1.
    pub(crate) fn record_invocation(&mut self, responses: &[MessageEnvelope], ts_ms: u64) {
        let failed =
            !responses.is_empty() && responses.iter().all(|r| r.kind == MessageKind::Error);
        if failed {
            let body = responses
                .iter()
                .find(|r| r.kind == MessageKind::Error)
                .map(|r| r.body.as_str())
                .unwrap_or_default();
            let hash = error_hash(body);
            self.error_streak = self.error_streak.saturating_add(1);
            self.identical_error_count = if self.last_error_hash.as_deref() == Some(hash.as_str()) {
                self.identical_error_count.saturating_add(1)
            } else {
                1
            };
            self.last_error_hash = Some(hash);
            self.last_error_diagnostic =
                Some(truncate_bytes(body, MAX_DIAGNOSTIC_BYTES).to_string());
            self.last_outcome = Some(ServeOutcome::Error);
        } else {
            self.error_streak = 0;
            self.identical_error_count = 0;
            self.last_error_hash = None;
            self.last_error_diagnostic = None;
            self.last_outcome = Some(ServeOutcome::Response);
        }
        self.last_ts_ms = Some(ts_ms);
        self.updated_at_ms = ts_ms;
    }
}

/// Reads the summary from `inbox`; `Ok(None)` when no serve has written one
/// yet. A present-but-malformed file is a hard error: the writer owns this
/// one file, so an unreadable summary is a real corruption, not a hint.
pub(crate) fn read(inbox: &Path) -> Result<Option<ServeOutcomeSummary>> {
    let path = inbox.join(OUTCOME_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(BatonError::Io(format!("could not read {path:?}: {err}")));
        }
    };
    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|err| BatonError::Decode(format!("malformed outcome summary {path:?}: {err}")))
}

/// Atomically writes `summary` into `inbox` (readers see old-or-new).
pub(crate) fn write(inbox: &Path, summary: &ServeOutcomeSummary) -> Result<()> {
    let json = serde_json::to_string(summary)
        .map_err(|err| BatonError::Io(format!("could not encode outcome summary: {err}")))?;
    mailbox::atomic_write(inbox, OUTCOME_FILE, &json)
}

/// Folds one finished invocation into `summary` and persists the file. The
/// caller runs this between invocations of a live drain, so a write failure
/// is downgraded to a stderr warning here — the drain must carry on.
pub(crate) fn record(
    inbox: &Path,
    summary: &mut ServeOutcomeSummary,
    responses: &[MessageEnvelope],
    ts_ms: u64,
) {
    summary.record_invocation(responses, ts_ms);
    if let Err(err) = write(inbox, summary) {
        eprintln!("warning: could not record the serve outcome summary: {err}");
    }
}

/// Resets the summary to the zeroed state, stamped `updated_at_ms` — a fresh
/// serve startup starts a fresh streak, discarding whatever the previous
/// lifetime recorded. Returns the reset summary for the caller to thread into
/// its drain loop.
pub(crate) fn reset(inbox: &Path, updated_at_ms: u64) -> Result<ServeOutcomeSummary> {
    let summary = ServeOutcomeSummary::zeroed(updated_at_ms);
    write(inbox, &summary)?;
    Ok(summary)
}

/// `sha256:<hex>` of an error body — the identity a client compares to decide
/// whether the same failure persists (`{:x}` formats the digest as lowercase
/// hex, the [`crate::mat`] precedent).
pub(crate) fn error_hash(body: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(body.as_bytes()))
}

/// Truncates `s` to at most `max` bytes without splitting a character.
fn truncate_bytes(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique self-cleaning temp dir, mirroring the sibling unit tests.
    struct TempRoot {
        path: std::path::PathBuf,
    }

    impl TempRoot {
        fn new(tag: &str) -> Self {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "baton-outcome-{}-{}-{tag}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed),
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create temp outcome dir");
            Self { path }
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// A batch of one error envelope, the unbatched failure shape.
    fn error_batch(body: &str) -> Vec<MessageEnvelope> {
        vec![error_envelope(body)]
    }

    fn error_envelope(body: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            format!("err-{}", error_hash(body)),
            "conv".to_string(),
            "svc".to_string(),
            "role".to_string(),
            MessageKind::Error,
            body.to_string(),
            0,
        )
    }

    fn response_envelope(body: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            format!("rep-{}", error_hash(body)),
            "conv".to_string(),
            "svc".to_string(),
            "role".to_string(),
            MessageKind::Response,
            body.to_string(),
            0,
        )
    }

    /// AC1: two failing invocations, then one success — the streak reports 2,
    /// then 0, with the invocation timestamps recorded along the way.
    #[test]
    fn two_errors_then_success_reset_the_streak_with_timestamps() {
        let mut summary = ServeOutcomeSummary::zeroed(0);
        assert_eq!(summary.last_outcome, None);
        assert_eq!(summary.error_streak, 0);

        summary.record_invocation(&error_batch("missing credential"), 1_000);
        assert_eq!(summary.last_outcome, Some(ServeOutcome::Error));
        assert_eq!(summary.last_ts_ms, Some(1_000));
        assert_eq!(summary.error_streak, 1);

        summary.record_invocation(&error_batch("missing credential"), 2_000);
        assert_eq!(summary.error_streak, 2);
        assert_eq!(summary.last_ts_ms, Some(2_000));
        assert_eq!(summary.identical_error_count, 2);

        summary.record_invocation(&[response_envelope("ok")], 3_000);
        assert_eq!(summary.last_outcome, Some(ServeOutcome::Response));
        assert_eq!(summary.last_ts_ms, Some(3_000));
        assert_eq!(summary.error_streak, 0);
        assert_eq!(summary.identical_error_count, 0);
        assert_eq!(summary.last_error_hash, None);
        assert_eq!(summary.last_error_diagnostic, None);
    }

    /// The multi-body rule: a batch fan-out failure synthesizes one identical
    /// error per member, so the **first** member's body supplies the hash and
    /// diagnostic, and the invocation counts once — `identical_error_count`
    /// moves by one, not by the batch size.
    #[test]
    fn multi_error_batch_counts_once_and_reports_the_first_body() {
        let mut summary = ServeOutcomeSummary::zeroed(0);
        let batch = vec![
            error_envelope("first failure"),
            error_envelope("first failure"),
            error_envelope("first failure"),
        ];

        summary.record_invocation(&batch, 1_000);
        assert_eq!(summary.error_streak, 1);
        assert_eq!(summary.identical_error_count, 1);
        assert_eq!(
            summary.last_error_hash,
            Some(error_hash("first failure")),
            "the first member's body supplies the hash"
        );
        assert_eq!(
            summary.last_error_diagnostic,
            Some("first failure".to_string())
        );

        // One more failing invocation with the same single body continues the
        // identical run.
        summary.record_invocation(&error_batch("first failure"), 2_000);
        assert_eq!(summary.error_streak, 2);
        assert_eq!(summary.identical_error_count, 2);
    }

    /// A different error body restarts `identical_error_count` at 1 while the
    /// streak keeps growing; a success clears both.
    #[test]
    fn a_new_error_body_restarts_the_identical_count() {
        let mut summary = ServeOutcomeSummary::zeroed(0);
        summary.record_invocation(&error_batch("first failure"), 1_000);
        summary.record_invocation(&error_batch("first failure"), 2_000);
        assert_eq!(summary.identical_error_count, 2);

        summary.record_invocation(&error_batch("disk full"), 3_000);
        assert_eq!(summary.error_streak, 3);
        assert_eq!(summary.identical_error_count, 1);
        assert_eq!(summary.last_error_hash, Some(error_hash("disk full")));
        assert_eq!(summary.last_error_diagnostic, Some("disk full".to_string()));

        summary.record_invocation(&[response_envelope("ok")], 4_000);
        assert_eq!(summary.identical_error_count, 0);
        assert_eq!(summary.last_error_hash, None);
    }

    /// A mixed batch is counted as a success (and today no participant
    /// produces one) — the summary reports what a client may rely on: the
    /// worker answered something.
    #[test]
    fn a_mixed_batch_counts_as_a_success() {
        let mut summary = ServeOutcomeSummary::zeroed(0);
        summary.record_invocation(&error_batch("boom"), 1_000);
        assert_eq!(summary.error_streak, 1);

        let mixed = vec![response_envelope("ok"), error_envelope("boom")];
        summary.record_invocation(&mixed, 2_000);
        assert_eq!(summary.last_outcome, Some(ServeOutcome::Response));
        assert_eq!(summary.error_streak, 0);
    }

    /// The diagnostic is capped at [`MAX_DIAGNOSTIC_BYTES`] bytes without
    /// splitting a character, so the file stays fixed-size even for a huge
    /// error body.
    #[test]
    fn the_diagnostic_is_truncated_on_a_char_boundary() {
        let mut summary = ServeOutcomeSummary::zeroed(0);
        let body = "é".repeat(MAX_DIAGNOSTIC_BYTES); // 2 bytes per char
        summary.record_invocation(&error_batch(&body), 1_000);
        let diagnostic = summary.last_error_diagnostic.clone().expect("diagnostic");
        assert_eq!(diagnostic.len(), MAX_DIAGNOSTIC_BYTES);
        assert!(diagnostic.chars().all(|c| c == 'é'));

        // A body whose `max` boundary lands mid-character truncates back.
        let uneven = format!("a{}", "é".repeat(MAX_DIAGNOSTIC_BYTES));
        summary.record_invocation(&error_batch(&uneven), 2_000);
        let diagnostic = summary.last_error_diagnostic.clone().expect("diagnostic");
        assert!(diagnostic.len() < MAX_DIAGNOSTIC_BYTES + 1);
        assert!(diagnostic.is_char_boundary(diagnostic.len()));
    }

    /// Write/read round-trip; a missing file is `Ok(None)`, a malformed one a
    /// hard error.
    #[test]
    fn write_read_roundtrip_and_missing_and_malformed_files() {
        let dir = TempRoot::new("outcome");
        assert_eq!(read(&dir.path).expect("missing is none"), None);

        let mut summary = ServeOutcomeSummary::zeroed(7);
        summary.record_invocation(&error_batch("boom"), 9);
        write(&dir.path, &summary).expect("write");
        let path = dir.path.join(OUTCOME_FILE);
        let raw = std::fs::read_to_string(&path).expect("raw");
        assert!(raw.contains(OUTCOME_SCHEMA), "schema-tagged: {raw}");
        assert_eq!(
            read(&dir.path).expect("read"),
            Some(summary),
            "byte-stable round-trip"
        );

        mailbox::atomic_write(&dir.path, OUTCOME_FILE, "not json").expect("write");
        assert!(read(&dir.path).is_err(), "malformed is a hard error");
    }

    /// A fresh startup resets whatever a previous lifetime recorded: a
    /// pre-seeded streak-5 summary comes back zeroed (restart reset).
    #[test]
    fn reset_zeroes_a_pre_seeded_streak() {
        let dir = TempRoot::new("outcome");
        let mut old = ServeOutcomeSummary::zeroed(0);
        for ts in 1..=5 {
            old.record_invocation(&error_batch("boom"), ts);
        }
        assert_eq!(old.error_streak, 5);
        write(&dir.path, &old).expect("seed");

        let fresh = reset(&dir.path, 10_000).expect("reset");
        assert_eq!(fresh, ServeOutcomeSummary::zeroed(10_000));
        assert_eq!(
            read(&dir.path).expect("read"),
            Some(ServeOutcomeSummary::zeroed(10_000)),
            "the file is reset, not just the in-memory copy"
        );
    }

    /// AC3: reading the summary costs the same with a 1-entry and a 1 000
    /// entry outbox — the read never scans the outbox, and a malformed old
    /// outbox file cannot poison it. The summary read is byte-identical with
    /// a huge, partly malformed outbox on disk.
    #[test]
    fn the_summary_read_is_unaffected_by_a_large_malformed_outbox() {
        let dir = TempRoot::new("outcome");
        let inbox = dir.path.join("inbox");
        let outbox = dir.path.join("outbox");
        std::fs::create_dir_all(&inbox).expect("inbox");
        std::fs::create_dir_all(&outbox).expect("outbox");

        let mut summary = ServeOutcomeSummary::zeroed(0);
        summary.record_invocation(&error_batch("boom"), 42);
        write(&inbox, &summary).expect("write");

        // A 1 000-entry outbox whose last file is malformed.
        for i in 0..1_000 {
            mailbox::atomic_write(&outbox, &format!("reply-{i}.json"), "{\"k\":1}")
                .expect("outbox entry");
        }
        mailbox::atomic_write(&outbox, "reply-broken.json", "{not json").expect("malformed");

        let with_big_outbox = read(&inbox).expect("read").expect("summary");
        assert_eq!(with_big_outbox, summary);

        // The read cost is the one summary file: with the outbox emptied the
        // read returns the same bytes.
        std::fs::remove_dir_all(&outbox).expect("remove outbox");
        let with_no_outbox = read(&inbox).expect("read").expect("summary");
        assert_eq!(with_big_outbox, with_no_outbox);
    }
}
