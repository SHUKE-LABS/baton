//! Generic session manifest: recipient name → mailbox pair by convention.
//!
//! A *session* is a set of named participants whose mailboxes share one root:
//! participant `<name>` is served at `<mailbox_root>/<name>/inbox` and replies
//! into `<mailbox_root>/<name>/outbox`. The manifest is what `baton send
//! --session <manifest> --to <name>` reads to find that pair — a lighter
//! alternative to a [`Registry`](crate::registry::Registry) when every
//! participant follows the layout, so the file names participants rather than
//! spelling out paths.
//!
//! Like the registry it is *pure lookup*: no governance, no liveness. Names are
//! validated with the mailbox's [`is_safe_key`](crate::mailbox::is_safe_key)
//! guard so a hostile name cannot escape `mailbox_root`, and an unknown
//! recipient fails fast as a [`BatonError::Config`] before anything is written.
//!
//! ## Manifest format (JSON, `baton.session/v1`)
//!
//! ```json
//! {
//!   "schema": "baton.session/v1",
//!   "mailbox_root": "/var/lib/team/mailbox",
//!   "participants": ["dev", "reviewer"]
//! }
//! ```
//!
//! A relative `mailbox_root` resolves against the manifest file's directory.
//! Unknown fields are ignored, so a manifest writer may record extra keys.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{BatonError, Result};
use crate::mailbox::is_safe_key;
use crate::registry::MailboxRef;

/// Schema discriminator a session manifest must carry.
pub const SCHEMA: &str = "baton.session/v1";

/// The on-disk manifest shape, before validation.
#[derive(Deserialize)]
struct RawManifest {
    schema: Option<String>,
    mailbox_root: Option<PathBuf>,
    participants: Option<Vec<String>>,
}

/// A validated session manifest: a mailbox root and the participant names
/// served under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionManifest {
    mailbox_root: PathBuf,
    participants: Vec<String>,
}

impl SessionManifest {
    /// Builds a manifest from parts, applying the same validation as
    /// [`from_path`](Self::from_path): a non-empty `mailbox_root` and safe,
    /// non-empty participant names.
    pub fn new(mailbox_root: impl Into<PathBuf>, participants: Vec<String>) -> Result<Self> {
        let mailbox_root = mailbox_root.into();
        if mailbox_root.as_os_str().is_empty() {
            return Err(BatonError::Config(
                "session manifest mailbox_root must not be empty".to_string(),
            ));
        }
        for name in &participants {
            if !is_safe_key(name) {
                return Err(BatonError::Config(format!(
                    "session manifest participant name is not a safe mailbox key: {name:?}"
                )));
            }
        }
        Ok(Self {
            mailbox_root,
            participants,
        })
    }

    /// Loads and validates the manifest at `path`.
    ///
    /// A missing or unreadable file, malformed JSON, a missing or foreign
    /// `schema`, a missing/empty `mailbox_root`, a missing `participants` list,
    /// or an unsafe participant name is a [`BatonError::Config`] naming the
    /// path.
    pub fn from_path(path: &Path) -> Result<Self> {
        let invalid =
            |what: &str| BatonError::Config(format!("session manifest {} {what}", path.display()));
        let raw = std::fs::read_to_string(path)
            .map_err(|err| invalid(&format!("could not be read: {err}")))?;
        let manifest: RawManifest = serde_json::from_str(&raw)
            .map_err(|err| invalid(&format!("is not valid JSON: {err}")))?;
        match manifest.schema.as_deref() {
            Some(SCHEMA) => {}
            Some(other) => {
                return Err(invalid(&format!(
                    "has schema {other:?}, expected {SCHEMA:?}"
                )));
            }
            None => return Err(invalid(&format!("is missing schema {SCHEMA:?}"))),
        }
        let root = manifest
            .mailbox_root
            .ok_or_else(|| invalid("is missing mailbox_root"))?;
        let participants = manifest
            .participants
            .ok_or_else(|| invalid("is missing participants"))?;
        // A relative root is anchored at the manifest, not the caller's cwd, so
        // the same file resolves identically from any working directory.
        let root = if root.is_relative() && !root.as_os_str().is_empty() {
            path.parent().unwrap_or(Path::new("")).join(root)
        } else {
            root
        };
        Self::new(root, participants).map_err(|err| invalid(&format!("is invalid: {err}")))
    }

    /// Resolves `name` to its mailbox pair, or a [`BatonError::Config`] naming
    /// the unknown recipient.
    pub fn resolve(&self, name: &str) -> Result<MailboxRef> {
        if !self.participants.iter().any(|p| p == name) {
            return Err(BatonError::Config(format!(
                "session manifest has no participant named {name:?}"
            )));
        }
        let base = self.mailbox_root.join(name);
        Ok(MailboxRef {
            inbox: base.join("inbox"),
            outbox: base.join("outbox"),
            max_runtime_ms: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(tag: &str, content: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("baton-session-{}-{tag}.json", std::process::id()));
        std::fs::write(&path, content).expect("write temp manifest");
        path
    }

    fn expect_config(result: Result<SessionManifest>, needle: &str) {
        match result.unwrap_err() {
            BatonError::Config(msg) => assert!(msg.contains(needle), "got: {msg}"),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn resolves_participant_by_convention() {
        // Absolute on every platform (`/srv/mb` is drive-relative on Windows).
        let root = std::env::temp_dir().join("abs-mb");
        let json = serde_json::json!({
            "schema": "baton.session/v1",
            "mailbox_root": root,
            "participants": ["dev", "reviewer"],
            "extra": 1,
        });
        let path = write_temp("valid", &json.to_string());
        let manifest = SessionManifest::from_path(&path).expect("loads");
        let mailbox = manifest.resolve("reviewer").expect("resolves");
        assert_eq!(mailbox.inbox, root.join("reviewer").join("inbox"));
        assert_eq!(mailbox.outbox, root.join("reviewer").join("outbox"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn relative_root_resolves_against_the_manifest_directory() {
        let path = write_temp(
            "relative",
            r#"{"schema":"baton.session/v1","mailbox_root":"mb","participants":["dev"]}"#,
        );
        let manifest = SessionManifest::from_path(&path).expect("loads");
        assert_eq!(
            manifest.resolve("dev").expect("resolves").inbox,
            std::env::temp_dir().join("mb").join("dev").join("inbox")
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unknown_recipient_is_config_error_naming_it() {
        let manifest = SessionManifest::new("/srv/mb", vec!["dev".to_string()]).expect("valid");
        match manifest.resolve("ghost").unwrap_err() {
            BatonError::Config(msg) => assert!(msg.contains("ghost"), "got: {msg}"),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_manifests() {
        for (tag, json, needle) in [
            ("malformed", "{ nope", "JSON"),
            (
                "no-schema",
                r#"{"mailbox_root":"/m","participants":[]}"#,
                "missing schema",
            ),
            (
                "bad-schema",
                r#"{"schema":"baton.registry/v1","mailbox_root":"/m","participants":[]}"#,
                "expected",
            ),
            (
                "no-root",
                r#"{"schema":"baton.session/v1","participants":[]}"#,
                "mailbox_root",
            ),
            (
                "empty-root",
                r#"{"schema":"baton.session/v1","mailbox_root":"","participants":[]}"#,
                "mailbox_root",
            ),
            (
                "no-participants",
                r#"{"schema":"baton.session/v1","mailbox_root":"/m"}"#,
                "participants",
            ),
            (
                "unsafe-name",
                r#"{"schema":"baton.session/v1","mailbox_root":"/m","participants":["../x"]}"#,
                "safe mailbox key",
            ),
        ] {
            let path = write_temp(tag, json);
            expect_config(SessionManifest::from_path(&path), needle);
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn missing_file_is_config_error_naming_the_path() {
        let mut path = std::env::temp_dir();
        path.push(format!("baton-session-{}-absent.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        expect_config(
            SessionManifest::from_path(&path),
            &path.display().to_string(),
        );
    }
}
