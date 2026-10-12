//! `baton upgrade`: replace a standalone release-archive install with another
//! GitHub Release, verified against that release's `SHA256SUMS`.
//!
//! ## Ownership
//!
//! Only a binary carrying an upgrade receipt is ever replaced. `--adopt`
//! writes one after proving the installed file byte-identical to the binary in
//! its own version's checksummed archive. Cargo-, npm-, and OS-package-managed
//! paths are refused with their manager's command instead.
//!
//! ## Tools
//!
//! Downloads use the system `curl` and extraction the system `tar` (on
//! Windows, `curl.exe`/`tar.exe` from `System32`, which also reads `.zip`), so
//! the default build links no HTTP, TLS, or archive crate.
//!
//! ## State
//!
//! `<baton home>/upgrade/` holds, per install (keyed by a digest of the
//! canonical executable path): `receipt-<key>.json`, the `lock-<key>` that
//! serializes mutating runs, and on Windows the deferred helper's
//! `plan-<key>.json`, `helper-<key>.exe` copy, and outcome
//! `helper-<key>.json`.
//!
//! ## Replacement
//!
//! On Unix the updater renames the verified binary over itself in-process: a
//! live `service run` and its sessions keep running and a later `service
//! start` launches the new file. Windows cannot replace a running image, so
//! the updater defers whenever a live service or managed session exists, and
//! otherwise hands the swap to a detached copy of itself that runs once the
//! updater has exited.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BatonError, Result};
use crate::mailbox;
use crate::roles::{self, RolesHome};
use crate::service::{self, ServiceSnapshot};

/// Overrides the release root (default [`RELEASE_BASE`]); tests point it at a
/// `file://` fixture tree laid out like GitHub's `download/v<ver>/<asset>`.
pub const RELEASE_BASE_ENV: &str = "BATON_UPGRADE_RELEASE_BASE";

const RELEASE_BASE: &str = "https://github.com/shukebeta/baton/releases";
const REPO_URL: &str = "https://github.com/shukebeta/baton";
const RECEIPT_SCHEMA: &str = "baton.upgrade-receipt/v1";
const STAGED_SUFFIX: &str = ".baton-upgrade-new";
const OLD_SUFFIX: &str = ".baton-upgrade-old";
/// How long the Windows helper waits for the updater to exit, and then for
/// the per-install lock it releases.
const HELPER_WAIT: Duration = Duration::from_secs(60);

#[cfg(windows)]
const EXE_NAME: &str = "baton.exe";
#[cfg(not(windows))]
const EXE_NAME: &str = "baton";

/// A parsed `baton upgrade` invocation.
#[derive(Debug, PartialEq, Eq)]
pub enum UpgradeCommand {
    /// `--check`: report only; writes nothing and takes no lock.
    Check {
        version: Option<String>,
        control: Option<String>,
    },
    /// `--adopt`: verify the installed binary and write its receipt.
    Adopt { yes: bool },
    /// Replace a receipted install with `version` (default: latest).
    Upgrade {
        version: Option<String>,
        yes: bool,
        control: Option<String>,
    },
    /// Hidden: the Windows helper finishing a deferred swap after the
    /// updater with `wait_pid` exits.
    Finish { plan: String, wait_pid: u32 },
}

/// Runs `cmd`, writing its operator-facing report to `out`.
pub fn execute(cmd: UpgradeCommand, pretty: bool, mut out: impl Write) -> Result<()> {
    match cmd {
        UpgradeCommand::Check { version, control } => {
            let report = check(version, control)?;
            let json = crate::cli::to_user_json(&report, pretty, "upgrade check")?;
            writeln!(out, "{json}").map_err(io_err)
        }
        UpgradeCommand::Adopt { yes } => adopt(yes, out),
        UpgradeCommand::Upgrade {
            version,
            yes,
            control,
        } => upgrade(version, yes, control, out),
        UpgradeCommand::Finish { plan, wait_pid } => finish(Path::new(&plan), wait_pid),
    }
}

/// The published archive target that runs on this host, if any.
pub fn host_target() -> Option<&'static str> {
    if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        Some("x86_64-unknown-linux-gnu")
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "aarch64",
        target_env = "gnu"
    )) {
        Some("aarch64-unknown-linux-gnu")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Some("x86_64-apple-darwin")
    } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("aarch64-apple-darwin")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        // The MSVC archive runs on any x86_64 Windows, whatever toolchain
        // built the running binary.
        Some("x86_64-pc-windows-msvc")
    } else {
        None
    }
}

/// The archive file name for `version` on `target`.
pub fn asset_name(version: &str, target: &str) -> String {
    let ext = if target.contains("windows") {
        "zip"
    } else {
        "tar.gz"
    };
    format!("baton-{version}-{target}.{ext}")
}

/// Accepts `1.2.3` or `v1.2.3`, returning the bare `1.2.3`.
pub fn normalize_version(raw: &str) -> std::result::Result<String, String> {
    let bare = raw.trim().strip_prefix('v').unwrap_or(raw.trim());
    if version_key(bare).is_some() {
        Ok(bare.to_string())
    } else {
        Err(format!(
            "invalid release version {raw:?}: expected <major>.<minor>.<patch>"
        ))
    }
}

fn version_key(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let mut next = || -> Option<u64> {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        part.parse().ok()
    };
    let key = (next()?, next()?, next()?);
    parts.next().is_none().then_some(key)
}

/// Extracts the version from the `.../releases/tag/v<ver>` URL that
/// `releases/latest` redirects to.
fn parse_latest_tag(url: &str) -> Option<String> {
    let (_, tail) = url.trim().rsplit_once("/tag/v")?;
    let version = tail.split(['/', '?', '#']).next()?;
    version_key(version).map(|_| version.to_string())
}

/// Returns the lowercase digest `SHA256SUMS` lists for `asset`.
fn checksum_for(sums: &str, asset: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let (digest, name) = line.trim().split_once(char::is_whitespace)?;
        let name = name.trim_start();
        let name = name.strip_prefix('*').unwrap_or(name);
        let valid = digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit());
        (valid && name == asset).then(|| digest.to_ascii_lowercase())
    })
}

// -- Install identity ------------------------------------------------------

/// The package manager owning an install path, when known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Manager {
    Cargo,
    Npm,
    Homebrew,
    Nix,
    Snap,
    System,
    Scoop,
    Chocolatey,
    Winget,
}

impl Manager {
    fn label(self) -> &'static str {
        match self {
            Manager::Cargo => "cargo",
            Manager::Npm => "npm",
            Manager::Homebrew => "homebrew",
            Manager::Nix => "nix",
            Manager::Snap => "snap",
            Manager::System => "system",
            Manager::Scoop => "scoop",
            Manager::Chocolatey => "chocolatey",
            Manager::Winget => "winget",
        }
    }

    /// The command that upgrades an install this manager owns.
    fn hint(self, version: &str) -> String {
        match self {
            Manager::Cargo => cargo_hint(version),
            Manager::Npm => npm_hint(),
            Manager::Homebrew => "brew upgrade baton".to_string(),
            Manager::Nix => "update it through the Nix configuration that installed it".to_string(),
            Manager::Snap => "snap refresh baton".to_string(),
            Manager::System => {
                "upgrade it with the OS package manager that installed it".to_string()
            }
            Manager::Scoop => "scoop update baton".to_string(),
            Manager::Chocolatey => "choco upgrade baton".to_string(),
            Manager::Winget => "winget upgrade baton".to_string(),
        }
    }
}

fn cargo_hint(version: &str) -> String {
    format!("cargo install --git {REPO_URL} --tag v{version} --locked")
}

fn npm_hint() -> String {
    if cfg!(windows) {
        "npm update -g @shukelabs/baton".to_string()
    } else {
        "npm update -g @shukelabs/baton (then `baton install` for the standalone copy)".to_string()
    }
}

/// Classifies `exe` (a canonical path) by the manager whose tree holds it.
/// `cargo_home` is the canonical `$CARGO_HOME`, when set.
fn classify(exe: &Path, cargo_home: Option<&Path>) -> Option<Manager> {
    let dir = exe.parent()?;
    if cargo_home.is_some_and(|home| dir == home.join("bin")) {
        return Some(Manager::Cargo);
    }
    let parts: Vec<String> = dir
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().to_lowercase()),
            _ => None,
        })
        .collect();
    let has = |name: &str| parts.iter().any(|part| part == name);
    let has_pair = |a: &str, b: &str| parts.windows(2).any(|w| w[0] == a && w[1] == b);
    if parts.ends_with(&[".cargo".to_string(), "bin".to_string()]) {
        return Some(Manager::Cargo);
    }
    if has("node_modules") {
        return Some(Manager::Npm);
    }
    if cfg!(windows) {
        return if has_pair("scoop", "apps") {
            Some(Manager::Scoop)
        } else if has("chocolatey") {
            Some(Manager::Chocolatey)
        } else if has_pair("winget", "packages") {
            Some(Manager::Winget)
        } else {
            None
        };
    }
    let starts = |prefix: &[&str]| {
        parts.len() >= prefix.len() && parts.iter().zip(prefix).all(|(part, p)| part == p)
    };
    if has("cellar") || has("linuxbrew") || starts(&["opt", "homebrew"]) {
        Some(Manager::Homebrew)
    } else if starts(&["nix", "store"]) {
        Some(Manager::Nix)
    } else if starts(&["snap"]) {
        Some(Manager::Snap)
    } else if matches!(parts.as_slice(), [a, b] if a == "usr" && (b == "bin" || b == "sbin"))
        || matches!(parts.as_slice(), [a] if a == "bin" || a == "sbin")
    {
        Some(Manager::System)
    } else {
        None
    }
}

/// The running executable and its per-install upgrade state paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Install {
    /// Canonical path of the installed executable.
    exe: PathBuf,
    /// The running (installed) release version.
    version: String,
    /// `<baton home>/upgrade`.
    state_dir: PathBuf,
    /// Digest of the canonical path, naming this install's state files.
    key: String,
}

impl Install {
    fn current() -> Result<Self> {
        let exe = std::env::current_exe()
            .and_then(fs::canonicalize)
            .map_err(|err| {
                BatonError::Io(format!("could not resolve the baton executable: {err}"))
            })?;
        let key =
            format!("{:x}", Sha256::digest(exe.to_string_lossy().as_bytes()))[..16].to_string();
        Ok(Self {
            exe,
            version: env!("CARGO_PKG_VERSION").to_string(),
            state_dir: RolesHome::from_env()?.root().join("upgrade"),
            key,
        })
    }

    fn path_string(&self) -> String {
        self.exe.to_string_lossy().into_owned()
    }

    fn shown(&self) -> String {
        shown(&self.exe)
    }

    fn receipt_name(&self) -> String {
        format!("receipt-{}.json", self.key)
    }

    fn receipt_path(&self) -> PathBuf {
        self.state_dir.join(self.receipt_name())
    }

    fn lock_path(&self) -> PathBuf {
        self.state_dir.join(format!("lock-{}", self.key))
    }

    fn plan_path(&self) -> PathBuf {
        self.state_dir.join(format!("plan-{}.json", self.key))
    }

    fn outcome_name(&self) -> String {
        format!("helper-{}.json", self.key)
    }

    fn outcome_path(&self) -> PathBuf {
        self.state_dir.join(self.outcome_name())
    }

    fn helper_exe_path(&self) -> PathBuf {
        self.state_dir.join(format!("helper-{}.exe", self.key))
    }

    fn staged_path(&self) -> PathBuf {
        sibling(&self.exe, STAGED_SUFFIX)
    }

    fn old_path(&self) -> PathBuf {
        sibling(&self.exe, OLD_SUFFIX)
    }
}

fn sibling(exe: &Path, suffix: &str) -> PathBuf {
    let mut name = exe.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// A path for operator messages, without Windows' `\\?\` verbatim prefix.
fn shown(path: &Path) -> String {
    let text = path.to_string_lossy();
    text.strip_prefix(r"\\?\").unwrap_or(&text).to_string()
}

fn canonical_cargo_home() -> Option<PathBuf> {
    let home = std::env::var_os("CARGO_HOME").filter(|value| !value.is_empty())?;
    fs::canonicalize(home).ok()
}

// -- Receipts --------------------------------------------------------------

/// Binds an install to the official archive its binary came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Receipt {
    schema: String,
    path: String,
    version: String,
    archive_sha256: String,
    exe_sha256: String,
}

/// Why `receipt` does not describe the current install, or `None` when every
/// field matches. `official_archive` is the digest the installed version's
/// `SHA256SUMS` lists for this target (`None` when it lists none).
fn receipt_mismatch(
    receipt: &Receipt,
    path: &str,
    version: &str,
    exe_sha256: &str,
    official_archive: Option<&str>,
) -> Option<String> {
    if receipt.path != path {
        Some(format!("receipt path {:?} is not {path:?}", receipt.path))
    } else if receipt.version != version {
        Some(format!(
            "receipt version {} is not the installed {version}",
            receipt.version
        ))
    } else if receipt.exe_sha256 != exe_sha256 {
        Some("the installed binary changed since its receipt was written".to_string())
    } else if official_archive != Some(receipt.archive_sha256.as_str()) {
        Some(format!(
            "receipt archive digest does not match the official v{version} SHA256SUMS"
        ))
    } else {
        None
    }
}

/// Where the installed binary came from, as far as `baton upgrade` can tell.
enum Provenance {
    Managed(Manager),
    Unreceipted,
    Mismatch(String),
    Receipted,
}

impl Provenance {
    fn label(&self) -> String {
        match self {
            Provenance::Managed(manager) => format!("managed:{}", manager.label()),
            Provenance::Unreceipted => "unreceipted".to_string(),
            Provenance::Mismatch(_) => "receipt-mismatch".to_string(),
            Provenance::Receipted => "receipted".to_string(),
        }
    }

    /// Why a normal upgrade refuses this install, with what to do instead.
    fn refusal(&self, install: &Install) -> Option<String> {
        let others = format!(
            "if another installer owns it, upgrade it there: `{}` or `{}`",
            cargo_hint("<version>"),
            npm_hint()
        );
        match self {
            Provenance::Receipted => None,
            Provenance::Managed(manager) => Some(format!(
                "{} is managed by {}; upgrade it with `{}`",
                install.shown(),
                manager.label(),
                manager.hint("<version>")
            )),
            Provenance::Unreceipted => Some(format!(
                "{} has no upgrade receipt, so it is never overwritten; if it came from a GitHub Release archive, run `baton upgrade --adopt` once; {others}",
                install.shown()
            )),
            Provenance::Mismatch(reason) => Some(format!(
                "{} does not match its upgrade receipt ({reason}), so it is never overwritten; rerun `baton upgrade --adopt` if it is an official release binary; {others}",
                install.shown()
            )),
        }
    }
}

fn provenance(install: &Install, release: &Release, scratch: &Scratch) -> Result<Provenance> {
    if let Some(manager) = classify(&install.exe, canonical_cargo_home().as_deref()) {
        return Ok(Provenance::Managed(manager));
    }
    let raw = match fs::read_to_string(install.receipt_path()) {
        Ok(raw) => raw,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Provenance::Unreceipted),
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not read upgrade receipt {:?}: {err}",
                install.receipt_path()
            )));
        }
    };
    let Ok(receipt) = serde_json::from_str::<Receipt>(&raw) else {
        return Ok(Provenance::Mismatch("the receipt is malformed".to_string()));
    };
    let sums = release.checksums(&install.version, scratch)?;
    let official = checksum_for(&sums, &release.asset(&install.version));
    let exe_sha256 = sha256_file(&install.exe)?;
    Ok(
        match receipt_mismatch(
            &receipt,
            &install.path_string(),
            &install.version,
            &exe_sha256,
            official.as_deref(),
        ) {
            Some(reason) => Provenance::Mismatch(reason),
            None => Provenance::Receipted,
        },
    )
}

fn write_receipt(install: &Install, receipt: &Receipt) -> Result<()> {
    let json = serde_json::to_string_pretty(receipt)
        .map_err(|err| BatonError::Io(format!("could not serialize upgrade receipt: {err}")))?;
    fs::create_dir_all(&install.state_dir).map_err(|err| {
        BatonError::Io(format!("could not create {:?}: {err}", install.state_dir))
    })?;
    mailbox::atomic_write(&install.state_dir, &install.receipt_name(), &json)
}

// -- Release source --------------------------------------------------------

struct Release {
    base: String,
    target: &'static str,
}

impl Release {
    fn from_env() -> Result<Self> {
        let target = host_target().ok_or_else(|| {
            BatonError::Config(
                "baton upgrade has no published release archive for this host target".to_string(),
            )
        })?;
        let base = std::env::var(RELEASE_BASE_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| RELEASE_BASE.to_string());
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            target,
        })
    }

    fn asset(&self, version: &str) -> String {
        asset_name(version, self.target)
    }

    fn url(&self, version: &str, file: &str) -> String {
        format!("{}/download/v{version}/{file}", self.base)
    }

    fn source(&self, version: &str) -> String {
        self.url(version, &self.asset(version))
    }

    /// The tag `releases/latest` redirects to.
    fn latest(&self) -> Result<String> {
        let url = format!("{}/latest", self.base);
        let output = run_tool(
            "curl",
            ["-fsSIL", "--write-out", "\n%{url_effective}", url.as_str()],
        )?;
        let text = String::from_utf8_lossy(&output.stdout);
        let effective = text.lines().last().unwrap_or_default();
        parse_latest_tag(effective).ok_or_else(|| {
            BatonError::Transport(format!(
                "could not resolve the latest release from {url} (landed on {effective:?}); pass --version <version>"
            ))
        })
    }

    /// Downloads `version`'s `SHA256SUMS` into `scratch` and returns its text.
    fn checksums(&self, version: &str, scratch: &Scratch) -> Result<String> {
        let dest = scratch.path.join(format!("SHA256SUMS-{version}"));
        download(&self.url(version, "SHA256SUMS"), &dest)?;
        fs::read_to_string(&dest)
            .map_err(|err| BatonError::Io(format!("could not read {dest:?}: {err}")))
    }

    /// Downloads, verifies, and extracts `version`'s archive.
    fn fetch(&self, version: &str, scratch: &Scratch) -> Result<Fetched> {
        let asset = self.asset(version);
        let sums = self.checksums(version, scratch)?;
        let expected = checksum_for(&sums, &asset).ok_or_else(|| {
            BatonError::Io(format!(
                "release v{version} SHA256SUMS lists no {asset}; nothing was changed"
            ))
        })?;
        let archive = scratch.path.join(&asset);
        download(&self.url(version, &asset), &archive)?;
        let actual = sha256_file(&archive)?;
        if actual != expected {
            return Err(BatonError::Io(format!(
                "checksum mismatch for {asset}: SHA256SUMS lists {expected}, the download is {actual}; nothing was changed"
            )));
        }
        let dir = scratch.path.join(format!("extract-{version}"));
        fs::create_dir_all(&dir)
            .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
        let flags = if asset.ends_with(".zip") {
            "-xf"
        } else {
            "-xzf"
        };
        run_tool(
            "tar",
            [
                OsStr::new(flags),
                archive.as_os_str(),
                OsStr::new("-C"),
                dir.as_os_str(),
            ],
        )?;
        let binary = dir.join(EXE_NAME);
        if !binary.is_file() {
            return Err(BatonError::Io(format!(
                "{asset} does not contain {EXE_NAME} at its root; nothing was changed"
            )));
        }
        Ok(Fetched {
            archive_sha256: actual,
            binary,
        })
    }
}

/// A verified, extracted release binary.
struct Fetched {
    archive_sha256: String,
    binary: PathBuf,
}

fn download(url: &str, dest: &Path) -> Result<()> {
    run_tool(
        "curl",
        [
            OsStr::new("-fsSL"),
            OsStr::new("--retry"),
            OsStr::new("2"),
            OsStr::new("-o"),
            dest.as_os_str(),
            OsStr::new(url),
        ],
    )
    .map(drop)
    .map_err(|err| BatonError::Transport(format!("could not download {url}: {err}")))
}

/// The system tool `name`; on Windows the `System32` copy, so an MSYS or Git
/// Bash `tar` earlier on `PATH` (which cannot read `.zip`) is never picked.
fn tool(name: &str) -> PathBuf {
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        let path = Path::new(&root)
            .join("System32")
            .join(format!("{name}.exe"));
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from(name)
}

fn run_tool<I, S>(name: &str, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(tool(name))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                BatonError::Config(format!(
                    "baton upgrade requires `{name}`, which was not found"
                ))
            } else {
                BatonError::Io(format!("could not run {name}: {err}"))
            }
        })?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(BatonError::Io(format!(
            "{name} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .map_err(|err| BatonError::Io(format!("could not open {path:?}: {err}")))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buf)
            .map_err(|err| BatonError::Io(format!("could not read {path:?}: {err}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// The version `path --version` reports.
fn binary_version(path: &Path) -> Result<String> {
    let mut command = Command::new(path);
    command.arg("--version").stdin(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    let output = command
        .output()
        .map_err(|err| BatonError::Io(format!("could not run {} --version: {err}", shown(path))))?;
    let text = String::from_utf8_lossy(&output.stdout);
    match text.trim().strip_prefix("baton ") {
        Some(version) if output.status.success() => Ok(version.trim().to_string()),
        _ => Err(BatonError::Io(format!(
            "{} --version printed {:?}",
            shown(path),
            text.trim()
        ))),
    }
}

/// A private temp directory removed on drop. Lives under the OS temp dir, so
/// a read-only `--check` never writes beside the install or under the baton
/// home.
struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new() -> Result<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path =
            std::env::temp_dir().join(format!("baton-upgrade-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path)
            .map_err(|err| BatonError::Io(format!("could not create {path:?}: {err}")))?;
        Ok(Self { path })
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// -- Locking, confirmation, and stale artifacts ----------------------------

fn acquire_lock(install: &Install) -> Result<File> {
    fs::create_dir_all(&install.state_dir).map_err(|err| {
        BatonError::Io(format!("could not create {:?}: {err}", install.state_dir))
    })?;
    let path = install.lock_path();
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|err| BatonError::Io(format!("could not open upgrade lock {path:?}: {err}")))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(BatonError::Io(format!(
            "another baton upgrade is running for {}",
            install.shown()
        ))),
        Err(TryLockError::Error(err)) => {
            Err(BatonError::Io(format!("could not lock {path:?}: {err}")))
        }
    }
}

/// Asks `question` on stderr. `--yes` answers for a non-interactive caller;
/// without it, a non-TTY stdin is an error rather than a silent default.
fn confirm(question: &str, yes: bool) -> Result<bool> {
    let stdin = io::stdin();
    let interactive = stdin.is_terminal();
    confirm_with(question, yes, interactive, stdin.lock(), io::stderr())
}

fn confirm_with(
    question: &str,
    yes: bool,
    interactive: bool,
    mut input: impl BufRead,
    mut prompt: impl Write,
) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    if !interactive {
        return Err(BatonError::Usage(
            "baton upgrade needs confirmation; pass --yes when stdin is not a terminal".to_string(),
        ));
    }
    write!(prompt, "{question} [y/N] ").map_err(io_err)?;
    prompt.flush().map_err(io_err)?;
    let mut answer = String::new();
    input.read_line(&mut answer).map_err(io_err)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// The Windows helper's hand-off: everything it needs after the updater exits.
#[derive(Debug, Serialize, Deserialize)]
struct HelperPlan {
    install: Install,
    new_version: String,
    source: String,
    receipt: Receipt,
    /// Set once the helper is spawned; a plan without one never ran.
    helper_pid: Option<u32>,
}

/// What the Windows helper did, for `--check` and the operator.
#[derive(Debug, Serialize, Deserialize)]
struct HelperOutcome {
    status: String,
    path: String,
    old_version: String,
    new_version: String,
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    recovery: String,
}

fn read_plan(path: &Path) -> Option<HelperPlan> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

/// The PID of a still-running helper for this install, if any.
fn pending_helper(install: &Install) -> Option<u32> {
    read_plan(&install.plan_path())
        .and_then(|plan| plan.helper_pid)
        .filter(|pid| process_alive(*pid))
}

fn stale_artifacts(install: &Install) -> Vec<PathBuf> {
    [
        install.staged_path(),
        install.old_path(),
        install.plan_path(),
        install.helper_exe_path(),
    ]
    .into_iter()
    .filter(|path| path.exists())
    .collect()
}

/// Why leftovers of an earlier run must not be touched yet: a helper is
/// still finishing, or `.baton-upgrade-old` may be the only copy of the
/// previous binary after a failed rollback. The copy is obsolete once the
/// install matches its receipt (the swap completed) or is byte-identical to it.
fn stale_blocker(install: &Install) -> Result<Option<String>> {
    if let Some(pid) = pending_helper(install) {
        return Ok(Some(format!(
            "an earlier upgrade helper (pid {pid}) is still finishing {}; wait for it, then see `baton upgrade --check`",
            install.shown()
        )));
    }
    let old = install.old_path();
    if !old.exists() {
        return Ok(None);
    }
    let exe_sha256 = sha256_file(&install.exe)?;
    let receipted = fs::read_to_string(install.receipt_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Receipt>(&raw).ok())
        .is_some_and(|receipt| {
            receipt.path == install.path_string()
                && receipt.version == install.version
                && receipt.exe_sha256 == exe_sha256
        });
    if receipted || sha256_file(&old)? == exe_sha256 {
        return Ok(None);
    }
    Ok(Some(format!(
        "{} may be the only copy of the previous binary after a failed upgrade; restore it with: mv \"{}\" \"{}\" (or delete it if {} is the binary you want), then rerun",
        shown(&old),
        shown(&old),
        install.shown(),
        install.shown()
    )))
}

/// Removes leftovers of an earlier interrupted run. Called only by mutating
/// runs holding the lock, after `stale_blocker` passed and the operator
/// confirmed.
fn clean_stale(install: &Install) -> Result<()> {
    for path in stale_artifacts(install) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not remove stale upgrade file {}: {err}",
                    shown(&path)
                )));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 only checks for the PID's existence; nothing is sent.
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    alive || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };
    // SAFETY: a synchronize-only handle is opened, polled without waiting,
    // and closed exactly once.
    unsafe {
        let handle = OpenProcess(PROCESS_SYNCHRONIZE, FALSE, pid);
        if handle == 0 {
            return false;
        }
        let alive = WaitForSingleObject(handle, 0) == WAIT_TIMEOUT;
        CloseHandle(handle);
        alive
    }
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    false
}

/// Without `--version` an upgrade only moves forward; an explicit version
/// may also roll back to an older release.
fn up_to_date(installed: &str, target: &str, explicit: bool) -> bool {
    target == installed || (!explicit && version_key(target) < version_key(installed))
}

// -- --check ----------------------------------------------------------------

#[derive(Debug, Serialize)]
struct CheckReport {
    installed_version: String,
    path: String,
    target: String,
    selected_version: String,
    available_version: Option<String>,
    source: String,
    provenance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance_detail: Option<String>,
    service: ServiceView,
    helper_outcome: Option<serde_json::Value>,
    stale_artifacts: Vec<String>,
    blockers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ServiceView {
    control: String,
    #[serde(flatten)]
    snapshot: ServiceSnapshot,
}

fn check(version: Option<String>, control: Option<String>) -> Result<CheckReport> {
    let install = Install::current()?;
    let release = Release::from_env()?;
    let scratch = Scratch::new()?;
    let explicit = version.is_some();
    let (selected, available) = match version {
        Some(version) => (version, release.latest().ok()),
        None => {
            let latest = release.latest()?;
            (latest.clone(), Some(latest))
        }
    };
    let provenance = provenance(&install, &release, &scratch)?;
    let control_dir = roles::resolve_control_dir(control)?;
    let snapshot = service::service_snapshot(&control_dir)?;

    let mut blockers = Vec::new();
    blockers.extend(provenance.refusal(&install));
    if up_to_date(&install.version, &selected, explicit) {
        blockers.push(format!("already up to date at {}", install.version));
    } else {
        let sums = release.checksums(&selected, &scratch)?;
        if checksum_for(&sums, &release.asset(&selected)).is_none() {
            blockers.push(format!(
                "release v{selected} lists no {}",
                release.asset(&selected)
            ));
        }
    }
    if cfg!(windows) {
        blockers.extend(windows_service_blocker(&control_dir, &snapshot));
    }
    blockers.extend(stale_blocker(&install)?);

    let helper_outcome = fs::read_to_string(install.outcome_path())
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok());
    Ok(CheckReport {
        installed_version: install.version.clone(),
        path: install.shown(),
        target: release.target.to_string(),
        source: release.source(&selected),
        selected_version: selected,
        available_version: available,
        provenance: provenance.label(),
        provenance_detail: match provenance {
            Provenance::Mismatch(reason) => Some(reason),
            _ => None,
        },
        service: ServiceView {
            control: shown(&control_dir),
            snapshot,
        },
        helper_outcome,
        stale_artifacts: stale_artifacts(&install).iter().map(|p| shown(p)).collect(),
        blockers,
    })
}

/// On Windows, why the binary cannot be replaced right now, with the
/// operator's steps. A live service defers even with no sessions: baton does
/// not know which supervisor owns it, so it never stops or restarts it.
fn windows_service_blocker(control: &Path, snapshot: &ServiceSnapshot) -> Option<String> {
    let control = shown(control);
    if snapshot.live {
        Some(format!(
            "a baton service is live on {control} ({} active session(s)); to upgrade:\n  1. stop the service with the supervisor that runs `baton service run` (or `baton service teardown --control {control}`)\n  2. rerun `baton upgrade`\n  3. start the service again with its supervisor",
            snapshot.active_sessions.len()
        ))
    } else if !snapshot.active_sessions.is_empty() {
        Some(format!(
            "managed session(s) {} on {control} still run baton; stop them with `baton service stop --control {control} --session <id>`, then rerun `baton upgrade`",
            snapshot.active_sessions.join(", ")
        ))
    } else {
        None
    }
}

// -- --adopt ----------------------------------------------------------------

fn adopt(yes: bool, mut out: impl Write) -> Result<()> {
    let install = Install::current()?;
    let release = Release::from_env()?;
    let _lock = acquire_lock(&install)?;
    if let Some(manager) = classify(&install.exe, canonical_cargo_home().as_deref()) {
        return Err(BatonError::Io(
            Provenance::Managed(manager)
                .refusal(&install)
                .unwrap_or_default(),
        ));
    }
    if let Some(blocker) = stale_blocker(&install)? {
        return Err(BatonError::Io(blocker));
    }
    let scratch = Scratch::new()?;
    let fetched = release.fetch(&install.version, &scratch)?;
    let exe_sha256 = sha256_file(&install.exe)?;
    if sha256_file(&fetched.binary)? != exe_sha256 {
        return Err(BatonError::Io(format!(
            "{} is not the official v{} {} binary, so it was not adopted; install from the release archive, or upgrade it with its own installer (`{}` or `{}`)",
            install.shown(),
            install.version,
            release.target,
            cargo_hint(&install.version),
            npm_hint()
        )));
    }
    if !confirm(
        &format!(
            "Adopt {} (baton {}) for `baton upgrade`?",
            install.shown(),
            install.version
        ),
        yes,
    )? {
        return writeln!(out, "adoption cancelled; nothing was changed").map_err(io_err);
    }
    clean_stale(&install)?;
    write_receipt(
        &install,
        &Receipt {
            schema: RECEIPT_SCHEMA.to_string(),
            path: install.path_string(),
            version: install.version.clone(),
            archive_sha256: fetched.archive_sha256,
            exe_sha256,
        },
    )?;
    writeln!(
        out,
        "adopted {} (baton {}); receipt: {}",
        install.shown(),
        install.version,
        shown(&install.receipt_path())
    )
    .map_err(io_err)
}

// -- upgrade ----------------------------------------------------------------

fn upgrade(
    version: Option<String>,
    yes: bool,
    control: Option<String>,
    mut out: impl Write,
) -> Result<()> {
    let install = Install::current()?;
    let release = Release::from_env()?;
    let _lock = acquire_lock(&install)?;
    let scratch = Scratch::new()?;
    if let Some(refusal) = provenance(&install, &release, &scratch)?.refusal(&install) {
        return Err(BatonError::Io(refusal));
    }
    let explicit = version.is_some();
    let target = match version {
        Some(version) => version,
        None => release.latest()?,
    };
    if up_to_date(&install.version, &target, explicit) {
        return writeln!(
            out,
            "baton {} at {} is already up to date (selected release: {target})",
            install.version,
            install.shown()
        )
        .map_err(io_err);
    }

    let control_dir = roles::resolve_control_dir(control)?;
    let before = service::service_snapshot(&control_dir)?;
    if cfg!(windows)
        && let Some(blocker) = windows_service_blocker(&control_dir, &before)
    {
        return writeln!(
            out,
            "upgrade to baton {target} deferred; nothing was changed: {blocker}"
        )
        .map_err(io_err);
    }
    if let Some(blocker) = stale_blocker(&install)? {
        return Err(BatonError::Io(blocker));
    }
    if !confirm(
        &format!(
            "Upgrade {} from baton {} to {target}?",
            install.shown(),
            install.version
        ),
        yes,
    )? {
        return writeln!(out, "upgrade cancelled; nothing was changed").map_err(io_err);
    }
    clean_stale(&install)?;

    let fetched = release.fetch(&target, &scratch)?;
    let reported = binary_version(&fetched.binary)?;
    if reported != target {
        return Err(BatonError::Io(format!(
            "the v{target} archive's binary reports baton {reported}; nothing was changed"
        )));
    }
    let receipt = Receipt {
        schema: RECEIPT_SCHEMA.to_string(),
        path: install.path_string(),
        version: target.clone(),
        archive_sha256: fetched.archive_sha256.clone(),
        exe_sha256: sha256_file(&fetched.binary)?,
    };
    stage(&fetched.binary, &install.staged_path())?;
    let source = release.source(&target);
    #[cfg(windows)]
    return schedule_helper(install, target, source, receipt, &control_dir, out);
    #[cfg(not(windows))]
    {
        replace_in_place(&install, &target, &receipt)?;
        writeln!(
            out,
            "upgraded baton {} -> {target} at {}",
            install.version,
            install.shown()
        )
        .and_then(|()| writeln!(out, "source: {source}"))
        .and_then(|()| {
            writeln!(
                out,
                "service: {}",
                unix_service_outcome(&control_dir, &before)
            )
        })
        .and_then(|()| {
            writeln!(
                out,
                "recovery: `baton upgrade --version {} --yes` reinstalls the previous release",
                install.version
            )
        })
        .map_err(io_err)
    }
}

/// Copies the verified binary beside the install so the final swap is a
/// same-directory rename.
fn stage(binary: &Path, staged: &Path) -> Result<()> {
    fs::copy(binary, staged).map_err(|err| {
        let _ = fs::remove_file(staged);
        BatonError::Io(format!(
            "could not stage {}: {err}; nothing was changed",
            shown(staged)
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staged, fs::Permissions::from_mode(0o755)).map_err(|err| {
            BatonError::Io(format!("could not mark {staged:?} executable: {err}"))
        })?;
    }
    Ok(())
}

/// Swaps the staged binary over the install, keeping the old binary and
/// receipt until the new version is verified and its receipt written.
#[cfg(not(windows))]
fn replace_in_place(install: &Install, target: &str, receipt: &Receipt) -> Result<()> {
    let (exe, staged, old) = (&install.exe, install.staged_path(), install.old_path());
    let receipt_backup = fs::read_to_string(install.receipt_path()).ok();
    if let Err(err) = fs::copy(exe, &old) {
        let _ = fs::remove_file(&staged);
        return Err(BatonError::Io(format!(
            "could not back up {}: {err}; nothing was changed",
            install.shown()
        )));
    }
    if let Err(err) = fs::rename(&staged, exe) {
        let _ = fs::remove_file(&staged);
        let _ = fs::remove_file(&old);
        return Err(BatonError::Io(format!(
            "could not replace {}: {err}; nothing was changed",
            install.shown()
        )));
    }
    let installed = binary_version(exe).and_then(|reported| {
        if reported == target {
            write_receipt(install, receipt)
        } else {
            Err(BatonError::Io(format!(
                "the installed binary reports baton {reported}, expected {target}"
            )))
        }
    });
    match installed {
        Ok(()) => {
            let _ = fs::remove_file(&old);
            Ok(())
        }
        Err(err) => Err(BatonError::Io(format!(
            "{err}; {}",
            roll_back(install, receipt_backup.as_deref())
        ))),
    }
}

/// Restores the backed-up binary and receipt, describing the result.
fn roll_back(install: &Install, receipt_backup: Option<&str>) -> String {
    let old = install.old_path();
    let binary = fs::rename(&old, &install.exe);
    let receipt = match receipt_backup {
        Some(raw) => mailbox::atomic_write(&install.state_dir, &install.receipt_name(), raw),
        None => Ok(()),
    };
    match (binary, receipt) {
        (Ok(()), Ok(())) => format!("rolled back to baton {}", install.version),
        (Err(err), _) => format!(
            "ROLLBACK FAILED ({err}); restore the previous binary with: mv \"{}\" \"{}\"",
            shown(&old),
            install.shown()
        ),
        (Ok(()), Err(err)) => format!(
            "restored the previous binary, but not its receipt ({err}); rerun `baton upgrade --adopt`"
        ),
    }
}

#[cfg(not(windows))]
fn unix_service_outcome(control: &Path, before: &ServiceSnapshot) -> String {
    let control_shown = shown(control);
    if !before.live {
        return format!("no live baton service on {control_shown}");
    }
    match service::service_snapshot(control) {
        Ok(after) if after.live => format!(
            "live on {control_shown}; {} existing session(s) unchanged; the next `baton service start` launches the new binary",
            after.active_sessions.len()
        ),
        Ok(_) => format!("the service on {control_shown} is no longer live; restart it"),
        Err(err) => format!("could not re-probe the service on {control_shown}: {err}"),
    }
}

/// Hands the swap to a detached copy of the current binary that waits for
/// this process to exit, since Windows cannot replace a running image.
#[cfg(windows)]
fn schedule_helper(
    install: Install,
    target: String,
    source: String,
    receipt: Receipt,
    control: &Path,
    mut out: impl Write,
) -> Result<()> {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};

    let helper = install.helper_exe_path();
    let plan_path = install.plan_path();
    let outcome_path = install.outcome_path();
    let staged = install.staged_path();
    let abandon = |err: String| {
        let _ = fs::remove_file(&staged);
        let _ = fs::remove_file(&helper);
        let _ = fs::remove_file(&plan_path);
        BatonError::Io(format!("{err}; nothing was changed"))
    };
    let _ = fs::remove_file(&outcome_path);
    fs::copy(&install.exe, &helper)
        .map_err(|err| abandon(format!("could not copy the upgrade helper: {err}")))?;
    let mut plan = HelperPlan {
        install,
        new_version: target,
        source,
        receipt,
        helper_pid: None,
    };
    write_plan(&plan).map_err(|err| abandon(err.to_string()))?;
    let child = Command::new(&helper)
        .arg("upgrade")
        .arg("--finish")
        .arg(&plan_path)
        .arg("--wait-pid")
        .arg(std::process::id().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .map_err(|err| abandon(format!("could not start the upgrade helper: {err}")))?;
    plan.helper_pid = Some(child.id());
    write_plan(&plan)?;
    let install = &plan.install;
    writeln!(
        out,
        "upgrade scheduled: baton {} -> {} at {}",
        install.version,
        plan.new_version,
        install.shown()
    )
    .and_then(|()| writeln!(out, "source: {}", plan.source))
    .and_then(|()| {
        writeln!(
            out,
            "service: no live service or managed session on {}",
            shown(control)
        )
    })
    .and_then(|()| {
        writeln!(
            out,
            "a helper finishes the swap after this process exits and records its outcome, with recovery steps, in {}",
            shown(&outcome_path)
        )
    })
    .and_then(|()| writeln!(out, "check it with `baton upgrade --check`"))
    .map_err(io_err)
}

#[cfg(windows)]
fn write_plan(plan: &HelperPlan) -> Result<()> {
    let json = serde_json::to_string_pretty(plan)
        .map_err(|err| BatonError::Io(format!("could not serialize upgrade plan: {err}")))?;
    let name = format!("plan-{}.json", plan.install.key);
    mailbox::atomic_write(&plan.install.state_dir, &name, &json)
}

/// The Windows helper: waits for the updater, swaps the staged binary in,
/// verifies it, writes the receipt, and records the outcome. Any failure
/// restores the previous binary.
fn finish(plan_path: &Path, wait_pid: u32) -> Result<()> {
    let raw = fs::read_to_string(plan_path).map_err(|err| {
        BatonError::Io(format!("could not read upgrade plan {plan_path:?}: {err}"))
    })?;
    let plan: HelperPlan = serde_json::from_str(&raw).map_err(|err| {
        BatonError::Decode(format!("malformed upgrade plan {plan_path:?}: {err}"))
    })?;
    let install = &plan.install;
    let result = if wait_until(|| !process_alive(wait_pid)) {
        let mut lock = None;
        wait_until(|| {
            lock = acquire_lock(install).ok();
            lock.is_some()
        });
        match lock {
            Some(_lock) => swap(&plan),
            None => Err("could not take the upgrade lock; nothing was changed".to_string()),
        }
    } else {
        Err("the updater did not exit; nothing was changed".to_string())
    };
    let (status, error, recovery) = match result {
        Ok(()) => (
            "succeeded",
            None,
            format!(
                "`baton upgrade --version {} --yes` reinstalls the previous release",
                install.version
            ),
        ),
        Err(err) => ("failed", Some(err.clone()), recovery_for(&err, install)),
    };
    let outcome = HelperOutcome {
        status: status.to_string(),
        path: install.shown(),
        old_version: install.version.clone(),
        new_version: plan.new_version.clone(),
        source: plan.source.clone(),
        error,
        recovery,
    };
    let json = serde_json::to_string_pretty(&outcome)
        .map_err(|err| BatonError::Io(format!("could not serialize upgrade outcome: {err}")))?;
    mailbox::atomic_write(&install.state_dir, &install.outcome_name(), &json)?;
    let _ = fs::remove_file(install.staged_path());
    let _ = fs::remove_file(plan_path);
    Ok(())
}

fn recovery_for(err: &str, install: &Install) -> String {
    if err.contains("ROLLBACK FAILED") {
        format!(
            "move {} back to {}",
            shown(&install.old_path()),
            install.shown()
        )
    } else {
        format!(
            "baton {} is still installed; rerun `baton upgrade` once the cause is fixed",
            install.version
        )
    }
}

fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + HELPER_WAIT;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The helper's swap. Renames are retried briefly: a scanner may still hold
/// the just-exited updater's image open.
fn swap(plan: &HelperPlan) -> std::result::Result<(), String> {
    let install = &plan.install;
    let (exe, staged, old) = (&install.exe, install.staged_path(), install.old_path());
    let receipt_backup = fs::read_to_string(install.receipt_path()).ok();
    retry_rename(exe, &old).map_err(|err| {
        format!(
            "could not move {} aside: {err}; nothing was changed",
            install.shown()
        )
    })?;
    if let Err(err) = retry_rename(&staged, exe) {
        return Err(format!(
            "could not install the new binary: {err}; {}",
            roll_back(install, receipt_backup.as_deref())
        ));
    }
    let installed = binary_version(exe)
        .map_err(|err| err.to_string())
        .and_then(|reported| {
            if reported == plan.new_version {
                write_receipt(install, &plan.receipt).map_err(|err| err.to_string())
            } else {
                Err(format!(
                    "the installed binary reports baton {reported}, expected {}",
                    plan.new_version
                ))
            }
        });
    match installed {
        Ok(()) => {
            let _ = fs::remove_file(&old);
            Ok(())
        }
        Err(err) => {
            let _ = fs::remove_file(exe);
            Err(format!(
                "{err}; {}",
                roll_back(install, receipt_backup.as_deref())
            ))
        }
    }
}

fn retry_rename(from: &Path, to: &Path) -> io::Result<()> {
    let mut attempts = 0;
    loop {
        match fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(_) if attempts < 20 => {
                attempts += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

fn io_err(err: io::Error) -> BatonError {
    BatonError::Io(format!("could not write output: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> Receipt {
        Receipt {
            schema: RECEIPT_SCHEMA.to_string(),
            path: "/opt/baton/bin/baton".to_string(),
            version: "1.2.3".to_string(),
            archive_sha256: "a".repeat(64),
            exe_sha256: "e".repeat(64),
        }
    }

    #[test]
    fn receipt_matches_only_when_every_field_matches() {
        let r = receipt();
        let archive = "a".repeat(64);
        let exe = "e".repeat(64);
        let ok = |path: &str, version: &str, exe: &str, archive: Option<&str>| {
            receipt_mismatch(&r, path, version, exe, archive)
        };
        assert_eq!(ok(&r.path, "1.2.3", &exe, Some(&archive)), None);
        assert!(
            ok("/elsewhere/baton", "1.2.3", &exe, Some(&archive))
                .unwrap()
                .contains("path")
        );
        assert!(
            ok(&r.path, "1.2.4", &exe, Some(&archive))
                .unwrap()
                .contains("version")
        );
        assert!(
            ok(&r.path, "1.2.3", &"f".repeat(64), Some(&archive))
                .unwrap()
                .contains("changed")
        );
        assert!(
            ok(&r.path, "1.2.3", &exe, Some(&"b".repeat(64)))
                .unwrap()
                .contains("SHA256SUMS")
        );
        assert!(ok(&r.path, "1.2.3", &exe, None).is_some());
    }

    #[test]
    fn checksum_lookup_reads_sha256sum_lines() {
        let digest = "0123456789abcdef".repeat(4);
        let sums = format!(
            "{digest}  baton-1.0.0-x86_64-unknown-linux-gnu.tar.gz\n{}  *baton-1.0.0-x86_64-pc-windows-msvc.zip\nnot-a-digest  baton-1.0.0-aarch64-apple-darwin.tar.gz\n",
            digest.to_uppercase()
        );
        assert_eq!(
            checksum_for(&sums, "baton-1.0.0-x86_64-unknown-linux-gnu.tar.gz"),
            Some(digest.clone())
        );
        assert_eq!(
            checksum_for(&sums, "baton-1.0.0-x86_64-pc-windows-msvc.zip"),
            Some(digest)
        );
        assert_eq!(
            checksum_for(&sums, "baton-1.0.0-aarch64-apple-darwin.tar.gz"),
            None
        );
        assert_eq!(checksum_for(&sums, "baton-1.0.0"), None);
    }

    #[test]
    fn latest_tag_parses_the_redirect_target() {
        assert_eq!(
            parse_latest_tag("https://github.com/shukebeta/baton/releases/tag/v0.17.2"),
            Some("0.17.2".to_string())
        );
        assert_eq!(
            parse_latest_tag("https://github.com/shukebeta/baton/releases/tag/v1.0.10/\r"),
            Some("1.0.10".to_string())
        );
        assert_eq!(
            parse_latest_tag("https://github.com/shukebeta/baton/releases/latest"),
            None
        );
        assert_eq!(parse_latest_tag(".../tag/vnext"), None);
    }

    #[test]
    fn versions_normalize_and_order() {
        assert_eq!(normalize_version("v0.17.2"), Ok("0.17.2".to_string()));
        assert_eq!(normalize_version("1.0.0"), Ok("1.0.0".to_string()));
        for bad in ["", "v", "1.0", "1.0.0.0", "1.0.x", "1.0.0-rc1", "../1.0.0"] {
            assert!(normalize_version(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(version_key("0.17.10") > version_key("0.17.9"));
        assert!(version_key("1.0.0") > version_key("0.99.9"));
    }

    #[test]
    fn asset_names_follow_the_release_matrix() {
        assert_eq!(
            asset_name("0.17.2", "x86_64-unknown-linux-gnu"),
            "baton-0.17.2-x86_64-unknown-linux-gnu.tar.gz"
        );
        assert_eq!(
            asset_name("0.17.2", "x86_64-pc-windows-msvc"),
            "baton-0.17.2-x86_64-pc-windows-msvc.zip"
        );
    }

    #[test]
    fn managed_paths_are_classified() {
        let cargo_home = Path::new("/srv/cargo");
        let cases: &[(&str, Option<Manager>)] = &[
            ("/srv/cargo/bin/baton", Some(Manager::Cargo)),
            ("/home/u/.cargo/bin/baton", Some(Manager::Cargo)),
            (
                "/home/u/.npm-global/lib/node_modules/@shukelabs/baton-linux-x64/bin/baton",
                Some(Manager::Npm),
            ),
            ("/home/u/.local/bin/baton", None),
            ("/opt/baton/baton", None),
        ];
        for (path, expected) in cases {
            assert_eq!(
                classify(Path::new(path), Some(cargo_home)),
                *expected,
                "{path}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_os_manager_paths_are_classified() {
        let cases: &[(&str, Option<Manager>)] = &[
            ("/usr/bin/baton", Some(Manager::System)),
            ("/bin/baton", Some(Manager::System)),
            ("/usr/local/bin/baton", None),
            ("/opt/homebrew/bin/baton", Some(Manager::Homebrew)),
            (
                "/usr/local/Cellar/baton/1.0.0/bin/baton",
                Some(Manager::Homebrew),
            ),
            (
                "/home/linuxbrew/.linuxbrew/bin/baton",
                Some(Manager::Homebrew),
            ),
            ("/nix/store/abc-baton/bin/baton", Some(Manager::Nix)),
            ("/snap/baton/12/bin/baton", Some(Manager::Snap)),
        ];
        for (path, expected) in cases {
            assert_eq!(classify(Path::new(path), None), *expected, "{path}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_manager_paths_are_classified() {
        let cases: &[(&str, Option<Manager>)] = &[
            (
                r"C:\Users\u\scoop\apps\baton\current\baton.exe",
                Some(Manager::Scoop),
            ),
            (
                r"C:\ProgramData\chocolatey\bin\baton.exe",
                Some(Manager::Chocolatey),
            ),
            (
                r"C:\Users\u\AppData\Local\Microsoft\WinGet\Packages\baton\baton.exe",
                Some(Manager::Winget),
            ),
            (r"C:\Users\u\.cargo\bin\baton.exe", Some(Manager::Cargo)),
            (r"C:\bin\baton.exe", None),
            (r"C:\Tools\baton\baton.exe", None),
        ];
        for (path, expected) in cases {
            assert_eq!(classify(Path::new(path), None), *expected, "{path}");
        }
    }

    #[test]
    fn confirmation_policy() {
        let none = io::empty();
        assert!(confirm_with("q", true, false, none, io::sink()).unwrap());
        let err = confirm_with("q", false, false, io::empty(), io::sink()).unwrap_err();
        assert!(err.to_string().contains("--yes"), "{err}");
        assert!(confirm_with("q", false, true, &b"y\n"[..], io::sink()).unwrap());
        assert!(confirm_with("q", false, true, &b"YES\n"[..], io::sink()).unwrap());
        assert!(!confirm_with("q", false, true, &b"\n"[..], io::sink()).unwrap());
        assert!(!confirm_with("q", false, true, &b""[..], io::sink()).unwrap());
    }

    #[test]
    fn windows_blocker_prints_the_operator_steps() {
        let live = ServiceSnapshot {
            live: true,
            active_sessions: Vec::new(),
        };
        let text = windows_service_blocker(Path::new("ctl"), &live).unwrap();
        for step in [
            "1. stop the service",
            "2. rerun `baton upgrade`",
            "3. start the service again",
        ] {
            assert!(text.contains(step), "{text}");
        }
        let sessions = ServiceSnapshot {
            live: false,
            active_sessions: vec!["s1".to_string()],
        };
        assert!(
            windows_service_blocker(Path::new("ctl"), &sessions)
                .unwrap()
                .contains("s1")
        );
        assert_eq!(
            windows_service_blocker(Path::new("ctl"), &ServiceSnapshot::default()),
            None
        );
    }
}
