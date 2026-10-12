//! `baton upgrade` end to end: an installed copy of the built binary is
//! checked, adopted, and upgraded against a `file://` release fixture laid
//! out like GitHub's `download/v<version>/<asset>` tree.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use baton::upgrade::{RELEASE_BASE_ENV, asset_name, host_target};
use sha2::{Digest, Sha256};

const CURRENT: &str = env!("CARGO_PKG_VERSION");
const NEW: &str = "99.0.0";

#[cfg(windows)]
const EXE: &str = "baton.exe";
#[cfg(not(windows))]
const EXE: &str = "baton";

type Tree = BTreeMap<PathBuf, Vec<u8>>;

struct Fixture {
    root: PathBuf,
    exe: PathBuf,
    home: PathBuf,
    releases: PathBuf,
    control: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::installed_at(name, "install")
    }

    /// Installs a copy of the built binary under `<root>/<install_dir>` and
    /// publishes that same binary as the current release.
    fn installed_at(name: &str, install_dir: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "baton-upgrade-{name}-{}-{nanos}",
            std::process::id()
        ));
        let dir = root.join(install_dir);
        fs::create_dir_all(&dir).expect("create install dir");
        let exe = dir.join(EXE);
        fs::copy(env!("CARGO_BIN_EXE_baton"), &exe).expect("install baton copy");
        let fixture = Self {
            exe,
            home: root.join("home"),
            releases: root.join("releases"),
            control: root.join("control"),
            root,
        };
        fixture.publish(CURRENT, Path::new(env!("CARGO_BIN_EXE_baton")));
        fixture
    }

    /// Publishes `binary` as release `version`'s archive plus `SHA256SUMS`.
    fn publish(&self, version: &str, binary: &Path) {
        let dir = self.releases.join("download").join(format!("v{version}"));
        let stage = self.root.join(format!("stage-{version}"));
        fs::create_dir_all(&dir).expect("create release dir");
        fs::create_dir_all(&stage).expect("create stage dir");
        fs::copy(binary, stage.join(EXE)).expect("stage release binary");
        let asset = asset_name(version, host_target().expect("published host target"));
        let archive = dir.join(&asset);
        archive_into(&stage, &archive);
        let digest = format!("{:x}", Sha256::digest(fs::read(&archive).unwrap()));
        fs::write(dir.join("SHA256SUMS"), format!("{digest}  {asset}\n")).expect("write sums");
    }

    /// A stand-in for release [`NEW`]: `--version` prints `baton 99.0.0`
    /// (or an `<exe>.version` sidecar's contents).
    #[cfg(windows)]
    fn new_release_binary(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_BIN_EXE_baton_upgrade_fixture"))
    }

    /// As on Windows, plus: any other invocation touches [`Self::marker`]
    /// and execs the real binary, so a service spawning it still works.
    #[cfg(unix)]
    fn new_release_binary(&self) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = self.root.join("new-release").join(EXE);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then\n  if [ -f \"$0.version\" ]; then cat \"$0.version\"; else echo 'baton {NEW}'; fi\n  exit 0\nfi\ntouch '{}'\nexec '{}' \"$@\"\n",
            self.marker().display(),
            env!("CARGO_BIN_EXE_baton")
        );
        fs::write(&path, script).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    fn marker(&self) -> PathBuf {
        self.root.join("new-release-ran")
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(&self.exe)
            .args(args)
            .env("BATON_HOME", &self.home)
            .env(RELEASE_BASE_ENV, file_url(&self.releases))
            .stdin(Stdio::null())
            .output()
            .expect("run installed baton")
    }

    fn adopt(&self) {
        succeeded(&self.run(&["upgrade", "--adopt", "--yes"]));
    }

    /// The install directory and baton home, byte for byte.
    fn tree(&self) -> Tree {
        let mut tree = snapshot(self.exe.parent().unwrap());
        tree.extend(snapshot(&self.home));
        tree
    }

    fn install_tree(&self) -> Tree {
        snapshot(self.exe.parent().unwrap())
    }

    fn state_file(&self, prefix: &str) -> Option<PathBuf> {
        fs::read_dir(self.home.join("upgrade"))
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with(prefix) && name.ends_with(".json")
            })
    }

    fn receipt(&self) -> serde_json::Value {
        let path = self.state_file("receipt-").expect("receipt written");
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    fn version(&self) -> String {
        let out = Command::new(&self.exe)
            .arg("--version")
            .output()
            .expect("run --version");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Waits for the Windows helper's outcome file.
    #[cfg(windows)]
    fn helper_outcome(&self) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(path) = self.state_file("helper-")
                && let Ok(data) = fs::read(&path)
                && let Ok(json) = serde_json::from_slice(&data)
            {
                return json;
            }
            assert!(
                Instant::now() < deadline,
                "upgrade helper recorded no outcome"
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn archive_into(stage: &Path, archive: &Path) {
    #[cfg(windows)]
    let status = Command::new(
        Path::new(&std::env::var_os("SystemRoot").expect("SystemRoot"))
            .join("System32")
            .join("tar.exe"),
    )
    .args(["-a", "-c", "-f"])
    .arg(archive)
    .arg("-C")
    .arg(stage)
    .arg(EXE)
    .status();
    #[cfg(not(windows))]
    let status = Command::new("tar")
        .arg("-C")
        .arg(stage)
        .arg("-czf")
        .arg(archive)
        .arg(EXE)
        .status();
    assert!(status.expect("run tar").success(), "archive {archive:?}");
}

fn file_url(path: &Path) -> String {
    let text = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace(' ', "%20");
    if text.starts_with('/') {
        format!("file://{text}")
    } else {
        format!("file:///{text}")
    }
}

fn snapshot(dir: &Path) -> Tree {
    let mut tree = Tree::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for path in entries.filter_map(Result::ok).map(|entry| entry.path()) {
            if path.is_dir() {
                pending.push(path);
            } else {
                let data = fs::read(&path).unwrap_or_default();
                tree.insert(path, data);
            }
        }
    }
    tree
}

fn succeeded(out: &Output) -> String {
    assert!(
        out.status.success(),
        "expected success; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn failed(out: &Output) -> String {
    assert!(
        !out.status.success(),
        "expected failure; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn check(fixture: &Fixture) -> serde_json::Value {
    let out = fixture.run(&["upgrade", "--check", "--version", NEW]);
    serde_json::from_str(&succeeded(&out)).expect("--check prints JSON")
}

#[test]
fn check_is_read_only_and_reports_an_unreceipted_install() {
    let fixture = Fixture::new("check");
    fixture.publish(NEW, &fixture.new_release_binary());
    let before = fixture.tree();

    let report = check(&fixture);

    assert_eq!(fixture.tree(), before, "--check must not write anything");
    assert_eq!(report["installed_version"], CURRENT);
    assert_eq!(report["selected_version"], NEW);
    assert_eq!(report["target"], host_target().unwrap());
    assert_eq!(report["provenance"], "unreceipted");
    assert!(report["source"].as_str().unwrap().ends_with(&format!(
        "/download/v{NEW}/{}",
        asset_name(NEW, host_target().unwrap())
    )));
    assert_eq!(report["service"]["live"], false);
    let blockers = report["blockers"].to_string();
    assert!(blockers.contains("--adopt"), "{blockers}");
}

#[test]
fn check_reports_stale_artifacts_without_removing_them() {
    let fixture = Fixture::new("check-stale");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    let receipt = fixture.state_file("receipt-").unwrap();
    let key = receipt.file_name().unwrap().to_string_lossy()["receipt-".len()..]
        .trim_end_matches(".json")
        .to_string();
    let exe = fixture.exe.to_string_lossy().into_owned();
    fs::write(format!("{exe}.baton-upgrade-new"), "staged").unwrap();
    fs::write(format!("{exe}.baton-upgrade-old"), "old").unwrap();
    fs::write(
        fixture
            .home
            .join("upgrade")
            .join(format!("helper-{key}.json")),
        r#"{"status":"failed","error":"seeded"}"#,
    )
    .unwrap();
    let before = fixture.tree();

    let report = check(&fixture);

    assert_eq!(fixture.tree(), before, "--check must not clean anything");
    assert_eq!(report["provenance"], "receipted");
    assert_eq!(report["helper_outcome"]["error"], "seeded");
    let stale = report["stale_artifacts"].to_string();
    assert!(stale.contains(".baton-upgrade-new"), "{stale}");
    assert!(stale.contains(".baton-upgrade-old"), "{stale}");
}

#[test]
fn adopt_writes_a_receipt_only_for_the_official_binary() {
    let fixture = Fixture::new("adopt");
    let out = fixture.run(&["upgrade", "--adopt"]);
    assert!(failed(&out).contains("--yes"), "adopt needs confirmation");
    assert!(fixture.state_file("receipt-").is_none());

    fixture.adopt();
    let receipt = fixture.receipt();
    assert_eq!(receipt["version"], CURRENT);
    assert_eq!(
        receipt["exe_sha256"],
        format!("{:x}", Sha256::digest(fs::read(&fixture.exe).unwrap()))
    );
    fixture.publish(NEW, &fixture.new_release_binary());
    assert_eq!(check(&fixture)["provenance"], "receipted");

    let mismatched = Fixture::new("adopt-mismatch");
    mismatched.publish(CURRENT, &mismatched.new_release_binary());
    let err = failed(&mismatched.run(&["upgrade", "--adopt", "--yes"]));
    assert!(err.contains("not the official"), "{err}");
    assert!(mismatched.state_file("receipt-").is_none());
}

#[test]
fn adopt_refuses_package_managed_paths() {
    let npm = Fixture::installed_at("adopt-npm", "lib/node_modules/@shukelabs/baton/bin");
    let err = failed(&npm.run(&["upgrade", "--adopt", "--yes"]));
    assert!(err.contains("npm update -g @shukelabs/baton"), "{err}");

    let cargo = Fixture::installed_at("adopt-cargo", ".cargo/bin");
    let err = failed(&cargo.run(&["upgrade", "--adopt", "--yes"]));
    assert!(err.contains("cargo install --git"), "{err}");
    assert!(npm.state_file("receipt-").is_none() && cargo.state_file("receipt-").is_none());
}

#[test]
fn upgrade_refuses_an_unreceipted_install() {
    let fixture = Fixture::new("unreceipted");
    fixture.publish(NEW, &fixture.new_release_binary());
    let before = fixture.install_tree();

    let err = failed(&fixture.run(&["upgrade", "--version", NEW, "--yes"]));

    assert!(err.contains("--adopt"), "{err}");
    assert!(err.contains("cargo install --git"), "{err}");
    assert_eq!(fixture.install_tree(), before);
    assert!(fixture.state_file("receipt-").is_none());
}

#[test]
fn upgrade_without_yes_off_a_terminal_is_refused() {
    let fixture = Fixture::new("no-yes");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    let before = fixture.tree();

    let err = failed(&fixture.run(&["upgrade", "--version", NEW]));

    assert!(err.contains("--yes"), "{err}");
    assert_eq!(fixture.tree(), before);
}

#[test]
fn checksum_mismatch_changes_nothing() {
    let fixture = Fixture::new("checksum");
    fixture.publish(NEW, &fixture.new_release_binary());
    let sums = fixture
        .releases
        .join("download")
        .join(format!("v{NEW}"))
        .join("SHA256SUMS");
    let text = fs::read_to_string(&sums).unwrap();
    fs::write(&sums, format!("{}{}", "0".repeat(64), &text[64..])).unwrap();
    fixture.adopt();
    let before = fixture.tree();

    let err = failed(&fixture.run(&["upgrade", "--version", NEW, "--yes"]));

    assert!(err.contains("checksum mismatch"), "{err}");
    assert_eq!(fixture.tree(), before);
}

#[test]
fn staged_version_mismatch_changes_nothing() {
    let fixture = Fixture::new("staged-version");
    // The v99 archive carries the current binary, which reports CURRENT.
    fixture.publish(NEW, Path::new(env!("CARGO_BIN_EXE_baton")));
    fixture.adopt();
    let before = fixture.tree();

    let err = failed(&fixture.run(&["upgrade", "--version", NEW, "--yes"]));

    assert!(err.contains(&format!("reports baton {CURRENT}")), "{err}");
    assert_eq!(fixture.tree(), before);
}

#[test]
fn post_install_version_mismatch_rolls_back_binary_and_receipt() {
    let fixture = Fixture::new("rollback");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    // Only the copy at the install path misreports its version.
    let sidecar = format!("{}.version", fixture.exe.to_string_lossy());
    fs::write(&sidecar, "baton 98.0.0\n").unwrap();
    let before = fixture.install_tree();
    let receipt = fixture.receipt();

    let out = fixture.run(&["upgrade", "--version", NEW, "--yes"]);

    #[cfg(windows)]
    {
        succeeded(&out);
        let outcome = fixture.helper_outcome();
        assert_eq!(outcome["status"], "failed", "{outcome}");
        assert!(outcome["error"].as_str().unwrap().contains("98.0.0"));
        assert!(outcome["error"].as_str().unwrap().contains("rolled back"));
    }
    #[cfg(unix)]
    {
        let err = failed(&out);
        assert!(
            err.contains("98.0.0") && err.contains("rolled back"),
            "{err}"
        );
    }
    assert_eq!(
        fixture.install_tree(),
        before,
        "binary restored byte for byte"
    );
    assert_eq!(fixture.receipt(), receipt, "receipt restored");
}

#[test]
fn upgrade_installs_the_verified_release_and_updates_the_receipt() {
    let fixture = Fixture::new("success");
    let new = fixture.new_release_binary();
    fixture.publish(NEW, &new);
    fixture.adopt();

    let out = succeeded(&fixture.run(&["upgrade", "--version", NEW, "--yes"]));

    assert!(out.contains("source: file://"), "{out}");
    #[cfg(windows)]
    {
        assert!(
            out.contains(&format!("upgrade scheduled: baton {CURRENT} -> {NEW}")),
            "{out}"
        );
        let outcome = fixture.helper_outcome();
        assert_eq!(outcome["status"], "succeeded", "{outcome}");
        assert!(outcome["recovery"].as_str().unwrap().contains(CURRENT));
    }
    #[cfg(unix)]
    {
        assert!(
            out.contains(&format!("upgraded baton {CURRENT} -> {NEW}")),
            "{out}"
        );
        assert!(out.contains("service: no live baton service"), "{out}");
        assert!(out.contains(&format!("--version {CURRENT}")), "{out}");
    }
    assert_eq!(fixture.version(), format!("baton {NEW}"));
    assert_eq!(fs::read(&fixture.exe).unwrap(), fs::read(&new).unwrap());
    let receipt = fixture.receipt();
    assert_eq!(receipt["version"], NEW);
    assert_eq!(
        receipt["exe_sha256"],
        format!("{:x}", Sha256::digest(fs::read(&new).unwrap()))
    );
    let exe = fixture.exe.to_string_lossy().into_owned();
    assert!(!Path::new(&format!("{exe}.baton-upgrade-new")).exists());
    assert!(!Path::new(&format!("{exe}.baton-upgrade-old")).exists());
}

fn service_status(control: &str) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "status", "--control", control])
        .output()
        .expect("run service status");
    serde_json::from_str(&succeeded(&out)).expect("service status is JSON")
}

fn wait_for_service(control: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while service_status(control)["service_running"] != true {
        assert!(Instant::now() < deadline, "service run did not come up");
        thread::sleep(Duration::from_millis(50));
    }
}

fn start_session(control: &str, root: &Path, name: &str) -> String {
    let inbox = root.join("mailboxes").join(name).join("inbox");
    let outbox = root.join("mailboxes").join(name).join("outbox");
    #[cfg(windows)]
    let agent = ["cmd.exe", "/D", "/C", "exit 0"];
    #[cfg(unix)]
    let agent = ["sh", "-c", "cat >/dev/null", "agent"];
    let mut command = Command::new(env!("CARGO_BIN_EXE_baton"));
    command.args(["service", "start", "--control", control]);
    command
        .arg("--inbox")
        .arg(&inbox)
        .arg("--outbox")
        .arg(&outbox);
    command.args(["--poll-ms", "50", "--agent-cmd", agent[0]]);
    for arg in &agent[1..] {
        command.args(["--agent-arg", arg]);
    }
    let session = succeeded(&command.output().expect("run service start"))
        .trim()
        .to_string();
    assert!(!session.is_empty(), "service start prints a session id");
    session
}

#[cfg(unix)]
fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(Instant::now() < deadline, "{path:?} never appeared");
        thread::sleep(Duration::from_millis(50));
    }
}

fn session_live(control: &str, session: &str) -> bool {
    service_status(control)["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["id"] == session && entry["live"] == true)
}

/// Tears the control plane down and reaps `run` however a test exits.
struct ServiceGuard {
    control: String,
    run: Option<std::process::Child>,
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "teardown", "--control", &self.control, "--force"])
            .output();
        if let Some(mut run) = self.run.take() {
            let _ = run.kill();
            let _ = run.wait();
        }
    }
}

fn spawn_service(program: &Path, fixture: &Fixture) -> ServiceGuard {
    let control = fixture.control.to_string_lossy().into_owned();
    let run = Command::new(program)
        .args(["service", "run", "--control", &control])
        .env("BATON_HOME", &fixture.home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn service run");
    wait_for_service(&control);
    ServiceGuard {
        control,
        run: Some(run),
    }
}

#[cfg(unix)]
#[test]
fn unix_upgrade_keeps_a_live_service_and_its_sessions() {
    let fixture = Fixture::new("unix-live");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    let mut guard = spawn_service(&fixture.exe, &fixture);
    let control = guard.control.clone();
    let session = start_session(&control, &fixture.root, "first");
    // `service start` returns before serve opens its mailbox; let the
    // layout settle so the comparison sees only upgrade-time changes.
    let mailboxes = fixture.root.join("mailboxes");
    wait_for_file(&mailboxes.join("first").join("inbox").join("serve.lock"));
    thread::sleep(Duration::from_millis(300));
    let record = fixture
        .control
        .join("sessions")
        .join(format!("{session}.json"));
    let record_before = fs::read(&record).expect("session record");
    let mailboxes_before = snapshot(&mailboxes);

    let out =
        succeeded(&fixture.run(&["upgrade", "--version", NEW, "--yes", "--control", &control]));

    assert!(out.contains("service: live on"), "{out}");
    assert!(out.contains("1 existing session(s) unchanged"), "{out}");
    assert_eq!(fixture.version(), format!("baton {NEW}"));
    assert!(service_status(&control)["service_running"] == true);
    assert!(
        session_live(&control, &session),
        "existing session survives"
    );
    assert_eq!(
        fs::read(&record).unwrap(),
        record_before,
        "record unchanged"
    );
    assert_eq!(
        snapshot(&mailboxes),
        mailboxes_before,
        "mailboxes unchanged"
    );
    assert!(guard.run.as_mut().unwrap().try_wait().unwrap().is_none());

    // The resident service launches the replaced file for a new session.
    let second = start_session(&control, &fixture.root, "second");
    wait_for_file(&fixture.marker());
    assert!(session_live(&control, &second));
}

#[cfg(windows)]
#[test]
fn windows_live_service_without_sessions_defers_with_operator_steps() {
    let fixture = Fixture::new("win-live");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    let mut guard = spawn_service(Path::new(env!("CARGO_BIN_EXE_baton")), &fixture);
    let control = guard.control.clone();
    let before = fixture.tree();

    let out =
        succeeded(&fixture.run(&["upgrade", "--version", NEW, "--yes", "--control", &control]));

    assert!(out.contains("deferred; nothing was changed"), "{out}");
    for step in [
        "1. stop the service",
        "2. rerun `baton upgrade`",
        "3. start the service again",
    ] {
        assert!(out.contains(step), "missing {step:?}: {out}");
    }
    assert_eq!(fixture.tree(), before);
    assert!(guard.run.as_mut().unwrap().try_wait().unwrap().is_none());
    assert_eq!(service_status(&control)["service_running"], true);
}

/// A session spawned by the installed binary holds its image, so the
/// deferral also covers the locked-executable case.
#[cfg(windows)]
#[test]
fn windows_live_session_without_service_defers() {
    let fixture = Fixture::new("win-session");
    fixture.publish(NEW, &fixture.new_release_binary());
    fixture.adopt();
    let mut guard = spawn_service(&fixture.exe, &fixture);
    let control = guard.control.clone();
    let session = start_session(&control, &fixture.root, "held");
    let mut run = guard.run.take().unwrap();
    run.kill().expect("stop service run");
    run.wait().unwrap();
    assert_eq!(service_status(&control)["service_running"], false);
    assert!(
        session_live(&control, &session),
        "session outlives service run"
    );
    let before = fixture.tree();

    let out =
        succeeded(&fixture.run(&["upgrade", "--version", NEW, "--yes", "--control", &control]));

    assert!(out.contains("deferred; nothing was changed"), "{out}");
    assert!(out.contains(&session), "{out}");
    assert_eq!(fixture.tree(), before);
    assert!(session_live(&control, &session));
}
