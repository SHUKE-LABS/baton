//! Test-only stand-in for a newer `baton` release, used by `tests/upgrade.rs`
//! on Windows (Unix tests use a shell wrapper). `--version` prints
//! `baton 99.0.0`, or the contents of a `<exe>.version` sidecar beside the
//! running file, so a test can make only the installed copy misreport.
fn main() {
    let mut sidecar = std::env::current_exe()
        .expect("fixture executable path")
        .into_os_string();
    sidecar.push(".version");
    match std::fs::read_to_string(&sidecar) {
        Ok(text) => print!("{text}"),
        Err(_) => println!("baton 99.0.0"),
    }
}
