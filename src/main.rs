use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Routes `baton mat …` to the optional mat adapter; everything else is core.
#[cfg(feature = "mat")]
fn run() -> baton::error::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("mat") {
        return baton::mat::run(&args[2..]);
    }
    baton::cli::run()
}

#[cfg(not(feature = "mat"))]
fn run() -> baton::error::Result<()> {
    baton::cli::run()
}
