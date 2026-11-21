//! The test product refuses a definition with problems in `run-once` and in a child process, not
//! only in `check` and the roles. The one problem reachable from outside the binary is an
//! environment the definition does not list.

use std::process::{Command, Output, Stdio};

/// Runs the test app with these arguments and variables, and waits for it to end.
fn app(args: &[&str], variables: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .args(args)
        .envs(variables.iter().copied())
        .env_remove("DOWNBEAT_CHILD_FD")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .output()
        .expect("the test app runs")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn run_once_refuses_an_environment_the_definition_does_not_list() {
    let output = app(&["run-once", "counts_letters", "--input", r#"{"n":3}"#], &[("DOWNBEAT_ENVIRONMENT", "nowhere")]);
    let stderr = text(&output.stderr);
    assert!(!output.status.success(), "run-once succeeded: {stderr}");
    assert!(stderr.contains("the definition has problems"), "{stderr}");
    assert!(stderr.contains("environment 'nowhere' is not one the definition lists"), "{stderr}");
    assert!(text(&output.stdout).is_empty(), "the job ran: {}", text(&output.stdout));
}

#[test]
fn run_once_refuses_the_environment_given_as_an_argument_too() {
    let output = app(&["run-once", "counts_letters", "--input", r#"{"n":3}"#, "--environment", "nowhere"], &[]);
    let stderr = text(&output.stderr);
    assert!(!output.status.success(), "run-once succeeded: {stderr}");
    assert!(stderr.contains("environment 'nowhere' is not one the definition lists"), "{stderr}");
}

#[test]
fn run_once_runs_the_job_in_a_listed_environment() {
    let output = app(&["run-once", "counts_letters", "--input", r#"{"n":3}"#], &[("DOWNBEAT_ENVIRONMENT", "test")]);
    assert!(output.status.success(), "{}", text(&output.stderr));
    assert!(text(&output.stdout).contains('3'), "{}", text(&output.stdout));
}

/// A child builds the definition before it touches its socket, so a descriptor that names no
/// socket is never used here.
#[test]
fn a_child_process_refuses_an_environment_the_definition_does_not_list() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_e2e-app"));
    command
        .env("DOWNBEAT_CHILD_FD", "1000")
        .env("DOWNBEAT_ENVIRONMENT", "nowhere")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null());
    let output = command.output().expect("the test app runs");
    let stderr = text(&output.stderr);
    assert!(!output.status.success(), "the child started: {stderr}");
    assert!(stderr.contains("downbeat child: the definition has problems"), "{stderr}");
    assert!(stderr.contains("environment 'nowhere' is not one the definition lists"), "{stderr}");
}
