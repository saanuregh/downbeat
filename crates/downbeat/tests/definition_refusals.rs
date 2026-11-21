//! A definition with problems is refused by `run-once` and by `InAppClient::connect`, not only by
//! `check` and the roles. Every job in this binary is registered, so a definition that leaves one
//! out of every pipeline has a problem.

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};

use downbeat::prelude::*;

/// One flag per test, set when `marks` runs with that index.
static MARKED: [AtomicBool; 2] = [AtomicBool::new(false), AtomicBool::new(false)];

#[job]
async fn marks(_run: Run, index: u32) -> Result<()> {
    MARKED[index as usize].store(true, Ordering::SeqCst);
    Ok(())
}

#[job]
async fn left_out(_run: Run) -> Result<()> {
    Ok(())
}

fn sound() -> Definition {
    let mut definition = Definition::new().zone("UTC");
    definition.pipeline("all", |pipeline| {
        pipeline.job(marks);
        pipeline.job(left_out);
    });
    definition
}

/// `left_out` is named in no pipeline.
fn broken() -> Definition {
    let mut definition = Definition::new().zone("UTC");
    definition.pipeline("all", |pipeline| {
        pipeline.job(marks);
    });
    definition
}

fn run_once(definition: fn() -> Definition, index: u32) -> ExitCode {
    let input = index.to_string();
    downbeat::run_as(["app", "run-once", "marks", "--input", &input], definition)
}

#[test]
fn run_once_refuses_a_definition_with_problems_without_calling_the_job() {
    assert_eq!(run_once(broken, 0), ExitCode::FAILURE);
    assert!(!MARKED[0].load(Ordering::SeqCst), "the job ran under a definition with problems");
}

#[test]
fn run_once_calls_the_job_under_a_sound_definition() {
    assert_eq!(run_once(sound, 1), ExitCode::SUCCESS);
    assert!(MARKED[1].load(Ordering::SeqCst));
}

/// Nothing listens on port 1, so a connection that gets as far as the database fails there.
const NO_DATABASE: &str = "postgres://nobody@127.0.0.1:1/nothing";

#[tokio::test]
async fn in_app_client_refuses_a_definition_with_problems() {
    let error = downbeat::InAppClient::connect(broken, NO_DATABASE).await.unwrap_err();
    let downbeat::Error::Definition { problems } = &error else { panic!("not a definition error: {error}") };
    assert!(problems.iter().any(|problem| problem.contains("left_out")), "the problem names the job: {problems:?}");
    let message = error.to_string();
    assert!(message.contains("the definition has problems"), "{message}");
}

#[tokio::test]
async fn in_app_client_gets_past_a_sound_definition() {
    let error = downbeat::InAppClient::connect(sound, NO_DATABASE).await.unwrap_err();
    assert!(matches!(error, downbeat::Error::Database(_)), "{error}");
}
