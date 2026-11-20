//! Jobs for tests of particular rules: what a child takes from its link, how often a run is
//! prepared, which child a retry runs in, retention, and stopping.

use std::time::Duration;

use downbeat::prelude::*;

use crate::{Number, Text};

/// Starts a line of children with policies of their own, for tests of what a child takes.
#[job]
pub async fn starts_line(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// On its own queue (served by no worker in the tests) and priority, after `starts_line`.
#[job(queue = "quick", priority = 5)]
pub async fn on_its_own_queue(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Linked through a `.map` adapter that counts the parent's letters.
#[job]
pub async fn counts_letters(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

/// Its condition appends one line to `<input>.conditions` each time it is tested; its function
/// takes three seconds. A test can see whether the run was prepared more than once.
#[job]
pub async fn prepared_once(_run: Run, input: Text) -> Result<()> {
    let _ = input;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

pub(crate) fn counts_conditions(input: &Text) -> bool {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(format!("{}.conditions", input.text));
    if let Ok(file) = &mut file {
        let _ = writeln!(file, "tested");
    }
    true
}

/// Its condition writes the child process's id to the file the input names; the first time, it
/// then outlasts the one-second timeout, so the next attempt runs in another child.
#[job(timeout = 1.seconds(), attempts = 2, retry_on = [Timeout])]
pub async fn records_its_child(_run: Run, input: Text) -> Result<()> {
    let _ = input;
    Ok(())
}

pub(crate) fn records_child_pid(input: &Text) -> bool {
    use std::io::Write as _;
    let first = !std::path::Path::new(&input.text).exists();
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&input.text) {
        let _ = writeln!(file, "{}", std::process::id());
    }
    if first {
        std::thread::sleep(Duration::from_secs(3));
    }
    true
}

/// Fails for a negative number; a `frequent` job, which keeps at most twenty chains.
#[job(preset = frequent)]
pub async fn frequent_flop(_run: Run, input: Number) -> Result<Number> {
    if input.n < 0 {
        return Err(JobError::msg("a negative number fails on purpose"));
    }
    Ok(input)
}

/// Sleeps this many milliseconds, ignoring a request to stop.
#[job]
pub async fn naps(_run: Run, input: Number) -> Result<()> {
    tokio::time::sleep(Duration::from_millis(u64::try_from(input.n).unwrap_or(0))).await;
    Ok(())
}

/// Exits with a failure as soon as it is asked to stop.
#[command]
pub fn fails_when_asked_to_stop() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "trap 'exit 3' TERM; sleep 60 & wait"])
}

/// A dependency whose build always fails: the definition provides it with a builder that errs.
#[derive(Debug)]
pub struct Warehouse;

/// Takes the warehouse, so its call fails while the dependency cannot be built.
#[job]
pub async fn needs_the_warehouse(_run: Run, _warehouse: &Warehouse) -> Result<()> {
    Ok(())
}
