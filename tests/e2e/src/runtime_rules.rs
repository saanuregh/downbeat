//! Jobs for tests of the run lifecycle: claims, retries, joins, turns and conditions.

use std::time::Duration;

use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{Number, Text};

/// Writes `written_only` but never reads it back, so a value it writes itself has a field its
/// decoder does not know.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Lopsided {
    pub kept: String,
    #[serde(skip_deserializing)]
    pub written_only: String,
}

/// Holds a `Lopsided`, so its unknown field is one level down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Wrapped {
    pub inner: Lopsided,
}

/// Which of `two_conditions`' conditions pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Flags {
    pub first: bool,
    pub second: bool,
}

#[job]
pub async fn takes_lopsided(_run: Run, input: Wrapped) -> Result<Text> {
    Ok(Text { text: input.inner.kept })
}

/// Starts `takes_lopsided` with an unknown field, and returns what the start answered.
#[job]
pub async fn starts_lopsided(run: Run) -> Result<Text> {
    let input = Wrapped { inner: Lopsided { kept: "a".into(), written_only: "b".into() } };
    let text = match run.start(takes_lopsided, input).await {
        Ok(started) => format!("started {}", started.run_id()),
        Err(error) => format!("{error:?}"),
    };
    Ok(Text { text })
}

/// Tries to save a checkpoint over 64 KiB, both ways, and returns what each answered.
#[job]
pub async fn checkpoints_too_much(run: Run) -> Result<Text> {
    let huge = "x".repeat(70 * 1024);
    let later = run.checkpoint(&huge).map_or_else(|error| format!("{error:?}"), |()| "saved".into());
    let now = run.checkpoint_now(&huge).await.map_or_else(|error| format!("{error:?}"), |()| "stored".into());
    Ok(Text { text: format!("{later}\n{now}") })
}

/// Kills its own shell with SIGKILL; may be tried twice.
#[command(attempts = 2, backoff = 1.seconds())]
pub fn kills_itself() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "kill -9 $$"])
}

/// Exits with 3; may be tried twice.
#[command(attempts = 2, backoff = 1.seconds())]
pub fn exits_with_three() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "exit 3"])
}

/// Goes past its 64 MiB limit; another attempt follows an out-of-memory failure.
#[job(max_memory = 64.mb(), timeout = 30.seconds(), attempts = 2, backoff = 1.seconds(), retry_on = [Memory])]
pub async fn eats_memory(_run: Run) -> Result<()> {
    let mut hoard: Vec<Vec<u8>> = Vec::new();
    for _ in 0..64 {
        hoard.push(vec![1_u8; 16 << 20]);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Panics, though every retryable kind is listed and attempts remain.
#[job(attempts = 3, backoff = 1.seconds(), retry_on = [Error, Timeout, Crash, Memory])]
pub async fn panics_with_attempts(_run: Run) -> Result<()> {
    panic!("this job panics on purpose");
}

/// Returns more than 64 KiB, though every retryable kind is listed and attempts remain.
#[job(attempts = 3, backoff = 1.seconds(), retry_on = [Error, Timeout, Crash, Memory])]
pub async fn too_big_with_attempts(_run: Run) -> Result<String> {
    Ok("x".repeat(70 * 1024))
}

/// Takes a number; a test stores an input that does not decode.
#[job(attempts = 3, backoff = 1.seconds(), retry_on = [Error, Timeout, Crash, Memory])]
pub async fn bad_input_with_attempts(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

/// Always fails, and waits a minute before its second attempt.
#[job(attempts = 2, backoff = 1.minutes())]
pub async fn fails_then_waits_a_minute(_run: Run) -> Result<()> {
    Err(JobError::msg("fails on purpose"))
}

/// On a queue no worker in the tests serves.
#[job(queue = "nobody_serves")]
pub async fn on_an_unserved_queue(_run: Run) -> Result<()> {
    Ok(())
}

/// One at a time; not ready until the file its input names exists.
#[job(one_at_a_time, recheck_every = 1.seconds())]
pub async fn waits_its_turn_for_file(_run: Run, input: Text) -> Result<()> {
    if !std::path::Path::new(&input.text).exists() {
        return not_ready(format!("{} is not there yet", input.text));
    }
    Ok(())
}

/// Two `only_if` conditions, in this order: `first`, then `second`.
#[job]
pub async fn two_conditions(_run: Run, input: Flags) -> Result<Flags> {
    Ok(input)
}

/// Needed first by `two_needs`.
#[job]
pub async fn needed_first(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Needed second by `two_needs`.
#[job]
pub async fn needed_second(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Needs `needed_first`, then `needed_second`, each done for the same input, or fails.
#[job]
pub async fn two_needs(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// `if_changed`, with no link.
#[job]
pub async fn only_when_changed(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Starts `only_when_changed` with the text `same`.
#[job]
pub async fn starts_only_when_changed(run: Run, input: Text) -> Result<Text> {
    let _ = input;
    let started = run.start(only_when_changed, Text { text: "same".into() }).await?;
    Ok(Text { text: started.run_id().to_string() })
}

/// Skipped unless its number is positive; `after_may_skip` follows it with `.after`.
#[job]
pub async fn may_skip(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

#[job]
pub async fn after_may_skip(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

/// Hands its text to `takes_turns_slowly`.
#[job]
pub async fn relays(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// One at a time, after `relays`; its condition appends a line to `<text>.conditions` each time
/// the run is prepared, and it takes three seconds.
#[job(one_at_a_time)]
pub async fn takes_turns_slowly(_run: Run, input: Text) -> Result<()> {
    let _ = input;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

/// Always fails; its backoff doubles from one second.
#[job(attempts = 3, backoff = 1.seconds()..=8.seconds())]
pub async fn doubles_its_wait(_run: Run) -> Result<()> {
    Err(JobError::msg("fails on purpose"))
}

/// Reports its text as progress, then takes four seconds.
#[job]
pub async fn reports_progress(run: Run, input: Text) -> Result<()> {
    run.progress(input.text);
    tokio::time::sleep(Duration::from_secs(4)).await;
    Ok(())
}

/// `if_changed`, on a schedule every second in the `rules` environment that always gives the text
/// `same`.
#[job]
pub async fn ticks_unless_unchanged(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Sleeps this many milliseconds on the `quick` queue, whose heartbeat is half a second.
#[job(queue = "quick")]
pub async fn naps_on_the_quick_queue(_run: Run, input: crate::Number) -> Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(u64::try_from(input.n).unwrap_or(0))).await;
    Ok(())
}

/// Ends its own child process with exit status 3, while its function runs.
#[job]
pub async fn exits_by_itself(_run: Run) -> Result<()> {
    std::process::exit(3);
}

/// Not ready until the file its input names exists; checked again every two seconds.
#[job(recheck_every = 2.seconds())]
pub async fn not_ready_for_two_seconds(_run: Run, input: Text) -> Result<()> {
    if !std::path::Path::new(&input.text).exists() {
        return not_ready(format!("{} is not there yet", input.text));
    }
    Ok(())
}

/// Exits 75 (not ready) until the file its input names exists; checked again every two seconds.
#[command(recheck_every = 2.seconds())]
pub fn exits_not_ready_for_two_seconds(input: Text) -> Program<()> {
    Program::new("/bin/sh").args(["-c", r#"[ -e "$WAIT_FOR" ] && exit 0; exit 75"#]).env("WAIT_FOR", input.text)
}

/// A program that does not exist; may be tried twice.
#[command(attempts = 2, backoff = 1.seconds())]
pub fn program_is_missing() -> Program<()> {
    Program::new("/nonexistent/downbeat-no-such-program")
}

/// Leaves its text as a pending checkpoint (saved by the next heartbeat), then takes four seconds.
#[job]
pub async fn checkpoints_then_naps(run: Run, input: Text) -> Result<()> {
    run.checkpoint(&input.text)?;
    tokio::time::sleep(Duration::from_secs(4)).await;
    Ok(())
}

/// Takes 300 ms and writes `<child pid> <start ns> <end ns>` to the file its input names.
#[job]
pub async fn records_its_span(_run: Run, input: Text) -> Result<()> {
    let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let start = now();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let end = now();
    std::fs::write(&input.text, format!("{} {start} {end}", std::process::id()))?;
    Ok(())
}

pub(crate) fn runtime_rules_jobs(pipeline: &mut Pipeline) {
    pipeline.job(takes_lopsided);
    pipeline.job(starts_lopsided);
    pipeline.job(checkpoints_too_much);
    pipeline.job(kills_itself);
    pipeline.job(exits_with_three);
    pipeline.job(eats_memory);
    pipeline.job(panics_with_attempts);
    pipeline.job(too_big_with_attempts);
    pipeline.job(bad_input_with_attempts);
    pipeline.job(fails_then_waits_a_minute);
    pipeline.job(on_an_unserved_queue);
    pipeline.job(waits_its_turn_for_file);
    pipeline
        .job(two_conditions)
        .only_if("first", |input: &Flags| input.first)
        .only_if("second", |input: &Flags| input.second);
    pipeline.job(needed_first);
    pipeline.job(needed_second);
    pipeline
        .job(two_needs)
        .needs(needed_first.done_for(|input: &Text| input.clone()).or_fail())
        .needs(needed_second.done_for(|input: &Text| input.clone()).or_fail());
    pipeline.job(only_when_changed).if_changed();
    pipeline.job(starts_only_when_changed);
    pipeline.job(may_skip).only_if("positive", |input: &Number| input.n > 0);
    pipeline.job(after_may_skip).after(may_skip);
    pipeline.job(relays);
    pipeline.job(takes_turns_slowly).after(relays).only_if("counted", crate::counts_conditions);
    pipeline.job(doubles_its_wait);
    pipeline.job(reports_progress);
    pipeline
        .job(ticks_unless_unchanged)
        .schedule(every(1.seconds()).in_environments(["rules"]).input(|_| Text { text: "same".into() }))
        .if_changed();
    pipeline.job(naps_on_the_quick_queue);
    pipeline.job(exits_by_itself);
    pipeline.job(not_ready_for_two_seconds);
    pipeline.job(exits_not_ready_for_two_seconds);
    pipeline.job(program_is_missing);
    pipeline.job(checkpoints_then_naps);
    pipeline.job(records_its_span);
}
