//! A small product used by the end-to-end tests: jobs that succeed, fail, panic, crash, hang,
//! wait and eat memory, so every path through the engine can be exercised with real processes.

use std::time::Duration;

use downbeat::Settings;
use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Text {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Number {
    pub n: i64,
}

/// Fails `fail_times` times, counting calls in a file, then succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Flaky {
    pub counter: String,
    pub fail_times: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Seconds {
    pub seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct ExitPlan {
    pub code: i32,
    pub output: String,
}

#[job]
pub async fn echo(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

#[job]
pub async fn shout(_run: Run, input: Text) -> Result<Text> {
    Ok(Text { text: input.text.to_uppercase() })
}

#[job]
pub async fn both(_run: Run, (quiet, loud): (Text, Text)) -> Result<Text> {
    Ok(Text { text: format!("{} {}", quiet.text, loud.text) })
}

#[job]
pub async fn after_change(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

#[job]
pub async fn needs_echo(_run: Run, input: Text) -> Result<()> {
    let _ = input;
    Ok(())
}

#[job]
pub async fn guarded(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

#[job]
pub async fn empty(_run: Run, input: Text) -> Result<Text> {
    if input.text.is_empty() {
        return skip("there is nothing to do");
    }
    Ok(input)
}

#[job(attempts = 3, backoff = 1.seconds())]
pub async fn flaky(_run: Run, input: Flaky) -> Result<u32> {
    let calls: u32 =
        std::fs::read_to_string(&input.counter).ok().and_then(|text| text.trim().parse().ok()).unwrap_or(0) + 1;
    std::fs::write(&input.counter, calls.to_string())?;
    if calls <= input.fail_times {
        return Err(JobError::msg(format!("call {calls} fails on purpose")));
    }
    Ok(calls)
}

#[job]
pub async fn panics(_run: Run) -> Result<()> {
    panic!("this job panics on purpose");
}

#[job]
pub async fn on_failure(_run: Run, failure: Failure) -> Result<Text> {
    Ok(Text { text: format!("{} failed: {}", failure.job, failure.kind) })
}

#[job]
pub async fn aborts(_run: Run) -> Result<()> {
    std::process::abort();
}

#[job(timeout = 2.seconds())]
pub async fn sleeps(_run: Run, input: Seconds) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(input.seconds)).await;
    Ok(())
}

/// Saves a checkpoint every second, and continues from the last one.
#[job(timeout = 1.minutes())]
pub async fn counts(run: Run, input: Seconds) -> Result<u64> {
    let mut done: u64 = run.last_checkpoint().unwrap_or(0);
    while done < input.seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;
        done += 1;
        run.checkpoint_now(&done).await?;
        run.progress(format!("{done} of {}", input.seconds));
    }
    Ok(done)
}

#[job(recheck_every = 1.seconds())]
pub async fn waits_for_file(_run: Run, input: Text) -> Result<Text> {
    if !std::path::Path::new(&input.text).exists() {
        return not_ready(format!("{} is not there yet", input.text));
    }
    Ok(input)
}

#[job(max_memory = 64.mb(), timeout = 30.seconds())]
pub async fn hungry(_run: Run) -> Result<()> {
    let mut hoard: Vec<Vec<u8>> = Vec::new();
    for _ in 0..64 {
        hoard.push(vec![1_u8; 16 << 20]);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

#[job(one_at_a_time)]
pub async fn single(_run: Run, input: Seconds) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(input.seconds)).await;
    Ok(())
}

#[job]
pub async fn starter(run: Run, input: Text) -> Result<Text> {
    let started = run.start(echo, Text { text: format!("started by {}", input.text) }).await?;
    Ok(Text { text: started.run_id().to_string() })
}

#[job(preset = frequent)]
pub async fn ticker(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn report(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn dated(_run: Run, day: NaiveDate) -> Result<NaiveDate> {
    Ok(day)
}

#[command]
pub fn exits(plan: ExitPlan) -> Program<Text> {
    Program::new("/bin/sh")
        .args(["-c", "echo \"$PLAN_OUTPUT\" > \"$JOB_OUTPUT_FILE\"; echo 'a line on stderr' >&2; exit \"$PLAN_CODE\""])
        .env("PLAN_OUTPUT", plan.output)
        .env("PLAN_CODE", plan.code)
}

fn basics(pipeline: &mut Pipeline) {
    pipeline.job(echo);
    pipeline.job(shout).after(echo);
    pipeline.job(both).after((echo, shout));
    pipeline.job(after_change).after(echo).if_changed();
    pipeline.job(needs_echo).needs(echo.done_for(|input: &Text| input.clone()));
    pipeline.job(guarded).only_if("positive", |input: &Number| input.n > 0);
    pipeline.job(empty);
    pipeline.job(starter);
}

fn failures(pipeline: &mut Pipeline) {
    pipeline.job(flaky);
    pipeline.job(panics);
    pipeline.job(on_failure).after_failure(panics);
    pipeline.job(aborts);
    pipeline.job(sleeps);
    pipeline.job(counts);
    pipeline.job(waits_for_file);
    pipeline.job(hungry);
    pipeline.job(single);
    pipeline.job(exits);
}

fn clock(pipeline: &mut Pipeline) {
    pipeline.job(ticker).schedule(every(1.seconds()).in_environments(["schedules"]));
    pipeline.job(report).schedule(daily_at(["09:00", "17:00"]));
    pipeline.job(dated).schedule(
        daily_at(["06:00"])
            .on_weekdays([Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri])
            .input(Occurrence::date),
    );
}

/// The test product's definition, with short timings so tests run quickly.
pub fn definition() -> Definition {
    let mut definition = Definition::new().zone("UTC").environments(["test", "schedules"]).settings(Settings {
        heartbeat: Duration::from_secs(1),
        lease: Duration::from_secs(3),
        kill_grace: Duration::from_secs(1),
        drain: Duration::from_secs(1),
        stop_within: Duration::from_secs(10),
        poll: Duration::from_millis(200),
        recycle_child_after: 500,
    });
    definition.pipeline("basics", basics);
    definition.pipeline("failures", failures);
    definition.pipeline("clock", clock);
    definition
}
