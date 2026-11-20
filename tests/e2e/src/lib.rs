//! A small product used by the end-to-end tests: jobs that succeed, fail, panic, crash, hang,
//! wait and eat memory, so every path through the engine can be exercised with real processes.

use std::time::Duration;

use downbeat::prelude::*;
use downbeat::{QueueTimings, Settings};
use serde::{Deserialize, Serialize};

mod condition_rules;
mod rules;
mod scheduling_rules;
#[allow(unused_imports)] // Until the module has jobs; remove then.
pub use condition_rules::*;
pub use rules::*;
#[allow(unused_imports)] // Until the module has jobs; remove then.
pub use scheduling_rules::*;

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

/// Work for one account that takes this long.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct AccountWork {
    pub account: String,
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

/// Says how `empty` ended, which it learns through an `.after_any` link.
#[job]
pub async fn tells_how_empty_ended(_run: Run, ended: Ended<Text>) -> Result<Text> {
    let text = match ended {
        Ended::Succeeded(output) => format!("succeeded with {}", output.text),
        Ended::Skipped(reason) => format!("skipped: {reason}"),
        Ended::Failed(failure) => format!("failed: {}", failure.reason),
        Ended::Cancelled(reason) | Ended::Expired(reason) => format!("ended: {reason}"),
    };
    Ok(Text { text })
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

/// Follows `panics` when it succeeds, which only "mark as succeeded" can make it do.
#[job]
pub async fn after_panics(_run: Run) -> Result<()> {
    Ok(())
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

/// Waits for the file its input names, then says it is not ready, so a test controls the moment
/// its run goes back to the queue.
#[job(recheck_every = 1.minutes())]
pub async fn not_ready_when_told(_run: Run, input: Text) -> Result<()> {
    while !std::path::Path::new(&input.text).exists() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    not_ready("told to wait")
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

#[job(one_per = account)]
pub async fn per_account(_run: Run, input: AccountWork) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(input.seconds)).await;
    Ok(())
}

#[job]
pub async fn starter(run: Run, input: Text) -> Result<Text> {
    let started = run.start(echo, Text { text: format!("started by {}", input.text) }).await?;
    Ok(Text { text: started.run_id().to_string() })
}

/// Returns more than the 64 KiB a value may hold.
#[job]
pub async fn too_big(_run: Run) -> Result<String> {
    Ok("x".repeat(70 * 1024))
}

/// A job that may be asked for a dry run, followed by `after_rehearses`.
#[job(supports_dry_run)]
pub async fn rehearses(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

#[job]
pub async fn after_rehearses(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

#[job(preset = frequent)]
pub async fn ticker(_run: Run) -> Result<()> {
    Ok(())
}

#[job]
pub async fn report(_run: Run) -> Result<()> {
    Ok(())
}

/// Follows `dated`, so a backfill's chain reaches a second job.
#[job]
pub async fn files_dated(_run: Run, day: NaiveDate) -> Result<NaiveDate> {
    Ok(day)
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

#[job]
pub async fn noop(_run: Run, input: Number) -> Result<()> {
    let _ = input;
    Ok(())
}

#[job(fresh_process)]
pub async fn fresh_noop(_run: Run, input: Number) -> Result<()> {
    let _ = input;
    Ok(())
}

/// Its child process's id, to see whether runs share a child.
#[job]
pub async fn child_pid(_run: Run, input: Number) -> Result<u32> {
    let _ = input;
    Ok(std::process::id())
}

/// Spawns a task that outlives the function.
#[job]
pub async fn leaves_a_task(_run: Run) -> Result<()> {
    tokio::spawn(async { tokio::time::sleep(Duration::from_secs(3600)).await });
    Ok(())
}

/// Its condition crashes the child the first time it is tested for an input (it leaves the file
/// the input names), so the run is prepared twice.
#[job(attempts = 2, backoff = 1.seconds(), retry_on = [Crash])]
pub async fn crashes_while_prepared(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

fn not_tested_before(input: &Text) -> bool {
    if !std::path::Path::new(&input.text).exists() {
        let _ = std::fs::write(&input.text, "tested");
        std::process::abort();
    }
    true
}

/// Its condition sleeps past the job's timeout the first time it is tested for an input (it leaves
/// the file the input names), so the run is prepared twice.
#[job(timeout = 1.seconds(), attempts = 2, backoff = 1.seconds(), retry_on = [Timeout])]
pub async fn times_out_while_prepared(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

fn slow_the_first_time(input: &Text) -> bool {
    if !std::path::Path::new(&input.text).exists() {
        let _ = std::fs::write(&input.text, "tested");
        std::thread::sleep(Duration::from_secs(3));
    }
    true
}

/// Its condition takes five seconds, so a test can act while the run is being prepared.
#[job]
pub async fn slow_to_prepare(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

fn after_five_seconds(_input: &Text) -> bool {
    std::thread::sleep(Duration::from_secs(5));
    true
}

/// Waits until it is asked to stop, then leaves the file its input names and returns.
#[job]
pub async fn stops_when_asked(run: Run, input: Text) -> Result<()> {
    run.cancelled().await;
    std::fs::write(&input.text, "asked")?;
    Ok(())
}

/// The root of the chains that reach `chorus`: hands its input to both singers.
#[job]
pub async fn hands_on(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// The root of the chains that reach `in_turn`.
#[job]
pub async fn lines_up(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Runs one at a time, for three seconds, after `lines_up`.
#[job(one_at_a_time)]
pub async fn in_turn(_run: Run, input: Text) -> Result<Text> {
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(input)
}

#[job]
pub async fn sings_loud(_run: Run, input: Text) -> Result<Text> {
    Ok(Text { text: input.text.to_uppercase() })
}

#[job]
pub async fn sings_soft(_run: Run, input: Text) -> Result<Text> {
    Ok(Text { text: input.text.to_lowercase() })
}

/// Joins `sings_loud` and `sings_soft`.
#[job]
pub async fn chorus(_run: Run, (loud, soft): (Text, Text)) -> Result<Text> {
    Ok(Text { text: format!("{} {}", loud.text, soft.text) })
}

/// Waits until it is asked to stop, then exits cleanly at once.
#[command]
pub fn finishes_when_asked_to_stop() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "trap 'exit 0' TERM; sleep 60 & wait"])
}

/// Starts a helper that ignores SIGTERM in the background, writes its process id to the file the
/// input names, then runs past its one-second timeout. Only killing the process group ends the
/// helper.
#[command(timeout = 1.seconds())]
pub fn times_out_with_a_helper(input: Text) -> Program<()> {
    Program::new("/bin/sh")
        .args(["-c", "(trap '' TERM; exec sleep 300) & echo $! > \"$PID_FILE\"; sleep 300"])
        .env("PID_FILE", input.text)
}

#[command]
pub fn leaves_a_process_behind() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "sleep 300 & echo started"])
}

fn basics(pipeline: &mut Pipeline) {
    pipeline.job(echo);
    pipeline.job(shout).after(echo);
    pipeline.job(both).after((echo, shout));
    pipeline.job(after_change).after(echo).if_changed();
    pipeline.job(needs_echo).needs(echo.done_for(|input: &Text| input.clone()));
    pipeline.job(guarded).only_if("positive", |input: &Number| input.n > 0);
    pipeline.job(empty);
    pipeline.job(tells_how_empty_ended).after_any(empty);
    pipeline.job(starter);
    pipeline.job(noop);
    pipeline.job(fresh_noop);
    pipeline.job(child_pid);
    pipeline.job(leaves_a_task);
    pipeline.job(rehearses);
    pipeline.job(after_rehearses).after(rehearses);
}

fn failures(pipeline: &mut Pipeline) {
    pipeline.job(flaky);
    pipeline.job(panics);
    pipeline.job(on_failure).after_failure(panics);
    pipeline.job(after_panics).after(panics);
    pipeline.job(aborts);
    pipeline.job(sleeps);
    pipeline.job(counts);
    pipeline.job(waits_for_file);
    pipeline.job(not_ready_when_told);
    pipeline.job(hungry);
    pipeline.job(single);
    pipeline.job(per_account);
    pipeline.job(exits);
    pipeline.job(leaves_a_process_behind);
    pipeline.job(times_out_with_a_helper);
    pipeline.job(crashes_while_prepared).only_if("not tested before", not_tested_before);
    pipeline.job(finishes_when_asked_to_stop);
    pipeline.job(too_big);
    pipeline.job(times_out_while_prepared).only_if("slow the first time", slow_the_first_time);
    pipeline.job(slow_to_prepare).only_if("after five seconds", after_five_seconds);
    pipeline.job(stops_when_asked);
}

fn links(pipeline: &mut Pipeline) {
    pipeline.job(starts_line);
    pipeline.job(on_its_own_queue).after(starts_line);
    pipeline
        .job(counts_letters)
        .after(starts_line.map(|line: Text| Number { n: i64::try_from(line.text.len()).unwrap_or(0) }));
    pipeline.job(prepared_once).only_if("counted", counts_conditions);
    pipeline.job(records_its_child).only_if("recorded", records_child_pid);
    pipeline.job(frequent_flop);
    pipeline.job(naps);
    pipeline.job(fails_when_asked_to_stop);
    pipeline.job(hands_on);
    pipeline.job(lines_up);
    pipeline.job(in_turn).after(lines_up);
    pipeline.job(sings_loud).after(hands_on);
    pipeline.job(sings_soft).after(hands_on);
    pipeline.job(chorus).after((sings_loud, sings_soft));
}

fn clock(pipeline: &mut Pipeline) {
    pipeline.job(ticker).schedule(every(1.seconds()).in_environments(["schedules"]));
    pipeline.job(report).schedule(daily_at(["09:00", "17:00"]));
    pipeline.job(files_dated).after(dated);
    pipeline.job(dated).schedule(
        daily_at(["06:00"])
            .on_weekdays([Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri])
            .input(Occurrence::date),
    );
}

/// The environment variable naming the directory `on_finish` writes to.
pub const FINISHED_DIR: &str = "E2E_FINISHED_DIR";

/// The test product's definition, with short timings so tests run quickly.
pub fn definition() -> Definition {
    let mut definition = definition_without_callbacks();
    // Each finished run is written to a file named by its id in this directory, when it is set.
    // A test can make delivery slow (a `slow` file: wait three seconds first) or fail (a `refuse`
    // file) to see how the scheduler handles a callback that is running or returns an error.
    definition.on_finish(|finished: downbeat::FinishedRun| async move {
        let Some(dir) = std::env::var_os(FINISHED_DIR) else { return Ok(()) };
        let dir = std::path::Path::new(&dir);
        if dir.join("slow").exists() {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        if dir.join("refuse").exists() {
            return Err(std::io::Error::other("the test refuses this delivery"));
        }
        std::fs::write(dir.join(finished.run_id.to_string()), finished.status)
    });
    // Each event is written to `event-<id>` in the same directory, holding its name.
    definition.on_event(|delivery: downbeat::EventDelivery| async move {
        match std::env::var_os(FINISHED_DIR) {
            Some(dir) => {
                let name = serde_json::to_value(&delivery.event)
                    .ok()
                    .and_then(|event| event["event"].as_str().map(str::to_owned))
                    .unwrap_or_default();
                std::fs::write(std::path::Path::new(&dir).join(format!("event-{}", delivery.id)), name)
            }
            None => Ok(()),
        }
    });
    definition
}

/// The same pipelines and settings, with no `on_finish` or `on_event`.
pub fn definition_without_callbacks() -> Definition {
    // `rules` is for tests of schedules that must not fire in any other test.
    let mut definition =
        Definition::new().zone("UTC").environments(["test", "schedules", "rules"]).settings(Settings {
            heartbeat: Duration::from_secs(1),
            lease: Duration::from_secs(3),
            kill_grace: Duration::from_secs(1),
            drain: Duration::from_secs(1),
            stop_within: Duration::from_secs(10),
            poll: Duration::from_millis(200),
            // A test of child recycling sets a small number through this variable.
            recycle_child_after: std::env::var("E2E_RECYCLE_CHILD_AFTER")
                .ok()
                .and_then(|n| n.parse().ok())
                .unwrap_or(500),
            queues: [(
                "quick".to_owned(),
                QueueTimings { heartbeat: Duration::from_millis(500), lease: Duration::from_secs(2) },
            )]
            .into(),
        });
    definition.pipeline("basics", basics);
    definition.pipeline("failures", failures);
    definition.pipeline("clock", clock);
    definition.pipeline("links", links);
    definition.pipeline("scheduling_rules", scheduling_rules_jobs);
    definition.pipeline("condition_rules", condition_rules_jobs);
    // Standard error kept from a failed command passes through this first.
    definition.redact(|text| text.replace("hunter2", "[redacted]"));
    definition
}
