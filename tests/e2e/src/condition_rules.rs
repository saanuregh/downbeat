//! Jobs for tests of conditions, needs and commands.

use downbeat::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{FINISHED_DIR, Number, Text};

/// A value with a key that decides its turn, and a flag that is left out of its comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Keyed {
    pub key: String,
    pub value: String,
    #[job_data(not_compared)]
    pub fail: bool,
}

/// Whether a run passes its `only_if`; left out of the comparison, so `if_changed` ignores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct OrderCase {
    pub tag: String,
    #[job_data(not_compared)]
    pub passes: bool,
}

/// What `writes_its_output` writes to its output file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct OutputPlan {
    pub kind: String,
}

/// The job other jobs need `done_for` an input.
#[job]
pub async fn needed_for(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Fails while a `fail-done-recently` file is in the app's finished directory; needed by
/// `done_within`.
#[job]
pub async fn done_recently(_run: Run) -> Result<()> {
    if let Some(dir) = std::env::var_os(FINISHED_DIR)
        && std::path::Path::new(&dir).join("fail-done-recently").exists()
    {
        return Err(JobError::msg("told to fail"));
    }
    Ok(())
}

/// Needs `needed_for` the same input, and waits for it.
#[job(recheck_every = 1.seconds())]
pub async fn waits_for_need(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Needs `needed_for` the same input, and fails without it.
#[job]
pub async fn fails_without_need(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

/// Needs a success of `done_recently` in the last four seconds, and fails without it.
#[job]
pub async fn needs_recent_success(_run: Run, input: Number) -> Result<Number> {
    Ok(input)
}

/// Its `only_if` and its need's closure each append a line to `<input>.only_if` and
/// `<input>.done_for`; the condition is true, and the need (with `.or_fail()`) is never met.
#[job]
pub async fn counts_its_tests(_run: Run, input: Text) -> Result<Text> {
    Ok(input)
}

fn count_in(input: &Text, what: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(format!("{}.{what}", input.text)) {
        let _ = writeln!(file, "called");
    }
}

fn counted_only_if(input: &Text) -> bool {
    count_in(input, "only_if");
    true
}

fn counted_done_for(input: &Text) -> Text {
    count_in(input, "done_for");
    Text { text: format!("{} never done", input.text) }
}

/// Hands its input to `checked_in_order`.
#[job]
pub async fn order_parent(_run: Run, input: OrderCase) -> Result<OrderCase> {
    Ok(input)
}

/// `only_if` passes, a need of `done_recently` within ten seconds (`.or_fail()`), and
/// `if_changed`, after `order_parent`.
#[job]
pub async fn checked_in_order(_run: Run, input: OrderCase) -> Result<OrderCase> {
    Ok(input)
}

/// Hands its input to `changed_per_key`.
#[job]
pub async fn keyed_parent(_run: Run, input: Keyed) -> Result<Keyed> {
    Ok(input)
}

/// One run per key at a time, `if_changed`, after `keyed_parent`; fails when told to.
#[job(one_per = key)]
pub async fn changed_per_key(_run: Run, input: Keyed) -> Result<Keyed> {
    if input.fail {
        return Err(JobError::msg("told to fail"));
    }
    Ok(input)
}

/// A new child process for each run; returns that child's id.
#[job(fresh_process)]
pub async fn fresh_child_pid(_run: Run, input: Number) -> Result<u32> {
    let _ = input;
    Ok(std::process::id())
}

/// Writes what its program received into the directory its input names: `env` (every exported
/// variable), `stdin`, and one file per variable it checks.
#[command]
pub fn shows_what_it_gets(input: Text) -> Program<()> {
    Program::new("/bin/sh")
        .args([
            "-c",
            r#"export -p > "$DIR/env"
IFS= read -r line || true
printf '%s' "$line" > "$DIR/stdin"
printf '%s' "$E2E_PASSED" > "$DIR/E2E_PASSED"
printf '%s' "$JOB_RUN_ID" > "$DIR/JOB_RUN_ID"
printf '%s' "$JOB_ATTEMPT" > "$DIR/JOB_ATTEMPT"
printf '%s' "$JOB_DRY_RUN" > "$DIR/JOB_DRY_RUN"
printf '%s' "$JOB_OUTPUT_FILE" > "$DIR/JOB_OUTPUT_FILE""#,
        ])
        .pass_env(["E2E_PASSED", "E2E_NOT_SET"])
        .env("DIR", input.text)
}

/// Takes its input as one argument.
#[command]
pub fn takes_a_value(input: Text) -> Program<()> {
    Program::new("/bin/sh").args(["-c", "exit 0", "sh"]).arg(input.text)
}

/// Takes its input as one argument, which may be an option.
#[command]
pub fn takes_an_option(input: Text) -> Program<()> {
    Program::new("/bin/sh").args(["-c", "exit 0", "sh"]).arg(input.text).values_may_be_options()
}

/// Writes to its output file as its plan says: `empty`, `skip`, `huge` (a JSON string over
/// 64 KiB), `not_json` or `not_utf8`.
#[command]
pub fn writes_its_output(plan: OutputPlan) -> Program<serde_json::Value> {
    Program::new("/bin/sh")
        .args([
            "-c",
            r#"case "$KIND" in
  empty) : > "$JOB_OUTPUT_FILE" ;;
  skip) printf '{"skip": "nothing new"}' > "$JOB_OUTPUT_FILE" ;;
  huge) { printf '"'; i=0; while [ "$i" -lt 70 ]; do printf '%1024s' ''; i=$((i+1)); done; printf '"'; } > "$JOB_OUTPUT_FILE" ;;
  not_json) printf 'not json' > "$JOB_OUTPUT_FILE" ;;
  not_utf8) printf '"\377\376"' > "$JOB_OUTPUT_FILE" ;;
esac"#,
        ])
        .env("KIND", plan.kind)
}

/// Fails with a secret on standard error.
#[command]
pub fn fails_with_a_secret() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "echo 'the password is hunter2' >&2; exit 4"])
}

/// Fails with standard error that makes the test product's `redact` hook panic.
#[command]
pub fn breaks_redact() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "echo 'this breaks redact' >&2; exit 4"])
}

pub(crate) fn condition_rules_jobs(pipeline: &mut Pipeline) {
    pipeline.job(needed_for);
    pipeline.job(done_recently);
    pipeline.job(waits_for_need).needs(needed_for.done_for(|input: &Text| input.clone()).or_wait());
    pipeline.job(fails_without_need).needs(needed_for.done_for(|input: &Text| input.clone()).or_fail());
    pipeline.job(needs_recent_success).needs(done_recently.done_within(4.seconds()).or_fail());
    pipeline
        .job(counts_its_tests)
        .only_if("counted", counted_only_if)
        .needs(needed_for.done_for(counted_done_for).or_fail());
    pipeline.job(order_parent);
    pipeline
        .job(checked_in_order)
        .after(order_parent)
        .only_if("passes", |input: &OrderCase| input.passes)
        .needs(done_recently.done_within(10.seconds()).or_fail())
        .if_changed();
    pipeline.job(keyed_parent);
    pipeline.job(changed_per_key).after(keyed_parent).if_changed();
    pipeline.job(fresh_child_pid);
    pipeline.job(shows_what_it_gets);
    pipeline.job(takes_a_value);
    pipeline.job(takes_an_option);
    pipeline.job(writes_its_output);
    pipeline.job(fails_with_a_secret);
    pipeline.job(breaks_redact);
}
