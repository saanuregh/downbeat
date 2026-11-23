//! Jobs for tests of child processes, commands and stopping.

use std::time::Duration;

use downbeat::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{FINISHED_DIR, Number, Text};

/// An output whose decoding reacts to its text: `panic` panics, and `slow` writes the decoding
/// process's id to `checking.pid` in the app's finished directory, then takes five seconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JobData)]
pub struct Touchy {
    #[serde(deserialize_with = "touchy_text")]
    pub text: String,
}

fn touchy_text<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<String, D::Error> {
    let text = String::deserialize(deserializer)?;
    match text.as_str() {
        "panic" => panic!("this output panics on purpose when it is decoded"),
        "slow" => {
            if let Some(dir) = std::env::var_os(FINISHED_DIR) {
                let _ = std::fs::write(std::path::Path::new(&dir).join("checking.pid"), std::process::id().to_string());
            }
            std::thread::sleep(Duration::from_secs(5));
        }
        _ => {}
    }
    Ok(text)
}

/// Writes its child's process id to the file its input names, then takes five seconds to name
/// its program, past its one-second timeout.
#[command(timeout = 1.seconds())]
pub fn names_its_program_slowly(input: Text) -> Program<()> {
    let _ = std::fs::write(&input.text, std::process::id().to_string());
    std::thread::sleep(Duration::from_secs(5));
    Program::new("/bin/true")
}

/// Writes `{"text": <input>}` as its output, or exits 1 for `fail`. With `slow`, checking the
/// output outlasts the two-second timeout.
#[command(timeout = 2.seconds())]
pub fn touchy_output(input: Text) -> Program<Touchy> {
    Program::new("/bin/sh")
        .args(["-c", r#"[ "$TEXT" = fail ] && exit 1; printf '{"text": "%s"}' "$TEXT" > "$JOB_OUTPUT_FILE""#])
        .env("TEXT", input.text)
}

/// Its condition panics for the text `panic`.
#[job]
pub async fn panics_when_tested(_run: Run, input: Text) -> JobResult<Text> {
    Ok(input)
}

fn calm(input: &Text) -> bool {
    assert!(input.text != "panic", "this condition panics on purpose");
    true
}

/// A `fresh_process` command: it writes the id of the child that names its program to the file
/// its input names.
#[command(fresh_process)]
pub fn fresh_command(input: Text) -> Program<()> {
    let _ = std::fs::write(&input.text, std::process::id().to_string());
    Program::new("/bin/true")
}

/// A program that holds about 64 MiB, over its 32 MB limit, and then spins.
#[command(max_memory = 32.mb(), timeout = 30.seconds())]
pub fn hungry_program() -> Program<()> {
    Program::new("/bin/sh")
        .args(["-c", r#"exec awk 'BEGIN { s = "x"; while (length(s) < 67108864) s = s s; while (1) {} }'"#])
}

/// Returns its input; its link to `keeps_its_number` maps it to the id of the process the
/// adapter runs in.
#[job]
pub async fn starts_a_mapped_link(_run: Run, input: Number) -> JobResult<Number> {
    Ok(input)
}

#[job]
pub async fn keeps_its_number(_run: Run, input: Number) -> JobResult<Number> {
    Ok(input)
}

/// Starts a helper that ignores SIGTERM, writes `<helper pid> <own pid> <own process group>` to
/// the file its input names, then runs past its one-second timeout. Only killing the child's
/// process group ends the helper.
#[job(timeout = 1.seconds())]
pub async fn function_times_out_with_a_helper(_run: Run, input: Text) -> JobResult<()> {
    #[expect(clippy::zombie_processes, reason = "the helper must outlive this call until its group is killed")]
    let helper = std::process::Command::new("/bin/sh")
        .args(["-c", "trap '' TERM; exec sleep 300"])
        .spawn()
        .expect("the helper starts");
    let stat = std::fs::read_to_string("/proc/self/stat").expect("its own process status");
    // The fields after the command name, which is in parentheses, start with state, parent, group.
    let group = stat.rsplit_once(')').and_then(|(_, rest)| rest.split_whitespace().nth(2)).unwrap_or("?");
    std::fs::write(&input.text, format!("{} {} {group}", helper.id(), std::process::id()))
        .expect("the file is written");
    tokio::time::sleep(Duration::from_secs(300)).await;
    Ok(())
}

/// Starts a helper in the background, writes its id to the file its input names, and exits 0.
#[command]
pub fn leaves_a_helper(input: Text) -> Program<()> {
    Program::new("/bin/sh").args(["-c", r#"sleep 300 & echo $! > "$PID_FILE""#]).env("PID_FILE", input.text)
}

/// Ends itself with SIGKILL.
#[command]
pub fn killed_by_a_signal() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "kill -KILL $$"])
}

/// Writes 40 000 bytes of `a`, then a line `the end`, to standard error, and exits 1.
#[command]
pub fn long_stderr() -> Program<()> {
    Program::new("/bin/sh")
        .args(["-c", "head -c 40000 /dev/zero | tr '\\0' a >&2; echo >&2; echo 'the end' >&2; exit 1"])
}

/// Says it is not ready (exit 75) as soon as it is asked to stop.
#[command]
pub fn not_ready_when_asked_to_stop() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "trap 'exit 75' TERM; sleep 60 & wait"])
}

/// A program that sleeps for a minute as the group leader itself.
#[command]
pub fn sleeps_a_minute() -> Program<()> {
    Program::new("/bin/sh").args(["-c", "exec sleep 60"])
}

/// An input that panics at one stage of being started, by its `stage`: `decode` when it is
/// decoded, `identity` when it is hashed, `one_per` when its `one_per` field is read. Any other
/// stage is harmless.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fragile {
    #[serde(deserialize_with = "fragile_stage")]
    pub stage: String,
    /// Written as plain text with the whole input; only its own `Serialize`, which `one_per`
    /// uses alone, panics.
    #[serde(serialize_with = "plain_account")]
    pub account: Account,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Account(pub String);

impl Serialize for Account {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        assert!(self.0 != "one_per", "this one_per field panics on purpose");
        serializer.serialize_str(&self.0)
    }
}

fn plain_account<S: serde::Serializer>(account: &Account, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&account.0)
}

fn fragile_stage<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<String, D::Error> {
    let stage = String::deserialize(deserializer)?;
    assert!(stage != "decode", "this input panics on purpose when it is decoded");
    Ok(stage)
}

impl JobData for Fragile {
    fn identity(&self) -> serde_json::Value {
        assert!(self.stage != "identity", "this input panics on purpose when it is hashed");
        serde_json::to_value(self).unwrap_or_default()
    }

    fn shape() -> downbeat::Shape {
        downbeat::Shape { type_name: "Fragile".into(), fields: Vec::new() }
    }
}

/// One per account, so starting it reads its input's `one_per` field.
#[job(one_per = account)]
pub async fn takes_fragile(_run: Run, input: Fragile) -> JobResult<()> {
    let _ = input;
    Ok(())
}

/// Needs `echo` done for its own text; the need's closure panics for the text `panic`.
#[job]
pub async fn has_a_fragile_need(_run: Run, input: Text) -> JobResult<Text> {
    Ok(input)
}

fn fragile_need(input: &Text) -> Text {
    assert!(input.text != "panic", "this need panics on purpose");
    input.clone()
}

/// Writes the permission bits its output file already had when it started, as its output text.
#[command]
pub fn reports_its_output_file_mode() -> Program<Text> {
    Program::new("/bin/sh")
        .args(["-c", r#"printf '{"text": "%s"}' "$(stat -c %a "$JOB_OUTPUT_FILE")" > "$JOB_OUTPUT_FILE""#])
}

/// More than the four MiB a message from the child may hold.
const OVER_A_MESSAGE: usize = 5 * 1024 * 1024;

/// Names a program with an argument too large for one message from the child; the child refuses
/// it before sending.
#[command]
pub fn names_a_huge_program() -> Program<()> {
    Program::new("/bin/true").args(["x".repeat(OVER_A_MESSAGE)])
}

/// Reports a progress line too large for one message, then fails with an error as large for the
/// text `fail`, or succeeds.
#[job]
pub async fn says_too_much(run: Run, input: Text) -> JobResult<()> {
    run.progress("p".repeat(OVER_A_MESSAGE));
    if input.text == "fail" {
        return Err(JobError::msg("e".repeat(OVER_A_MESSAGE)));
    }
    Ok(())
}

pub(crate) fn process_rules_jobs(pipeline: &mut Pipeline) {
    pipeline.job(names_its_program_slowly);
    pipeline.job(touchy_output);
    pipeline.job(panics_when_tested).only_if("calm", calm);
    pipeline.job(fresh_command);
    pipeline.job(hungry_program);
    pipeline.job(leaves_a_helper);
    pipeline.job(killed_by_a_signal);
    pipeline.job(long_stderr);
    pipeline.job(not_ready_when_asked_to_stop);
    pipeline.job(sleeps_a_minute);
    pipeline.job(function_times_out_with_a_helper);
    pipeline.job(starts_a_mapped_link);
    pipeline
        .job(keeps_its_number)
        .after(starts_a_mapped_link.map(|_: Number| Number { n: i64::from(std::process::id()) }));
    pipeline.job(takes_fragile);
    pipeline.job(has_a_fragile_need).needs(crate::echo.done_for(fragile_need));
    pipeline.job(names_a_huge_program);
    pipeline.job(says_too_much);
    pipeline.job(reports_its_output_file_mode);
}
