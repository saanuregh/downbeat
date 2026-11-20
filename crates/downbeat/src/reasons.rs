//! The reason sentences: one plain sentence for every state, saying what is happening and why.
//! Kept in one place so every part of the engine words the same thing the same way.

use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use uuid::Uuid;

use crate::units::format_duration;

/// The first eight characters of an id, as people read them.
pub(crate) fn short(id: Uuid) -> String {
    id.to_string()[..8].to_owned()
}

fn clock(at: DateTime<Utc>, zone: Tz) -> String {
    at.with_timezone(&zone).format("%H:%M").to_string()
}

pub(crate) fn succeeded() -> String {
    "Succeeded.".to_owned()
}

pub(crate) fn job_skipped(reason: &str) -> String {
    format!("Skipped: {reason}.")
}

pub(crate) fn condition_false(name: &str) -> String {
    format!("Skipped: condition '{name}' was false.")
}

pub(crate) fn input_unchanged(since: DateTime<Utc>, zone: Tz) -> String {
    format!("Skipped: input unchanged since the run at {}.", clock(since, zone))
}

pub(crate) fn need_unmet(job: &str, what: &str) -> String {
    format!("{job} has not succeeded {what}")
}

pub(crate) fn not_ready(reason: &str, next: DateTime<Utc>, zone: Tz) -> String {
    format!("Not ready, next check {}: {reason}.", clock(next, zone))
}

pub(crate) fn waiting_turn(holder: Option<Uuid>, whole_job: bool) -> String {
    let holder = holder.map_or_else(|| "another run".to_owned(), |id| format!("run {}", short(id)));
    if whole_job {
        format!("Queued: waiting for {holder}; this job runs one at a time.")
    } else {
        format!("Queued: waiting for {holder}, which is doing the same work.")
    }
}

pub(crate) fn another_attempt(attempt: u32, attempts: u16, error: &str, next: DateTime<Utc>, zone: Tz) -> String {
    format!("Attempt {attempt} of {attempts} failed ({}); attempt {} at {}.", first_line(error), attempt + 1, clock(next, zone))
}

pub(crate) fn failed(error: &str, attempt: u32, attempts: u16) -> String {
    if attempts > 1 {
        format!("Failed on attempt {attempt} of {attempts}: {}.", first_line(error))
    } else {
        format!("Failed: {}.", first_line(error))
    }
}

pub(crate) fn panicked(message: &str) -> String {
    format!("Failed: the function panicked: {}.", first_line(message))
}

pub(crate) fn timed_out(after: Duration, attempt: u32, attempts: u16) -> String {
    format!("Failed: timed out after {} on attempt {attempt} of {attempts}.", format_duration(after))
}

pub(crate) fn crashed(code: Option<i32>, signal: Option<i32>, after: Duration) -> String {
    match (code, signal) {
        (_, Some(signal)) => format!("Failed: the process was killed by signal {signal} after {}.", format_duration(after)),
        (Some(code), None) => format!("Failed: the process exited with status {code} after {}.", format_duration(after)),
        (None, None) => format!("Failed: the process ended unexpectedly after {}.", format_duration(after)),
    }
}

pub(crate) fn out_of_memory(limit_mib: u64) -> String {
    format!("Failed: the process went over its memory limit of {limit_mib} MiB.")
}

pub(crate) fn command_failed(code: Option<i32>, signal: Option<i32>) -> String {
    match (code, signal) {
        (_, Some(signal)) => format!("Failed: the program was killed by signal {signal}."),
        (Some(code), None) => format!("Failed: the program exited with status {code}."),
        (None, None) => "Failed: the program ended unexpectedly.".to_owned(),
    }
}

pub(crate) fn output_too_large(bytes: usize) -> String {
    format!("Failed: the output is {bytes} bytes; the limit is 64 KiB.")
}

pub(crate) fn bad_input(error: &str) -> String {
    format!("Failed: the input does not fit the job: {}.", first_line(error))
}

pub(crate) fn cancelled(by: &str) -> String {
    format!("Cancelled by {by}.")
}

pub(crate) fn interrupted(running_for: Duration, has_checkpoint: bool) -> String {
    let continuing = if has_checkpoint { "; continuing from its checkpoint" } else { "" };
    format!("Re-queued: interrupted by a shutdown {} into the run{continuing}.", format_duration(running_for))
}

pub(crate) fn worker_lost(worker: &str, running_for: Duration) -> String {
    format!("Re-queued: worker {worker} stopped responding {} into the run.", format_duration(running_for))
}

pub(crate) fn worker_lost_twice(worker: &str) -> String {
    format!("Failed: worker {worker} stopped responding while the function ran, for the second time.")
}

pub(crate) fn expired(due_at: DateTime<Utc>, expires_at: DateTime<Utc>, zone: Tz, why: &str) -> String {
    format!("Expired: due at {}, not started by {} ({why}).", clock(due_at, zone), clock(expires_at, zone))
}

pub(crate) fn gave_up(after: Duration, last_reason: Option<&str>) -> String {
    match last_reason {
        Some(reason) => format!("Failed: gave up after {}; last reason: {}", format_duration(after), reason.trim_end_matches('.')),
        None => format!("Failed: gave up after {} of waiting.", format_duration(after)),
    }
}

pub(crate) fn not_registered() -> String {
    "Failed: no running worker has had this job in its build for an hour.".to_owned()
}

pub(crate) fn joined(run_id: Uuid) -> String {
    format!("Joined run {}, which was already queued for the same work.", short(run_id))
}

pub(crate) fn marked_succeeded(by: &str, note: &str) -> String {
    format!("Marked as succeeded by {by}: {note}")
}

/// The first line of a message, for a sentence that must stay one line.
fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or(text).trim_end_matches('.')
}
