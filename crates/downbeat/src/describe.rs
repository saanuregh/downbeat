//! Jobs and schedules in words: what `check` prints, what the schedule snapshot holds, and what the
//! API returns as "what starts it, its conditions and policies in plain words".

use std::fmt::Write as _;

use crate::pipeline::{LinkKind, NeedTest, WhenUnmet};
use crate::registry::{RegisteredJob, Registry};
use crate::schedule::{CompiledSchedule, Times};
use crate::units::format_duration;

/// A schedule's times in words: `Asia/Kolkata every 30m from 18:45 to 22:30 on Mon, Tue`.
pub(crate) fn schedule(schedule: &CompiledSchedule) -> String {
    let mut text = format!("{} ", schedule.zone);
    match &schedule.times {
        Times::DailyAt(times) => {
            let times: Vec<String> = times.iter().map(|time| time.format("%H:%M").to_string()).collect();
            write!(text, "daily at {}", times.join(", ")).ok();
        }
        Times::Every { interval, window: None } => {
            write!(text, "every {}", format_duration(*interval)).ok();
        }
        Times::Every { interval, window: Some((start, end)) } => {
            write!(
                text,
                "every {} from {} to {}",
                format_duration(*interval),
                start.format("%H:%M"),
                end.format("%H:%M")
            )
            .ok();
        }
    }
    if let Some(days) = &schedule.weekdays {
        let days: Vec<String> = days.iter().map(ToString::to_string).collect();
        write!(text, " on {}", days.join(", ")).ok();
    }
    if let Some(days) = &schedule.month_days {
        let days: Vec<String> = days.iter().map(ToString::to_string).collect();
        write!(text, " on day {} of the month", days.join(", ")).ok();
    }
    if let Some(rule) = &schedule.calendar {
        write!(text, ", by calendar {}", rule.name).ok();
    }
    if let Some(expire) = schedule.expire_after {
        write!(text, ", expires after {}", format_duration(expire)).ok();
    }
    text
}

/// What starts a job and on what conditions, one phrase each.
pub(crate) fn triggers_and_conditions(registry: &Registry, job: &RegisteredJob) -> Vec<String> {
    let mut lines = Vec::new();
    for name in &job.schedules {
        if let Some(compiled) = registry.schedules.get(name) {
            let label = if name == job.spec.name { String::new() } else { format!(" ({name})") };
            lines.push(format!("schedule{label}: {}", schedule(compiled)));
        }
    }
    if let Some(link) = &job.link {
        let parents = link.parents.join(" and ");
        let adapted = if link.mapped { ", adapted" } else { "" };
        lines.push(match link.kind {
            LinkKind::After => format!("after {parents}{adapted}"),
            LinkKind::AfterFailure => format!("after {parents} fails"),
            LinkKind::AfterAny => format!("after {parents} ends in any way"),
        });
    }
    if job.if_changed {
        lines.push("if its input changed".to_owned());
    }
    for need in &job.needs {
        let what = match need.test {
            NeedTest::DoneFor(_) => "for the input this run needs".to_owned(),
            NeedTest::DoneWithin(within) => format!("within {}", format_duration(within)),
        };
        let unmet = match need.when_unmet {
            WhenUnmet::Skip => "",
            WhenUnmet::Wait => ", or wait",
            WhenUnmet::Fail => ", or fail",
        };
        lines.push(format!("needs {} {what}{unmet}", need.job));
    }
    for condition in &job.conditions {
        lines.push(format!("only if '{}'", condition.name));
    }
    if lines.is_empty() {
        lines.push("started only by people and programs".to_owned());
    }
    lines
}

/// Every pipeline with its jobs, sorted, as `check` prints it and the snapshot stores it.
pub(crate) fn snapshot(registry: &Registry) -> String {
    let mut text = String::new();
    for (pipeline, jobs) in &registry.pipelines {
        writeln!(text, "{pipeline}").ok();
        let mut jobs = jobs.clone();
        jobs.sort_unstable();
        for name in jobs {
            let Some(job) = registry.job(name) else { continue };
            let kind = if job.spec.is_command() { " (command)" } else { "" };
            writeln!(text, "  {name}{kind}").ok();
            for line in triggers_and_conditions(registry, job) {
                writeln!(text, "    {line}").ok();
            }
            for line in job.spec.policy.describe() {
                writeln!(text, "    {line}").ok();
            }
        }
    }
    text
}
