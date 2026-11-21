//! `check` reports a lease shorter than two heartbeats, for the default timings and for a queue.

use std::time::Duration;

use downbeat::prelude::*;
use downbeat::{QueueTimings, Settings};

#[job]
async fn plain(_run: Run) -> JobResult<()> {
    Ok(())
}

fn jobs(pipeline: &mut Pipeline) {
    pipeline.job(plain);
}

fn definition(settings: Settings) -> Definition {
    let mut definition = Definition::new().zone("UTC").settings(settings);
    definition.pipeline("jobs", jobs);
    definition
}

#[test]
fn a_lease_shorter_than_two_heartbeats_is_reported() {
    let tight = QueueTimings { heartbeat: Duration::from_secs(20), lease: Duration::from_secs(30) };
    let settings = Settings {
        heartbeat: Duration::from_secs(10),
        lease: Duration::from_secs(15),
        queues: [("tight".to_owned(), tight)].into(),
        ..Settings::default()
    };
    let problems = downbeat::check_problems(definition(settings));
    assert_eq!(problems.len(), 2, "{problems:#?}");
    assert!(problems.iter().any(|p| p.contains("every queue") && p.contains("two heartbeats")), "{problems:#?}");
    assert!(problems.iter().any(|p| p.contains("queue tight") && p.contains("two heartbeats")), "{problems:#?}");
}

#[test]
fn a_lease_of_exactly_two_heartbeats_is_sound() {
    let settings =
        Settings { heartbeat: Duration::from_secs(10), lease: Duration::from_secs(20), ..Settings::default() };
    assert_eq!(downbeat::check_problems(definition(settings)), Vec::<String>::new());
}
