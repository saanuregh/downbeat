//! What happens when a worker dies or is shut down in the middle of a run.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, eventually, finished};

#[tokio::test]
async fn a_run_whose_worker_died_is_taken_back_and_continues_from_its_checkpoint() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 6 }).await.unwrap();
    let checkpointed = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND checkpoint IS NOT NULL)",
        run.run_id()
    );
    eventually(&database.pool, &checkpointed, Duration::from_secs(20)).await;
    first.kill();

    let _second = App::start(&database);
    let done = finished(&database.pool, run.run_id(), Duration::from_secs(60)).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.lost_count, 1);
    assert_eq!(done.failures, 0);
    assert_eq!(done.output, Some(serde_json::json!(6)));
}

#[tokio::test]
async fn a_shutdown_releases_a_long_run_for_another_worker() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let first = App::start(&database);

    let run = client.start(counts, Seconds { seconds: 6 }).await.unwrap();
    let checkpointed = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND checkpoint IS NOT NULL)",
        run.run_id()
    );
    eventually(&database.pool, &checkpointed, Duration::from_secs(20)).await;
    first.stop();

    let released = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND state = 'queued' AND interruptions = 1)",
        run.run_id()
    );
    eventually(&database.pool, &released, Duration::from_secs(5)).await;

    let _second = App::start(&database);
    let done = finished(&database.pool, run.run_id(), Duration::from_secs(30)).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!((done.interruptions, done.lost_count, done.failures), (1, 0, 0));
}

/// The child processes a worker started: those whose parent it is and that carry the child marker.
fn children_of(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(child) = entry.file_name().to_string_lossy().parse::<u32>() else { continue };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{child}/stat")) else { continue };
        // The parent's id is the second field after the command, which is in parentheses.
        let parent = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(1))
            .and_then(|field| field.parse::<u32>().ok());
        let marked = std::fs::read(format!("/proc/{child}/environ")).is_ok_and(|environ| {
            environ.split(|byte| *byte == 0).any(|variable| variable.starts_with(b"DOWNBEAT_CHILD_FD="))
        });
        if parent == Some(pid) && marked {
            children.push(child);
        }
    }
    children
}

#[tokio::test]
async fn children_are_the_first_to_go_when_memory_runs_out_and_die_with_their_worker() {
    let database = Database::fresh().await;
    let app = App::start(&database);
    let pid = app.pid();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let children = loop {
        let children = children_of(pid);
        if children.len() >= 2 {
            break children;
        }
        assert!(tokio::time::Instant::now() < deadline, "the worker started no children");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    for child in &children {
        let score = std::fs::read_to_string(format!("/proc/{child}/oom_score_adj")).unwrap();
        assert_eq!(score.trim(), "1000", "child {child} is not the preferred out-of-memory victim");
    }

    app.kill();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let alive: Vec<&u32> =
        children.iter().filter(|child| std::path::Path::new(&format!("/proc/{child}")).exists()).collect();
    assert!(alive.is_empty(), "children outlived their worker: {alive:?}");
}
