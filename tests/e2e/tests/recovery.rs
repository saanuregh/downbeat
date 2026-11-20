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
    let checkpointed = format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND checkpoint IS NOT NULL)", run.run_id());
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
    let checkpointed = format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND checkpoint IS NOT NULL)", run.run_id());
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
