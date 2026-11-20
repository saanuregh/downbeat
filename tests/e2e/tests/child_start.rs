//! A child process must say it is ready within a minute; one that does not is given up on, and the
//! run waits in the queue with the reason.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, eventually};

#[tokio::test]
async fn a_child_that_is_not_ready_within_a_minute_is_given_up_on() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with_env(
        &database,
        &["scheduler", "worker", "--queues", "default=1"],
        "test",
        &[("E2E_CHILD_STALLS", "1")],
    );

    let run = client.start(echo, Text { text: "never called".into() }).await.unwrap().run_id();
    let given_up = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'queued'
          AND queued_reason LIKE '%did not become ready within a minute%')"
    );
    // The slot's first child takes a minute to be given up on, and the run's own another.
    eventually(&database.pool, &given_up, Duration::from_secs(150)).await;
    let (starts, failures): (i32, i16) = sqlx::query_as("SELECT starts, failures FROM downbeat.run WHERE run_id = $1")
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!((starts, failures), (0, 0), "the function was never called, and nothing was counted");
}
