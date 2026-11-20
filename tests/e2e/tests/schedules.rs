//! Schedules fire on time, and two schedulers never fire the same occurrence twice.

mod support;

use std::time::Duration;

use support::{App, Database, eventually};

#[tokio::test]
async fn two_schedulers_fire_each_occurrence_once() {
    let database = Database::fresh().await;
    let _one = App::with(&database, &["scheduler", "worker", "--queues", "default=2"], "schedules");
    let _two = App::with(&database, &["scheduler"], "schedules");

    eventually(
        &database.pool,
        "SELECT count(*) >= 4 FROM downbeat.run_history WHERE job_name = 'ticker' AND status = 'succeeded'",
        Duration::from_secs(20),
    )
    .await;
    let duplicates: i64 = sqlx::query_scalar(
        "SELECT count(*) - count(DISTINCT due_at) FROM downbeat.run_history WHERE job_name = 'ticker' AND joined_run IS NULL",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(duplicates, 0, "an occurrence fired twice");
}

#[tokio::test]
async fn a_schedule_limited_to_another_environment_does_not_fire() {
    let database = Database::fresh().await;
    let _app = App::start(&database);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE job_name = 'ticker'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(runs, 0);
}
