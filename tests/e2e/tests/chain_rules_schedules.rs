//! Shifted windows, the default expiry of an `every` occurrence, and backfills (spec section 8,
//! rules 4, 11, 19 and 20), with schedules that run only in the `rules` environment.

mod support;

use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, Timelike, Utc};
use downbeat::measure;
use downbeat_client::Client;
use downbeat_client::types::BackfillRequest;
use downbeat_e2e::definition;
use sqlx::PgPool;
use support::{Database, SOON, eventually, finished, with_api_in};
use uuid::Uuid;

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

// Rule 4 for a day rule's `Shifted`, and rule 11 for `every` with no `expire_after`.
#[tokio::test]
async fn a_shifted_day_moves_the_window_and_an_every_run_expires_after_one_interval() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    measure::register_schedules(pool, definition(), "shifting").await.unwrap();
    // Only `shifted_window` is due, as if it had missed the last ninety minutes.
    let watermark = Utc::now() - TimeDelta::minutes(90);
    sqlx::query("UPDATE downbeat.schedule_state SET next_due_at = NULL WHERE schedule <> 'shifted_window'")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE downbeat.schedule_state SET next_due_at = now(), watermark = $1 WHERE schedule = 'shifted_window'",
    )
    .bind(watermark)
    .execute(pool)
    .await
    .unwrap();
    measure::fire_due_schedules(pool, definition(), "shifting").await.unwrap();

    let runs: Vec<(DateTime<Utc>, Option<DateTime<Utc>>)> =
        sqlx::query_as("SELECT due_at, expires_at FROM downbeat.run WHERE schedule = 'shifted_window'")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(runs.len(), 1, "{runs:?}");
    let (due_at, expires_at) = runs[0];
    // Hourly from 00:00 to 23:00, moved half an hour later: the latest occurrence is at half past.
    assert_eq!((due_at.minute(), due_at.second()), (30, 0), "{due_at}");
    assert!(due_at > watermark && due_at <= Utc::now(), "{due_at}");
    assert_eq!(expires_at, Some(due_at + TimeDelta::hours(1)), "one interval after the occurrence");
}

async fn start_backfill(client: &Client, job: &str, from: &str, to: &str, continue_on_failure: bool) -> i64 {
    let request =
        BackfillRequest { job: job.into(), schedule: None, from: date(from), to: date(to), continue_on_failure };
    client.as_caller().start_backfill(&request).await.unwrap().id.expect("a backfill names its id")
}

/// The id of the backfill's run, once it has created one.
async fn backfill_run(pool: &PgPool) -> Uuid {
    eventually(pool, "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE started_by = 'backfill')", SOON).await;
    sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE started_by = 'backfill'").fetch_one(pool).await.unwrap()
}

async fn backfill_state(pool: &PgPool, id: i64) -> (String, Option<String>) {
    sqlx::query_as("SELECT state, reason FROM downbeat.backfill WHERE backfill_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn wait_for_state(pool: &PgPool, id: i64, state: &str) {
    let sql = format!("SELECT state = '{state}' FROM downbeat.backfill WHERE backfill_id = {id}");
    eventually(pool, &sql, SOON).await;
}

// Rule 20 (started_by, due now, expiry, never joins, the chain's backfill id), then rule 19 for a
// cancelled run.
#[tokio::test]
async fn a_backfill_run_is_due_now_expires_by_its_schedule_and_never_joins_queued_work() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    database.hold("backfilled_day").await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let queued = client.start("backfilled_day", &"2030-01-31").send().await.unwrap();
    assert!(!queued.joined);

    let id = start_backfill(&client, "backfilled_day", "2030-01-31", "2030-03-31", false).await;
    let run = backfill_run(pool).await;
    let (input, same_work_key, due_in, expires_in, backfill_id): (
        serde_json::Value,
        Option<String>,
        f64,
        f64,
        Option<i64>,
    ) = sqlx::query_as(
        "SELECT r.input, r.same_work_key, extract(epoch FROM r.due_at - r.created_at)::float8,
                extract(epoch FROM r.expires_at - r.created_at)::float8, c.backfill_id
           FROM downbeat.run r JOIN downbeat.chain c USING (chain_id) WHERE r.run_id = $1",
    )
    .bind(run)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(input, serde_json::json!("2030-01-31"));
    assert!(due_in.abs() < 1.0, "due when it was created, not at the occurrence: {due_in}");
    assert!((expires_in - 3600.0).abs() < 1.0, "the schedule's hour from now: {expires_in}");
    assert_eq!(backfill_id, Some(id));
    assert_eq!(same_work_key, None, "not covered by rule 1");
    assert_ne!(run, queued.run_id);
    let queued_runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name = 'backfilled_day'")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(queued_runs, 2, "the backfill's run did not join the same work already queued");

    client.cancel(run).await.unwrap();
    assert_eq!(finished(pool, run, SOON).await.status, "cancelled");
    wait_for_state(pool, id, "failed").await;
    let (_, reason) = backfill_state(pool, id).await;
    assert!(reason.as_deref().is_some_and(|reason| reason.contains("cancelled")), "{reason:?}");
}

// Rule 19 for an expired run.
#[tokio::test]
async fn a_backfill_stops_when_a_date_s_run_expires() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    database.hold("backfilled_day").await;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let id = start_backfill(&client, "backfilled_day", "2030-01-31", "2030-03-31", false).await;
    let run = backfill_run(pool).await;
    sqlx::query("UPDATE downbeat.run SET expires_at = now() - interval '1 minute' WHERE run_id = $1")
        .bind(run)
        .execute(pool)
        .await
        .unwrap();

    assert_eq!(finished(pool, run, SOON).await.status, "expired");
    wait_for_state(pool, id, "failed").await;
    let (_, reason) = backfill_state(pool, id).await;
    assert!(reason.as_deref().is_some_and(|reason| reason.contains("expired")), "{reason:?}");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let later: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name = 'backfilled_day'")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(later, 0, "no later date starts");
}

// Rule 20: an error starting a date makes the backfill failed.
#[tokio::test]
async fn an_error_starting_a_date_fails_the_backfill() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    let id = start_backfill(&client, "backfill_cannot_start", "2030-01-01", "2030-12-31", true).await;

    wait_for_state(pool, id, "failed").await;
    let (_, reason) = backfill_state(pool, id).await;
    assert!(reason.as_deref().is_some_and(|reason| reason.contains("panicked")), "{reason:?}");
    let runs: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'backfill_cannot_start')
              + (SELECT count(*) FROM downbeat.run_history WHERE job_name = 'backfill_cannot_start')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(runs, 0);
}

// Rule 19: the driver runs every two polls (the test product's poll is 200 ms), so dates start at
// least about 400 ms apart. Timed loosely, because the processes share a busy machine.
#[tokio::test]
async fn a_backfill_moves_on_at_most_every_two_polls() {
    let database = Database::fresh().await;
    let pool = &database.pool;
    let (_app, client, _) = with_api_in(&database, "rules").await;
    // The 31sts of 2030: seven dates.
    let id = start_backfill(&client, "backfilled_day", "2030-01-01", "2030-12-31", false).await;
    eventually(
        pool,
        &format!("SELECT state = 'done' FROM downbeat.backfill WHERE backfill_id = {id}"),
        Duration::from_secs(60),
    )
    .await;

    let created: Vec<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT created_at FROM downbeat.run_history WHERE job_name = 'backfilled_day' ORDER BY created_at",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(created.len(), 7);
    for pair in created.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(gap >= TimeDelta::milliseconds(350), "dates started {gap} apart: {created:?}");
        assert!(gap <= TimeDelta::seconds(10), "dates started {gap} apart: {created:?}");
    }
}
