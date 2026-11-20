//! The same-work rules and conditions: a join keeps the later expiry, a run keeps its turn through
//! a wait, a lost worker and an interruption, conditions and needs are checked in the order
//! written, and `if_changed` leaves runs started on request alone.

mod support;

use std::time::Duration;

use chrono::{DateTime, Utc};
use downbeat::InAppClient;
use downbeat_client::Scope;
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use support::{App, Database, Finished, SOON, eventually, finished, with_api};
use uuid::Uuid;

const LONG: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_scheduled_run_that_joins_keeps_the_later_expiry() {
    let database = Database::fresh().await;
    database.hold("ticker").await;
    downbeat::measure::register_schedules(&database.pool, definition(), "schedules").await.unwrap();
    let fire = || downbeat::measure::fire_due_schedules(&database.pool, definition(), "schedules");
    let expiry = || async {
        sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
            "SELECT count(*), max(expires_at) FROM downbeat.run WHERE job_name = 'ticker'",
        )
        .fetch_one(&database.pool)
        .await
        .unwrap()
    };

    tokio::time::sleep(Duration::from_millis(1100)).await;
    fire().await.unwrap();
    let (runs, first) = expiry().await;
    assert_eq!(runs, 1);
    let first = first.expect("an `every` schedule's run expires");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fire().await.unwrap();

    let (runs, joined) = expiry().await;
    assert_eq!(runs, 1, "the second occurrence joined the queued run");
    let joined = joined.expect("both had an expiry, so the join keeps one");
    assert!(joined > first, "the join kept {joined}, not the later expiry; the first was {first}");
    let joins: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM downbeat.run_history WHERE job_name = 'ticker' AND skipped_because = 'joined'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(joins, 1);
}

async fn started_at(pool: &sqlx::PgPool, run: Uuid) -> (DateTime<Utc>, DateTime<Utc>) {
    sqlx::query_as("SELECT started_at, finished_at FROM downbeat.run_history WHERE run_id = $1")
        .bind(run)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The second run of a one-at-a-time job started only after the first finished.
async fn ran_after(pool: &sqlx::PgPool, first: &Finished, second: &Finished) {
    let (_, first_finished) = started_at(pool, first.run_id).await;
    let (second_started, _) = started_at(pool, second.run_id).await;
    assert!(
        second_started >= first_finished,
        "the second run started at {second_started}, before the first finished at {first_finished}"
    );
}

async fn wait_started(pool: &sqlx::PgPool, run: Uuid) {
    eventually(
        pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND starts > 0)"),
        SOON,
    )
    .await;
}

#[tokio::test]
async fn a_run_keeps_its_turn_while_it_waits_as_not_ready() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let missing = std::env::temp_dir().join(format!("turn-{}", Uuid::new_v4()));
    let present = std::env::temp_dir().join(format!("turn-{}", Uuid::new_v4()));
    std::fs::write(&present, "here").unwrap();

    let first = client.start(waits_its_turn_for_file, Text { text: missing.display().to_string() }).await.unwrap();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0 AND queued_because = 'not_ready')",
            first.run_id()
        ),
        SOON,
    )
    .await;
    let second = client.start(waits_its_turn_for_file, Text { text: present.display().to_string() }).await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let starts: i32 = sqlx::query_scalar("SELECT starts FROM downbeat.run WHERE run_id = $1")
        .bind(second.run_id())
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(starts, 0, "the second run started while the first waited");

    std::fs::write(&missing, "here").unwrap();
    let first = finished(&database.pool, first.run_id(), SOON).await;
    let second = finished(&database.pool, second.run_id(), SOON).await;
    let _ = std::fs::remove_file(&missing);
    let _ = std::fs::remove_file(&present);
    assert_eq!((first.status.as_str(), second.status.as_str()), ("succeeded", "succeeded"));
    ran_after(&database.pool, &first, &second).await;
}

#[tokio::test]
async fn a_run_keeps_its_turn_when_its_worker_is_lost() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let lost = App::start(&database);

    let first = client.start(single, Seconds { seconds: 6 }).await.unwrap().run_id();
    wait_started(&database.pool, first).await;
    let second = client.start(single, Seconds { seconds: 1 }).await.unwrap().run_id();
    lost.kill();
    let _app = App::start(&database);

    let first = finished(&database.pool, first, LONG).await;
    let second = finished(&database.pool, second, LONG).await;
    assert_eq!(first.lost_count, 1, "{}", first.reason);
    assert_eq!((first.status.as_str(), second.status.as_str()), ("succeeded", "succeeded"));
    ran_after(&database.pool, &first, &second).await;
}

#[tokio::test]
async fn a_run_keeps_its_turn_when_it_is_interrupted() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let stopping = App::start(&database);

    // Longer than the app's ten seconds to stop, so the run is interrupted.
    let first = client.start(single, Seconds { seconds: 15 }).await.unwrap().run_id();
    wait_started(&database.pool, first).await;
    let second = client.start(single, Seconds { seconds: 1 }).await.unwrap().run_id();
    stopping.stop();
    let _app = App::start(&database);

    let first = finished(&database.pool, first, LONG).await;
    let second = finished(&database.pool, second, LONG).await;
    assert_eq!(first.interruptions, 1, "{}", first.reason);
    assert_eq!((first.status.as_str(), second.status.as_str()), ("succeeded", "succeeded"));
    ran_after(&database.pool, &first, &second).await;
}

#[tokio::test]
async fn only_if_conditions_are_checked_in_the_order_written() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let both_false = client.start(two_conditions, Flags { first: false, second: false }).await.unwrap();
    let second_false = client.start(two_conditions, Flags { first: true, second: false }).await.unwrap();
    let done = finished(&database.pool, both_false.run_id(), SOON).await;
    assert_eq!(done.skipped_because.as_deref(), Some("condition_false"));
    assert!(done.reason.contains("'first'"), "{}", done.reason);
    let done = finished(&database.pool, second_false.run_id(), SOON).await;
    assert_eq!(done.skipped_because.as_deref(), Some("condition_false"));
    assert!(done.reason.contains("'second'"), "{}", done.reason);
}

#[tokio::test]
async fn needs_are_checked_in_the_order_written() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let neither = client.start(two_needs, Text { text: "neither".into() }).await.unwrap();
    let done = finished(&database.pool, neither.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("need_not_met"), "{}", done.reason);
    assert!(done.reason.contains("needed_first") && !done.reason.contains("needed_second"), "{}", done.reason);

    let met = client.start(needed_first, Text { text: "first only".into() }).await.unwrap();
    finished(&database.pool, met.run_id(), SOON).await;
    let first_only = client.start(two_needs, Text { text: "first only".into() }).await.unwrap();
    let done = finished(&database.pool, first_only.run_id(), SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("need_not_met"), "{}", done.reason);
    assert!(done.reason.contains("needed_second"), "{}", done.reason);
}

#[tokio::test]
async fn if_changed_leaves_runs_started_by_a_program_a_person_or_a_job_alone() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let app_client = InAppClient::connect(definition, &database.url).await.unwrap();
    let same = || Text { text: "same".into() };

    let earlier = app_client.start(only_when_changed, same()).await.unwrap();
    assert_eq!(finished(&database.pool, earlier.run_id(), SOON).await.status, "succeeded");

    let by_program = app_client.start(only_when_changed, same()).await.unwrap().run_id();
    let done = finished(&database.pool, by_program, SOON).await;
    assert_eq!((done.started_by.as_str(), done.status.as_str()), ("program", "succeeded"), "{}", done.reason);

    let alice = client.acting_for("alice", Scope::Operate, None);
    let by_person = alice.start("only_when_changed", &same()).send().await.unwrap().run_id;
    let done = finished(&database.pool, by_person, SOON).await;
    assert_eq!((done.started_by.as_str(), done.status.as_str()), ("person", "succeeded"), "{}", done.reason);

    let parent = app_client.start(starts_only_when_changed, Text { text: "go".into() }).await.unwrap();
    let parent = finished(&database.pool, parent.run_id(), SOON).await;
    assert_eq!(parent.status, "succeeded", "{}", parent.reason);
    let by_job: Uuid = parent.output.unwrap()["text"].as_str().unwrap().parse().unwrap();
    let done = finished(&database.pool, by_job, SOON).await;
    assert_eq!((done.started_by.as_str(), done.status.as_str()), ("job", "succeeded"), "{}", done.reason);
}
