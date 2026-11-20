//! The last unchecked engine rules: an ended pause holds nothing (2.5, 3.1), a join never lowers
//! what the queued run had (5.2), `if_changed` looks only at the latest success (6.7), which
//! children an ending creates (7), and no transaction is open while `on_finish` runs (12.2).

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use downbeat::{InAppClient, Started};
use downbeat_e2e::*;
use sqlx::PgPool;
use support::{App, Database, SOON, eventually, finished, free_port, with_api};
use uuid::Uuid;

/// The test product as a scheduler with the `--local` API and no worker, so runs stay queued.
async fn without_worker(database: &Database) -> (App, downbeat_client::Client) {
    let listen = format!("127.0.0.1:{}", free_port());
    let app = App::with(database, &["scheduler", "--local", "--listen", &listen], "test");
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        match downbeat_client::Client::builder(&format!("http://{listen}")).local().connect().await {
            Ok(client) => return (app, client),
            Err(error) => {
                assert!(tokio::time::Instant::now() < deadline, "the API did not come up: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

// Rules 2.5 and 3.1: a pause whose `until` has passed is no longer active.
#[tokio::test]
async fn a_pause_whose_until_has_passed_neither_holds_a_queued_run_nor_stops_a_claim() {
    let database = Database::fresh().await;
    let (_app, client) = without_worker(&database).await;
    let run = client.start("echo", &Text { text: "after the pause".into() }).send().await.unwrap().run_id;
    for (kind, name) in [("job", "echo"), ("queue", "default"), ("all", "all")] {
        sqlx::query(
            "INSERT INTO downbeat.pause (kind, name, until, paused_by, note)
             VALUES ($1, $2, now() - interval '1 minute', 'test', 'ended a minute ago')",
        )
        .bind(kind)
        .bind(name)
        .execute(&database.pool)
        .await
        .unwrap();
    }

    let because = client.run(run).await.unwrap().queued_because.unwrap_or_default();
    assert_eq!(because, "no_free_worker", "an ended pause is not reported");
    let jobs = vec!["echo".to_owned()];
    let claimed = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap();
    assert_eq!(claimed.map(|(id, _)| id), Some(run), "an ended pause does not stop the claim");
}

/// The queued run's priority, whether its due time is still about an hour ahead, and whether its
/// expiry is still about two hours ahead (null when it has none).
async fn kept_values(pool: &PgPool, run: Uuid) -> (i16, bool, Option<bool>) {
    sqlx::query_as(
        "SELECT priority, due_at > now() + interval '50 minutes',
                expires_at > now() + interval '110 minutes'
           FROM downbeat.run WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Gives a queued run a high priority, a due time an hour ahead and an expiry two hours ahead.
async fn raise(pool: &PgPool, run: Uuid) {
    sqlx::query(
        "UPDATE downbeat.run SET priority = 9, due_at = now() + interval '1 hour', expires_at = now() + interval '2 hours'
          WHERE run_id = $1",
    )
    .bind(run)
    .execute(pool)
    .await
    .unwrap();
}

// Rule 5.2: a plain request that joins keeps the queued run's higher priority and later due time.
#[tokio::test]
async fn a_plain_request_that_joins_keeps_the_higher_priority_and_the_later_due_time() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let input = Text { text: "raised".into() };
    let first = client.start(echo, input.clone()).await.unwrap().run_id();
    raise(&database.pool, first).await;

    assert_eq!(client.start(echo, input).await.unwrap(), Started::Joined(first));
    let (priority, later_due, expiry) = kept_values(&database.pool, first).await;
    assert_eq!(priority, 9, "the join kept the greater priority");
    assert!(later_due, "the join kept the later due time");
    assert_eq!(expiry, None, "a request that never expires lifts the expiry");
}

// Rule 5.2: a scheduled request with a smaller priority, an earlier due time and an earlier
// expiry joins without lowering any of them.
#[tokio::test]
async fn a_scheduled_request_that_joins_lowers_neither_priority_nor_due_time_nor_expiry() {
    let database = Database::fresh().await;
    downbeat::measure::register_schedules(&database.pool, definition(), "schedules").await.unwrap();
    let fire = || downbeat::measure::fire_due_schedules(&database.pool, definition(), "schedules");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fire().await.unwrap();
    let first: Uuid = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE job_name = 'ticker'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    raise(&database.pool, first).await;

    tokio::time::sleep(Duration::from_millis(1100)).await;
    fire().await.unwrap();
    let (runs, joins): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'ticker'),
                (SELECT count(*) FROM downbeat.run_history WHERE job_name = 'ticker' AND joined_run = $1)",
    )
    .bind(first)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!((runs, joins), (1, 1), "the second occurrence joined the queued run");
    assert_eq!(kept_values(&database.pool, first).await, (9, true, Some(true)));
}

/// How the `after_change` child of a new `echo` chain with this text ended.
async fn after_change_of(client: &InAppClient, pool: &PgPool, text: &str) -> (String, Option<String>) {
    let parent = client.start(echo, Text { text: text.into() }).await.unwrap().run_id();
    let chain = finished(pool, parent, SOON).await.chain_id;
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'after_change' AND chain_id = '{chain}')"
    );
    eventually(pool, &sql, SOON).await;
    sqlx::query_as(
        "SELECT status, skipped_because FROM downbeat.run_history WHERE job_name = 'after_change' AND chain_id = $1",
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .unwrap()
}

// Rule 6.7: an older success with the same input does not count once a newer success differs.
#[tokio::test]
async fn if_changed_compares_with_the_latest_success_only() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let ran = ("succeeded".to_owned(), None);

    assert_eq!(after_change_of(&client, &database.pool, "first").await, ran);
    assert_eq!(after_change_of(&client, &database.pool, "second").await, ran);
    assert_eq!(
        after_change_of(&client, &database.pool, "first").await,
        ran,
        "the latest success was for `second`, so `first` counts as changed"
    );
}

/// How many runs of `job` the chain has, queued, running or finished.
async fn runs_in_chain(pool: &PgPool, job: &str, chain: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = $1 AND chain_id = $2)
              + (SELECT count(*) FROM downbeat.run_history WHERE job_name = $1 AND chain_id = $2)",
    )
    .bind(job)
    .bind(chain)
    .fetch_one(pool)
    .await
    .unwrap()
}

// Section 7, child creation: `.after` needs `succeeded`; `.after_failure` needs `failed`.
// Children are created in the transaction that ends the parent, so the counts are final once the
// parent's history row exists.
#[tokio::test]
async fn only_the_link_that_fits_the_parents_ending_creates_a_child() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    let pool = &database.pool;
    let children = |chain: Uuid| async move {
        (runs_in_chain(pool, "follows_success", chain).await, runs_in_chain(pool, "follows_failure", chain).await)
    };

    let failed = client.start("ends_as_told", &Text { text: "fail".into() }).send().await.unwrap().run_id;
    let failed = finished(pool, failed, SOON).await;
    assert_eq!(failed.status, "failed", "{}", failed.reason);
    assert_eq!(children(failed.chain_id).await, (0, 1), "a failed parent: no `.after` child, one `.after_failure`");

    let skipped = client.start("ends_as_told", &Text { text: "skip".into() }).send().await.unwrap().run_id;
    let skipped = finished(pool, skipped, SOON).await;
    assert_eq!(skipped.status, "skipped", "{}", skipped.reason);
    assert_eq!(children(skipped.chain_id).await, (0, 0), "a skipped parent: neither child");

    database.hold("ends_as_told").await;
    let cancelled = client.start("ends_as_told", &Text { text: "cancel".into() }).send().await.unwrap().run_id;
    client.cancel(cancelled).await.unwrap();
    let cancelled = finished(pool, cancelled, SOON).await;
    assert_eq!(cancelled.status, "cancelled", "{}", cancelled.reason);
    assert_eq!(children(cancelled.chain_id).await, (0, 0), "a cancelled parent: neither child");
    database.release().await;

    let succeeded = client.start("ends_as_told", &Text { text: "fine".into() }).send().await.unwrap().run_id;
    let succeeded = finished(pool, succeeded, SOON).await;
    assert_eq!(children(succeeded.chain_id).await, (1, 0), "the job's links do create children when they fit");
}

// Rule 12.2: the callback runs with no transaction open on the database.
#[tokio::test]
async fn no_transaction_is_open_while_on_finish_runs() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    // A worker alone delivers no callbacks, so the history row stays pending.
    let app = App::with(&database, &["worker", "--queues", "default=1"], "test");
    let run = client.start(echo, Text { text: "delivered".into() }).await.unwrap().run_id();
    finished(&database.pool, run, SOON).await;
    app.stop();
    sqlx::query("UPDATE downbeat.run_history SET notify_after = now() WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();

    // What the callback saw: the run, the sessions idle in a transaction, and whether the row's
    // push five minutes ahead was already visible to another session.
    let seen: Arc<Mutex<Vec<(Uuid, i64, bool)>>> = Arc::default();
    let mut definition = definition_without_callbacks();
    let (url, record) = (database.url.clone(), Arc::clone(&seen));
    definition.on_finish(move |finished: downbeat::FinishedRun| {
        let (url, record) = (url.clone(), Arc::clone(&record));
        async move {
            use sqlx::Connection;
            let mut connection = sqlx::PgConnection::connect(&url).await.map_err(|error| error.to_string())?;
            let (open, pushed): (i64, bool) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM pg_stat_activity
                          WHERE datname = current_database() AND state LIKE 'idle in transaction%'),
                        (SELECT notify_after > now() + interval '4 minutes' FROM downbeat.run_history WHERE run_id = $1)",
            )
            .bind(finished.run_id)
            .fetch_one(&mut connection)
            .await
            .map_err(|error| error.to_string())?;
            record.lock().unwrap().push((finished.run_id, open, pushed));
            Ok::<(), String>(())
        }
    });

    downbeat::measure::deliver_callbacks(&database.pool, definition).await.unwrap();
    // The run's children in its chain are pending too, so every delivery is checked.
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().any(|(id, _, _)| *id == run), "the run was delivered: {seen:?}");
    for (id, open, pushed) in seen {
        assert_eq!(open, 0, "a transaction was open while on_finish ran for {id}");
        assert!(pushed, "the push five minutes ahead for {id} was not committed before on_finish ran");
    }
}
