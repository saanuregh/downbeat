//! Rules of a run's life not covered elsewhere: the checkpoint limit, strict decoding of every
//! kind of request, who started a run, why a queued run waits, what a claim writes, and how a
//! failed decision write is retried.

mod support;

use downbeat_client::types::QueuedBecause;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};
use downbeat::InAppClient;
use downbeat_client::PauseKind;
use downbeat_client::types::{BackfillRequest, PauseRequest};
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished, free_port, with_api};
use uuid::Uuid;

/// Puts a queued run of a job straight into the table, with this stored input.
async fn insert_queued(database: &Database, job: &str, input: serde_json::Value) -> Uuid {
    let run_id = Uuid::new_v4();
    let chain_id = Uuid::new_v4();
    sqlx::query("INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by) VALUES ($1, 'runtime_rules', $2, 'program')")
        .bind(chain_id)
        .bind(job)
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO downbeat.run (run_id, job_name, queue, priority, due_at, started_by, chain_id, input, input_hash, turn_key)
         VALUES ($1, $2, 'default', 0, now(), 'program', $3, $4, md5($1::text), $2 || ':' || md5($1::text))",
    )
    .bind(run_id)
    .bind(job)
    .bind(chain_id)
    .bind(input)
    .execute(&database.pool)
    .await
    .unwrap();
    run_id
}

fn lopsided() -> Wrapped {
    Wrapped { inner: Lopsided { kept: "a".into(), written_only: "b".into() } }
}

#[tokio::test]
async fn a_checkpoint_over_64_kib_is_refused() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(checkpoints_too_much, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let answers = done.output.unwrap()["text"].as_str().unwrap().to_owned();
    let (later, now) = answers.split_once('\n').unwrap();
    assert!(later.contains("the limit is 65536"), "checkpoint: {later}");
    assert!(now.contains("the limit is 65536"), "checkpoint_now: {now}");
}

#[tokio::test]
async fn in_app_client_decodes_strictly_and_names_the_unknown_field() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();

    let refused = client.start(takes_lopsided, lopsided()).await.unwrap_err().to_string();
    assert!(refused.contains("inner.written_only"), "{refused}");
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run").fetch_one(&database.pool).await.unwrap();
    assert_eq!(runs, 0, "nothing was created");
}

#[tokio::test]
async fn run_start_from_a_job_decodes_strictly_and_names_the_unknown_field() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(starts_lopsided, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let answer = done.output.unwrap()["text"].as_str().unwrap().to_owned();
    assert!(answer.contains("inner.written_only"), "{answer}");
    let started: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'takes_lopsided')
              + (SELECT count(*) FROM downbeat.run_history WHERE job_name = 'takes_lopsided')",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(started, 0, "no run of takes_lopsided was created");
}

#[test]
fn run_once_decodes_strictly_and_names_the_unknown_field() {
    let output = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .args(["run-once", "takes_lopsided", "--input", r#"{"inner":{"kept":"a","written_only":"b"}}"#])
        .env("DOWNBEAT_ENVIRONMENT", "test")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let printed = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    assert!(!output.status.success(), "run-once accepted an unknown field: {printed}");
    assert!(printed.contains("inner.written_only"), "{printed}");

    let accepted = Command::new(env!("CARGO_BIN_EXE_e2e-app"))
        .args(["run-once", "takes_lopsided", "--input", r#"{"inner":{"kept":"a"}}"#])
        .env("DOWNBEAT_ENVIRONMENT", "test")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(accepted.status.success(), "{}", String::from_utf8_lossy(&accepted.stderr));
}

#[tokio::test]
async fn a_stored_input_with_extra_fields_is_still_read() {
    let database = Database::fresh().await;
    let stored = serde_json::json!({ "inner": { "kept": "read", "written_only": "b", "unknown": 1 }, "more": true });
    let run = insert_queued(&database, "takes_lopsided", stored).await;
    let _app = App::start(&database);

    let done = finished(&database.pool, run, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.output.unwrap()["text"], "read");
}

#[tokio::test]
async fn in_app_client_starts_runs_as_a_program() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = client.start(echo, Text { text: "from the app".into() }).await.unwrap();
    let started_by: String = sqlx::query_scalar("SELECT started_by FROM downbeat.run WHERE run_id = $1")
        .bind(run.run_id())
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(started_by, "program");
}

#[tokio::test]
async fn a_schedule_starts_runs_as_schedule() {
    let database = Database::fresh().await;
    database.hold("ticker").await;
    downbeat::measure::register_schedules(&database.pool, definition(), "schedules").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    downbeat::measure::fire_due_schedules(&database.pool, definition(), "schedules").await.unwrap();

    let started_by: Vec<String> = sqlx::query_scalar("SELECT started_by FROM downbeat.run WHERE job_name = 'ticker'")
        .fetch_all(&database.pool)
        .await
        .unwrap();
    assert_eq!(started_by, vec!["schedule".to_owned()]);
}

#[tokio::test]
async fn a_backfill_starts_runs_as_backfill_and_never_joins_queued_work() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    database.hold("dated").await;
    let day = NaiveDate::from_ymd_opt(2030, 1, 7).unwrap();

    let queued = client.start("dated", &day).send().await.unwrap();
    let backfill = BackfillRequest::new("dated", day, day);
    client.as_caller().start_backfill(&backfill).await.unwrap();
    eventually(&database.pool, "SELECT count(*) = 2 FROM downbeat.run WHERE job_name = 'dated'", SOON).await;

    let (started_by, key, same_input): (String, Option<String>, bool) = sqlx::query_as(
        "SELECT b.started_by, b.same_work_key, b.input_hash = q.input_hash
           FROM downbeat.run b, downbeat.run q
          WHERE b.job_name = 'dated' AND q.run_id = $1 AND b.run_id <> q.run_id",
    )
    .bind(queued.run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(started_by, "backfill");
    assert!(same_input, "the backfill's run is the same work as the queued one");
    assert_eq!(key, None, "a backfill run is not covered by rule 1");

    database.release().await;
    let done = finished(&database.pool, queued.run_id, SOON).await;
    assert_eq!(done.status, "succeeded", "the queued run was not joined: {}", done.reason);
}

#[tokio::test]
async fn a_command_killed_by_a_signal_fails_as_signal_after_its_attempts() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(kills_itself, ()).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "failed", "{}", done.reason);
    assert_eq!(done.failed_because.as_deref(), Some("signal"), "{}", done.reason);
    assert_eq!(done.starts, 2, "a signal is a command failure of kind Error, so it was tried again");
}

/// The test product with the `--local` API and one worker slot on `default`.
async fn with_one_slot(database: &Database) -> (App, downbeat_client::Client) {
    let listen = format!("127.0.0.1:{}", free_port());
    let app =
        App::with(database, &["scheduler", "worker", "--queues", "default=1", "--local", "--listen", &listen], "test");
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

#[tokio::test]
async fn a_queued_run_says_whether_its_queue_is_paused_unserved_or_busy() {
    let database = Database::fresh().await;
    let (_app, client) = with_one_slot(&database).await;

    let busy = client.start("naps", &Number { n: 15_000 }).send().await.unwrap();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{}' AND starts > 0)", busy.run_id),
        SOON,
    )
    .await;
    let waiting = client.start("naps", &Number { n: 1 }).send().await.unwrap();
    let view = client.run(waiting.run_id).await.unwrap();
    assert_eq!(view.queued_because, Some(QueuedBecause::NoFreeWorker));
    assert!(view.reason.contains("every slot on queue default is busy"), "{}", view.reason);

    let unserved = client.start("on_an_unserved_queue", &()).send().await.unwrap();
    let view = client.run(unserved.run_id).await.unwrap();
    assert_eq!(view.queued_because, Some(QueuedBecause::NoFreeWorker));
    assert!(view.reason.contains("no running worker serves queue nobody_serves"), "{}", view.reason);

    let pause = PauseRequest::new("testing").until_resumed();
    client.as_caller().pause(PauseKind::Queue, "default", &pause).await.unwrap();
    let view = client.run(waiting.run_id).await.unwrap();
    assert_eq!(view.queued_because, Some(QueuedBecause::Paused), "{}", view.reason);
    assert!(view.reason.contains("testing"), "{}", view.reason);
}

#[derive(Debug, sqlx::FromRow)]
struct Claimed {
    state: String,
    fence: i64,
    lease_owner: Option<String>,
    lease_seconds: Option<f64>,
    function_started_at: Option<DateTime<Utc>>,
}

async fn claimed(database: &Database, run: Uuid) -> Claimed {
    sqlx::query_as(
        "SELECT state, fence, lease_owner, extract(epoch FROM lease_expires_at - now())::float8 AS lease_seconds,
                function_started_at
           FROM downbeat.run WHERE run_id = $1",
    )
    .bind(run)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn a_claim_marks_the_run_running_raises_its_fence_sets_its_lease_and_clears_its_start() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = client.start(echo, Text { text: "claimed".into() }).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET function_started_at = now() - interval '1 hour' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    let before = claimed(&database, run).await;

    let jobs = vec!["echo".to_owned()];
    let (id, fence) = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap().unwrap();
    let after = claimed(&database, run).await;
    assert_eq!(id, run);
    assert_eq!(after.state, "running");
    assert_eq!((after.fence, fence), (before.fence + 1, before.fence + 1), "the fence was raised");
    assert_eq!(after.lease_owner.as_deref(), Some("measure"));
    let lease = after.lease_seconds.unwrap_or_default();
    assert!((170.0..=180.0).contains(&lease), "the lease is now + 180 s: {lease}");
    assert_eq!(after.function_started_at, None);
}

#[tokio::test]
async fn a_claim_skips_a_run_whose_queue_is_paused_or_whose_cancel_was_requested() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let jobs = vec!["echo".to_owned()];
    let run = client.start(echo, Text { text: "held".into() }).await.unwrap().run_id();

    sqlx::query("INSERT INTO downbeat.pause (kind, name, paused_by, note) VALUES ('queue', 'default', 'test', 'held')")
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap(), None, "a queue pause");
    database.release().await;

    sqlx::query("UPDATE downbeat.run SET cancel_requested_by = 'someone' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    assert_eq!(downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap(), None, "a pending cancel");

    sqlx::query("UPDATE downbeat.run SET cancel_requested_by = NULL WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    let claimed = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap();
    assert_eq!(claimed.map(|(id, _)| id), Some(run), "with neither, the run is claimed");
}

#[tokio::test]
async fn a_decision_that_cannot_be_written_is_tried_three_times_a_second_apart_then_left_to_the_reaper() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE SEQUENCE public.history_insert_tries",
        "CREATE FUNCTION public.fail_three_times() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.job_name = 'echo' AND nextval('public.history_insert_tries') <= 3 THEN
                 RAISE EXCEPTION 'refused on purpose';
             END IF;
             RETURN NEW;
         END $$",
        "CREATE TRIGGER fail_three_times BEFORE INSERT ON downbeat.run_history
             FOR EACH ROW EXECUTE FUNCTION public.fail_three_times()",
    ] {
        sqlx::query(statement).execute(&database.pool).await.unwrap();
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let run = client.start(echo, Text { text: "written by the second worker call".into() }).await.unwrap();

    // A sequence is not rolled back with the failed insert, so watching it times each try.
    let mut tries: Vec<Instant> = Vec::new();
    let deadline = Instant::now() + SOON;
    while tries.len() < 3 {
        assert!(Instant::now() < deadline, "only {} tries were seen", tries.len());
        let (value, called): (i64, bool) =
            sqlx::query_as("SELECT last_value, is_called FROM public.history_insert_tries")
                .fetch_one(&database.pool)
                .await
                .unwrap();
        let seen = if called { usize::try_from(value).unwrap() } else { 0 };
        while tries.len() < seen.min(3) {
            tries.push(Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for pair in tries.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(gap >= Duration::from_millis(800) && gap <= Duration::from_millis(2000), "tries were {gap:?} apart");
    }

    let done = finished(&database.pool, run.run_id(), Duration::from_secs(40)).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.lost_count, 1, "the reaper took the run back after the worker gave up");
    assert_eq!(done.starts, 2, "the run was called again after the reaper took it back");
    let last: i64 = sqlx::query_scalar("SELECT last_value FROM public.history_insert_tries")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(last, 4, "the worker stopped after three tries; the fourth insert is the second call's");
}
