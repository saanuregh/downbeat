//! Parts of a run's life: a run not yet due is not claimed (spec 3.1), a
//! child that exits by itself is a crash (3.5), a program that cannot start is an error that is
//! retried (4.2), a wait goes back to the queue after `recheck_every` (3.5, 4.4), and a heartbeat
//! moves the lease forward and saves a pending checkpoint (9.1).

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

async fn execute(database: &Database, statement: &str) {
    sqlx::query(statement).execute(&database.pool).await.unwrap();
}

// 3.1: a queued run whose `run_at` is in the future is claimable only once it is due.
#[tokio::test]
async fn a_run_due_in_the_future_is_not_claimed_until_it_is_due() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = client.start(echo, Text { text: "later".into() }).await.unwrap().run_id();
    sqlx::query("UPDATE downbeat.run SET run_at = now() + interval '2 seconds' WHERE run_id = $1")
        .bind(run)
        .execute(&database.pool)
        .await
        .unwrap();
    let jobs = vec!["echo".to_owned()];

    let early = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap();
    assert_eq!(early, None, "a run not yet due is not claimed");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let due = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap();
    assert_eq!(due.map(|(claimed, _)| claimed), Some(run), "once due, it is claimed");
}

// 3.5: a child that exits with a status of its own while its function runs.
#[tokio::test]
async fn a_child_that_exits_by_itself_fails_the_run_as_a_crash() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(exits_by_itself, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("crash")), "{}", done.reason);
    assert_eq!(done.starts, 1);
}

// 4.2: a program that cannot start is a failure of kind `Error`, retried while attempts remain.
#[tokio::test]
async fn a_program_that_cannot_start_is_an_error_and_is_tried_again() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(program_is_missing, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    // A program that never started fails as an `error`, the kind the default `retry_on` lists, so
    // a second attempt is made; the reason says why it could not start.
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("error")), "{}", done.reason);
    assert_eq!((done.starts, done.failures), (2, 2), "both attempts were made: {}", done.reason);
    assert!(done.reason.starts_with("Failed: the program could not be started"), "{}", done.reason);
}

// 3.5 and 4.4: `not_ready` from a function, and exit 75 from a command, go back to the queue as
// `not_ready` after the job's `recheck_every` (two seconds), with nothing counted.
#[tokio::test]
async fn a_wait_goes_back_to_the_queue_for_recheck_every() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE TABLE public.waits (job text, delay float8, failures int2, state text)",
        "CREATE FUNCTION public.record_wait() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO public.waits
                 VALUES (NEW.job_name, extract(epoch FROM NEW.run_at - now()), NEW.failures, NEW.state);
             RETURN NEW;
         END $$",
        "CREATE TRIGGER record_wait BEFORE UPDATE ON downbeat.run
             FOR EACH ROW WHEN (NEW.queued_because = 'not_ready' AND OLD.state = 'running')
             EXECUTE FUNCTION public.record_wait()",
    ] {
        execute(&database, statement).await;
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let base = std::env::temp_dir().join(format!("waits-{}", Uuid::new_v4())).display().to_string();

    let function = client.start(not_ready_for_two_seconds, Text { text: format!("{base}-f") }).await.unwrap().run_id();
    let command =
        client.start(exits_not_ready_for_two_seconds, Text { text: format!("{base}-c") }).await.unwrap().run_id();
    eventually(
        &database.pool,
        "SELECT count(DISTINCT job) = 2 AND count(*) >= 4 FROM public.waits",
        Duration::from_secs(30),
    )
    .await;
    for side in ["f", "c"] {
        std::fs::write(format!("{base}-{side}"), "here").unwrap();
    }
    for run in [function, command] {
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!((done.status.as_str(), done.failures), ("succeeded", 0), "{}", done.reason);
    }
    for side in ["f", "c"] {
        std::fs::remove_file(format!("{base}-{side}")).unwrap();
    }

    let waits: Vec<(String, f64, i16, String)> =
        sqlx::query_as("SELECT job, delay, failures, state FROM public.waits").fetch_all(&database.pool).await.unwrap();
    for (job, delay, failures, state) in &waits {
        assert!((1.5..=2.6).contains(delay), "{job} waited {delay} s, not its two-second recheck_every");
        assert_eq!((*failures, state.as_str()), (0, "queued"), "{job}: back to the queue, nothing counted");
    }
}

// 9.1: every heartbeat moves the lease forward, and saves a checkpoint left with `run.checkpoint`.
#[tokio::test]
async fn a_heartbeat_moves_the_lease_forward_and_saves_the_pending_checkpoint() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE TABLE public.renewals (earlier timestamptz, later timestamptz)",
        "CREATE FUNCTION public.record_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO public.renewals VALUES (OLD.lease_expires_at, NEW.lease_expires_at);
             RETURN NEW;
         END $$",
        "CREATE TRIGGER record_renewal AFTER UPDATE ON downbeat.run FOR EACH ROW
             WHEN (OLD.state = 'running' AND NEW.state = 'running' AND OLD.fence = NEW.fence
                   AND OLD.starts = NEW.starts AND NEW.starts > 0)
             EXECUTE FUNCTION public.record_renewal()",
    ] {
        execute(&database, statement).await;
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(checkpoints_then_naps, Text { text: "left for later".into() }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'running'
               AND checkpoint = '\"left for later\"'::jsonb)"
        ),
        SOON,
    )
    .await;
    assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");

    let renewals: Vec<(f64,)> =
        sqlx::query_as("SELECT extract(epoch FROM later - earlier)::float8 FROM public.renewals")
            .fetch_all(&database.pool)
            .await
            .unwrap();
    assert!(renewals.len() >= 2, "a four-second run had {} heartbeats", renewals.len());
    for (moved,) in &renewals {
        assert!(*moved > 0.0, "a heartbeat moved the lease by {moved} s: {renewals:?}");
    }
}
