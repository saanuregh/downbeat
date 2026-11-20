//! More of a run's life: the status the API shows, which workers count as serving a queue, how
//! preparing is bounded and fenced, what the function-start write does, which writes a worker's
//! ownership guards, how the backoff doubles, and the progress a heartbeat saves.

mod support;

use std::time::Duration;

use downbeat::InAppClient;
use downbeat_client::types::StartResponse;
use downbeat_e2e::runtime_rules::*;
use downbeat_e2e::*;
use serde_json::json;
use support::{App, Database, SOON, eventually, finished, with_api, with_api_at};
use uuid::Uuid;

async fn execute(database: &Database, statement: &str) {
    sqlx::query(statement).execute(&database.pool).await.unwrap();
}

#[tokio::test]
async fn the_api_shows_a_queued_run_as_queued_and_a_running_run_as_running() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    database.hold("naps").await;

    let run = client.start("naps", &Number { n: 4000 }).send().await.unwrap().run_id;
    assert_eq!(client.run(run).await.unwrap().status, "queued");

    database.release().await;
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND starts > 0)"),
        SOON,
    )
    .await;
    assert_eq!(client.run(run).await.unwrap().status, "running");
    finished(&database.pool, run, SOON).await;
    assert_eq!(client.run(run).await.unwrap().status, "succeeded");
}

#[tokio::test]
async fn only_a_worker_seen_in_the_last_minute_counts_as_serving_a_queue() {
    let database = Database::fresh().await;
    let (_app, client) = with_api(&database).await;
    execute(
        &database,
        "INSERT INTO downbeat.worker (worker_id, role, queues, build, last_seen_at)
         VALUES ('ghost', 'worker', '{\"nobody_serves\": 1}', 'old', now() - interval '2 minutes')",
    )
    .await;

    let run = client.start("on_an_unserved_queue", &()).send().await.unwrap().run_id;
    let view = client.run(run).await.unwrap();
    assert_eq!(view.queued_because.as_deref(), Some("no_free_worker"));
    assert!(view.reason.contains("no running worker serves queue nobody_serves"), "{}", view.reason);

    execute(&database, "UPDATE downbeat.worker SET last_seen_at = now() WHERE worker_id = 'ghost'").await;
    let view = client.run(run).await.unwrap();
    assert!(view.reason.contains("every slot on queue nobody_serves is busy"), "{}", view.reason);
}

#[tokio::test]
async fn preparing_is_bounded_by_the_timeout_a_person_set() {
    let database = Database::fresh().await;
    let (_app, client, _) = with_api_at(&database).await;

    // Its condition takes five seconds, well within the job's own timeout.
    let body = json!({ "input": { "text": "in a hurry" }, "timeout_seconds": 1 });
    let started: StartResponse =
        client.as_caller().post("/v1/jobs/slow_to_prepare/runs", Some(&body), None).await.unwrap();
    let done = finished(&database.pool, started.run_id, SOON).await;
    assert_eq!(done.failed_because.as_deref(), Some("timeout"), "{}", done.reason);
    assert!(done.reason.contains("preparing"), "{}", done.reason);
    assert_eq!(done.starts, 0, "the function was never called");
}

#[tokio::test]
async fn nothing_is_written_for_a_run_whose_fence_was_raised_while_it_was_prepared() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let run = client.start(slow_to_prepare, Text { text: "fenced".into() }).await.unwrap().run_id();
    let _app = App::start(&database);
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND state = 'running')"),
        SOON,
    )
    .await;

    // Marked, so a prepared write would show; the reaper leaves the run alone for 14 s.
    sqlx::query(
        "UPDATE downbeat.run SET fence = fence + 1, input_hash = 'untouched', turn_key = 'untouched' WHERE run_id = $1",
    )
    .bind(run)
    .execute(&database.pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;

    let (input_hash, turn_key, starts): (Option<String>, Option<String>, i32) =
        sqlx::query_as("SELECT input_hash, turn_key, starts FROM downbeat.run WHERE run_id = $1")
            .bind(run)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(input_hash.as_deref(), Some("untouched"));
    assert_eq!(turn_key.as_deref(), Some("untouched"));
    assert_eq!(starts, 0, "the function was not started");
    let ended: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run_history WHERE run_id = $1")
        .bind(run)
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(ended, 0, "the run was not ended");
}

#[tokio::test]
async fn a_run_whose_turn_was_taken_is_prepared_again_on_its_next_claim() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let base = std::env::temp_dir().join(format!("turns-{}", Uuid::new_v4())).display().to_string();

    // Linked runs have no turn key until prepared, so both are claimed and one finds its turn
    // taken when its function would start.
    for side in ["a", "b"] {
        client.start(relays, Text { text: format!("{base}-{side}") }).await.unwrap();
    }
    eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = 'takes_turns_slowly' AND queued_because = 'waiting_turn')",
        SOON,
    )
    .await;
    let (waiting, text): (Uuid, String) = sqlx::query_as(
        "SELECT run_id, input->>'text' FROM downbeat.run
          WHERE job_name = 'takes_turns_slowly' AND queued_because = 'waiting_turn'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();

    let done = finished(&database.pool, waiting, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    let tested = std::fs::read_to_string(format!("{text}.conditions")).unwrap().lines().count();
    for side in ["a", "b"] {
        let _ = std::fs::remove_file(format!("{base}-{side}.conditions"));
    }
    assert!(tested >= 2, "the run that waited its turn was prepared {tested} time(s)");
}

#[tokio::test]
async fn the_function_start_write_renews_the_lease() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE TABLE public.function_starts (lease_expires_at timestamptz, earlier_lease timestamptz, at timestamptz)",
        "CREATE FUNCTION public.record_function_start() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO public.function_starts VALUES (NEW.lease_expires_at, OLD.lease_expires_at, now());
             RETURN NEW;
         END $$",
        "CREATE TRIGGER record_function_start BEFORE UPDATE ON downbeat.run
             FOR EACH ROW WHEN (NEW.starts > OLD.starts) EXECUTE FUNCTION public.record_function_start()",
    ] {
        execute(&database, statement).await;
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(echo, Text { text: "leased".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");
    let (lease_seconds, renewed): (f64, bool) = sqlx::query_as(
        "SELECT extract(epoch FROM lease_expires_at - at)::float8, lease_expires_at > earlier_lease
           FROM public.function_starts",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!((lease_seconds - 3.0).abs() < 0.001, "the write set the lease to now + {lease_seconds} s, not now + 3 s");
    assert!(renewed, "the lease moved forward");
}

#[tokio::test]
async fn a_worker_write_fails_when_the_lease_owner_or_the_state_changed() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    client.start(echo, Text { text: "held".into() }).await.unwrap();
    let jobs = vec!["echo".to_owned()];
    let (run, fence) = downbeat::measure::claim(&database.pool, "default", &jobs).await.unwrap().unwrap();
    let set = async |assignment: &str| {
        sqlx::query(&format!("UPDATE downbeat.run SET {assignment} WHERE run_id = $1"))
            .bind(run)
            .execute(&database.pool)
            .await
            .unwrap();
    };
    let beats = || downbeat::measure::heartbeat(&database.pool, run, fence);

    assert!(beats().await.unwrap(), "the worker holds the run");
    set("lease_owner = 'someone else'").await;
    assert!(!beats().await.unwrap(), "a heartbeat after the lease owner changed");
    set("lease_owner = 'measure'").await;
    set("state = 'queued'").await;
    assert!(!beats().await.unwrap(), "a heartbeat after the state changed");
    set("state = 'running'").await;
    assert!(beats().await.unwrap(), "with both back, the worker holds the run again");
}

#[tokio::test]
async fn the_ending_matches_on_id_and_fence_only() {
    let database = Database::fresh().await;
    // When the function starts, the run's lease owner or state changes but its fence stays. The
    // echo returns at once, before its next heartbeat would notice.
    for statement in [
        "CREATE FUNCTION public.change_hands_quietly() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.input->>'text' = 'owner' THEN
                 NEW.lease_owner := 'someone else';
             ELSIF NEW.input->>'text' = 'state' THEN
                 NEW.state := 'queued';
                 NEW.run_at := now() + interval '1 hour';
             END IF;
             RETURN NEW;
         END $$",
        "CREATE TRIGGER change_hands_quietly BEFORE UPDATE ON downbeat.run
             FOR EACH ROW WHEN (NEW.starts > OLD.starts) EXECUTE FUNCTION public.change_hands_quietly()",
    ] {
        execute(&database, statement).await;
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    for changed in ["owner", "state"] {
        let run = client.start(echo, Text { text: changed.into() }).await.unwrap().run_id();
        let done = finished(&database.pool, run, SOON).await;
        assert_eq!(done.status, "succeeded", "{changed}: {}", done.reason);
        assert_eq!((done.starts, done.lost_count), (1, 0), "{changed}: the worker's own ending was written");
    }
}

#[tokio::test]
async fn the_backoff_the_worker_applies_doubles_from_one_attempt_to_the_next() {
    let database = Database::fresh().await;
    for statement in [
        "CREATE TABLE public.backoffs (failures int2, delay float8)",
        "CREATE FUNCTION public.record_backoff() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO public.backoffs VALUES (NEW.failures, extract(epoch FROM NEW.run_at - now()));
             RETURN NEW;
         END $$",
        "CREATE TRIGGER record_backoff BEFORE UPDATE ON downbeat.run
             FOR EACH ROW WHEN (NEW.failures > OLD.failures) EXECUTE FUNCTION public.record_backoff()",
    ] {
        execute(&database, statement).await;
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(doubles_its_wait, ()).await.unwrap().run_id();
    let done = finished(&database.pool, run, SOON).await;
    assert_eq!((done.status.as_str(), done.starts), ("failed", 3), "{}", done.reason);
    let delays: Vec<f64> = sqlx::query_scalar("SELECT delay FROM public.backoffs ORDER BY failures")
        .fetch_all(&database.pool)
        .await
        .unwrap();
    let [first, second] = delays[..] else { panic!("two retries were expected: {delays:?}") };
    assert!((0.8..=1.5).contains(&first), "the first retry waited {first} s");
    let ratio = second / first;
    assert!((1.6..=2.4).contains(&ratio), "the second retry waited {second} s, {ratio} times the first");
}

#[tokio::test]
async fn a_heartbeat_saves_the_progress_the_function_reported() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(reports_progress, Text { text: "halfway there".into() }).await.unwrap().run_id();
    eventually(
        &database.pool,
        &format!("SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE run_id = '{run}' AND progress = 'halfway there')"),
        SOON,
    )
    .await;
    assert_eq!(finished(&database.pool, run, SOON).await.status, "succeeded");
}
