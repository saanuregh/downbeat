//! The run lifecycle: preparing a run, taking its turn, and writing its decision.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually, finished};
use uuid::Uuid;

#[tokio::test]
async fn a_timeout_while_a_run_is_prepared_gets_another_attempt() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    let marker = std::env::temp_dir().join(format!("prepared-{}", Uuid::new_v4()));

    let run = client.start(times_out_while_prepared, Text { text: marker.display().to_string() }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    let _ = std::fs::remove_file(&marker);
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.failures, 1, "the timeout counted as a failed attempt");
}

#[tokio::test]
async fn a_run_whose_turn_is_taken_waits_uncounted_and_never_overlaps() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    // A linked run has no turn key until it is prepared, so both are claimed and one finds its
    // turn taken only when its function would start.
    for text in ["first", "second"] {
        client.start(lines_up, Text { text: text.into() }).await.unwrap();
    }
    eventually(
        &database.pool,
        "SELECT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name = 'in_turn' AND queued_because = 'waiting_turn')",
        SOON,
    )
    .await;
    let waiting: Uuid = sqlx::query_scalar(
        "SELECT run_id FROM downbeat.run WHERE job_name = 'in_turn' AND queued_because = 'waiting_turn'",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    let (failures, run_at_in_past): (i16, bool) =
        sqlx::query_as("SELECT failures, run_at <= now() FROM downbeat.run WHERE run_id = $1")
            .bind(waiting)
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(failures, 0, "a taken turn is not a failure");
    assert!(run_at_in_past, "a run waiting its turn goes back with no delay");

    let done = finished(&database.pool, waiting, SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!((done.failures, done.starts, done.interruptions), (0, 1, 0), "nothing was counted");
    eventually(
        &database.pool,
        "SELECT count(*) = 2 FROM downbeat.run_history WHERE job_name = 'in_turn' AND status = 'succeeded'",
        SOON,
    )
    .await;
    let overlap: bool = sqlx::query_scalar(
        "SELECT a.started_at < b.finished_at AND b.started_at < a.finished_at
           FROM downbeat.run_history a, downbeat.run_history b
          WHERE a.job_name = 'in_turn' AND b.job_name = 'in_turn' AND a.run_id < b.run_id",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert!(!overlap, "the two runs of a one-at-a-time job ran at the same time");
}

#[tokio::test]
async fn a_failed_write_of_a_decision_is_tried_again() {
    let database = Database::fresh().await;
    // A sequence counts the tries, because a counter row written in the failing transaction would
    // be rolled back with it.
    for statement in [
        "CREATE SEQUENCE public.history_insert_tries",
        "CREATE FUNCTION public.fail_twice() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.job_name = 'echo' AND nextval('public.history_insert_tries') <= 2 THEN
                 RAISE EXCEPTION 'refused on purpose';
             END IF;
             RETURN NEW;
         END $$",
        "CREATE TRIGGER fail_twice BEFORE INSERT ON downbeat.run_history
             FOR EACH ROW EXECUTE FUNCTION public.fail_twice()",
    ] {
        sqlx::query(statement).execute(&database.pool).await.unwrap();
    }
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let run = client.start(echo, Text { text: "written at last".into() }).await.unwrap();
    let done = finished(&database.pool, run.run_id(), SOON).await;
    assert_eq!(done.status, "succeeded", "{}", done.reason);
    assert_eq!(done.lost_count, 0, "the worker wrote the ending itself, not the reaper");
    let tries: i64 = sqlx::query_scalar("SELECT last_value FROM public.history_insert_tries")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(tries, 3, "the history row was inserted on the third try");
}
