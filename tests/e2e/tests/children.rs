//! What a linked child takes: its own queue and priority, never joining, its input through the
//! link's adapter, and only the children that have not run after a retry. Also that joined rows
//! stay out of the chain view and "latest run".

mod support;

use std::collections::HashSet;

use downbeat::InAppClient;
use downbeat_client::types::Done;
use downbeat_e2e::*;
use serde_json::json;
use support::{App, Database, SOON, eventually, finished, finished_job, with_api, with_api_in};
use uuid::Uuid;

async fn chain_of(database: &Database, run_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "SELECT chain_id FROM downbeat.run WHERE run_id = $1
         UNION ALL SELECT chain_id FROM downbeat.run_history WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&database.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn joined_rows_are_hidden_from_the_chain_view_and_the_latest_run() {
    let database = Database::fresh().await;
    database.hold("ticker").await;
    let (_app, client, _) = with_api_in(&database, "schedules").await;
    eventually(
        &database.pool,
        "SELECT count(*) >= 2 FROM downbeat.run_history WHERE job_name = 'ticker' AND joined_run IS NOT NULL",
        SOON,
    )
    .await;
    let held: Uuid = sqlx::query_scalar("SELECT run_id FROM downbeat.run WHERE job_name = 'ticker'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    let joined: HashSet<Uuid> = sqlx::query_scalar(
        "SELECT run_id FROM downbeat.run_history WHERE job_name = 'ticker' AND joined_run IS NOT NULL",
    )
    .fetch_all(&database.pool)
    .await
    .unwrap()
    .into_iter()
    .collect();

    let chain = client.chain(chain_of(&database, held).await).await.unwrap();
    let shown: Vec<Uuid> = chain.nodes.iter().flat_map(|node| node.runs.iter().map(|run| run.run_id)).collect();
    assert!(shown.contains(&held), "the held run is in its chain: {chain:?}");
    assert!(shown.iter().all(|run_id| !joined.contains(run_id)), "no joined row in the chain: {chain:?}");

    let job = client.job("ticker").await.unwrap();
    if let Some(last) = &job.last_run {
        assert!(!joined.contains(&last.run_id), "the latest run is not a joined row: {last:?}");
    }
}

#[tokio::test]
async fn a_child_takes_its_own_queue_and_priority_and_never_joins() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let input = Text { text: "own line".into() };
    let alone = client.start(on_its_own_queue, input.clone()).await.unwrap().run_id();
    let _app = App::start(&database);

    let parent = client.start(starts_line, input).priority(9).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, parent, SOON).await.status, "succeeded");
    let chain = chain_of(&database, parent).await;
    let child: (Uuid, String, i16) = sqlx::query_as(
        "SELECT run_id, queue, priority FROM downbeat.run WHERE job_name = 'on_its_own_queue' AND chain_id = $1",
    )
    .bind(chain)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_ne!(child.0, alone, "the child is a run of its own, not the one already queued");
    assert_eq!((child.1.as_str(), child.2), ("quick", 5));
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM downbeat.run WHERE job_name = 'on_its_own_queue'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(queued, 2);
}

#[tokio::test]
async fn a_child_receives_what_the_map_adapter_made() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);
    client.start(starts_line, Text { text: "seven!!".into() }).await.unwrap();

    let counted = finished_job(&database.pool, "counts_letters", SOON).await;
    assert_eq!(counted.status, "succeeded", "{counted:?}");
    assert_eq!(counted.output, Some(json!({ "n": 7 })));
}

#[tokio::test]
async fn a_parent_output_that_no_longer_fits_fails_the_child_as_bad_input() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    database.hold("counts_letters").await;
    let _app = App::start(&database);
    let parent = client.start(starts_line, Text { text: "soon broken".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, parent, SOON).await.status, "succeeded");
    sqlx::query(r#"UPDATE downbeat.run_history SET output = '{"nope": 1}' WHERE run_id = $1"#)
        .bind(parent)
        .execute(&database.pool)
        .await
        .unwrap();
    database.release().await;

    let counted = finished_job(&database.pool, "counts_letters", SOON).await;
    assert_eq!(counted.status, "failed", "{counted:?}");
    assert_eq!(counted.failed_because.as_deref(), Some("bad_input"));
}

#[tokio::test]
async fn after_a_retry_only_children_that_have_not_run_follow() {
    let database = Database::fresh().await;
    let product = InAppClient::connect(definition, &database.url).await.unwrap();
    let (_app, client) = with_api(&database).await;
    let parent = product.start(starts_line, Text { text: "again".into() }).await.unwrap().run_id();
    let counted = finished_job(&database.pool, "counts_letters", SOON).await;
    assert_eq!(counted.status, "succeeded", "{counted:?}");
    let chain = chain_of(&database, parent).await;

    let _: Done = client.as_caller().post(&format!("/v1/runs/{parent}/retry"), None::<&()>, None).await.unwrap();
    eventually(
        &database.pool,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM downbeat.run_history WHERE job_name = 'starts_line'
               AND chain_id = '{chain}' AND run_id <> '{parent}' AND status = 'succeeded')"
        ),
        SOON,
    )
    .await;
    let letters: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM downbeat.run WHERE job_name = 'counts_letters' AND chain_id = $1)
              + (SELECT count(*) FROM downbeat.run_history WHERE job_name = 'counts_letters' AND chain_id = $1)",
    )
    .bind(chain)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(letters, 1, "the child that ran is not created again");
}
