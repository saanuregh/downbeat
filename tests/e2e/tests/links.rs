//! Links that join several parents: the child is created once, from every parent.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, eventually};

const CHAINS: usize = 8;

#[tokio::test]
async fn two_parents_ending_together_create_their_child_once() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::with(&database, &["scheduler", "worker", "--queues", "default=8"], "test");

    let mut roots = Vec::new();
    for index in 0..CHAINS {
        roots.push(client.start(hands_on, Text { text: format!("Voice {index}") }).await.unwrap().run_id());
    }
    eventually(
        &database.pool,
        &format!(
            "SELECT count(*) = {CHAINS} FROM downbeat.run_history WHERE job_name = 'chorus'
              AND NOT EXISTS (SELECT 1 FROM downbeat.run WHERE job_name IN ('sings_loud', 'sings_soft', 'chorus'))"
        ),
        SOON,
    )
    .await;

    for (index, root) in roots.iter().enumerate() {
        let choruses: Vec<(Option<serde_json::Value>, i64)> = sqlx::query_as(
            "SELECT h.output, cardinality(h.parents)::bigint FROM downbeat.run_history h
              WHERE h.job_name = 'chorus'
                AND h.chain_id = (SELECT chain_id FROM downbeat.run_history WHERE run_id = $1)",
        )
        .bind(root)
        .fetch_all(&database.pool)
        .await
        .unwrap();
        assert_eq!(choruses.len(), 1, "chain {index} has exactly one chorus: {choruses:?}");
        let (output, parents) = &choruses[0];
        assert_eq!(*output, Some(serde_json::json!({ "text": format!("VOICE {index} voice {index}") })));
        assert_eq!(*parents, 2, "the chorus names both singers as its parents");
    }
}
