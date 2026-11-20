//! A dependency that cannot be built fails the call that needs it, and nothing else.

mod support;

use downbeat::InAppClient;
use downbeat_e2e::*;
use support::{App, Database, SOON, finished};

#[tokio::test]
async fn a_dependency_that_cannot_be_built_fails_the_call_as_an_error() {
    let database = Database::fresh().await;
    let client = InAppClient::connect(definition, &database.url).await.unwrap();
    let _app = App::start(&database);

    let needs = client.start(needs_the_warehouse, ()).await.unwrap().run_id();
    let done = finished(&database.pool, needs, SOON).await;
    assert_eq!((done.status.as_str(), done.failed_because.as_deref()), ("failed", Some("error")));
    assert!(done.reason.contains("the warehouse is closed"), "{}", done.reason);

    let other = client.start(echo, Text { text: "unaffected".into() }).await.unwrap().run_id();
    assert_eq!(finished(&database.pool, other, SOON).await.status, "succeeded", "other jobs still run");
}
