//! `downbeat::migrate` refuses a schema with a newer breaking migration, with an error a deploy
//! script can match on.

mod support;

use support::Database;

#[tokio::test]
async fn migrate_refuses_a_schema_with_a_newer_breaking_migration() {
    let database = Database::fresh().await;
    sqlx::query("INSERT INTO downbeat.migration (version, name, breaking) VALUES (999, 'from the future', true)")
        .execute(&database.pool)
        .await
        .unwrap();

    let error = downbeat::migrate(&database.url).await.unwrap_err();
    let downbeat::Error::NewerSchema { found, known } = error else { panic!("not a schema error: {error}") };
    assert_eq!(found, 999);
    assert!(known < found);
    assert!(error.to_string().contains("deploy a newer build"), "{error}");
}

#[tokio::test]
async fn migrate_reports_a_database_it_cannot_reach() {
    let error = downbeat::migrate("postgres://nobody@127.0.0.1:1/nothing").await.unwrap_err();
    let downbeat::Error::Database(database) = &error else { panic!("not a database error: {error}") };
    let source = std::error::Error::source(database).expect("the database library's error is kept");
    assert_eq!(source.to_string(), database.to_string());
}
