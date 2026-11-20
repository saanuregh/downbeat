//! The console's database: people, grants, sessions, sign-ins in progress, its audit and
//! preferences, in schema `console`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const MIGRATIONS: &[(i32, &str)] = &[(1, include_str!("../migrations/0001_initial.sql"))];
const MIGRATION_LOCK: i64 = 0x636f_6e73_6f6c_6500; // "console"

/// Connects and brings the schema up to date. Refuses a schema newer than this console knows.
pub(crate) async fn connect(url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new().max_connections(10).acquire_timeout(Duration::from_secs(10)).connect(url).await?;
    let mut connection = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)").bind(MIGRATION_LOCK).execute(&mut *connection).await?;
    let result = migrate(&mut connection).await;
    sqlx::query("SELECT pg_advisory_unlock($1)").bind(MIGRATION_LOCK).execute(&mut *connection).await?;
    result?;
    drop(connection);
    Ok(pool)
}

async fn migrate(connection: &mut sqlx::PgConnection) -> anyhow::Result<()> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS console").execute(&mut *connection).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS console.migration (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        .execute(&mut *connection)
        .await?;
    let applied: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM console.migration").fetch_all(&mut *connection).await?;
    let known = MIGRATIONS.last().map_or(0, |(version, _)| *version);
    if let Some(newer) = applied.iter().find(|version| **version > known) {
        anyhow::bail!(
            "the console database is at schema {newer}, newer than this console knows ({known}); deploy a newer console"
        );
    }
    for (version, sql) in MIGRATIONS.iter().filter(|(version, _)| !applied.contains(version)) {
        let mut transaction = sqlx::Connection::begin(&mut *connection).await?;
        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO console.migration (version) VALUES ($1)")
            .bind(version)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
    }
    Ok(())
}

/// A person the console has seen.
#[derive(Debug, Clone, sqlx::FromRow)]
#[allow(clippy::struct_field_names)] // `person_id` is the column's name.
pub(crate) struct Person {
    pub(crate) person_id: i64,
    pub(crate) email: String,
    pub(crate) name: String,
    pub(crate) groups: Vec<String>,
}

/// Records a sign-in: the person, their name, email and groups as the provider reported them.
pub(crate) async fn upsert_person(
    pool: &PgPool,
    subject: &str,
    email: &str,
    name: &str,
    groups: &[String],
) -> sqlx::Result<Person> {
    sqlx::query_as(
        "INSERT INTO console.person (subject, email, name, groups) VALUES ($1, $2, $3, $4)
         ON CONFLICT (subject) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name, groups = EXCLUDED.groups, last_seen_at = now()
         RETURNING person_id, email, name, groups",
    )
    .bind(subject)
    .bind(email)
    .bind(name)
    .bind(groups)
    .fetch_one(pool)
    .await
}

pub(crate) async fn audit(pool: &PgPool, actor: &str, action: &str, detail: Value) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO console.audit (actor, action, detail) VALUES ($1, $2, $3)")
        .bind(actor)
        .bind(action)
        .bind(detail)
        .execute(pool)
        .await?;
    Ok(())
}

/// Removes what has expired: sessions, and sign-ins never completed.
pub(crate) async fn prune(pool: &PgPool, idle: Duration) -> sqlx::Result<()> {
    let idle = sqlx::postgres::types::PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(idle.as_micros()).unwrap_or(i64::MAX),
    };
    sqlx::query("DELETE FROM console.session WHERE expires_at < now() OR last_used_at < now() - $1")
        .bind(idle)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM console.sign_in WHERE created_at < now() - interval '10 minutes'").execute(pool).await?;
    sqlx::query("DELETE FROM console.audit WHERE at < now() - interval '1 year'").execute(pool).await?;
    Ok(())
}

/// A grant, as stored, with the person's email for display.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
#[allow(clippy::struct_field_names)] // `grant_id` is the column's name.
pub(crate) struct Grant {
    pub(crate) grant_id: i64,
    pub(crate) to_person: Option<String>,
    pub(crate) to_group: Option<String>,
    pub(crate) scheduler: String,
    pub(crate) scope: String,
    pub(crate) pipelines: Option<Vec<String>>,
    pub(crate) created_by: String,
    pub(crate) created_at: DateTime<Utc>,
}

pub(crate) async fn grants(pool: &PgPool) -> sqlx::Result<Vec<Grant>> {
    sqlx::query_as(
        "SELECT g.grant_id, p.email AS to_person, g.to_group, g.scheduler, g.scope, g.pipelines, g.created_by, g.created_at
           FROM console.grant g LEFT JOIN console.person p ON p.person_id = g.to_person
          ORDER BY g.scheduler, g.grant_id",
    )
    .fetch_all(pool)
    .await
}

/// The grants that apply to a person, directly or through a group.
pub(crate) async fn grants_of(pool: &PgPool, person: &Person) -> sqlx::Result<Vec<Grant>> {
    sqlx::query_as(
        "SELECT g.grant_id, p.email AS to_person, g.to_group, g.scheduler, g.scope, g.pipelines, g.created_by, g.created_at
           FROM console.grant g LEFT JOIN console.person p ON p.person_id = g.to_person
          WHERE g.to_person = $1 OR g.to_group = ANY($2)",
    )
    .bind(person.person_id)
    .bind(&person.groups)
    .fetch_all(pool)
    .await
}
