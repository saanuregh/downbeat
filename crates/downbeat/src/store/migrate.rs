//! The engine's own migrations: SQL files shipped in the crate, applied in order at start.
//!
//! Every migration is additive, so an engine one version older keeps working against a newer
//! schema during a rolling deploy. An engine refuses to start against a schema that has a
//! migration it does not know and that is marked as breaking.

use sqlx::PgPool;

struct Migration {
    version: i32,
    name: &'static str,
    sql: &'static str,
    /// Whether engines that do not know this migration must stop.
    breaking: bool,
}

const MIGRATIONS: &[Migration] = &[
    Migration { version: 1, name: "initial", sql: include_str!("../../migrations/0001_initial.sql"), breaking: false },
    Migration {
        version: 2,
        name: "global pause",
        sql: include_str!("../../migrations/0002_global_pause.sql"),
        breaking: false,
    },
    Migration {
        version: 3,
        name: "marked at",
        sql: include_str!("../../migrations/0003_marked_at.sql"),
        breaking: false,
    },
    Migration {
        version: 4,
        name: "day totals",
        sql: include_str!("../../migrations/0004_day_totals.sql"),
        breaking: false,
    },
];

/// Arbitrary, fixed: the advisory lock that serialises migrations across processes.
const MIGRATION_LOCK: i64 = 0x646f_776e_6265_6174; // "downbeat"

pub(crate) async fn migrate(pool: &PgPool) -> Result<(), crate::Error> {
    let mut connection = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)").bind(MIGRATION_LOCK).execute(&mut *connection).await?;
    let applied = apply(&mut connection).await;
    let unlocked = sqlx::query("SELECT pg_advisory_unlock($1)").bind(MIGRATION_LOCK).execute(&mut *connection).await;
    if let Err(error) = &unlocked {
        // Closing the session releases its lock, so a connection still holding it never returns
        // to the pool.
        connection.close_on_drop();
        if applied.is_err() {
            tracing::warn!(%error, "the migration lock could not be released after a migration failed");
        }
    }
    applied?;
    unlocked?;
    Ok(())
}

async fn apply(connection: &mut sqlx::PgConnection) -> Result<(), crate::Error> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS downbeat").execute(&mut *connection).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS downbeat.migration (
            version integer PRIMARY KEY,
            name text NOT NULL,
            breaking boolean NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )",
    )
    .execute(&mut *connection)
    .await?;

    let applied: Vec<(i32, bool)> =
        sqlx::query_as("SELECT version, breaking FROM downbeat.migration").fetch_all(&mut *connection).await?;
    let known_newest = MIGRATIONS.last().map_or(0, |migration| migration.version);
    if let Some((version, _)) = applied.iter().find(|(version, breaking)| *version > known_newest && *breaking) {
        return Err(crate::Error::NewerSchema { found: *version, known: known_newest });
    }

    for migration in
        MIGRATIONS.iter().filter(|migration| !applied.iter().any(|(version, _)| *version == migration.version))
    {
        let mut transaction = sqlx::Connection::begin(&mut *connection).await?;
        sqlx::raw_sql(migration.sql).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO downbeat.migration (version, name, breaking) VALUES ($1, $2, $3)")
            .bind(migration.version)
            .bind(migration.name)
            .bind(migration.breaking)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        tracing::info!(version = migration.version, name = migration.name, "applied a downbeat migration");
    }
    Ok(())
}
