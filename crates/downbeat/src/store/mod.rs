//! The store: every statement the engine runs against its tables, in the `downbeat` schema.
//!
//! The rest of the engine decides what should happen; this module makes it so in the database.
//! Every transaction runs at READ COMMITTED, and every time comparison uses the database's `now()`.

mod migrate;

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

pub(crate) use migrate::migrate;

/// The engine's connection to its database.
#[derive(Debug, Clone)]
pub(crate) struct Store {
    pub(crate) pool: PgPool,
}

impl Store {
    /// Connects, and brings the schema up to date.
    pub(crate) async fn connect(url: &str, connections: u32) -> Result<Store, crate::Error> {
        let options: PgConnectOptions = url.parse()?;
        let pool = PgPoolOptions::new()
            .max_connections(connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options.application_name("downbeat").options([("statement_timeout", "60s")]))
            .await?;
        migrate(&pool).await?;
        Ok(Store { pool })
    }
}

/// A Postgres interval from a duration.
pub(crate) fn interval(duration: Duration) -> sqlx::postgres::types::PgInterval {
    sqlx::postgres::types::PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
    }
}

pub(crate) mod audit;
pub(crate) mod cancel;
pub(crate) mod ending;
pub(crate) mod history;
pub(crate) mod runs;
