//! The control API (filled in by the next milestone).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::registry::Registry;

pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
}

impl Config {
    pub(crate) fn load(listen: Option<SocketAddr>, _callers: Option<&Path>, _local: bool) -> anyhow::Result<Option<Config>> {
        Ok(listen.map(|listen| Config { listen }))
    }
}

pub(crate) async fn serve(_pool: PgPool, _registry: Arc<Registry>, _config: Config, _stop: CancellationToken) -> anyhow::Result<()> {
    Ok(())
}
