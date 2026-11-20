//! `InAppClient`: starting jobs from code inside the product's binary, such as its admin server,
//! straight through the engine's tables. Programs outside the binary use the control API.

use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use sqlx::PgPool;

use crate::definition::Definition;
use crate::job::{Job, Started};
use crate::registry::Registry;
use crate::starting::{self, StartJob, StartOptions};
use crate::store::Store;
use crate::store::runs::StartedBy;

/// Starts jobs from inside the product's binary.
#[derive(Clone)]
pub struct InAppClient {
    pool: PgPool,
    registry: Arc<Registry>,
}

impl fmt::Debug for InAppClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InAppClient").finish_non_exhaustive()
    }
}

impl InAppClient {
    /// Connects to the product's database, where the engine's tables are.
    pub async fn connect(definition: fn() -> Definition, database_url: &str) -> anyhow::Result<InAppClient> {
        let environment = std::env::var(crate::child::ENVIRONMENT).ok();
        let registry = Registry::build(definition(), environment.as_deref())
            .map_err(|problems| anyhow::anyhow!("the definition has problems: {problems:?}"))?;
        let store = Store::connect(database_url, 4).await?;
        Ok(InAppClient { pool: store.pool, registry: Arc::new(registry) })
    }

    /// Start a job. Await the result, optionally after `.priority(…)`, `.even_if_queued()`,
    /// `.actor(…)` or `.idempotency_key(…)`.
    #[allow(clippy::needless_pass_by_value)] // By value, as `run.start` takes it.
    pub fn start<J: Job>(&self, _job: J, input: J::Input) -> InAppStart<'_> {
        InAppStart {
            client: self,
            job: J::NAME,
            input: serde_json::to_value(&input).map_err(|error| error.to_string()),
            options: StartOptions::default(),
            actor: None,
            key: None,
        }
    }
}

/// A pending start; await it.
#[must_use = "a start does nothing until it is awaited"]
pub struct InAppStart<'a> {
    client: &'a InAppClient,
    job: &'static str,
    input: Result<serde_json::Value, String>,
    options: StartOptions,
    actor: Option<String>,
    key: Option<String>,
}

impl fmt::Debug for InAppStart<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InAppStart").field("job", &self.job).finish_non_exhaustive()
    }
}

impl InAppStart<'_> {
    pub fn priority(mut self, priority: i16) -> Self {
        self.options.priority = Some(priority);
        self
    }

    /// Create a run even when the same work is already queued.
    pub fn even_if_queued(mut self) -> Self {
        self.options.even_if_queued = true;
        self
    }

    /// Who is asking, for the audit trail. Default: `app`.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// A key that makes repeating this request return the run it first produced.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

impl<'a> IntoFuture for InAppStart<'a> {
    type Output = anyhow::Result<Started>;
    type IntoFuture = Pin<Box<dyn Future<Output = anyhow::Result<Started>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let job = self
                .client
                .registry
                .job(self.job)
                .ok_or_else(|| anyhow::anyhow!("there is no job named {}", self.job))?;
            let ops = &job.spec.input_ops;
            let input = (ops.normalize)(self.input.map_err(anyhow::Error::msg)?, true).map_err(anyhow::Error::msg)?;
            let input_hash = (ops.hash)(&input).map_err(anyhow::Error::msg)?;
            let turn_value = ops.turn_value.map(|value_of| value_of(&input)).transpose().map_err(anyhow::Error::msg)?;
            let actor = self.actor.unwrap_or_else(|| "app".to_owned());
            let request = StartJob {
                job: self.job.to_owned(),
                input,
                input_hash,
                turn_value,
                options: self.options,
                started_by: StartedBy::Program,
                idempotency: self.key.map(|key| (actor.clone(), key)),
                actor: Some(actor),
            };
            Ok(starting::start(&self.client.pool, &self.client.registry, request).await?.started)
        })
    }
}
