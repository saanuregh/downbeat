//! A start being set up, and sending it with safe retries.

use std::time::Duration;

use downbeat_api::types::{Preview, StartRequest, StartResponse};
use uuid::Uuid;

use crate::Error;
use crate::client::ActingClient;
use crate::paths_and_answers::path;

/// How many times a start is tried when the scheduler cannot be reached or is busy.
const START_TRIES: u32 = 4;

/// A pending start; send it with [`Start::send`].
#[must_use = "a start does nothing until it is sent"]
#[derive(Debug)]
pub struct Start<'a> {
    pub(crate) client: ActingClient<'a>,
    pub(crate) job: String,
    pub(crate) request: Result<StartRequest, Error>,
    pub(crate) key: Option<String>,
}

impl Start<'_> {
    /// The run's priority in its queue; higher runs first.
    pub fn priority(mut self, priority: i16) -> Self {
        if let Ok(request) = &mut self.request {
            request.priority = Some(priority);
        }
        self
    }

    /// Create a run even when the same work is already queued.
    pub fn even_if_queued(mut self) -> Self {
        if let Ok(request) = &mut self.request {
            request.even_if_queued = true;
        }
        self
    }

    /// Ask the job to do a dry run, for a job that supports one.
    pub fn dry_run(mut self) -> Self {
        if let Ok(request) = &mut self.request {
            request.dry_run = true;
        }
        self
    }

    /// The idempotency key: repeating the request with it returns the first run. When not given,
    /// one is generated, so the client's own retries never start a second run.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sends the start, retrying with the same key when the scheduler cannot be reached or is busy.
    ///
    /// # Errors
    ///
    /// Fails when the input does not serialise to JSON, the scheduler cannot be reached, or it
    /// answers with an error.
    pub async fn send(self) -> Result<StartResponse, Error> {
        let key = self.key.unwrap_or_else(|| Uuid::new_v4().to_string());
        let request = self.request?;
        let path = path(&["v1", "jobs", &self.job, "runs"]);
        let mut tries = 0;
        loop {
            tries += 1;
            let result = self.client.post::<_, StartResponse>(&path, Some(&request), Some(&key)).await;
            match result {
                Err(error) if error.is_transient() && tries < START_TRIES => {
                    tokio::time::sleep(Duration::from_millis(250 * 2_u64.pow(tries))).await;
                }
                other => return other,
            }
        }
    }

    /// What the start would do, changing nothing.
    ///
    /// # Errors
    ///
    /// Fails when the input does not serialise to JSON, the scheduler cannot be reached, or it
    /// answers with an error.
    pub async fn preview(self) -> Result<Preview, Error> {
        let request = self.request?;
        let path = format!("{}?preview=true", path(&["v1", "jobs", &self.job, "runs"]));
        self.client.post(&path, Some(&request), None).await
    }
}
