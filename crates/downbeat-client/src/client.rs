//! The client: a builder that sets up the secure connection, and one method per endpoint.

use std::time::Duration;

use downbeat_api::types::{
    ApiError, AuditEntry, BackfillRequest, BackfillView, CalendarDay, ChainView, Done, JobView, MarkSucceededRequest,
    OverrideRequest, OverrideView, Page, PauseRequest, PipelineView, Preview, QueueView, Report, RunView, ScheduleView,
    StartRequest, Version,
};
use downbeat_api::with_defaults;
use downbeat_api::{
    API_VERSION, Claims, DEVELOPMENT_AUDIENCE, IDEMPOTENCY_HEADER, JobName, PrivateKey, RequestBinding, Scope,
    development_key, sign,
};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{Error, Start};

/// Sets up a [`Client`].
#[derive(Debug)]
pub struct Builder {
    base: String,
    caller: Option<String>,
    audience: Option<String>,
    keys: Vec<PrivateKey>,
    ca: Option<reqwest::Certificate>,
    scope: Scope,
    jobs: Option<Vec<String>>,
    timeout: Duration,
}

impl Builder {
    /// The trusted caller this client signs as: its name in the scheduler's callers file.
    #[must_use]
    pub fn caller(mut self, name: &str) -> Self {
        self.caller = Some(name.to_owned());
        self
    }

    /// The scheduler's audience, as its callers file names it.
    #[must_use]
    pub fn audience(mut self, audience: &str) -> Self {
        self.audience = Some(audience.to_owned());
        self
    }

    /// A private key to sign with. Several may be added; the newest signs, so a key is rotated by
    /// adding the new one before the scheduler stops trusting the old.
    #[must_use]
    pub fn key(mut self, key: PrivateKey) -> Self {
        self.keys.push(key);
        self
    }

    /// The private key from a PKCS#8 PEM file.
    pub fn key_from_file(self, path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let pem = std::fs::read_to_string(path.as_ref())
            .map_err(|error| Error::Setup(format!("reading {}: {error}", path.as_ref().display())))?;
        Ok(self.key(PrivateKey::from_pem(&pem)?))
    }

    /// The private key from an environment variable holding PKCS#8 PEM.
    pub fn key_from_env(self, variable: &str) -> Result<Self, Error> {
        // The error's own text would quote a value that is not UTF-8, and this one is a secret.
        let pem = std::env::var(variable).map_err(|error| {
            Error::Setup(match error {
                std::env::VarError::NotPresent => format!("{variable} is not set"),
                std::env::VarError::NotUnicode(_) => format!("{variable} is not valid UTF-8"),
            })
        })?;
        Ok(self.key(PrivateKey::from_pem(&pem)?))
    }

    /// Trust this certificate authority (PEM) instead of the system's.
    pub fn certificate_authority(mut self, pem: &[u8]) -> Result<Self, Error> {
        self.ca = Some(reqwest::Certificate::from_pem(pem)?);
        Ok(self)
    }

    /// The most a token from this client asks for. Default: `operate`.
    #[must_use]
    pub fn scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    /// Limit every token to these jobs; required when the scheduler limits this caller to some.
    #[must_use]
    pub fn jobs<I: IntoIterator<Item = S>, S: Into<String>>(mut self, jobs: I) -> Self {
        self.jobs = Some(jobs.into_iter().map(Into::into).collect());
        self
    }

    /// How long one request may take. Default: 30 seconds.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// For a scheduler started with `--local`: the development key, the `local` audience, plain
    /// HTTP on this machine.
    #[must_use]
    pub fn local(mut self) -> Self {
        self.caller = Some("local".to_owned());
        self.audience = Some(DEVELOPMENT_AUDIENCE.to_owned());
        self.keys = vec![development_key()];
        self.scope = Scope::Administer;
        self
    }

    /// Checks the setup and the scheduler's API version.
    pub async fn connect(self) -> Result<Client, Error> {
        let base = Url::parse(&self.base).map_err(|error| Error::Setup(format!("{}: {error}", self.base)))?;
        let local = matches!(base.host_str(), Some("localhost" | "127.0.0.1" | "::1" | "[::1]"));
        if base.scheme() != "https" && !local {
            return Err(Error::Setup(
                "the scheduler's address must be https; plain http is only for a scheduler on this machine".into(),
            ));
        }
        let caller = self.caller.ok_or_else(|| Error::Setup("name the caller with .caller(…)".into()))?;
        let audience =
            self.audience.ok_or_else(|| Error::Setup("name the scheduler's audience with .audience(…)".into()))?;
        let key = self
            .keys
            .last()
            .cloned()
            .ok_or_else(|| Error::Setup("add a key with .key(…), .key_from_file(…) or .key_from_env(…)".into()))?;
        let mut http = reqwest::Client::builder()
            .timeout(self.timeout)
            .https_only(!local)
            .user_agent(concat!("downbeat-client/", env!("CARGO_PKG_VERSION")));
        if let Some(ca) = self.ca {
            http = http.tls_certs_only([ca]);
        }
        let client = Client { http: http.build()?, base, caller, audience, key, scope: self.scope, jobs: self.jobs };
        let version: Version = client.unsigned_get("/version").await?;
        let supported = vec![API_VERSION.saturating_sub(1), API_VERSION];
        if !supported.contains(&version.api) {
            return Err(Error::Version { spoken: version.api, supported });
        }
        Ok(client)
    }
}

/// A connection to one scheduler's control API. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    base: Url,
    caller: String,
    audience: String,
    key: PrivateKey,
    scope: Scope,
    jobs: Option<Vec<String>>,
}

impl Client {
    /// Starts setting up a client for the scheduler at `base_url`.
    pub fn builder(base_url: &str) -> Builder {
        Builder {
            base: base_url.trim_end_matches('/').to_owned(),
            caller: None,
            audience: None,
            keys: Vec::new(),
            ca: None,
            scope: Scope::Operate,
            jobs: None,
            timeout: Duration::from_secs(30),
        }
    }

    /// The same client, acting for someone else: a console passes on the signed-in person.
    #[must_use]
    pub fn acting_for(&self, actor: &str, scope: Scope, jobs: Option<Vec<String>>) -> ActingClient<'_> {
        ActingClient { client: self, actor: actor.to_owned(), scope, jobs }
    }

    fn as_self(&self) -> ActingClient<'_> {
        ActingClient { client: self, actor: self.caller.clone(), scope: self.scope, jobs: self.jobs.clone() }
    }

    async fn unsigned_get<T: DeserializeOwned>(&self, path: &str) -> Result<T, Error> {
        let url = self.base.join(path).map_err(|error| Error::Setup(error.to_string()))?;
        decode(self.http.get(url).send().await?).await
    }

    /// Start a job by name, with any input that serialises to what the job takes. Send it with
    /// [`Start::send`]. The scheduler checks the input against the job's type and refuses one that
    /// does not fit.
    pub fn start<I: Serialize>(&self, job: &str, input: &I) -> Start<'_> {
        self.as_self().start(job, input)
    }

    /// Start a job declared with `downbeat_api::job_name!`: the compiler checks the input type.
    pub fn start_job<J: JobName>(&self, _job: J, input: &J::Input) -> Start<'_> {
        self.as_self().start(J::NAME, input)
    }

    /// Every job the client may see.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn jobs(&self) -> Result<Vec<JobView>, Error> {
        self.as_self().get("/v1/jobs").await
    }

    /// One job by name.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn job(&self, name: &str) -> Result<JobView, Error> {
        self.as_self().get(&path(&["v1", "jobs", name])).await
    }

    /// One run by id.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn run(&self, id: Uuid) -> Result<RunView, Error> {
        self.as_self().get(&format!("/v1/runs/{id}")).await
    }

    /// Runs, newest first; `query` is the filter part of the URL, such as `status=failed&job=x`.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn runs(&self, query: &str) -> Result<Page<RunView>, Error> {
        self.as_self().get(&format!("/v1/runs?{query}")).await
    }

    /// One chain by id.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn chain(&self, id: Uuid) -> Result<ChainView, Error> {
        self.as_self().get(&format!("/v1/chains/{id}")).await
    }

    /// Every pipeline the client may see.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn pipelines(&self) -> Result<Vec<PipelineView>, Error> {
        self.as_self().get("/v1/pipelines").await
    }

    /// The planned and past runs of one day.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn calendar(&self, date: chrono::NaiveDate) -> Result<CalendarDay, Error> {
        self.as_self().get(&format!("/v1/calendar?date={date}")).await
    }

    /// Each job's last `days` days (1 to 30, ending today), and yesterday's failures.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn report(&self, days: u32) -> Result<Report, Error> {
        self.as_self().report(days).await
    }

    /// Cancels a run.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn cancel(&self, run: Uuid) -> Result<Done, Error> {
        self.as_self().post(&format!("/v1/runs/{run}/cancel"), None::<&()>, None).await
    }

    /// Acts on the client's own behalf; see [`Client::acting_for`] for everything else.
    pub fn as_caller(&self) -> ActingClient<'_> {
        self.as_self()
    }
}

/// A client acting for one actor, with the scope a console decided they have.
#[derive(Debug, Clone)]
pub struct ActingClient<'a> {
    client: &'a Client,
    actor: String,
    scope: Scope,
    jobs: Option<Vec<String>>,
}

impl<'a> ActingClient<'a> {
    /// Start a job, as this actor.
    pub fn start<I: Serialize>(&self, job: &str, input: &I) -> Start<'a> {
        let request = serde_json::to_value(input)
            .map(StartRequest::new)
            .map_err(|error| Error::Setup(format!("the input for {job} does not serialise to JSON: {error}")));
        Start { client: self.clone(), job: job.to_owned(), request, key: None }
    }

    /// Sends one signed request.
    async fn send<B: Serialize>(
        &self,
        method: Method,
        path_and_query: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
    ) -> Result<reqwest::Response, Error> {
        let bytes = match body {
            Some(body) => serde_json::to_vec(body).map_err(|error| Error::Setup(error.to_string()))?,
            None => Vec::new(),
        };
        let url = self.client.base.join(path_and_query).map_err(|error| Error::Setup(error.to_string()))?;
        let sent = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().to_owned(),
        };
        let binding = RequestBinding { method: method.as_str(), path_and_query: &sent, body: &bytes };
        let now = chrono::Utc::now().timestamp();
        let claims = Claims::for_request(
            &self.client.caller,
            &self.actor,
            &self.client.audience,
            self.scope,
            self.jobs.clone(),
            binding,
            now,
        );
        let token = sign(&self.client.key, &claims);
        let mut request = self.client.http.request(method, url).bearer_auth(token);
        if !bytes.is_empty() {
            request = request.header(reqwest::header::CONTENT_TYPE, "application/json").body(bytes);
        }
        if let Some(key) = idempotency_key {
            request = request.header(IDEMPOTENCY_HEADER, key);
        }
        Ok(request.send().await?)
    }

    /// A signed GET of `path_and_query`, decoded as `T`.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn get<T: DeserializeOwned>(&self, path_and_query: &str) -> Result<T, Error> {
        decode(self.send(Method::GET, path_and_query, None::<&()>, None).await?).await
    }

    /// A signed POST of `body` to `path_and_query`, decoded as `T`.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path_and_query: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
    ) -> Result<T, Error> {
        decode(self.send(Method::POST, path_and_query, body, idempotency_key).await?).await
    }

    /// A signed DELETE of `path_and_query`, decoded as `T`.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn delete<T: DeserializeOwned>(&self, path_and_query: &str) -> Result<T, Error> {
        decode(self.send(Method::DELETE, path_and_query, None::<&()>, None).await?).await
    }

    /// Pauses one `job`, `queue` or `schedule` by name.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn pause(&self, kind: &str, name: &str, request: &PauseRequest) -> Result<Done, Error> {
        self.post(&path(&["v1", &format!("{kind}s"), name, "pause"]), Some(request), None).await
    }

    /// Ends a pause of one `job`, `queue` or `schedule`.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn resume(&self, kind: &str, name: &str) -> Result<Done, Error> {
        self.post(&path(&["v1", &format!("{kind}s"), name, "resume"]), None::<&()>, None).await
    }

    /// Holds every run of every job until resumed or the pause ends.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn pause_everything(&self, request: &PauseRequest) -> Result<Done, Error> {
        self.post("/v1/pause", Some(request), None).await
    }

    /// Ends a pause of everything.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn resume_everything(&self) -> Result<Done, Error> {
        self.post("/v1/resume", None::<&()>, None).await
    }

    /// Marks a failed run as succeeded.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn mark_succeeded(&self, run: Uuid, request: &MarkSucceededRequest) -> Result<Done, Error> {
        self.post(&format!("/v1/runs/{run}/mark-succeeded"), Some(request), None).await
    }

    /// Saves a dated override of one or more schedules.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn add_override(&self, request: &OverrideRequest) -> Result<Done, Error> {
        self.post("/v1/overrides", Some(request), None).await
    }

    /// What an override would change, saving nothing.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn preview_override(&self, request: &OverrideRequest) -> Result<Preview, Error> {
        self.post("/v1/overrides?preview=true", Some(request), None).await
    }

    /// Overrides from the last 30 days on.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn overrides(&self) -> Result<Vec<OverrideView>, Error> {
        self.get("/v1/overrides").await
    }

    /// Starts a backfill of a schedule over a range of dates.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn start_backfill(&self, request: &BackfillRequest) -> Result<Done, Error> {
        self.post("/v1/backfills", Some(request), None).await
    }

    /// The newest backfills.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn backfills(&self) -> Result<Vec<BackfillView>, Error> {
        self.get("/v1/backfills").await
    }

    /// Every schedule, with its next time.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn schedules(&self) -> Result<Vec<ScheduleView>, Error> {
        self.get("/v1/schedules").await
    }

    /// Every queue, with its waiting and running runs.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn queues(&self) -> Result<Vec<QueueView>, Error> {
        self.get("/v1/queues").await
    }

    /// The newest audit entries.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn audit(&self) -> Result<Page<AuditEntry>, Error> {
        self.get("/v1/audit").await
    }

    /// Each job's last `days` days (1 to 30, ending today), and yesterday's failures, as this actor
    /// sees them.
    ///
    /// # Errors
    ///
    /// Fails when the scheduler cannot be reached or answers with an error.
    pub async fn report(&self, days: u32) -> Result<Report, Error> {
        self.get(&format!("/v1/report?days={days}")).await
    }
}

/// The absolute path made of `segments`, each percent-encoded, so a name with a space or a `/`
/// stays one segment.
pub(crate) fn path(segments: &[&str]) -> String {
    let mut url = Url::parse("http://scheduler/").expect("a fixed URL parses");
    url.path_segments_mut().expect("an http URL has a path").clear().extend(segments);
    url.path().to_owned()
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(response.json().await?);
    }
    let body = response.bytes().await?;
    let error: ApiError = serde_json::from_slice(&body).unwrap_or_else(|_| {
        with_defaults!(ApiError { error: "unknown".into(), message: undecoded_message(status, &body) })
    });
    if status == StatusCode::UNAUTHORIZED && error.message.contains("expired") {
        return Err(Error::Api {
            status: status.as_u16(),
            code: error.error,
            message: format!("{} — check this machine's clock", error.message),
        });
    }
    Err(Error::Api { status: status.as_u16(), code: error.error, message: error.message })
}

/// The status's reason and the start of a body that is not an `ApiError`, such as a proxy's page.
fn undecoded_message(status: StatusCode, body: &[u8]) -> String {
    const SHOWN: usize = 200;
    let reason = status.canonical_reason().unwrap_or("error");
    let text = String::from_utf8_lossy(&body[..body.len().min(SHOWN)]);
    let text = text.trim();
    if text.is_empty() {
        reason.to_owned()
    } else if body.len() > SHOWN {
        format!("{reason}: {text}…")
    } else {
        format!("{reason}: {text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_path_segment_is_percent_encoded() {
        assert_eq!(path(&["v1", "jobs", "a b/c?d#e%f", "runs"]), "/v1/jobs/a%20b%2Fc%3Fd%23e%25f/runs");
        assert_eq!(path(&["v1", "jobs", "plain_name"]), "/v1/jobs/plain_name");
    }

    #[test]
    fn an_undecodable_error_body_keeps_its_start() {
        let long = "x".repeat(500);
        assert_eq!(undecoded_message(StatusCode::BAD_GATEWAY, b""), "Bad Gateway");
        assert_eq!(undecoded_message(StatusCode::BAD_GATEWAY, b" <h1>down</h1> "), "Bad Gateway: <h1>down</h1>");
        assert_eq!(
            undecoded_message(StatusCode::BAD_GATEWAY, long.as_bytes()),
            format!("Bad Gateway: {}…", &long[..200])
        );
    }
}
