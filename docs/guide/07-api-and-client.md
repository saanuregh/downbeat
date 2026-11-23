# 7. The control API and the client

The control API is the one HTTP interface on the scheduler. Programs use it to start jobs, and
the console uses it for everything people see and do. Every endpoint is listed in the
[HTTP API reference](../reference/http-api.md).

There are three ways to start a job from code:

| From | Use | Goes through |
| --- | --- | --- |
| A job function | `run.start(job, input)` | The worker, which writes the run |
| Other code in your product's binary | `InAppClient` | The engine's tables directly; no key needed |
| Any other program | `downbeat-client` (Rust), or a signed HTTP request (any language) | The control API |

All three check the input against the job's type before anything is stored, apply
[the same-work rules](03-pipelines.md#the-same-work-is-done-once), and record who asked.

## Serving the API

A scheduler serves the API when started with `--listen <address>` and `--callers <file>`. The
callers file names the scheduler's **audience** (the name tokens must be addressed to) and each
trusted caller with its public keys and limits. See the
[configuration reference](../reference/configuration.md#the-callers-file).

```toml
audience = "market-production"

[[caller]]
name = "billing-service"
keys = ["<base64url public key>"]   # Ed25519; list several while rotating
max_scope = "operate"
jobs = ["daily_report"]
```

Keep the API on a private network and encrypt the connection, with a proxy or a load balancer
that terminates TLS. The signed tokens answer "who is this and what may they do"; the network
answers "who can even try".

## Scopes

| Scope | Covers |
| --- | --- |
| `read` | Every GET |
| `operate` | Start, cancel, retry, restart, check now, cancel a chain |
| `administer` | Pauses, dated overrides, backfills, mark as succeeded, and a start with `ignore_conditions` or `timeout_seconds` |

Each scope includes the ones before it. A token can ask for no more than its caller's
`max_scope`. A caller limited to some `jobs` must name them in every token, sees only those jobs
in every list, and cannot do what reaches jobs outside its list: pause a queue or everything, or
cancel a chain or a backfill whose chain includes another job.

## Signed requests from any language

Every request to `/v1/…` carries `Authorization: Bearer <token>`. The token is a compact JWS
(RFC 7515) holding a JWT (RFC 7519), signed with Ed25519, and good for one request only.

**Header** (base64url without padding):

```json
{ "alg": "Ed25519", "typ": "downbeat-request+jwt", "kid": "Q2hhbmdlTWVQ" }
```

- `alg` is `Ed25519` (RFC 9864). `EdDSA` is accepted too.
- `typ` must be exactly `downbeat-request+jwt`.
- `kid` is the first 12 characters of the base64url public key. It must be present.

**Payload**, for `POST /v1/jobs/daily_report/runs` with body `{"input":{"date":"2026-11-20"}}`:

```json
{
  "iss": "billing-service",
  "sub": "billing-service",
  "aud": "market-production",
  "iat": 1795132800,
  "exp": 1795132860,
  "jti": "kL0x9c2Vq3tYbW1pZ3rS8A",
  "scope": "operate",
  "jobs": ["daily_report"],
  "htm": "POST",
  "htu": "/v1/jobs/daily_report/runs",
  "body_sha256": "<base64url SHA-256 of the exact body bytes>"
}
```

| Claim | Rule |
| --- | --- |
| `iss` | The caller's `name` in the callers file. |
| `sub` | The actor. It must equal `iss` unless the caller has `acts_for_people = true`. |
| `aud` | The scheduler's `audience`. |
| `iat`, `exp` | Seconds since the epoch. `exp - iat` is at most 60. Clocks may differ by 5 seconds. |
| `jti` | A random id. A token for a request that is not a GET is accepted once. |
| `scope` | `read`, `operate` or `administer`. |
| `jobs` | Optional. The only jobs the token covers. Required when the caller is limited to some jobs. |
| `htm` | The request's method. |
| `htu` | The request's path and query exactly as sent, such as `/v1/runs?status=failed`. Not the host. |
| `body_sha256` | The SHA-256 of the exact body bytes, base64url without padding. Leave it out when the body is empty. |

The **signature** is Ed25519 over the ASCII bytes of `<header>.<payload>`, appended as a third
base64url part. Any JWT library that supports Ed25519 can make one: set the header fields above
and sign the claims. Sign a fresh token for every request.

The scheduler checks, in order: the form, `typ` and `alg`; the signature against the issuing
caller's keys; `aud`; the lifetime; that `htm`, `htu` and the body hash match the request; that
the actor, scope and jobs are within the caller's limits; the caller's rate limit; and, for a
request that is not a GET, that `jti` has not been used. Then the endpoint checks the scope it
needs. A missing, invalid, expired or reused token is answered 401; a valid token that does not
cover the request, 403.

Health checks (`/healthz`, `/readyz`) and `/version` need no token.

### Making a key pair

```sh
downbeat-console keygen billing-service.pem
```

It writes the private key as PKCS#8 PEM and prints the public key for the callers file.
`openssl genpkey -algorithm ed25519` writes the same private-key format; the public key the
callers file wants is the raw 32 bytes in base64url.

## `downbeat-client`

`downbeat-client` is a small crate for Rust programs outside your binary. It does the signing,
uses HTTPS only (plain HTTP is allowed only for a scheduler on `localhost` or `127.0.0.1`), and
checks the API version when it connects. It does not depend on the engine.

```rust
let scheduler = downbeat_client::Client::builder("https://scheduler.internal:7400")
    .caller("billing-service")
    .audience("market-production")
    .key_from_env("DOWNBEAT_KEY")?          // or .key_from_file(path)?, or .key(PrivateKey)
    .jobs(["daily_report"])                 // when the scheduler limits this caller
    .connect()
    .await?;

let started = scheduler
    .start("daily_report", &serde_json::json!({ "date": "2026-11-20" }))
    .priority(10)
    .idempotency_key("report-2026-11-20")
    .send()
    .await?;
println!("{} joined={} repeated={}", started.run_id, started.joined, started.repeated);
```

- **Typed starts.** A product declares the jobs its services may start, with their input types, in
  a small crate those services share. `start_job` then checks the input type at compile time:

  ```rust
  // In the shared crate. Its types derive `Serialize`; the product also derives `JobData` on them,
  // for example behind a cargo feature only the product enables.
  downbeat_client::job_name!(pub send_report: ReportRequest);

  // In a service.
  scheduler.start_job(send_report, &ReportRequest { user_id: 42 }).send().await?;
  ```

  `start(name, &input)` does the same by name, for when there is no shared crate. Either way the
  scheduler checks the input against the job's real type and answers 422 when it does not fit.
- **Retries are safe.** `send` gives every start an idempotency key (yours, or a generated one)
  and retries with the same key after a connection error, a timeout, a 429, 502, 503 or 504, up to
  four tries. A refused request (401, 403, 422) or a 500 is not retried.
- **Keys rotate by overlap.** Add several keys with `.key(…)`; the last one added signs.
- **Other builder options**: `.scope(Scope::Administer)` (default `operate`),
  `.certificate_authority(pem)?` to trust your own CA, `.timeout(duration)` (default 30 seconds),
  and `.local()` for a scheduler started with `--local`.
- **Other calls**: `jobs()`, `job(name)`, `run(id)`, `runs("status=failed&job=x")`, `chain(id)`,
  `pipelines()`, `calendar(date)`, `cancel(run_id)`, and `start(…).preview()`.
  `as_caller()` returns an `ActingClient` with `pause`, `resume`, `pause_everything`,
  `resume_everything`, `mark_succeeded`,
  `add_override`, `preview_override`, `overrides`, `start_backfill`, `backfills`, `schedules`,
  `queues`, `audit`, and generic `get`, `post` and `delete` for any other endpoint, such as
  `post("/v1/runs/{id}/retry", None::<&()>, None)`.
- **Requests and answers can grow.** Build a request with `new(…)` from its required fields and a
  setter for each optional one, such as `PauseRequest::new("bad upstream data").until(end)` or
  `BackfillRequest::new("daily_report", from, to).continue_on_failure()`. Every request, answer
  and error type is `#[non_exhaustive]`, so a match needs a `_` arm and a later version can add a
  field or a variant without breaking your code.
- **Acting for someone else**: `client.acting_for("asha@example.com", Scope::Operate, None)`
  signs as that actor. Only a caller with `acts_for_people = true` may do this.
- **Errors**: `Error::Api { status, code, message }` carries the scheduler's answer.
  `Error::is_transient()` says whether retrying could help. A 401 for an expired token adds
  "check this machine's clock".

## `InAppClient`

Code inside your product's binary, such as its admin web server, starts jobs through the engine's
tables, with typed handles and no key:

```rust
let jobs = downbeat::InAppClient::connect(definition, &database_url).await?;
let started = jobs
    .start(daily_report, TradingDay { date })
    .actor("admin-ui:asha")
    .idempotency_key(format!("report-{date}"))
    .await?;
match started {
    Started::New(run_id) => println!("started {run_id}"),
    Started::Joined(run_id) => println!("already queued as {run_id}"),
    _ => println!("started {}", started.run_id()),
}
```

It builds the definition (using `DOWNBEAT_ENVIRONMENT`), opens a pool of four connections, and
migrates the schema. The actor defaults to `app`. Options: `.priority(n)`, `.even_if_queued()`,
`.actor(name)`, `.idempotency_key(key)`. It connects by URL, so it works whatever `sqlx` version
your product uses ([decision 9](../decisions.md#9-inappclient-connects-with-a-database-url)).

Connecting and starting return `downbeat::Error`, so a caller can tell a definition with
problems (`Definition`), a database it cannot reach (`Database`), a newer breaking schema
(`NewerSchema`), a job not in the definition (`UnknownJob`), a dry run the job does not support
(`NoDryRun`) and an input that does not fit (`Input`) apart.

## Idempotency

A start with an idempotency key (the `Idempotency-Key` header, or `.idempotency_key(…)`) stores
the key beside the run it produced, in the same transaction. Repeating the request returns that
run with `repeated: true` and creates nothing, even if the run has finished since.

- Keys are separate per caller and actor, and per job. Through the API the key's owner is
  `<caller>/<actor>`; through `InAppClient` it is the actor.
- Keys are kept for seven days.
- Two retries in flight at once are safe: the second waits for the first to commit, then gets its
  run.
- There is no transaction with your own database. A caller that must not lose a job writes its
  own record first, then starts the job with a key derived from that record, retrying until it
  gets an answer.

Without a key, [rule 1](03-pipelines.md#the-same-work-is-done-once) still joins a request to a
queued run for the same work, and the answer says `joined: true` (HTTP 200 rather than 201).

## Previews

Every action accepts `?preview=true`. It changes nothing and returns
`Preview { effects, false_conditions }`: one sentence per effect, and for a start that ignores
conditions, the conditions that are false right now. A preview is still a request that is not a
GET, so it needs a fresh token and the action's scope.
