# HTTP API reference

The control API a scheduler serves when started with `--listen` or `--local`. The routes are in
[`crates/downbeat/src/api/mod.rs`](../../crates/downbeat/src/api/mod.rs) (`router`); the request
and response types are in
[`crates/downbeat-api/src/types.rs`](../../crates/downbeat-api/src/types.rs). How to sign
requests is in [the API chapter](../guide/07-api-and-client.md#signed-requests-from-any-language).

## Conventions

- Every path below `/v1` needs `Authorization: Bearer <token>`. A token is good for one request.
- Bodies are JSON. Send `Content-Type: application/json` with a body; the body is limited to
  256 KiB.
- Times are RFC 3339 in UTC. Dates are `YYYY-MM-DD`. Ids of runs and chains are UUIDs.
- Every response carries `downbeat-api-version: 1` (the version this scheduler speaks).
- **Scopes**: R = `read`, O = `operate`, A = `administer`. A token limited to some jobs sees only
  those jobs in lists, and gets 403 for anything about another job.
- **Preview**: every endpoint marked with ✓ accepts `?preview=true`. It runs the same scope and
  input checks, changes nothing, and answers 200 with `Preview`.

## Endpoints

| Method and path | Scope | Request | Response | Preview |
| --- | --- | --- | --- | --- |
| `GET /healthz` | none | | `Health` | |
| `GET /readyz` | none | | `Health`; 503 when the database cannot be reached | |
| `GET /version` | none | | `Version` | |
| `GET /v1/jobs` | R | | `JobView[]` | |
| `GET /v1/jobs/{name}` | R | | `JobView` | |
| `POST /v1/jobs/{name}/runs` | O; A with `ignore_conditions` or `timeout_seconds` | `StartRequest`, optional `Idempotency-Key` header | `StartResponse` | ✓ |
| `POST /v1/jobs/{name}/pause` | A | `PauseRequest` | `Done` | ✓ |
| `POST /v1/jobs/{name}/resume` | A | | `Done` | ✓ |
| `GET /v1/pipelines` | R | | `PipelineView[]` (without recent chains) | |
| `GET /v1/pipelines/{name}` | R | | `PipelineView` with up to 20 recent chains | |
| `GET /v1/chains/{id}` | R (the chain's first job) | | `ChainView` | |
| `POST /v1/chains/{id}/cancel` | O (every job in the chain) | | `Done` | ✓ |
| `GET /v1/runs` | R | query, see below | `Page<RunView>` | |
| `GET /v1/runs/{id}` | R | | `RunView` with `detail` | |
| `POST /v1/runs/{id}/cancel` | O | | `Done` | ✓ |
| `POST /v1/runs/{id}/retry` | O | | `Done` with the new `run_id` | ✓ |
| `POST /v1/runs/{id}/restart` | O | | `Done` with the new `run_id` | ✓ |
| `POST /v1/runs/{id}/mark-succeeded` | A | `MarkSucceededRequest` | `Done` | ✓ |
| `POST /v1/runs/{id}/check-now` | O | | `Done` | ✓ |
| `GET /v1/calendar?date=YYYY-MM-DD` | R | `date` defaults to today in the definition's zone | `CalendarDay` | |
| `GET /v1/schedules` | R | | `ScheduleView[]` | |
| `POST /v1/schedules/{name}/pause` | A | `PauseRequest` | `Done` | ✓ |
| `POST /v1/schedules/{name}/resume` | A | | `Done` | ✓ |
| `GET /v1/queues` | R | | `QueueView[]` | |
| `POST /v1/queues/{name}/pause` | A (token covering every job) | `PauseRequest` | `Done` | ✓ |
| `POST /v1/queues/{name}/resume` | A (token covering every job) | | `Done` | ✓ |
| `GET /v1/pause` | R | | `PauseView?`: the pause of everything, if any | |
| `POST /v1/pause` | A (token covering every job) | `PauseRequest` | `Done`: holds every run of every job | ✓ |
| `POST /v1/resume` | A (token covering every job) | | `Done` | ✓ |
| `GET /v1/overrides` | R | | `OverrideView[]`, from 30 days ago on | |
| `POST /v1/overrides` | A | `OverrideRequest` | `Done` with the last override's `id` | ✓ |
| `DELETE /v1/overrides/{id}` | A | | `Done` | ✓ |
| `GET /v1/backfills` | R | | `BackfillView[]`, newest 200 | |
| `POST /v1/backfills` | A | `BackfillRequest` | `Done` with the backfill's `id` | ✓ |
| `POST /v1/backfills/{id}/cancel` | A | | `Done` | ✓ |
| `GET /v1/audit?limit=&before=` | R | | `Page<AuditEntry>` | |

### Endpoint notes

- **Start.** The input is checked strictly against the job's input type. 201 when a run was
  created. 200 with `joined: true` when the same work was already queued and the request joined it
  (only for a request that is not deliberate; see
  [the same-work rules](../spec.md#5-the-same-work-rules)). 200 with `repeated: true` when the
  `Idempotency-Key` was used before by the same caller and actor for this job. `status` in the
  answer is always `queued`.
- **Pause.** `note` must not be empty; give a future `until`, or `until_resumed: true`. Pausing
  again replaces the earlier pause. Resuming something not paused, or whose pause has already ended, is 409.
- **Cancel a run.** 409 when the run has finished. A running run is asked to stop at its next
  heartbeat; the answer says so.
- **Retry** needs a finished run (409 otherwise, and 409 in a cancelled chain). **Restart** works
  on a finished or unfinished run whose input is known (409 when it has not been prepared yet).
- **Mark as succeeded** needs a `failed` run (409 otherwise) and a non-empty `note` (400). The
  `output` must fit the job's output type (422).
- **Check now** works only on a queued run that is `not_ready`, `need_not_met` or
  `backing_off` (409 otherwise).
- **Cancel a chain** is 404 for a chain with no runs, 409 when it was already cancelled.
- **Overrides.** `change` is `off`, `times` (a `daily_at` schedule, at least one time) or `window`
  (an `every(…).between(…)` schedule, exactly two times); `note` must not be empty. One override
  per schedule and date: a second replaces the first.
- **Backfills.** `schedule` defaults to the job's first schedule, and must be one of its
  schedules; `from` must not be after `to`. Cancelling a backfill that is not running is 409.
- **Audit.** `limit` defaults to 50, at most 200. Pass `next_cursor` as `before` for the next
  page.

### `GET /v1/runs`

One list of unfinished runs (newest created first) followed by finished runs (newest finished
first). `joined` rows are never listed. Unknown query parameters are refused (400).

| Parameter | Meaning |
| --- | --- |
| `job` | Only this job. |
| `pipeline` | Only jobs in this pipeline (404 for an unknown pipeline). |
| `status` | `queued`, `running`, `succeeded`, `skipped`, `failed`, `cancelled` or `expired`. |
| `queued_because`, `skipped_because`, `failed_because` | Only runs with this reason code. |
| `started_by` | `schedule`, `link`, `person`, `program`, `backfill` or `job`. |
| `queue` | Only this queue. |
| `from`, `to` | Created (unfinished) or finished (finished) at or after `from` and before `to`. RFC 3339. |
| `input` | `field=value`: only runs whose input's top-level `field` equals `value` as text, such as `date=2026-11-08`. |
| `limit` | Default 50, at most 200. |
| `cursor` | The previous page's `next_cursor`. |

## Types

All in `downbeat_api::types`. Optional fields are `null` when absent, unless noted.

| Type | Fields |
| --- | --- |
| `Health` | `ok: bool`, `api: u32` |
| `Version` | `api: u32`, `engine: string` (the engine crate's version) |
| `Page<T>` | `items: T[]`, `next_cursor?: string` (absent on the last page) |
| `ApiError` | `error: string` (a code), `message: string` (a sentence that says what to do) |
| `JobView` | `name`, `pipeline`, `kind` (`function` or `command`), `queue`, `triggers: string[]`, `policies: string[]` (those that differ from the defaults), `input` and `output` (shapes: `type_name`, `fields[]` of `name`, `type_name`, `compared`), `input_schema` (the input as a JSON Schema, draft 2020-12; a type downbeat does not recognise accepts any value there, but the scheduler still checks the real type), `supports_dry_run`, `last_run: RunSummary?`, `next_occurrence?`, `expected_seconds?`, `paused: PauseView?`, `overdue: bool` |
| `RunView` | `run_id`, `job`, `pipeline?`, `chain_id`, `status`, `queued_because?`, `skipped_because?`, `failed_because?`, `reason`, `started_by`, `actor?`, `schedule?`, `queue`, `priority`, `due_at`, `created_at`, `started_at?`, `finished_at?`, `attempt`, `attempts`, `calls`, `lost_count`, `interruptions`, `dry_run`, `progress?`, `has_checkpoint`, `slow`, `expected_seconds?`, `input?`, `output?`, `log_link?`, `marked_by?`, `note?`, `detail?: RunDetail` (single-run view only; absent otherwise) |
| `RunDetail` | `error_chain: string[]`, `stderr_tail?`, `call_log: CallView[]` (the last ten calls), `parents: uuid[]`, `children: uuid[]` |
| `CallView` | `worker`, `build`, `started_at`, `ended_at`, `ended` (a few words, such as `succeeded`, `error`, `not ready`, `interrupted`, `worker lost`) |
| `RunSummary` | `run_id`, `status`, `reason`, `at` |
| `PauseView` | `by`, `note`, `until?`, `since` |
| `PipelineView` | `name`, `jobs: PipelineNode[]`, `recent_chains: ChainSummary[]` |
| `PipelineNode` | `job`, `link?` (`after`, `after_failure` or `after_any`), `parents: string[]`, `schedules: string[]` |
| `ChainSummary` | `chain_id`, `first_job`, `started_at`, `statuses: {job: status}`, `cancelled: bool` |
| `ChainView` | `chain_id`, `pipeline`, `first_job`, `started_at`, `cancelled_at?`, `cancelled_by?`, `nodes: ChainNode[]` (every job the chain could reach, from its first job) |
| `ChainNode` | `job`, `parents: string[]`, `runs: RunSummary[]` (more than one after a retry), `why_not?` (when it has no run: why) |
| `ScheduleView` | `name`, `job`, `times` (in words), `next_occurrence?`, `paused: PauseView?` |
| `CalendarDay` | `date`, `entries: CalendarEntry[]` (in time order), `days_off: DayOff[]` |
| `CalendarEntry` | `schedule`, `job`, `at`, `source` (`normal`, `rule` or `override`), `note?`, `paused: bool`, `run: RunSummary?` |
| `DayOff` | `schedule`, `reason` |
| `QueueView` | `name`, `queued`, `running`, `queued_because: {code: count}`, `longest_wait_seconds?`, `workers: WorkerView[]` (seen in the last minute), `paused: PauseView?` |
| `WorkerView` | `worker_id`, `slots`, `build`, `last_seen_at` |
| `BackfillView` | `backfill_id`, `job`, `schedule`, `from`, `to`, `reached` (the next date to run, or the one running), `state` (`running`, `done`, `failed`, `cancelled`), `reason?`, `continue_on_failure`, `created_by`, `created_at` |
| `AuditEntry` | `audit_id`, `at`, `actor`, `action`, `job?`, `run_id?`, `schedule?`, `detail` (JSON) |
| `OverrideView` | `override_id`, `schedule`, `date`, `change`, `times: string[]` (`HH:MM`), `note`, `created_by` |
| `StartRequest` | `input` (default `null`), `priority?: i16`, `not_before?: time`, `dry_run`, `without_children`, `even_if_queued`, `ignore_conditions` (booleans, default `false`), `timeout_seconds?: u64` (0 for none). Unknown fields refused. |
| `StartResponse` | `run_id`, `status`, `joined: bool`, `repeated: bool` |
| `Preview` | `effects: string[]`, `false_conditions: string[]` |
| `PauseRequest` | `note: string`, `until?: time`, `until_resumed: bool` (default `false`). Unknown fields refused. |
| `MarkSucceededRequest` | `output` (default `null`), `note: string`. Unknown fields refused. |
| `OverrideRequest` | `schedules: string[]`, `date`, `change`, `times: string[]` (default `[]`), `note`. Unknown fields refused. |
| `BackfillRequest` | `job`, `schedule?`, `from`, `to`, `continue_on_failure` (default `false`). Unknown fields refused. |
| `Done` | `message`, `run_id?`, `id?` (both absent when not set) |

## Errors

An error from downbeat is JSON `ApiError`:

```json
{ "error": "forbidden", "message": "this needs the administer scope; the token has operate" }
```

| Status | `error` | When |
| --- | --- | --- |
| 400 | `bad_request` | A rule about the request is broken: a pause with no note or no end, an override that does not fit its schedule, a bad cursor or filter, a backfill range that ends before it begins. |
| 401 | `unauthorized` | No token, or one that is malformed, unsigned by a trusted caller's key, for another audience, expired, made for another request, or already used. |
| 403 | `forbidden` | A valid token that does not cover the request: the scope, the jobs, or a caller acting for someone it may not. |
| 404 | `not_found` | No such job, pipeline, chain, run, schedule, queue, override, backfill or endpoint. |
| 405 | `method_not_allowed` | The endpoint exists but not with this method. |
| 409 | `conflict` | The action does not fit the current state, such as retrying an unfinished run. |
| 413 | `too_large` | The body is over 256 KiB. |
| 422 | `bad_input` | The input or output does not fit the job's type, or a dry run of a job that does not support it. |
| 429 | `rate_limited` | Over 20 requests a second on average (bursts of 100) for this caller and actor. |
| 503 | `database` | The scheduler could not reach its database. Try again. |

A body that is not valid JSON, or that does not fit the request type, is rejected before it
reaches downbeat's handlers, with status 400, 415 or 422. That answer is an `ApiError` too, with
code `bad_request` (400 and 415) or `bad_input` (422) and the framework's sentence as its message.
