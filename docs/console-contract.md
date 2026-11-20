# The console's HTTP contract

What the console server gives its screens. Everything is same-origin JSON with a session cookie.
Errors are `{ "error": "<code>", "message": "<sentence>" }` with a 4xx/5xx status.

## Session

- `GET /api/me` →
  ```json
  { "person": { "email": "asha@example.com", "name": "Asha" },
    "csrf_token": "…",
    "local": false,
    "schedulers": [ { "name": "axon-production", "label": "Axon production",
                      "scope": "operate", "reachable": true, "api_version": 1 } ] }
  ```
  `scope` is `read`, `operate`, `administer`, or `null` (no grant: the scheduler is listed but
  nothing in it is shown). 401 when not signed in: the screens then send the browser to
  `/auth/login?return=<path>`.
- Every request that is not a GET must carry the header `x-csrf-token: <csrf_token>`.
- `POST /auth/logout` ends the session; then go to `/`.

## A scheduler's control API

`/api/schedulers/{name}/v1/…` forwards to that scheduler's control API (`/v1/…`), signed for the
signed-in person. Method, query and JSON body pass through; the response is the scheduler's. The
request and response types are in `crates/downbeat-api/src/types.rs`; the endpoints are in
`crates/downbeat/src/api/mod.rs` (`router`). Every action accepts `?preview=true` and then returns
`Preview { effects: string[], false_conditions: string[] }` without changing anything.

## Access

- `GET /api/grants` → `[{ grant_id, to_person: string|null, to_group: string|null, scheduler, scope,
  pipelines: string[]|null, created_by, created_at }]`
- `POST /api/grants` with `{ to_person?: email, to_group?: string, scheduler, scope, pipelines?: string[] }`
  (exactly one of `to_person`, `to_group`) → the new grant. Needs `administer` on that scheduler.
- `DELETE /api/grants/{id}` → `{ "message": "…" }`.
- `GET /api/people` → `[{ email, name, groups: string[], last_seen_at }]`. Needs `administer`
  somewhere.

## Console audit and preferences

- `GET /api/audit?before=<id>` → `{ items: [{ audit_id, at, actor, action, detail }], next_cursor }`:
  sign-ins, sign-outs, grant changes, refused actions. What people did to jobs is the scheduler's
  audit: `/api/schedulers/{name}/v1/audit`.
- `GET /api/preferences` → `{ time_zone: string|null, filters: object }`;
  `PUT /api/preferences` with the same shape.
