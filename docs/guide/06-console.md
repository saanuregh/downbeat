# 6. The console

The console is where people see and control jobs. It is a separate program, `downbeat-console`,
the same for every product. It signs people in, decides what each person may do, serves the
screens, and talks to one or more schedulers through their control API. It contains no product
code and has no credentials to your product's data: only its own database and a signing key.

```
browser ──HTTPS──► downbeat-console ──signed requests──► scheduler (control API) ──► Postgres
                       │
                       └──► its own Postgres (people, grants, sessions, audit)
```

The browser never talks to a scheduler and never holds a scheduler token. For every request, the
console checks the person's grants, then signs a request naming that person as the actor, with
exactly the scope and jobs they hold. The scheduler checks it again.

## Try it locally

```sh
export DOWNBEAT_DATABASE_URL=postgres://localhost/myproduct
<binary> scheduler worker --queues default=4 --local
DOWNBEAT_CONSOLE_DATABASE_URL=$DOWNBEAT_DATABASE_URL downbeat-console --local
```

`--local` on the scheduler makes it trust a published development key and listen on
`127.0.0.1:7400`. On the console, it uses that key, skips sign-in, and treats every visitor as
one stand-in person who administers everything. It listens on `127.0.0.1:7401` and connects to
`http://127.0.0.1:7400` (change these with `--listen` and `--scheduler`). Both programs refuse
`--local` unless they listen on a loopback address, so the development key is worthless anywhere
else.

## Build it

The screens are a React app built with Bun. Their built files are compiled into the binary, so
there is one file to deploy:

```sh
cd crates/downbeat-console/web && bun install --frozen-lockfile && bun run build && cd -
cargo build --release -p downbeat-console
```

`mise run web` does the first step. A console built without the screens still serves its API,
and its pages say that the screens were not built.

## Deploy it

1. **Give it a database.** Any Postgres database; the console creates and migrates a `console`
   schema there at start, and refuses to start against a newer schema than it knows. It may be on
   the same server as the product's database, but it never needs the product's credentials.
2. **Make a key pair** for each scheduler it will connect to (or one for all):

   ```sh
   downbeat-console keygen /secrets/console-production.pem
   ```

   This writes the private key (PKCS#8 PEM, readable by its owner only, and never overwriting an
   existing file) and prints the public key. Keep the private key in your secret store.
3. **Trust the console on each scheduler.** Add it to the scheduler's
   [callers file](../reference/configuration.md#the-callers-file):

   ```toml
   audience = "market-production"

   [[caller]]
   name = "console"
   keys = ["<the printed public key>"]
   max_scope = "administer"
   acts_for_people = true
   ```

   `acts_for_people = true` lets the console's tokens name a person as the actor. Without it, a
   caller may only act as itself. `max_scope` caps what any person can do through this console on
   this scheduler.
4. **Register the console with your identity provider** as an OpenID Connect client using the
   authorization-code flow. Its redirect URI is `<public_url>/auth/callback`.
5. **Write the configuration file** and start it:

   ```sh
   DOWNBEAT_CONSOLE_DATABASE_URL=postgres://… CONSOLE_OIDC_SECRET=… \
     downbeat-console --config /etc/downbeat/console.toml
   ```

6. Put it behind HTTPS. The session cookie is marked `Secure`, so browsers send it only over an
   encrypted connection.

### Configuration file

```toml
listen = "0.0.0.0:7401"
public_url = "https://jobs.example.com"
first_administrators = ["ops-lead@example.com"]
session_idle_hours = 12
session_absolute_hours = 168

[sign_in]
issuer = "https://login.example.com"
client_id = "downbeat-console"
client_secret_env = "CONSOLE_OIDC_SECRET"
allowed_domains = ["example.com"]
allowed_groups = []
groups_claim = "groups"

[[scheduler]]
name = "production"
label = "Market production"
url = "https://scheduler.internal:7400"
audience = "market-production"
caller = "console"
key_file = "/secrets/console-production.pem"
```

Every field, its default and its checks are in the
[configuration reference](../reference/configuration.md#the-console-configuration-file). Secrets
never go in this file: the client secret and the signing key come from the environment or from
files it names, and the database URL from `DOWNBEAT_CONSOLE_DATABASE_URL`.

## Signing in

The console uses OpenID Connect with the authorization-code flow, PKCE (`S256`), `state` and
`nonce`. It holds no passwords.

- At start it reads the provider's configuration from the issuer's discovery document. It accepts
  identity tokens signed with any asymmetric algorithm the provider lists, never `none` or a
  shared-secret (HMAC) algorithm.
- It asks for the `openid`, `email` and `profile` scopes. The identity token must carry an
  `email`; groups are read from the claim named by `groups_claim`.
- A person may sign in when their email's domain is in `allowed_domains`, or one of their groups
  is in `allowed_groups`. The console refuses to start with both lists empty. A refused sign-in is
  written to the console's audit.
- The console records the person (the provider's stable subject id, their email, name and groups)
  and starts a session: a random id in a cookie that scripts cannot read
  (`HttpOnly; Secure; SameSite=Strict`). Only a hash of the id is stored. A session ends after
  `session_idle_hours` without use, after `session_absolute_hours` in all, or at sign-out.
- Every request that is not a GET must carry the session's CSRF token in an `x-csrf-token` header.

The sign-in flow is tested end to end only against a small fake provider
([`tests/e2e/tests/sign_in.rs`](../../tests/e2e/tests/sign_in.rs)).

## Who may do what: grants

A **grant** gives a person, or a group the identity provider reports, a scope on one scheduler,
optionally limited to some pipelines.

| Field | Values |
| --- | --- |
| To | A person (by email; they must have signed in once) or a group |
| Scheduler | The `name` of one `[[scheduler]]` |
| Scope | `read`, `operate` or `administer`: the same words as the control API |
| Pipelines | All, or a list of pipeline names |

- With no grant, a person sees the scheduler listed and nothing in it.
- The **first administrators**, listed by email in the configuration, administer every scheduler
  without any grant, so the console can never be locked.
- Each request carries exactly the scope it needs, limited to the pipelines of every grant that
  reaches that scope. A read is signed for `read`; a change for `operate`, and again for
  `administer` if the scheduler says the action needs it.
- A grant limited to pipelines becomes a list of jobs in the token. The console learns which
  jobs are in which pipeline from the scheduler, and remembers it for a minute.
- Grants are made and removed by people who administer the whole scheduler (every pipeline), on
  the Access screen or with `POST /api/grants`, so nobody can grant more than they hold. Each
  change is audited with who made it.
- The identity provider must report the person's email as verified; grants follow emails.

The [specification](../spec.md#15-the-consoles-access-model) has the exact rules, and the
[console contract](../console-contract.md) the console's own HTTP API.

## The screens

Each screen is a view of control API endpoints, and shows only what the person may see. An
action they lack the scope for is not offered, and every action shows its preview before it is
confirmed. When the console connects to several schedulers, one is in view at a time.

| Screen | Answers |
| --- | --- |
| Overview | Is anything failing, stuck, slow, overdue or held right now? |
| Calendar | What runs on a date, after day rules, overrides and pauses. Dated overrides are added here. |
| Pipelines and Pipeline | Each pipeline's jobs and links, coloured by its recent chains. |
| Chain | One chain job by job, with reasons, "retry this step", and cancel. |
| Jobs and Job | What exists; for one job, what starts it, its policies, its runs; start, pause, backfill. |
| Runs | One list of runs, filtered by status, reason, job, pipeline, queue, time or an input field. |
| Run | The reason first, then timing, every call, progress, input, output, error and logs; cancel, check now, retry, restart, mark as succeeded. |
| Queues | Queued and running per queue, why the queued ones wait, the workers serving each; pause. |
| Audit | Who did what to jobs (from the scheduler), and sign-ins and grant changes (from the console). |
| Access | Grants, for people who administer a scheduler. |

## Versions

The console and a scheduler are released separately, so the control API is versioned. A console
supports the API version it was built with and the one before. `GET /api/me` reports each
scheduler's version and whether it is supported.
