# Configuration reference

Two files configure downbeat at runtime. Both are TOML, and both reject unknown fields, so a typo
fails at start instead of silently widening or narrowing access. Everything else (jobs,
schedules, policies, timings) is code in your product's definition.

## The callers file

Read by a scheduler started with `--callers <file>` (or `DOWNBEAT_CALLERS`). It names the
scheduler's audience and every caller it trusts. Source:
[`crates/downbeat/src/api/mod.rs`](../../crates/downbeat/src/api/mod.rs).

```toml
# The name tokens must carry in `aud` to be for this scheduler. Give each scheduler its own,
# such as the product and environment.
audience = "market-production"

# The console: may act for the people it signs in, up to administer, on every job.
[[caller]]
name = "console"
keys = ["<console public key, as keygen prints it>"]
max_scope = "administer"
acts_for_people = true

# A service that only starts its own report, as itself. Two keys while one is being rotated.
[[caller]]
name = "billing-service"
keys = [
  "<new public key>",
  "<old public key>",
]
max_scope = "operate"
jobs = ["daily_report"]
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `audience` | string | Yes | This scheduler's name in tokens' `aud`. |
| `[[caller]]` | table, repeated | No | One trusted caller. With none, every request is refused. |
| `caller.name` | string | Yes | The caller's name, matched against tokens' `iss`. Must be unique. |
| `caller.keys` | array of strings | Yes, non-empty | Ed25519 public keys, each the base64url (no padding) encoding of 32 bytes, as `downbeat-console keygen` prints them. A token signed by any of them is accepted. |
| `caller.max_scope` | `"read"`, `"operate"` or `"administer"` | Yes | The most any of its tokens may ask for. |
| `caller.jobs` | array of strings | No | When set, the caller may touch only these jobs, and every token it signs must list jobs within them. |
| `caller.acts_for_people` | boolean | No, default `false` | Whether its tokens may name an actor other than the caller itself. Set it for a console. |

The file is read once at start. To add, limit, cut off a caller, or rotate a key, edit the file
and restart the scheduler. Rotate a key by adding the new one, switching the caller to it, then
removing the old one.

A scheduler started with `--local` reads no callers file. It trusts one caller, `local`, holding
the published development key, with `administer` and `acts_for_people`, and audience `local`.

## The console configuration file

Read by `downbeat-console --config <file>` (or `DOWNBEAT_CONSOLE_CONFIG`). Source:
[`crates/downbeat-console/src/config.rs`](../../crates/downbeat-console/src/config.rs). Secrets
are never in this file: the database URL comes from `DOWNBEAT_CONSOLE_DATABASE_URL`, the
identity provider's client secret from the variable `client_secret_env` names, and each signing
key from a file or a variable.

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
allowed_groups = ["job-operators"]
groups_claim = "groups"

[[scheduler]]
name = "production"
label = "Market production"
url = "https://scheduler.internal:7400"
audience = "market-production"
caller = "console"
key_file = "/run/secrets/console-production.pem"

[[scheduler]]
name = "staging"
url = "https://scheduler.staging.internal:7400"
audience = "market-staging"
key_env = "CONSOLE_STAGING_KEY"
```

### Top level

| Field | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `listen` | socket address | Yes | | Where the console listens, such as `0.0.0.0:7401`. |
| `public_url` | string | Yes | | The address people reach the console at. Sign-in redirects back to `<public_url>/auth/callback`. A trailing `/` is removed. Must be `https`, or `http` on a loopback host such as `127.0.0.1` or `localhost`. |
| `first_administrators` | array of emails | No | `[]` | People who administer every scheduler without any grant, matched by email, ignoring case. |
| `session_idle_hours` | integer | No | `12` | A session ends after this long without use. |
| `session_absolute_hours` | integer | No | `168` (7 days) | A session ends this long after sign-in, whatever its use. |
| `[sign_in]` | table | Yes | | How people sign in. |
| `[[scheduler]]` | table, repeated | Yes, at least one | | Each scheduler the console connects to. |

### `[sign_in]`

| Field | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `issuer` | URL | Yes | | The OpenID Connect issuer. Its configuration is discovered from `<issuer>/.well-known/openid-configuration` at start. |
| `client_id` | string | Yes | | The console's client id at the provider. |
| `client_secret_env` | string | Yes | | The name of the environment variable that holds the client secret. The console refuses to start when it is not set. |
| `allowed_domains` | array of strings | One of the two must be non-empty | `[]` | People whose email is in one of these domains may sign in. |
| `allowed_groups` | array of strings | | `[]` | People in one of these provider groups may sign in. |
| `groups_claim` | string | No | `"groups"` | The identity token claim that lists a person's groups, as an array of strings. |

### `[[scheduler]]`

| Field | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `name` | string | Yes | | A short name, used in URLs (`/api/schedulers/<name>/…`) and in grants. Unique among the schedulers. |
| `label` | string | No | `name` | The name people see. |
| `url` | URL | Yes | | The scheduler's control API base address, such as `https://scheduler.internal:7400`. A trailing `/` is removed. |
| `audience` | string | Yes | | That scheduler's `audience`, from its callers file. |
| `caller` | string | No | `"console"` | The console's `name` in that scheduler's callers file. |
| `key_file` | path | Exactly one of the two | | A file holding the console's private key for this scheduler, as PKCS#8 PEM. |
| `key_env` | string | | | An environment variable holding that PEM instead. |

The matching callers-file entry on the scheduler needs `acts_for_people = true`, and a
`max_scope` as high as anything people should do there.

### Local mode

`downbeat-console --local` reads no file. It listens on `127.0.0.1:7401` (or `--listen`, which
must be a loopback address), connects to one scheduler named `local` at
`http://127.0.0.1:7400` (or `--scheduler`) with the development key, caller `local` and audience
`local`, and has no sign-in: every request is the stand-in person `local@localhost`, who
administers everything. Sessions keep their default lengths.
