# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it through GitHub's
[private vulnerability reporting](https://github.com/downbeat-rs/downbeat/security/advisories/new)
with a description, the affected version or commit, and steps to reproduce. Only the maintainers
see it. We aim to acknowledge a report within a few working days, and to agree a disclosure date
with you.

## Supported versions

downbeat is at 0.1 and not yet released. Until 1.0, only the latest release (and the default
branch) receives security fixes.

## Threat model in brief

downbeat runs a product's own code with the product's own credentials. It is built for one
product's environment, not for tenants who distrust each other. The full set of rules is in the
[specification](docs/spec.md); this is a summary.

| Party | Trusted to |
| --- | --- |
| Code in the product (jobs, commands, callbacks, day rules) | Do anything; it is reviewed and deployed like any code. |
| A trusted caller of the control API (a console, a service) | Make requests up to the scope and jobs its scheduler's callers file allows. |
| A person | Whatever their grants in the console allow. Schedulers never see people, only what the console signed. |
| The identity provider | Say who a person is and which groups they belong to. |
| Anyone else, including others on the private network | Nothing. |

What downbeat protects against:

- **One run harming others.** Every function runs in a child process, killed at its timeout,
  memory limit, cancel or a lost lease. Children never write to the engine's tables, and a child
  dies with its worker (parent-death signal).
- **A worker that lost its run still writing.** Every write carries a fencing counter; once a run
  is taken back, the old worker's writes change nothing.
- **Command injection.** A command's arguments are a list passed without a shell; a value can
  never add an argument, and a value that looks like an option is refused unless allowed. A
  command receives only the environment variables it names.
- **Forged or replayed API requests.** Each request carries an Ed25519-signed token that expires
  within a minute, is addressed to one scheduler, is bound to the request's method, path, query
  and body, and (for a changing request) is accepted once. Schedulers hold only public keys.
- **A caller or person exceeding its rights.** Each caller has a maximum scope and optionally a
  list of jobs. The console checks a person's grants, then the scheduler checks the console's
  token again.
- **Untraceable actions.** The actor comes from the verified token, and every change writes an
  audit row naming the actor and the caller.
- **Runtime changes without review.** The API cannot create or edit jobs, schedules, policies,
  queues or commands. Only pauses and dated overrides change without a deploy, and both need the
  `administer` scope.
- **Console sessions.** OpenID Connect with PKCE, `state` and `nonce`; sessions in an
  `HttpOnly; Secure; SameSite=Strict` cookie, stored hashed; a CSRF token on every change; a
  strict Content Security Policy for the screens.
- **The local try-out mode used for real.** `--local` uses a published development key, so both
  the scheduler and the console refuse it unless they listen on a loopback address.

What downbeat does not protect against, and leaves to you:

- A job misusing files, the network or shared credentials: children run as the same user, with
  the same credentials, and are not sandboxed.
- A job using all CPU: there is no CPU limit. Use a one-slot worker in a sized container.
- Exposure of the control API: keep it on a private network and encrypt the connection with a
  proxy or load balancer. The console must be served over HTTPS.
- Key management: generating each caller's key pair, keeping private keys in a secret store, and
  rotating them by overlap.
- Secrets in data: never put secrets in inputs, outputs, progress, checkpoints or error messages.
  Use the definition's `redact` hook for a failed command's standard error.
- Database roles: the engine needs its own schema only; what jobs can reach is the product's
  choice.

Known limits are listed in the guide under
[known limits](docs/guide/05-running.md#known-limits).
