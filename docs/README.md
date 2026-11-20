# downbeat documentation

Start with the guide if you are adopting downbeat. Read the specification when you need the exact
rule, and the architecture when you change the engine.

## User guide

| Document | What it covers |
| --- | --- |
| [1. Getting started](guide/01-getting-started.md) | Add the crate, write a first job and a definition, run it on your machine. |
| [2. Jobs](guide/02-jobs.md) | Job functions, inputs and outputs, how a function ends, policies and presets, commands, dependencies, tests. |
| [3. Pipelines](guide/03-pipelines.md) | Links, `.map`, joins, failure links, needs, conditions, `if_changed`, chains, the same-work rules. |
| [4. Schedules](guide/04-schedules.md) | `daily_at`, `every`, windows, day rules, overrides, expiry, environments, zones, deploys. |
| [5. Running in production](guide/05-running.md) | Roles, the command line, queues and slots, memory, shutdown, health checks, migrations, CI checks. |
| [6. The console](guide/06-console.md) | Deploying the console, OpenID Connect, keys, grants, local mode, the screens. |
| [7. The API and the client](guide/07-api-and-client.md) | Signed tokens from any language, `downbeat-client`, `InAppClient`, idempotency. |
| [8. Operations](guide/08-operations.md) | Reading reasons, pausing, overrides, backfills, retry and restart, callbacks, alerting. |

## Reference

| Document | What it covers |
| --- | --- |
| [Command line](reference/cli.md) | Every command, flag and environment variable of a product binary and of `downbeat-console`. |
| [Configuration](reference/configuration.md) | The callers file and the console's configuration file, field by field. |
| [HTTP API](reference/http-api.md) | Every control API endpoint, with its scope, types, preview support and errors. |
| [Console contract](console-contract.md) | The HTTP API the console server gives its screens. |

## For contributors

| Document | What it covers |
| --- | --- |
| [Specification](spec.md) | The behavioural rules, stated normatively and tied to the modules that implement them. |
| [Architecture](architecture.md) | Processes, crates and modules, the data model, the main flows, failure handling, tests. |
| [Decisions](decisions.md) | Choices made while building, with their reasons. |
| [Contributing](../CONTRIBUTING.md) | Setup, checks, tests, code style, migrations. |
| [Security policy](../SECURITY.md) | Threat model summary and how to report a vulnerability. |
| [Changelog](../CHANGELOG.md) | What each release contains. |
| [Performance](performance.md) | What the hot paths cost, measured, and how to measure them again. |
