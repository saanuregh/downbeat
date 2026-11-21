# Contributing to downbeat

Thank you for helping. This page says how to set up, how to check your change, and the rules the
code follows. Read [docs/architecture.md](docs/architecture.md) first if you are changing the
engine, and [docs/spec.md](docs/spec.md) for the rules it must keep.

## Setup

You need:

- Linux. The worker uses process groups, the parent-death signal and `/proc`, and the tests run
  real worker processes.
- [mise](https://mise.jdx.dev), which installs the tools pinned in [`mise.toml`](mise.toml)
  and locked in [`mise.lock`](mise.lock): Bun for the console's screens, and actionlint and
  shellcheck. The Rust toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml) and
  installed by rustup.
- [Podman](https://podman.io), which runs the tests' throwaway Postgres in a container. The tests
  need no running database and no credentials.

Then:

```sh
mise install
mise run check
```

## Tasks

`mise tasks` lists them all. The longer ones are plain bash files in
[`mise-tasks/`](mise-tasks), which run without mise too.

| Command | What it does |
| --- | --- |
| `mise run check` | Everything below except `web`, `demo` and `measure`. Run it before you open a pull request. |
| `mise run fmt` / `fmt-check` | Format the Rust code and the console's screens, or fail if anything is not formatted. |
| `mise run lint` | Clippy with warnings as errors, the screens' type-check and oxlint, actionlint on the CI workflow, and shellcheck on the task scripts. |
| `mise run doc` | Build the public crates' API docs, warnings as errors. |
| `mise run snapshot` | Fail if the example product's schedule snapshot is stale. |
| `mise run test` | `test-web` and `test-rust`. |
| `mise run test-web` | The screens' unit tests. |
| `mise run test-rust [args]` | Every Rust test against a throwaway Postgres; arguments go to `cargo test`. |
| `mise run web` | Install, type-check and build the console's screens. |
| `mise run demo` | Run the example product and the console locally, with a throwaway database. |
| `mise run measure` | Measure the engine's hot paths ([docs/performance.md](docs/performance.md)). |

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) does not use mise. It installs its own
tools, runs the same steps except actionlint, and runs `mise-tasks/test-rust` directly, with the
Dex sign-in test always on.

## How the tests run

[`mise-tasks/test-rust`](mise-tasks/test-rust) starts Postgres in a podman container (the image is
named in [`mise-tasks/postgres.sh`](mise-tasks/postgres.sh)), with its data in memory, trust
authentication and its port on 127.0.0.1 only, and removes it on exit. It exports
`DOWNBEAT_TEST_DATABASE_URL`, builds every binary (the end-to-end tests start the console binary),
and runs `cargo test --workspace`. Arguments are passed to `cargo test`:

```sh
mise run test-rust -p downbeat-e2e --test runs     # one end-to-end test file
mise run test-rust -p downbeat schedule            # unit tests whose name matches
```

| Layer | Where | Use it for |
| --- | --- | --- |
| Unit | `#[cfg(test)] mod tests` beside the code | Pure calculation and parsing with many edge cases: schedule times, hashes, tokens, access rules. |
| Authoring | `crates/downbeat/tests/authoring.rs` | The product-facing API compiles and behaves as the guide says. |
| Check | `crates/downbeat/tests/check.rs`, `registry_problems.rs` | `check` names each mistake. |
| Compile errors | `crates/downbeat/tests/macro_signatures.rs` with `compile_fail/` | `trybuild` cases: a wrong job or command signature does not compile, with a clear message. Keep only cases whose compiler output lists no engine internals. |
| Engine steps | `crates/downbeat/tests/pruning_rules.rs` and others that use `downbeat::measure` | One engine step against the throwaway cluster, run through `mise run test-rust`. |
| End to end | `tests/e2e/tests/` | Behaviour through real processes: a test product (`tests/e2e/src/lib.rs`) as scheduler and workers, the API through `downbeat-client`, the console. |
| Real sign-in | `tests/e2e/tests/sign_in_dex.rs` | The console signing a person in through Dex. Needs `DOWNBEAT_TEST_DEX` naming a `dex` binary; `mise-tasks/test-rust` sets it when `dex` is on `PATH` (for example `nix shell nixpkgs#dex-oidc`). Otherwise it reports itself skipped. CI always runs it. |
| Screens | `tests/e2e/tests/screens.rs` | Every console screen renders its data in headless Chromium with no error. Runs when `DOWNBEAT_TEST_BROWSER` names a Chromium-based browser or one is on `PATH`; CI always runs it. |
| Docs | `tests/guide` | Every Rust sample in the README and the user guide compiles; samples written with "…" are syntax templates and are skipped. A sample may use the names in `tests/guide/src/scaffold.rs`. |
| Example | `examples/market/tests` | The test kit (`downbeat::testing`) on a real definition: schedules on any date, with no database. |
| Measurement | `tests/e2e/tests/measure.rs` | Opt-in (`-- --ignored`): the numbers in [docs/performance.md](docs/performance.md). |

Prefer an end-to-end test that drives the public API through a realistic scenario. Each test gets
its own database (`Database::fresh()`), so tests run in parallel. Add a job to the test product
when a scenario needs one: general jobs in `tests/e2e/src/lib.rs`, and jobs for one area's rules in
its `*_rules.rs` module. A schedule that must not fire in other tests belongs in the `rules`
environment (or one of its own), which only tests that ask for it start. To see the API's raw HTTP answers
(status codes, error bodies, a reused token), sign requests with `support::Signer` rather than
`downbeat-client`, which retries.

If you change `examples/market`, update its snapshot:

```sh
cargo run -q -p market -- check --snapshot examples/market/schedules.snapshot
```

## Code style

The code follows these rules; review applies them.

- **Plain, self-describing names.** `queued_because`, `start_next_date`, `take_back_lost`. Use the
  domain's own words (run, chain, turn, fence, occurrence) as the guide defines them.
- **Comments say why, not what.** A comment explains a non-obvious reason, an invariant or an
  intent. Do not restate the code.
- **Every module starts with a `//!` doc** saying what it owns. Public items have `///` docs.
- **Formatting and lints.** `rustfmt` with [`rustfmt.toml`](rustfmt.toml) (120 columns), and
  Clippy's pedantic set with warnings as errors. `unsafe` is denied except where a process is
  spawned (`worker/process.rs`, `child.rs`), and each `unsafe` block has a `SAFETY` comment.
- **One place for each rule.** Reason sentences live in `reasons.rs`; every way a run ends goes
  through `store::ending`; every cancel through `store::cancel`; starting through `starting.rs`.
- **Errors keep their source.** Add context; never swallow or replace an error.
- **Typed errors in the libraries.** The library crates use `thiserror` enums and never `anyhow`;
  only applications (the console) use `anyhow` ([decision 69](docs/decisions.md)).
- **Dependencies.** Justify a new dependency against the standard library and what the workspace
  already uses, in the pull request. Versions are set once in the workspace `Cargo.toml`.
- **Decisions.** When the code must choose something the design leaves open, or departs from it,
  add a numbered entry to [docs/decisions.md](docs/decisions.md) with the reason.
- **Docs.** A change to behaviour updates the guide, the reference and the specification in the
  same pull request.

## Adding a migration

The engine's schema is versioned in
[`crates/downbeat/src/store/migrate.rs`](crates/downbeat/src/store/migrate.rs).

1. Add `crates/downbeat/migrations/000N_<what>.sql`. Qualify every name with the `downbeat`
   schema.
2. Add an entry to `MIGRATIONS` with the next `version`, a `name`, and `breaking`.
3. Keep it additive (new tables, nullable or defaulted columns, new indexes), so the previous
   engine keeps working during a rolling deploy. Set `breaking: true` only when an older engine
   must not run against the new schema; it then refuses to start.
4. Never edit a migration that has been released.
5. Each migration runs in one transaction under an advisory lock. Cover it with an end-to-end
   test that exercises the new column or table.

The console's schema is simpler: a list of `(version, sql)` in
[`crates/downbeat-console/src/store.rs`](crates/downbeat-console/src/store.rs), with the files in
`crates/downbeat-console/migrations/`. There is no `breaking` flag: a console refuses to start
against any newer schema.

## Pull requests

- Keep a pull request to one change. Describe what changes for users, and what else it could
  affect.
- Run `mise run check` and say so.
- For a change to the console's screens, run `mise run demo`, look at the changed screens in light
  and dark themes and at phone width, and attach screenshots.
- By contributing, you agree that your contribution is licensed under the MIT license, as the
  [README](README.md#license) says.
