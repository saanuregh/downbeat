# Contributing to downbeat

Thank you for helping. This page says how to set up, how to check your change, and the rules the
code follows. Read [docs/architecture.md](docs/architecture.md) first if you are changing the
engine, and [docs/spec.md](docs/spec.md) for the rules it must keep.

## Setup

You need:

- Linux. The worker uses process groups, the parent-death signal and `/proc`, and the tests run
  real worker processes.
- [mise](https://mise.jdx.dev), which installs the pinned tools from [`mise.toml`](mise.toml)
  (Bun, for the console's screens). The Rust toolchain is pinned in
  [`rust-toolchain.toml`](rust-toolchain.toml) and installed by rustup.
- PostgreSQL's server programs (`initdb`, `pg_ctl`, `postgres`). The tests start their own
  throwaway cluster, so you need no running database and no credentials. If the programs are not
  on your `PATH`, set `PG_BIN` to their directory, for example in a `mise.local.toml` (it is
  git-ignored):

  ```toml
  [env]
  PG_BIN = "/usr/lib/postgresql/16/bin"
  ```

Then:

```sh
mise install
mise run check
```

## Tasks

| Command | What it does |
| --- | --- |
| `mise run check` | Format check, Clippy, and every test. Run it before you open a pull request. |
| `mise run fmt` | Format the Rust code and the console's screens. |
| `mise run lint` | `cargo clippy --workspace --all-targets -- -D warnings`. |
| `mise run test` | Every test, through `scripts/test.sh`. |
| `mise run web` | Install, type-check and build the console's screens. |
| `mise run demo` | Run the example product and the console locally, with a throwaway database. |

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) does not use mise. It runs the same
checks, plus the screens' lint and unit tests and a check that the example's schedule snapshot is
current.

## How the tests run

[`scripts/test.sh`](scripts/test.sh) starts a Postgres cluster in a temporary directory, with
trust authentication and a Unix socket only, and removes it on exit. It exports
`DOWNBEAT_TEST_DATABASE_URL`, builds every binary (the end-to-end tests start the console binary),
and runs `cargo test --workspace`. Arguments are passed to `cargo test`:

```sh
scripts/test.sh -p downbeat-e2e --test runs     # one end-to-end test file
scripts/test.sh -p downbeat schedule            # unit tests whose name matches
```

| Layer | Where | Use it for |
| --- | --- | --- |
| Unit | `#[cfg(test)] mod tests` beside the code | Pure calculation and parsing with many edge cases: schedule times, hashes, tokens, access rules. |
| Authoring | `crates/downbeat/tests/authoring.rs` | The product-facing API compiles and behaves as the guide says. |
| Check | `crates/downbeat/tests/check.rs` | `check` names each mistake. |
| End to end | `tests/e2e/tests/` | Behaviour through real processes: a test product (`tests/e2e/src/lib.rs`) as scheduler and workers, the API through `downbeat-client`, the console. |
| Real sign-in | `tests/e2e/tests/sign_in_dex.rs` | The console signing a person in through Dex. Runs when `dex` is on `PATH` (for example `nix shell nixpkgs#dex-oidc`) or `DOWNBEAT_TEST_DEX` names it; otherwise it reports itself skipped. |
| Docs | `tests/guide` | Every Rust sample in the README and the user guide compiles; samples written with "…" are syntax templates and are skipped. A sample may use the names in `tests/guide/src/scaffold.rs`. |
| Example | `examples/market/tests` | The test kit (`downbeat::testing`) on a real definition: schedules on any date, with no database. |
| Measurement | `tests/e2e/tests/measure.rs` | Opt-in (`-- --ignored`): the numbers in [docs/performance.md](docs/performance.md). |

Prefer an end-to-end test that drives the public API through a realistic scenario. Each test gets
its own database (`Database::fresh()`), so tests run in parallel. Add a job to the test product
when a scenario needs one, and name it in one of its pipelines.

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

The console's schema works the same way, in
[`crates/downbeat-console/src/store.rs`](crates/downbeat-console/src/store.rs) and
`crates/downbeat-console/migrations/`. A console refuses to start against any newer schema.

## Pull requests

- Keep a pull request to one change. Describe what changes for users, and what else it could
  affect.
- Run `mise run check` and say so.
- By contributing, you agree that your contribution is dual licensed under MIT OR Apache-2.0, as
  the [README](README.md#license) says.
