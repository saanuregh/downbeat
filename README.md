# downbeat

downbeat schedules, chains, retries and observes jobs that you write as typed async Rust
functions. You embed it as a library in your own binary, and it needs only your existing
Postgres. Each run happens in a child process that the worker supervises. Every state comes with
one plain sentence that says why. A signed control API and a separate web console let people and
programs see and act on everything.

## Why downbeat

- **Postgres is the only dependency.** It needs no broker, no leader election and no extra
  service. Runs, schedules, leases and history live in a `downbeat` schema in your database.
- **Jobs are typed.** A job is an `async fn` with `#[job]`. Its input and output are Rust types.
  A link from one job to another fails to compile when the types do not fit.
- **Every outcome has a reason.** Every queued, skipped, failed or expired run, and every
  occurrence that did not run, carries a sentence such as "Skipped: input unchanged since the run
  at 19:15."
- **Child processes keep failures contained.** A function that panics, aborts, leaks memory or
  hangs breaks only its own run. The worker kills the child, records what happened and goes on.
  Leases and fencing (a counter that locks out a worker that lost its run) make sure a run is
  recorded as ended exactly once.
- **The control API is signed.** Each request carries a short-lived Ed25519 token that is bound
  to that exact request. The scheduler holds only public keys, so it cannot forge a request.
- **The console is a separate program.** It signs people in with OpenID Connect, decides what
  each person may do, and serves the screens. It contains no product code and holds no product
  credentials.

## A taste

This code is taken from [`examples/market`](examples/market/src/lib.rs). Every half hour during
market hours the product fetches prices. Whenever they changed, it builds candles.

```rust
#[job(attempts = 3, backoff = 10.seconds()..=2.minutes(), keep_runs_at_most = 50)]
async fn fetch_prices(run: Run, day: TradingDay, exchange: &Exchange) -> Result<Prices> {
    run.progress("asking the exchange");
    let count = exchange.prices_for(day);
    if count == 0 {
        return skip("the exchange has published nothing yet");
    }
    Ok(Prices { day, count, digest: format!("{:x}", u64::from(count) * 2_654_435_761), fetched_at: chrono_now() })
}

#[job(preset = heavy, timeout = 30.minutes(), max_memory = 512.mb())]
async fn build_candles(run: Run, prices: Prices) -> Result<Candles> {
    let mut built: u32 = run.last_checkpoint().unwrap_or(0);
    while built < prices.count {
        built = (built + 10).min(prices.count);
        run.progress(format!("{built} of {} candles", prices.count));
        run.checkpoint(&built)?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if run.is_cancelled() {
            return Err(JobError::msg("stopped on request"));
        }
    }
    Ok(Candles { day: prices.day, rows: built })
}

fn market_hours(pipeline: &mut Pipeline) {
    pipeline.job(fetch_prices).schedule(
        every(30.minutes())
            .between("09:15", "15:30")
            .on_weekdays([Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri])
            .by_calendar(trading_days)
            .input(|occurrence| TradingDay { date: occurrence.date() }),
    );
    pipeline.job(build_candles).after(fetch_prices).if_changed();
    pipeline.job(export_csv).after(build_candles);
}
```

Each job's policies (`attempts`, `timeout`, `max_memory`) are written on the job itself. When it
runs and what it follows are written in the pipeline function.

## Quick start

You need Linux, the Rust toolchain named in [`rust-toolchain.toml`](rust-toolchain.toml), and
PostgreSQL's server programs (`initdb`, `pg_ctl`) on your `PATH`, or `PG_BIN` set to their
directory.

With [mise](https://mise.jdx.dev), which installs the pinned tools (Bun, for the screens):

```sh
mise install
mise run demo
```

This starts a throwaway Postgres, runs the example product as scheduler and worker, and starts
the console without sign-in. Open <http://127.0.0.1:7401>. Ctrl-C stops everything and removes
the database.

Without mise, with a running Postgres and [Bun](https://bun.sh): build the screens first (they
are compiled into the console), create a database, and run the two programs:

```sh
(cd crates/downbeat-console/web && bun install --frozen-lockfile && bun run build)
createdb downbeat_demo
export DOWNBEAT_DATABASE_URL=postgres://localhost/downbeat_demo
export DOWNBEAT_CONSOLE_DATABASE_URL=$DOWNBEAT_DATABASE_URL DOWNBEAT_ENVIRONMENT=dev
cargo run -p market -- scheduler worker --queues default=4,heavy=1 --local &
cargo run -p downbeat-console -- --local
```

Without the screens, the console's API still works, but its pages say that the screens were not
built.

To add downbeat to your own product, read [Getting started](docs/guide/01-getting-started.md).

## Crates

| Crate | What it is |
| --- | --- |
| [`downbeat`](crates/downbeat) | The engine: jobs, pipelines, schedules, the scheduler and worker roles, the control API. Your product depends on this one. |
| [`downbeat-macros`](crates/downbeat-macros) | `#[job]`, `#[command]` and `#[derive(JobData)]`. Re-exported by `downbeat`. |
| [`downbeat-api`](crates/downbeat-api) | The control API's request and response types, and the signing and checking of request tokens. |
| [`downbeat-client`](crates/downbeat-client) | A Rust client for the control API, for programs outside your product's binary. |
| [`downbeat-console`](crates/downbeat-console) | The console: sign-in, grants and the screens, for any number of schedulers. |
| [`examples/market`](examples/market) | A small, complete example product. |
| [`tests/e2e`](tests/e2e) | End-to-end tests that run a test product as real processes. |

## Documentation

- [Documentation index](docs/README.md)
- [User guide](docs/guide/01-getting-started.md): how to write jobs, pipelines and schedules, and
  how to run them
- [Behavioural specification](docs/spec.md): the exact rules
- [Architecture](docs/architecture.md): for contributors
- [Reference](docs/reference/cli.md): command line, configuration, HTTP API

## Status

downbeat is at version 0.1 and has not been released yet. Its APIs, its database schema and its
control API may change before 1.0. The crates need Rust 1.98 or newer. It runs on Linux only, because it relies on process groups,
the parent-death signal and `/proc`. See [CHANGELOG.md](CHANGELOG.md) for what 0.1 contains, and
the guide for [known limits](docs/guide/05-running.md#known-limits).

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [code of conduct](CODE_OF_CONDUCT.md). Please
report vulnerabilities as described in [SECURITY.md](SECURITY.md), not in a public issue.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option. Unless you explicitly state otherwise, any
contribution you intentionally submit for inclusion in this work, as defined in the Apache-2.0
license, is dual licensed as above, without any additional terms or conditions.
