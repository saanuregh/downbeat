# downbeat

The engine of [downbeat](https://github.com/downbeat-rs/downbeat): jobs written as typed async Rust functions, pipelines that chain them, schedules, and the scheduler and worker roles that run them, with Postgres as the only dependency.

```rust
use downbeat::prelude::*;

#[job(attempts = 3, timeout = 10.minutes())]
async fn send_report(run: Run, day: NaiveDate) -> JobResult<()> {
    run.progress("rendering");
    Ok(())
}
```

Start with the [user guide](https://github.com/downbeat-rs/downbeat/blob/main/docs/guide/01-getting-started.md).

Licensed under the MIT license.
