# downbeat-console

The console of [downbeat](https://github.com/downbeat-rs/downbeat): OpenID Connect sign-in, grants by scheduler, scope and pipeline, and the screens for seeing and controlling jobs, in front of any number of schedulers.

From a checkout of the repository (the screens are built with Bun and compiled into the binary):

```sh
(cd crates/downbeat-console/web && bun install --frozen-lockfile && bun run build)
cargo build --release -p downbeat-console
target/release/downbeat-console --local   # beside a scheduler started with --local
```

Deploying it is described in [the console guide](https://github.com/downbeat-rs/downbeat/blob/main/docs/guide/06-console.md).

Licensed under the MIT license.
