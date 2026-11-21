# downbeat-client

A Rust client for [downbeat](https://github.com/downbeat-rs/downbeat)'s control API, for programs outside the product's binary: HTTPS only, every request signed, starts retried safely with idempotency keys.

```rust
let scheduler = downbeat_client::Client::builder("https://scheduler.internal:7400")
    .caller("billing-service")
    .audience("market-production")
    .key_from_env("DOWNBEAT_KEY")?
    .connect()
    .await?;
scheduler.start("daily_report", &serde_json::json!({ "date": "2026-11-20" })).send().await?;
```

See [the control API and the client](https://github.com/downbeat-rs/downbeat/blob/main/docs/guide/07-api-and-client.md).

Licensed under the MIT license.
