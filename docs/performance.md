# Performance

What the engine's hot paths cost, measured against a real Postgres. The measurement is
[`tests/e2e/tests/measure.rs`](../tests/e2e/tests/measure.rs); run it alone, printing the numbers, with:

```sh
mise run measure
```

## Results

Measured on 2026-11-20 on one laptop (16 cores), with Postgres 18 on the same machine over a Unix
socket and `fsync=off`, as `mise-tasks/test-rust` starts it. A production database with durable writes
and a network between it and the workers is slower per statement; the shape of the results is what
carries over.

| What | Result |
| --- | --- |
| Claiming a run, with 100,000 runs queued in one queue | mean 0.23 ms, 95th percentile 0.33 ms (500 claims) |
| A heartbeat of a running run | mean 0.12 ms, 95th percentile 0.16 ms (500 heartbeats) |
| 2,000 queued runs on one worker with 8 slots | 0.81 s, about 2,500 runs a second |
| One run alone, from the request to its end, on a reused child | mean 203 ms |
| The same on a fresh child (`fresh_process`) | mean 213 ms; 95th percentile 404 ms |
| One pruning pass over 60,000 chains past their retention | 8.0 s; every chain removed except the one holding the job's latest success (9 s before pruning moved to lock-then-check, [decision 47](decisions.md#47-pruning-locks-first-then-checks)) |

## What the numbers say

- **The claim does not slow down with a long queue.** It walks the partial index on queued runs in
  priority order and stops at the first it can take.
- **A heartbeat is cheap**, because it changes no indexed column and Postgres rewrites the row in
  place. An hour-long run's 240 heartbeats cost about 30 ms of database time.
- **A single run's latency is the poll interval.** Workers look for work every `poll` (200 ms in
  the test product, 1 s by default). Throughput, not latency, is what the slots give.
- **A fresh child adds about 10 ms** to a run on this machine. Its 95th percentile includes one
  missed poll. A product with a heavy start-up (large dependencies, connections made eagerly) should
  measure its own binary before using `fresh_process` for frequent jobs.
- **Pruning keeps up.** The first measurement found that one hourly pass removed only 500 chains,
  about 12,000 runs a day, below the volume the engine is sized for. Pruning now deletes batch
  after batch, for up to five minutes a pass, at about 7,500 chains a second here (60,000 in 8 s).
- **An index on chains by first job and start time did not help pruning** (12.7 s with it against
  9 s without), so it was not added.
