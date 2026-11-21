# Agent instructions

downbeat is a job scheduler for Rust products, with Postgres as its only dependency. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before changing anything: it has the setup, the tasks, the test
layers and the code style, and they apply to agents too.

- Run `mise run check` before you say a change is done. `mise run test-rust [cargo test args]`
  runs a subset. The Rust tests need podman, which runs a throwaway Postgres.
- The rules the engine must keep are in [docs/spec.md](docs/spec.md); the code's layout is in
  [docs/architecture.md](docs/architecture.md).
- A behaviour change updates the guide, the reference and the spec in the same change. A choice
  the design leaves open gets a numbered entry in [docs/decisions.md](docs/decisions.md).
- mise is the one place for recurring commands. Keep `mise.toml` formatted and locked
  (`mise lock`), with its `min_version`. CI does not use mise, so keep the steps in `mise-tasks/`
  runnable as plain bash.

## Design rules

- **Keep old data shapes working.** When a change touches an API type or stored data, keep old
  fields and stored rows readable until no client, export or row reads them. Never invent an API
  field, and persist an owned struct rather than the wire type, because old clients and exports
  break silently.
- **Store each fact once.** Add a column only when no existing column or row fact carries the
  value: no copy in a second column, JSON blob or join, and no backfill unless asked. Say in the
  change why a new column earns its place.
- **Bound every query.** Every query the API or the console makes is bounded and paginated in the
  database, with real pagination, because one unbounded query can overload the product's database.
- **Indexes need a reason.** Add an index only for a query already in the code or a measured need,
  and prefer one partial index that serves several readers, because every index slows every write.
- **Design the API from its callers.** Count database round trips per call, cut repeated lookups,
  and keep calls few, so the console and other clients stay simple.
- **Use narrow types.** Typed enums, narrow types and exhaustive matches, not `any` or untyped maps,
  in Rust and in the screens. A type-only fix changes no behaviour.
- **Do not over-engineer.** Accept a rare corner case when fixing it needs a lot of work, because
  simpler designs are easier to build, review and change.
