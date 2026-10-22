# AGENTS.md

Rekka is a Rust library for Redis-backed job queues and a typed cache, built to run on many instances at once. It is part of the Dinoco ecosystem (the ORM lives in `dinoco-rs/dinoco`; the shared documentation site lives outside both repositories).

## Project structure

```
.
├── Cargo.toml              Workspace (resolver 3) + the `rekka-tests` root package that owns tests/
├── crates/
│   ├── rekka/              Public crate
│   │   ├── src/
│   │   │   ├── runtime.rs  QueuesBuilder (configuration, validation, start) and Queues (running handle)
│   │   │   ├── worker.rs   RekkaWorker options and the queue loop: claim, heartbeat, reaper, finalize
│   │   │   ├── recursive.rs Recurring workers (`interval` or `cron_time`) and reconcile (one instance per cycle via a Redis lock; reconcile counts its interval from the end of the previous run)
│   │   │   ├── cron.rs     Cron expression parser (node-cron format, optional seconds) and next-occurrence search
│   │   │   ├── client.rs   RekkaClient (enqueue, remove, state, stats, purge), ReconcileHandler
│   │   │   ├── conn.rs     RedisConn: the shared connection (fails fast while Redis is down, replaces silent connections, signals recoveries)
│   │   │   ├── buffer.rs   Offline buffer: enqueues made while Redis is down go to `<buffer_dir>/<prefix>.<n>.jsonl` and are sent in order
│   │   │   ├── job.rs      RekkaQueue, JobHandler, RetryPolicy, RekkaExecutionResult
│   │   │   ├── handler.rs  JobFn / TaskFn: handlers with up to 8 extractors
│   │   │   ├── context.rs  FromContext, RekkaContext, RekkaData
│   │   │   ├── cache.rs    Cache, CacheBuilder, RekkaCache
│   │   │   ├── event.rs    JobEvent, JobEventKind, EventSource
│   │   │   ├── scripts.rs  Every Lua script plus the Redis data model of a queue (read its header)
│   │   │   ├── janitor.rs  Periodic repair of inconsistent state, purge
│   │   │   └── keys.rs, util.rs, error.rs
│   │   └── examples/basic.rs
│   └── rekka_derives/      `#[derive(RekkaQueue)]` and `#[derive(RekkaCache)]`
├── tests/suite/            Integration suite (one module per topic, registered in main.rs)
├── scripts/publish-crates.js  Publishes the crates in dependency order (`--dry-run`)
├── readme.md, crates.md    Repository README and crates.io README (absolute URLs); keep them in sync
└── rustfmt.toml            Same settings as the Dinoco repository
```

## Conventions

- Code comments and rustdoc are written in Portuguese; READMEs and the public documentation are written in English and Portuguese.
- Every state change on a queue happens inside a Lua script in `scripts.rs`, using the Redis clock (`TIME`). Keep it that way: no read-modify-write from Rust.
- All keys of a queue share the `{queue}` hash tag. Recurring workers use `{rec:<name>}`.
- Configuration errors are caught in `QueuesBuilder::check` and extractor validation in `start()`, so a misconfigured service fails at boot, not on the first job.

## Tests

- The suite needs a Redis server: `REKKA_REDIS_URL` (default `redis://127.0.0.1:6379`).
- `tests/suite/common.rs` gives each test its own prefix (`rekka-test-*`) and deletes its keys on drop, so tests run in parallel against one server.
- New topics go in `tests/suite/<topic>.rs` and must be added to `tests/suite/main.rs`.
- `tests/suite/outage.rs` has `RedisProxy`, a TCP proxy in front of the test Redis: `down()`/`up()` simulate an outage, `stall()` a connection that goes silent. Build with `env.builder_at(proxy.url())`.

```bash
REKKA_REDIS_URL=redis://127.0.0.1:6379 cargo test
cargo run -p rekka --example basic   # also reads REDIS_URL
```

## Releasing

The version lives once in `[workspace.package]`. `crates/rekka/Cargo.toml` pins `rekka_derives` with `=<version>`, so bump both together, update `readme.md` and `crates.md`, and add a release-notes entry to the Rekka docs. Then run `node scripts/publish-crates.js --dry-run` before publishing.
