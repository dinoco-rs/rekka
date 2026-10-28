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
│   │   │   ├── worker.rs   RekkaWorker options and the queue loop: claim, heartbeat, reaper, finalize (shared by queues and workflow steps)
│   │   │   ├── workflow.rs register_workflow steps (`start_with`/`then`), StepResult, IntoStepResult; each step is a queue under the entry queue's hash tag
│   │   │   ├── recursive.rs Recurring workers (`interval` or `cron_time`) and reconcile (one instance per cycle via a Redis lock; reconcile counts its interval from the end of the previous run)
│   │   │   ├── cron.rs     Cron expression parser (node-cron format, optional seconds) and next-occurrence search
│   │   │   ├── client.rs   RekkaClient (enqueue, remove, state, stats, purge), ReconcileHandler
│   │   │   ├── conn.rs     RedisConn: the shared connection (fails fast while Redis is down, replaces silent connections, signals recoveries)
│   │   │   ├── buffer.rs   Offline buffer: enqueues made while Redis is down go to `<buffer_dir>/<prefix>.<n>.jsonl` and are sent in order
│   │   │   ├── job.rs      RekkaQueue, JobHandler, RetryPolicy, RekkaExecutionResult
│   │   │   ├── handler.rs  JobFn / TaskFn: the job first, then up to 8 extractors; `call` boxes the handler future; FutureSize
│   │   │   ├── context.rs  FromContext, RekkaContext, RekkaData; extractor impls for JobHandler and RekkaClient
│   │   │   ├── cache.rs    Cache, CacheBuilder, RekkaCache
│   │   │   ├── event.rs    JobEvent, JobEventKind, EventSource
│   │   │   ├── scripts.rs  Every Lua script plus the Redis data model of a queue (read its header)
│   │   │   ├── janitor.rs  Periodic repair of inconsistent state, purge
│   │   │   └── keys.rs, util.rs, error.rs
│   │   └── examples/basic.rs
│   └── rekka_derives/      `#[derive(RekkaQueue)]` and `#[derive(RekkaCache)]`
├── tests/suite/            Integration suite (one module per topic, registered in main.rs)
├── tests/future_sizes.rs   Size budgets of rekka's internal futures (no Redis needed)
├── tests/stack.rs          Large handlers on worker threads with a reduced stack (own binary: an overflow aborts it)
├── scripts/publish-crates.js  Publishes the crates in dependency order (`--dry-run`)
├── readme.md, crates.md    Repository README and crates.io README (absolute URLs); keep them in sync
├── CHANGELOG.md            One entry per version
└── rustfmt.toml            Same settings as the Dinoco repository
```

## Conventions

- Code comments and rustdoc are written in Portuguese; READMEs and the public documentation are written in English and Portuguese.
- Every state change on a queue happens inside a Lua script in `scripts.rs`, using the Redis clock (`TIME`). Keep it that way: no read-modify-write from Rust.
- All keys of a queue share the `{queue}` hash tag. Recurring workers use `{rec:<name>}`. Workflow step `n` lives under `prefix:{entry}:n:`, in the entry queue's slot, so `ADVANCE` completes one step and enqueues the next in one script.
- `JobHandler` is an extractor like any other (`FromContext`): handlers get the job first and only declare what they use. New per-run data goes into `JobHandler`/`RekkaContext::job`, not into the handler signature.
- Configuration errors are caught in `QueuesBuilder::check` and extractor validation in `start()`, so a misconfigured service fails at boot, not on the first job.

## Tests

- The suite needs a Redis server: `REKKA_REDIS_URL` (default `redis://127.0.0.1:6379`).
- `tests/suite/common.rs` gives each test its own prefix (`rekka-test-*`) and deletes its keys on drop, so tests run in parallel against one server.
- New topics go in `tests/suite/<topic>.rs` and must be added to `tests/suite/main.rs`.
- `tests/suite/outage.rs` has `RedisProxy`, a TCP proxy in front of the test Redis: `down()`/`up()` simulate an outage, `stall()` a connection that goes silent. Build with `env.builder_at(proxy.url())`.
- `tests/stack.rs` reuses `tests/suite/common.rs` through `#[path]`.

```bash
REKKA_REDIS_URL=redis://127.0.0.1:6379 cargo test
cargo test --test future_sizes        # no Redis needed
cargo run -p rekka --example basic   # also reads REDIS_URL
```

## Performance: tamanho de future e stack

- O future do handler do usuário tem tamanho arbitrário. O rekka o boxeia uma única vez, na fronteira (`call`), e nunca o embute nem o copia de novo. Os futures internos do rekka têm tamanho constante, independente do handler.
- `tests/future_sizes.rs` guarda o orçamento de cada future interno (`run_job`, `execute`, `finalize`, loops). Se uma mudança passar do orçamento, o teste falha; aumente o orçamento só com o motivo medido na mensagem de commit.
- `tests/stack.rs` roda handlers grandes num runtime com `thread_stack_size` reduzido, em debug. Ele tem que continuar passando.
- Todo worker valida `size_of::<F::Future>()` no registro contra `max_future_size`. Não remova essa validação.
- Não receba um future como argumento de `async fn` só para reembrulhar; prefira combinadores ou `impl Future`. O argumento e o local podem ocupar slots separados no estado.
- Temporários do scrutinee de `match`/`if let` vivem até o fim do statement, inclusive através de um `.await` dentro dele. Faça o binding antes e só depois dê o await.
- Não segure payloads, buffers ou structs grandes através de `.await` nos loops quentes.
- Para inspecionar layouts: `RUSTC_BOOTSTRAP=1 cargo rustc --lib -- -Zprint-type-sizes`. PRs que mexem em claim/execute/finalize ou nos loops reportam tamanhos antes/depois.

## Releasing

The version lives once in `[workspace.package]`. `crates/rekka/Cargo.toml` pins `rekka_derives` with `=<version>`, so bump both together, update `readme.md`, `crates.md` and `CHANGELOG.md`, and add a release-notes entry to the Rekka docs. Then run `node scripts/publish-crates.js --dry-run` before publishing.
