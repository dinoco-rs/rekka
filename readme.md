<p align="center">
	<img src="./assets/logo.svg" alt="Rekka logo" width="96" />
</p>

<h1 align="center">Rekka</h1>

<p align="center">
	<strong>Redis-backed job queues and a typed cache for Rust, safe to scale horizontally.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/rekka"><img src="https://img.shields.io/crates/v/rekka.svg?color=black" alt="Crates.io"></a>
  <a href="https://dinoco.io/docs/rekka"><img src="https://img.shields.io/badge/docs-dinoco.io-black.svg" alt="Docs"></a>
  <a href="#-license"><img src="https://img.shields.io/crates/l/rekka.svg?color=black" alt="License"></a>
</p>

---

**Rekka** is part of the [Dinoco ecosystem](https://dinoco.io): open-source Rust tools built by a developer who moved from TypeScript to Rust and missed the tooling left behind. [Dinoco ORM](https://github.com/dinoco-rs/dinoco) handles your database; Rekka handles the background work — job queues with retries and scheduling, recurring tasks, and a typed cache, all on top of the Redis you already run.

## 💖 Support the Project

**Rekka** is and will always be open source, but ongoing development takes time, coffee, and a lot of dedication.

Be part of this journey, help accelerate the development of the Dinoco ecosystem.

<a href="https://www.buymeacoffee.com/theuszastro" target="_blank">
	<img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px; width: 217px;" />
</a>

## 🚀 Features

- **Typed jobs:** derive `RekkaQueue` on a serde struct. Its `task_id` makes every enqueue an upsert, so a job is never duplicated.
- **Retries and scheduling:** run now, at an instant, or after a delay. Retry policies, attempt limits, and timeouts are set per queue, and panics are caught.
- **Safe horizontal scaling:** jobs are leased and heartbeated. If a process dies, its jobs return to the queue, and per-key concurrency holds across every instance.
- **Recurring workers and reconcile:** interval tasks run on exactly one instance per cycle, and a reconcile hook, scheduled the same way, restores jobs from your own source of truth.
- **Typed cache:** derive `RekkaCache` with a TTL and a key, either a field or a template such as `"payments-{business_id}"`, then read and write single items or batches.
- **Graceful shutdown and events:** stop taking work on SIGINT/SIGTERM, wait for running jobs, and observe every job through one event hook.

## 📦 Installation

Add Rekka to your `Cargo.toml` dependencies:

```toml
[dependencies]
rekka = "0.0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Rekka requires Rust 1.88 or newer and a Redis 6+ server.

## ⚡ Quick example

```rust
use std::time::Duration;

use rekka::{Cache, JobHandler, Queues, RekkaCache, RekkaExecutionResult, RekkaQueue, RekkaState, RetryPolicy};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "emails")]
struct SendEmail {
    task_id: String,
    user_id: i64,
    to: String,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(key = id, ttl = "10m")]
struct User {
    id: i64,
    name: String,
}

struct Mailer {
    from: String,
}

async fn send_email(
    handler: JobHandler,
    job: SendEmail,
    RekkaState(mailer): RekkaState<Mailer>,
    cache: Cache,
) -> RekkaExecutionResult {
    let user = match cache.get::<User>(job.user_id).await {
        Ok(user) => user,
        Err(error) => return handler.err(error),
    };
    let Some(user) = user else {
        return handler.cancel();
    };

    println!("{} -> {} <{}> (attempt {})", mailer.from, user.name, job.to, handler.attempt());
    handler.success()
}

#[tokio::main]
async fn main() -> rekka::Result<()> {
    let queues = Queues::new("redis://127.0.0.1:6379")
        .data(Mailer { from: "no-reply@example.com".into() })
        .register::<SendEmail>(|w| {
            w.concurrency(10)
                .retry(RetryPolicy::RetryAfterRange(Duration::from_secs(1), Duration::from_secs(30)))
                .build_fn(send_email)
        })
        .start()
        .await?;

    queues.cache().insert(&User { id: 42, name: "Ana".into() }).await?;
    queues.immediate(SendEmail { task_id: "welcome:42".into(), user_id: 42, to: "ana@example.com".into() }).await?;

    // Blocks until SIGINT/SIGTERM, then lets running jobs finish.
    queues.join().await
}
```

A longer example covering reconcile, recurring workers, events, and the cache lives in [`crates/rekka/examples/basic.rs`](./crates/rekka/examples/basic.rs):

```bash
REDIS_URL=redis://127.0.0.1:6379 cargo run -p rekka --example basic
```

## 📚 Documentation

The full guide lives at [dinoco.io/docs/rekka](https://dinoco.io/docs/rekka), in English and Portuguese:

- [Quickstart](https://dinoco.io/en-us/docs/rekka/guide/quickstart) — from an empty crate to a running worker.
- [Jobs and enqueueing](https://dinoco.io/en-us/docs/rekka/queues/jobs) — `task_id`, upserts, and scheduling.
- [Workers](https://dinoco.io/en-us/docs/rekka/queues/workers) — handlers, extractors, and worker options.
- [Typed cache](https://dinoco.io/en-us/docs/rekka/cache/cache-types) — keys, TTLs, and batch operations.
- [Reliability](https://dinoco.io/en-us/docs/rekka/operations/reliability) — leases, recovery, and what Rekka stores in Redis.

## 🧪 Running the tests

The integration suite needs a reachable Redis. Every test uses its own key prefix, so the suite runs in parallel against a shared server and only deletes its own keys:

```bash
REKKA_REDIS_URL=redis://127.0.0.1:6379 cargo test
```

## 📄 License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))
- MIT license ([LICENSE-MIT](./LICENSE-MIT))

at your option.
