//! Medições de vazão. Ignoradas por padrão; rode com:
//! `cargo test --release -- --ignored throughput --nocapture`

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rekka::{JobHandler, RekkaCache, RekkaQueue, RekkaState};
use serde::{Deserialize, Serialize};

use crate::common::{TestEnv, eventually};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "bench")]
struct BenchJob {
    task_id: u32,
    key: u32,
}

async fn noop(_h: JobHandler, _job: BenchJob, RekkaState(done): RekkaState<AtomicUsize>) {
    done.fetch_add(1, Ordering::Relaxed);
}

async fn run(label: &str, total: u32, keys: Option<u32>, instances: usize, concurrency: usize) {
    let env = TestEnv::new();
    let done = Arc::new(AtomicUsize::new(0));
    let mut queues = Vec::new();
    for _ in 0..instances {
        let q = env
            .builder()
            .data_arc(done.clone())
            .register::<BenchJob>(|w| {
                let w = w.concurrency(concurrency);
                if keys.is_some() { w.concurrency_by_key(|j| j.key) } else { w }.build_fn(noop)
            })
            .start()
            .await
            .unwrap();
        queues.push(q);
    }

    let started = Instant::now();
    queues[0]
        .immediate_many((0..total).map(|task_id| BenchJob { task_id, key: keys.map_or(0, |k| task_id % k) }))
        .await
        .unwrap();
    let enqueued = started.elapsed();
    assert!(eventually(Duration::from_secs(120), || done.load(Ordering::Relaxed) == total as usize).await);
    let elapsed = started.elapsed();

    println!(
        "{label:<40} {total} jobs | enqueue {:>6.0} jobs/s | end-to-end {:>6.0} jobs/s ({elapsed:.2?})",
        f64::from(total) / enqueued.as_secs_f64(),
        f64::from(total) / elapsed.as_secs_f64(),
    );
    for q in &queues {
        q.shutdown().await.unwrap();
    }
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = "1m")]
struct BenchItem {
    id: u32,
    name: String,
}

async fn cache_bench(total: u32) {
    let env = TestEnv::new();
    let cache = env.cache().await;
    let items: Vec<BenchItem> = (0..total).map(|id| BenchItem { id, name: format!("item {id}") }).collect();

    let started = Instant::now();
    cache.insert_many(&items).await.unwrap();
    let insert = started.elapsed();
    let started = Instant::now();
    let got = cache.get_many::<BenchItem>(0..total).await.unwrap();
    let get = started.elapsed();
    assert!(got.iter().all(Option::is_some));
    let started = Instant::now();
    let tasks: Vec<_> = (0..2000).map(|i| {
        let cache = cache.clone();
        tokio::spawn(async move { cache.get::<BenchItem>(i).await.unwrap() })
    }).collect();
    for t in tasks {
        assert!(t.await.unwrap().is_some());
    }
    let single = started.elapsed();
    println!(
        "{:<40} insert_many {:>7.0}/s | get_many {:>7.0}/s | 2000 concurrent get {:>7.0}/s",
        format!("cache ({total} items)"),
        f64::from(total) / insert.as_secs_f64(),
        f64::from(total) / get.as_secs_f64(),
        2000.0 / single.as_secs_f64(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark"]
async fn throughput() {
    cache_bench(50_000).await;
    run("1 instance x 200", 20_000, None, 1, 200).await;
    run("4 instances x 50", 20_000, None, 4, 50).await;
    run("1 instance x 200, 1000 keys", 20_000, Some(1000), 1, 200).await;
    run("4 instances x 50, 1000 keys", 20_000, Some(1000), 4, 50).await;
    run("4 instances x 50, 10 keys", 5_000, Some(10), 4, 50).await;
}
