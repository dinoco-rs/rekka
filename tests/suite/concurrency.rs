use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rekka::{JobHandler, Queues, RekkaQueue, RekkaState};
use serde::{Deserialize, Serialize};

use crate::common::{Instance, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "work")]
struct WorkJob {
    task_id: u32,
    sleep_ms: u64,
}

async fn work(h: JobHandler, job: WorkJob, RekkaState(probe): RekkaState<Probe>, RekkaState(instance): RekkaState<Instance>) {
    let _running = probe.gauge.enter();
    probe.record(h.task_id(), h.attempt(), 0, instance.0);
    tokio::time::sleep(ms(job.sleep_ms)).await;
}

async fn instance(env: &TestEnv, probe: &Arc<Probe>, id: u32, concurrency: usize) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(id))
        .register::<WorkJob>(|w| w.concurrency(concurrency).build_fn(work))
        .start()
        .await
        .unwrap()
}

fn jobs(n: u32, sleep_ms: u64) -> impl Iterator<Item = WorkJob> {
    (0..n).map(move |task_id| WorkJob { task_id, sleep_ms })
}

fn assert_each_once(probe: &Probe, n: u32) {
    let per_task = probe.per_task();
    assert_eq!(per_task.len(), n as usize, "every job must run");
    let dup: Vec<_> = per_task.iter().filter(|(_, c)| **c != 1).collect();
    assert!(dup.is_empty(), "jobs executed more than once: {dup:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_concurrency_limit_is_respected() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = instance(&env, &probe, 0, 3).await;

    q.immediate_many(jobs(15, 100)).await.unwrap();
    assert!(eventually(Duration::from_secs(10), || probe.count() == 15).await);
    q.shutdown().await.unwrap();

    assert_eq!(probe.gauge.max(), 3);
    assert_each_once(&probe, 15);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_one_is_sequential() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = instance(&env, &probe, 0, 1).await;

    q.immediate_many(jobs(10, 20)).await.unwrap();
    assert!(eventually(Duration::from_secs(10), || probe.count() == 10).await);
    q.shutdown().await.unwrap();

    assert_eq!(probe.gauge.max(), 1);
    assert_each_once(&probe, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn horizontal_scaling_runs_each_job_exactly_once() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let instances = [
        instance(&env, &probe, 1, 4).await,
        instance(&env, &probe, 2, 4).await,
        instance(&env, &probe, 3, 4).await,
    ];

    instances[0].immediate_many(jobs(300, 5)).await.unwrap();
    assert!(eventually(Duration::from_secs(20), || probe.count() >= 300).await);
    tokio::time::sleep(ms(300)).await;
    for q in &instances {
        q.shutdown().await.unwrap();
    }

    assert_each_once(&probe, 300);
    // Concorrência por instância multiplica com o número de instâncias.
    assert!(probe.gauge.max() <= 12, "max concurrency {}", probe.gauge.max());
    assert!(probe.gauge.max() > 4, "instances did not run in parallel: {}", probe.gauge.max());
    let used: HashSet<u32> = probe.runs().iter().map(|r| r.instance).collect();
    assert_eq!(used.len(), 3, "work must be spread across instances: {used:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_concurrency_uses_batched_claims() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = instance(&env, &probe, 0, 500).await;

    q.immediate_many(jobs(2000, 50)).await.unwrap();
    assert!(eventually(Duration::from_secs(30), || probe.count() == 2000).await);
    q.shutdown().await.unwrap();

    assert_each_once(&probe, 2000);
    assert!(probe.gauge.max() <= 500);
    assert!(probe.gauge.max() > 128, "claims must go beyond one batch: {}", probe.gauge.max());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_workers_wake_up_on_enqueue() {
    let env = TestEnv::new();
    let probe = Probe::new();
    // Polling bem longo: só a notificação explica uma execução rápida.
    let q = env
        .builder()
        .poll_interval(Duration::from_secs(30))
        .data_arc(probe.clone())
        .data(Instance(0))
        .register::<WorkJob>(|w| w.build_fn(work))
        .start()
        .await
        .unwrap();
    tokio::time::sleep(ms(500)).await;

    let enqueued = Instant::now();
    q.immediate(WorkJob { task_id: 1, sleep_ms: 0 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    let latency = probe.runs()[0].at - enqueued;
    assert!(latency < ms(1000), "woke up after {latency:?}");
    q.shutdown().await.unwrap();
}
