use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rekka::{EventSource, JobEventKind, JobHandler, Queues, RekkaClient, RekkaQueue, RekkaState};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Instance, Probe, TestEnv, eventually, ms};

struct TaskConfig {
    sleep_ms: u64,
    /// Instância em que a execução trava (simula crash no meio).
    hang_on: u32,
}

async fn tick(RekkaState(probe): RekkaState<Probe>, RekkaState(instance): RekkaState<Instance>, RekkaState(cfg): RekkaState<TaskConfig>) {
    let _running = probe.gauge.enter();
    probe.record("tick", 0, 0, instance.0);
    if instance.0 == cfg.hang_on {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(ms(cfg.sleep_ms)).await;
}

async fn instance(env: &TestEnv, probe: &Arc<Probe>, events: &Events, id: u32, interval: Duration, cfg: TaskConfig) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(id))
        .data(cfg)
        .on_event(events.handler())
        .register_recursive("tick", move |w| w.interval(interval).lease(ms(400)).build_fn(tick))
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_on_every_interval() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = instance(&env, &probe, &events, 1, ms(200), TaskConfig { sleep_ms: 0, hang_on: 0 }).await;

    tokio::time::sleep(ms(1100)).await;
    q.shutdown().await.unwrap();
    let n = probe.count();
    assert!((4..=7).contains(&n), "ran {n} times in ~1.1s with a 200ms interval");

    let succeeded: Vec<_> = events.all().into_iter().filter(|e| e.is_success()).collect();
    assert!(succeeded.iter().all(|e| e.source() == EventSource::Recursive && e.job_name() == "tick"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_instance_runs_each_cycle() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let mut instances = Vec::new();
    for id in 1..=3 {
        instances.push(instance(&env, &probe, &events, id, ms(300), TaskConfig { sleep_ms: 50, hang_on: 0 }).await);
    }

    tokio::time::sleep(ms(1600)).await;
    for q in &instances {
        q.shutdown().await.unwrap();
    }
    let n = probe.count();
    // ~1.6s / 300ms ≈ 6 ciclos no total (e não 18).
    assert!((4..=8).contains(&n), "ran {n} times across 3 instances");
    assert_eq!(probe.gauge.max(), 1, "executions overlapped");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_overlaps_when_slower_than_interval() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let a = instance(&env, &probe, &events, 1, ms(50), TaskConfig { sleep_ms: 250, hang_on: 0 }).await;
    let b = instance(&env, &probe, &events, 2, ms(50), TaskConfig { sleep_ms: 250, hang_on: 0 }).await;

    tokio::time::sleep(ms(1200)).await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    assert_eq!(probe.gauge.max(), 1);
    assert!(probe.count() >= 3, "ran {}", probe.count());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_instance_takes_over_after_crash() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let a = instance(&env, &probe, &events, 1, ms(200), TaskConfig { sleep_ms: 0, hang_on: 1 }).await;
    assert!(eventually(Duration::from_secs(2), || probe.count() == 1).await);
    let b = instance(&env, &probe, &events, 2, ms(200), TaskConfig { sleep_ms: 0, hang_on: 1 }).await;

    // `a` travou segurando o lock; `b` só pode assumir depois que `a` morrer e a lease expirar.
    tokio::time::sleep(ms(300)).await;
    assert_eq!(probe.runs().iter().filter(|r| r.instance == 2).count(), 0, "lock was not respected");
    a.abort();

    assert!(
        eventually(Duration::from_secs(3), || probe.runs().iter().filter(|r| r.instance == 2).count() >= 2).await,
        "instance 2 did not take over"
    );
    b.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "report")]
struct ReportJob {
    task_id: u32,
}

async fn enqueue_report(client: RekkaClient, RekkaState(counter): RekkaState<AtomicU32>) -> rekka::Result<()> {
    let n = counter.fetch_add(1, Ordering::SeqCst);
    client.immediate(ReportJob { task_id: n }).await?;
    Ok(())
}

async fn report(h: JobHandler, _job: ReportJob, RekkaState(probe): RekkaState<Probe>) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
}

async fn failing_task() -> Result<(), String> {
    Err("nope".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_workers_use_extractors_and_report_errors() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data(AtomicU32::new(0))
        .on_event(events.handler())
        .register::<ReportJob>(|w| w.build_fn(report))
        .register_recursive("producer", |w| w.interval(ms(150)).build_fn(enqueue_report))
        .register_recursive("failing", |w| w.interval(ms(150)).build_fn(failing_task))
        .start()
        .await
        .unwrap();

    assert!(eventually(Duration::from_secs(3), || probe.count() >= 3).await);
    assert!(
        eventually(Duration::from_secs(2), || events
            .all()
            .iter()
            .any(|e| e.job_name() == "failing" && e.kind() == JobEventKind::Failed && e.error() == Some("nope")))
        .await
    );
    q.shutdown().await.unwrap();
}
