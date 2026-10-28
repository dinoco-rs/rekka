use std::sync::Arc;
use std::time::Duration;

use rekka::{JobEventKind, JobHandler, Queues, RekkaQueue, RekkaData};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Instance, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "fragile")]
struct FragileJob {
    task_id: String,
    /// Instância em que o job "trava" (simula um processo que morre no meio).
    hang_on: u32,
    sleep_ms: u64,
}

async fn fragile(
    job: FragileJob,
    RekkaData(probe): RekkaData<Probe>,
    RekkaData(instance): RekkaData<Instance>,
    h: JobHandler,
) {
    probe.record(h.task_id(), h.attempt(), 0, instance.0);
    if instance.0 == job.hang_on {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(ms(job.sleep_ms)).await;
}

async fn instance(env: &TestEnv, probe: &Arc<Probe>, events: &Events, id: u32, max_attempts: Option<u32>) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(id))
        .on_event(events.handler())
        .register::<FragileJob>(|w| {
            let w = w.lease(ms(500)).concurrency(4);
            match max_attempts {
                Some(n) => w.max_attempts(n),
                None => w,
            }
            .build_fn(fragile)
        })
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_of_crashed_instance_is_recovered() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());

    let a = instance(&env, &probe, &events, 1, None).await;
    a.immediate(FragileJob { task_id: "j".into(), hang_on: 1, sleep_ms: 0 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    a.abort();

    let b = instance(&env, &probe, &events, 2, None).await;
    assert!(eventually(Duration::from_secs(5), || probe.count() == 2).await, "job was not recovered");
    assert!(eventually(Duration::from_secs(2), || events.kinds("j").contains(&JobEventKind::Succeeded)).await);

    let runs = probe.runs_of("j");
    assert_eq!((runs[0].instance, runs[0].attempt), (1, 1));
    assert_eq!((runs[1].instance, runs[1].attempt), (2, 2));
    assert!(events.kinds("j").contains(&JobEventKind::Stalled));
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_job_past_max_attempts_is_dead() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());

    let a = instance(&env, &probe, &events, 1, Some(1)).await;
    a.immediate(FragileJob { task_id: "d".into(), hang_on: 1, sleep_ms: 0 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    a.abort();

    let b = instance(&env, &probe, &events, 2, Some(1)).await;
    assert!(eventually(Duration::from_secs(5), || events.kinds("d").contains(&JobEventKind::Dead)).await);
    tokio::time::sleep(ms(500)).await;
    assert_eq!(probe.count(), 1, "a dead job must not run again");
    let dead = events.last("d").unwrap();
    assert!(dead.payload().is_some(), "dead event carries the payload");
    assert!(dead.get_job::<FragileJob>().is_some());
    assert_eq!(b.state::<FragileJob>("d").await.unwrap(), None);
    b.shutdown().await.unwrap();
}

/// A lease é renovada enquanto o job executa: um job mais longo que a lease
/// não pode ser "recuperado" por outra instância.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_jobs_keep_their_lease() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let a = instance(&env, &probe, &events, 1, None).await;
    let b = instance(&env, &probe, &events, 2, None).await;

    a.immediate_many((0..4).map(|i| FragileJob { task_id: i.to_string(), hang_on: 0, sleep_ms: 1800 })).await.unwrap();
    assert!(eventually(Duration::from_secs(6), || events.count(JobEventKind::Succeeded) == 4).await);

    assert_eq!(probe.count(), 4, "jobs ran more than once");
    assert_eq!(events.count(JobEventKind::Stalled), 0);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "mixed")]
struct MixedJob {
    task_id: u32,
    sleep_ms: u64,
}

async fn mixed(job: MixedJob, RekkaData(probe): RekkaData<Probe>, h: JobHandler) {
    if job.sleep_ms > 0 {
        probe.record(h.task_id(), h.attempt(), 0, 0);
        tokio::time::sleep(ms(job.sleep_ms)).await;
    }
}

/// Com backlog grande de jobs rápidos, o loop faz claims sem parar; mesmo assim o
/// heartbeat e o shutdown precisam ser atendidos.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backlog_does_not_starve_heartbeat_or_shutdown() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .on_event(events.handler())
        .register::<MixedJob>(|w| w.lease(ms(300)).concurrency(64).build_fn(mixed))
        .start()
        .await
        .unwrap();

    q.immediate(MixedJob { task_id: 0, sleep_ms: 1200 }).await.unwrap();
    q.immediate_many((1..80_000).map(|task_id| MixedJob { task_id, sleep_ms: 0 })).await.unwrap();
    tokio::time::sleep(ms(600)).await;

    let started = std::time::Instant::now();
    q.shutdown().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(3), "shutdown took {:?}", started.elapsed());
    assert!(q.stats::<MixedJob>().await.unwrap().waiting > 0, "backlog should still be pending");
    assert_eq!(probe.count_of("0"), 1, "long job ran more than once");
    assert_eq!(events.count(JobEventKind::Stalled), 0, "lease expired while the job was running");
}
