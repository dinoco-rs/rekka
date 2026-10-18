use std::sync::Arc;
use std::time::{Duration, Instant};

use rekka::{JobEventKind, JobHandler, JobState, RekkaError, RekkaQueue, RekkaState};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Instance, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "slow")]
struct SlowJob {
    task_id: u32,
    sleep_ms: u64,
    /// Instância em que o job nunca termina.
    hang_on: u32,
}

async fn slow(h: JobHandler, job: SlowJob, RekkaState(probe): RekkaState<Probe>, RekkaState(instance): RekkaState<Instance>) {
    probe.record(h.task_id(), h.attempt(), 0, instance.0);
    if instance.0 == job.hang_on {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(ms(job.sleep_ms)).await;
}

fn builder(env: &TestEnv, probe: &Arc<Probe>, events: &Events, instance: u32) -> rekka::QueuesBuilder {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(instance))
        .on_event(events.handler())
        .register::<SlowJob>(|w| w.concurrency(1).build_fn(slow))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_running_jobs_and_stops_claiming() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = builder(&env, &probe, &events, 1).start().await.unwrap();

    q.immediate_many((0..3).map(|task_id| SlowJob { task_id, sleep_ms: 500, hang_on: 0 })).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);

    let started = Instant::now();
    q.shutdown().await.unwrap();
    assert!(started.elapsed() >= ms(300), "shutdown returned before the job finished");
    assert!(!q.is_running());

    assert_eq!(probe.count(), 1, "no new jobs after shutdown");
    assert_eq!(events.count(JobEventKind::Succeeded), 1);
    let stats = q.stats::<SlowJob>().await.unwrap();
    assert_eq!((stats.waiting, stats.running), (2, 0), "remaining jobs stay in the queue");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_returns_after_shutdown_is_requested() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = builder(&env, &probe, &events, 1).start().await.unwrap();

    let joiner = tokio::spawn({
        let q = q.clone();
        async move { q.join().await }
    });
    q.immediate(SlowJob { task_id: 1, sleep_ms: 300, hang_on: 0 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    tokio::time::sleep(ms(50)).await;
    assert!(!joiner.is_finished(), "join must block while running");

    // `shutdown()` dispara o encerramento na hora; quem espera é o `join()`.
    drop(q.shutdown());
    tokio::time::timeout(Duration::from_secs(5), joiner).await.unwrap().unwrap().unwrap();
    assert_eq!(events.count(JobEventKind::Succeeded), 1);
}

/// Ao estourar o `shutdown_timeout`, o job é abortado e devolvido para a fila na hora
/// (sem esperar a lease expirar), e outra instância o executa.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_timeout_returns_jobs_to_the_queue() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let a = builder(&env, &probe, &events, 1).shutdown_timeout(ms(200)).start().await.unwrap();

    a.immediate(SlowJob { task_id: 7, sleep_ms: 0, hang_on: 1 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);

    let started = Instant::now();
    let res = a.shutdown().await;
    assert!(matches!(res, Err(RekkaError::ShutdownTimeout { abandoned: 1 })), "{res:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(a.state::<SlowJob>(7).await.unwrap(), Some(JobState::Waiting { attempts: 1, .. })));

    // Lease padrão é 30s: se o job não tivesse sido devolvido, `b` não o pegaria a tempo.
    let b = builder(&env, &probe, &events, 2).start().await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 2).await);
    assert!(eventually(Duration::from_secs(3), || events.count(JobEventKind::Succeeded) == 1).await);
    assert_eq!(probe.runs()[1].attempt, 2);
    b.shutdown().await.unwrap();
}
