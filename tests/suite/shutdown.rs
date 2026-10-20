use std::sync::Arc;
use std::time::{Duration, Instant};

use rekka::{JobEventKind, JobHandler, JobState, RekkaError, RekkaQueue, RekkaData};
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

async fn slow(h: JobHandler, job: SlowJob, RekkaData(probe): RekkaData<Probe>, RekkaData(instance): RekkaData<Instance>) {
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

/// Escala horizontal: enquanto uma instância encerra (esperando um job em execução), outros
/// servidores continuam enfileirando. Ela não pode pegar nada novo, mesmo com slots livres e
/// recebendo as notificações da fila; quem pega é a instância que continua no ar.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutting_down_instance_ignores_jobs_enqueued_by_other_servers() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let stopping = env
        .builder()
        .data_arc(probe.clone())
        .data(Instance(1))
        .on_event(events.handler())
        .register::<SlowJob>(|w| w.concurrency(8).build_fn(slow))
        .start()
        .await
        .unwrap();
    let producer = env.producer().await;

    producer.immediate(SlowJob { task_id: 0, sleep_ms: 1500, hang_on: 0 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);

    let shutdown = tokio::spawn({
        let q = stopping.clone();
        async move { q.shutdown().await }
    });
    tokio::time::sleep(ms(50)).await;
    // Outro servidor enfileira durante o encerramento: imediatos e um agendado para daqui a pouco.
    producer.immediate_many((1..=5).map(|task_id| SlowJob { task_id, sleep_ms: 0, hang_on: 0 })).await.unwrap();
    producer.schedule_in(SlowJob { task_id: 6, sleep_ms: 0, hang_on: 0 }, ms(300)).await.unwrap();
    tokio::time::sleep(ms(700)).await;
    assert_eq!(probe.count(), 1, "the stopping instance claimed new jobs: {:?}", probe.per_task());

    shutdown.await.unwrap().unwrap();
    assert_eq!(probe.count(), 1);
    let stats = producer.stats::<SlowJob>().await.unwrap();
    assert_eq!((stats.waiting, stats.running), (6, 0), "new jobs stay in the queue for other instances");

    // Uma instância que continua no ar processa tudo.
    let other = builder(&env, &probe, &events, 2).start().await.unwrap();
    assert!(eventually(Duration::from_secs(5), || probe.count() == 7).await);
    assert!(probe.runs()[1..].iter().all(|r| r.instance == 2));
    other.shutdown().await.unwrap();
}

/// Shutdown em um momento qualquer no meio de um backlog grande (inclusive durante um claim):
/// nenhum job fica preso como "executando" e os que não executaram voltam sem gastar tentativa.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_mid_backlog_leaves_no_claimed_jobs_behind() {
    for round in 0..5u64 {
        let env = TestEnv::new();
        let (probe, events) = (Probe::new(), Events::default());
        let producer = env.producer().await;
        let total = 1500u32;
        producer.immediate_many((0..total).map(|task_id| SlowJob { task_id, sleep_ms: 0, hang_on: 0 })).await.unwrap();

        let q = env
            .builder()
            .data_arc(probe.clone())
            .data(Instance(1))
            .on_event(events.handler())
            .register::<SlowJob>(|w| w.concurrency(128).build_fn(slow))
            .start()
            .await
            .unwrap();
        tokio::time::sleep(ms(5 + round * 7)).await;
        q.shutdown().await.unwrap();

        let ran = probe.count() as u64;
        let stats = producer.stats::<SlowJob>().await.unwrap();
        assert_eq!(stats.running, 0, "round {round}: jobs left claimed after shutdown");
        assert_eq!(stats.waiting + ran, u64::from(total), "round {round}: lost or duplicated jobs");
        let done: std::collections::HashSet<String> = probe.runs().into_iter().map(|r| r.task_id).collect();
        for task_id in (0..total).filter(|id| !done.contains(&id.to_string())).take(200) {
            let state = producer.state::<SlowJob>(task_id).await.unwrap();
            assert!(
                matches!(state, Some(JobState::Waiting { attempts: 0, .. })),
                "round {round}: job {task_id} is {state:?}"
            );
        }
    }
}
