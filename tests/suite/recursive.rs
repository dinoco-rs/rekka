use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rekka::chrono::{self, Datelike, FixedOffset, TimeZone, Timelike, Utc};
use rekka::{EventSource, JobEventKind, JobHandler, Queues, RekkaClient, RekkaQueue, RekkaData};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Instance, Probe, TestEnv, eventually, ms};

struct TaskConfig {
    sleep_ms: u64,
    /// Instância em que a execução trava (simula crash no meio).
    hang_on: u32,
}

async fn tick(RekkaData(probe): RekkaData<Probe>, RekkaData(instance): RekkaData<Instance>, RekkaData(cfg): RekkaData<TaskConfig>) {
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

async fn enqueue_report(client: RekkaClient, RekkaData(counter): RekkaData<AtomicU32>) -> rekka::Result<()> {
    let n = counter.fetch_add(1, Ordering::SeqCst);
    client.immediate(ReportJob { task_id: n }).await?;
    Ok(())
}

async fn report(h: JobHandler, _job: ReportJob, RekkaData(probe): RekkaData<Probe>) {
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

async fn cron_tick(RekkaData(probe): RekkaData<Probe>, RekkaData(instance): RekkaData<Instance>) {
    let _running = probe.gauge.enter();
    // Segundo mais próximo: o agendamento segue o relógio do Redis, que pode estar alguns ms
    // à frente do local (executar às 10:00:02.000 do Redis pode ser 10:00:01.975 aqui).
    let second = (Utc::now() + chrono::Duration::milliseconds(500)).second();
    probe.record("cron", 0, i64::from(second), instance.0);
    tokio::time::sleep(ms(100)).await;
}

async fn cron_instance(env: &TestEnv, probe: &Arc<Probe>, id: u32, name: &'static str, expr: &'static str) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(id))
        .register_recursive(name, move |w| w.cron_time(expr).lease(ms(400)).build_fn(cron_tick))
        .start()
        .await
        .unwrap()
}

/// `* * * * * *` = todo segundo: com 3 instâncias, uma execução por segundo no total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_time_runs_once_per_occurrence_across_instances() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let mut instances = Vec::new();
    for id in 1..=3 {
        instances.push(cron_instance(&env, &probe, id, "every_second", "* * * * * *").await);
    }

    tokio::time::sleep(ms(3300)).await;
    for q in &instances {
        q.shutdown().await.unwrap();
    }
    let runs = probe.runs();
    assert!((2..=4).contains(&runs.len()), "ran {} times in ~3.3s", runs.len());
    assert_eq!(probe.gauge.max(), 1, "executions overlapped");
    for pair in runs.windows(2) {
        let gap = pair[1].at - pair[0].at;
        assert!(gap >= ms(800), "two runs {gap:?} apart for a one-second cron");
    }
}

/// `*/2` no campo de segundos: só segundos pares.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_time_steps() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = cron_instance(&env, &probe, 1, "even", "*/2 * * * * *").await;

    tokio::time::sleep(ms(4500)).await;
    q.shutdown().await.unwrap();
    let runs = probe.runs();
    assert!((2..=3).contains(&runs.len()), "ran {} times in ~4.5s", runs.len());
    assert!(runs.iter().all(|r| r.value % 2 == 0), "ran on odd seconds: {:?}", runs.iter().map(|r| r.value).collect::<Vec<_>>());
}

/// Ao contrário de `interval`, um cron não executa na subida: só agenda o próximo instante.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_time_does_not_run_on_start() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = cron_instance(&env, &probe, 1, "yearly", "0 0 1 1 *").await;

    tokio::time::sleep(ms(500)).await;
    q.shutdown().await.unwrap();
    assert_eq!(probe.count(), 0);

    let mut conn = env.redis().await;
    let next: i64 = redis::cmd("GET").arg(format!("{}:{{rec:yearly}}:next", env.prefix)).query_async(&mut conn).await.unwrap();
    let expected = Utc.with_ymd_and_hms(Utc::now().year() + 1, 1, 1, 0, 0, 0).unwrap();
    assert_eq!(next, expected.timestamp_millis());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_time_in_a_timezone() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let sao_paulo = FixedOffset::west_opt(3 * 3600).unwrap();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data(Instance(1))
        .register_recursive("morning", move |w| w.cron_time("0 9 * * *").timezone(sao_paulo).build_fn(cron_tick))
        .start()
        .await
        .unwrap();
    tokio::time::sleep(ms(300)).await;
    q.shutdown().await.unwrap();

    let mut conn = env.redis().await;
    let next: i64 = redis::cmd("GET").arg(format!("{}:{{rec:morning}}:next", env.prefix)).query_async(&mut conn).await.unwrap();
    let next = Utc.timestamp_millis_opt(next).unwrap().with_timezone(&sao_paulo);
    assert_eq!((next.hour(), next.minute(), next.second()), (9, 0, 0));
    assert!(next > Utc::now() && next < Utc::now() + chrono::Duration::days(1));
}

/// O lock some enquanto a tarefa executa (Redis reiniciado sem persistência, lease estourada
/// durante uma queda): o heartbeat o retoma, para nenhuma outra instância começar junto.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_lock_is_reacquired_while_running() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = instance(&env, &probe, &events, 1, ms(50), TaskConfig { sleep_ms: 1500, hang_on: 0 }).await;
    assert!(eventually(Duration::from_secs(2), || probe.count() == 1).await);

    let lock = format!("{}:{{rec:tick}}:lock", env.prefix);
    let mut conn = env.redis().await;
    let _: () = redis::cmd("DEL").arg(&lock).query_async(&mut conn).await.unwrap();
    tokio::time::sleep(ms(250)).await;
    let pttl: i64 = redis::cmd("PTTL").arg(&lock).query_async(&mut conn).await.unwrap();
    assert!(pttl > 0, "lock was not re-acquired (pttl {pttl})");
    q.shutdown().await.unwrap();
}
