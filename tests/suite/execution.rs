use std::sync::Arc;
use std::time::Duration;

use rekka::chrono::Utc;
use rekka::{
    JobEventKind, JobHandler, JobState, RekkaClient, RekkaExecutionResult, RekkaQueue, RekkaData, Removed, RetryPolicy,
};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue, Clone, Debug)]
#[rekka(name = "exec")]
struct ExecJob {
    task_id: String,
    mode: String,
}

fn job(id: &str, mode: &str) -> ExecJob {
    ExecJob { task_id: id.into(), mode: mode.into() }
}

/// Comportamento escolhido pelo payload; a 2ª tentativa sempre tem sucesso.
async fn behave(job: ExecJob, RekkaData(probe): RekkaData<Probe>, h: JobHandler) -> RekkaExecutionResult {
    probe.record(h.task_id(), h.attempt(), 0, 0);
    assert_eq!(h.queue(), "exec");
    assert_eq!(h.task_id(), job.task_id);
    if h.attempt() > 1 {
        return h.success();
    }
    match job.mode.as_str() {
        "ok" => h.success(),
        "cancel" => h.cancel(),
        "err" => h.err("expected failure"),
        "retry_after" => h.retry_after(ms(300)),
        "retry_at" => h.retry_at(Utc::now() + chrono::Duration::milliseconds(300)),
        "panic" => panic!("kaboom"),
        "sleep" => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            h.success()
        }
        other => panic!("unknown mode {other}"),
    }
}

async fn start(env: &TestEnv, probe: &Arc<Probe>, events: &Events) -> rekka::Queues {
    env.builder()
        .data_arc(probe.clone())
        .on_event(events.handler())
        .register::<ExecJob>(|w| {
            w.concurrency(8).retry(RetryPolicy::RetryImmediate).timeout(ms(200)).build_fn(behave)
        })
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn success_removes_job_and_emits_events() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate(job("ok", "ok")).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("ok").len() == 2).await);
    assert_eq!(events.kinds("ok"), vec![JobEventKind::Started, JobEventKind::Succeeded]);
    assert_eq!(probe.count_of("ok"), 1);
    assert_eq!(q.state::<ExecJob>("ok").await.unwrap(), None);
    q.shutdown().await.unwrap();
    assert!(env.keys("*j:ok").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_finishes_without_retry() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate(job("c", "cancel")).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("c").contains(&JobEventKind::Cancelled)).await);
    tokio::time::sleep(ms(300)).await;
    assert_eq!(probe.count_of("c"), 1);
    assert_eq!(q.state::<ExecJob>("c").await.unwrap(), None);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_panic_and_timeout_follow_retry_policy() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    for (id, mode) in [("e", "err"), ("p", "panic"), ("t", "sleep")] {
        q.immediate(job(id, mode)).await.unwrap();
    }
    for id in ["e", "p", "t"] {
        assert!(eventually(Duration::from_secs(5), || events.kinds(id).contains(&JobEventKind::Succeeded)).await, "{id}");
        assert_eq!(
            events.kinds(id),
            vec![JobEventKind::Started, JobEventKind::Failed, JobEventKind::Started, JobEventKind::Succeeded]
        );
        let attempts: Vec<u32> = probe.runs_of(id).iter().map(|r| r.attempt).collect();
        assert_eq!(attempts, vec![1, 2]);
    }

    let error_of = |id: &str| {
        events.all().into_iter().find(|e| e.task_id() == Some(id) && e.kind() == JobEventKind::Failed).unwrap().error().unwrap().to_owned()
    };
    assert_eq!(error_of("e"), "expected failure");
    assert!(error_of("p").contains("kaboom"), "{}", error_of("p"));
    assert!(error_of("t").contains("timed out"), "{}", error_of("t"));
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_after_and_retry_at_reschedule() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate(job("ra", "retry_after")).await.unwrap();
    q.immediate(job("rt", "retry_at")).await.unwrap();
    for id in ["ra", "rt"] {
        assert!(eventually(Duration::from_secs(5), || probe.count_of(id) == 2).await, "{id}");
        let runs = probe.runs_of(id);
        let gap = runs[1].at - runs[0].at;
        assert!(gap >= ms(250), "{id} rescheduled after only {gap:?}");
        assert!(events.kinds(id).contains(&JobEventKind::Rescheduled));
    }
    q.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "unit")]
struct UnitJob {
    task_id: u32,
}

async fn returns_unit(_job: UnitJob, RekkaData(probe): RekkaData<Probe>, h: JobHandler) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "result")]
struct ResultJob {
    task_id: u32,
}

async fn returns_result(_job: ResultJob, RekkaData(probe): RekkaData<Probe>, h: JobHandler) -> Result<(), std::io::Error> {
    probe.record(h.task_id(), h.attempt(), 0, 0);
    if h.attempt() == 1 { Err(std::io::Error::other("io failed")) } else { Ok(()) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unit_and_result_return_types() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .on_event(events.handler())
        .register::<UnitJob>(|w| w.build_fn(returns_unit))
        .register::<ResultJob>(|w| w.retry(RetryPolicy::RetryImmediate).build_fn(returns_result))
        .start()
        .await
        .unwrap();

    q.immediate(UnitJob { task_id: 1 }).await.unwrap();
    q.immediate(ResultJob { task_id: 2 }).await.unwrap();

    assert!(eventually(Duration::from_secs(5), || events.count(JobEventKind::Succeeded) == 2).await);
    assert_eq!(events.kinds("1"), vec![JobEventKind::Started, JobEventKind::Succeeded]);
    assert_eq!(
        events.kinds("2"),
        vec![JobEventKind::Started, JobEventKind::Failed, JobEventKind::Started, JobEventKind::Succeeded]
    );
    let failed = events.all().into_iter().find(|e| e.kind() == JobEventKind::Failed).unwrap();
    assert_eq!(failed.error(), Some("io failed"));
    q.shutdown().await.unwrap();
}

/// Duas structs com o mesmo nome de fila e formatos incompatíveis.
#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "shape")]
struct ProducerShape {
    task_id: String,
    count: String,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "shape")]
struct ConsumerShape {
    task_id: String,
    count: u32,
}

async fn never_called(_job: ConsumerShape) {
    panic!("must not run with an invalid payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_payload_goes_through_retry_policy() {
    let env = TestEnv::new();
    let events = Events::default();
    let q = env
        .builder()
        .on_event(events.handler())
        .register::<ConsumerShape>(|w| w.retry(RetryPolicy::Cancel).build_fn(never_called))
        .start()
        .await
        .unwrap();

    q.immediate(ProducerShape { task_id: "bad".into(), count: "not a number".into() }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("bad") == vec![JobEventKind::Dead]).await);
    let dead = events.last("bad").unwrap();
    assert!(dead.error().unwrap().contains("deserialize"), "{:?}", dead.error());
    assert_eq!(q.state::<ConsumerShape>("bad").await.unwrap(), None);
    q.shutdown().await.unwrap();
}

struct Config {
    multiplier: i64,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "first")]
struct FirstJob {
    task_id: String,
    value: i64,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "second")]
struct SecondJob {
    task_id: String,
    value: i64,
}

/// Vários extractors + encadeamento: um job enfileira outro pelo `RekkaClient`.
async fn first(
    job: FirstJob,
    RekkaData(probe): RekkaData<Probe>,
    config: RekkaData<Config>,
    client: RekkaClient,
    h: JobHandler,
) -> rekka::Result<()> {
    probe.record(h.task_id(), h.attempt(), job.value, 0);
    client
        .immediate(SecondJob { task_id: format!("{}-next", job.task_id), value: job.value * config.multiplier })
        .await?;
    Ok(())
}

async fn second(job: SecondJob, RekkaData(probe): RekkaData<Probe>, h: JobHandler) {
    probe.record(h.task_id(), h.attempt(), job.value, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extractors_and_job_chaining() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data(Config { multiplier: 10 })
        .register::<FirstJob>(|w| w.build_fn(first))
        .register::<SecondJob>(|w| w.build_fn(second))
        .start()
        .await
        .unwrap();

    q.immediate(FirstJob { task_id: "chain".into(), value: 4 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || probe.count() == 2).await);
    assert_eq!(probe.runs_of("chain-next")[0].value, 40);
    q.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "orchestrator")]
struct Orchestrator {
    task_id: String,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "reminder")]
struct Reminder {
    task_id: String,
}

/// Sem `.data(queues)`: o extractor `RekkaClient` basta para adicionar e remover jobs.
async fn orchestrate(
    job: Orchestrator,
    client: RekkaClient,
    RekkaData(probe): RekkaData<Probe>,
    h: JobHandler,
) -> rekka::Result<()> {
    client.immediate(SecondJob { task_id: format!("{}-next", job.task_id), value: 7 }).await?;
    client.schedule_in(Reminder { task_id: format!("{}-later", job.task_id) }, Duration::from_secs(3600)).await?;
    let removed = client.remove::<Reminder>(format!("{}-reminder", job.task_id)).await?;
    probe.record(h.task_id(), h.attempt(), i64::from(removed == Removed::Removed), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_add_and_remove_jobs_through_the_client_extractor() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .register::<Orchestrator>(|w| w.build_fn(orchestrate))
        .register::<SecondJob>(|w| w.build_fn(second))
        .start()
        .await
        .unwrap();

    q.schedule_in(Reminder { task_id: "o1-reminder".into() }, Duration::from_secs(3600)).await.unwrap();
    q.immediate(Orchestrator { task_id: "o1".into() }).await.unwrap();

    assert!(eventually(Duration::from_secs(5), || probe.count_of("o1-next") == 1).await, "the enqueued job ran");
    assert_eq!(probe.runs_of("o1-next")[0].value, 7);
    assert_eq!(probe.runs_of("o1")[0].value, 1, "remove() found the scheduled job");
    assert_eq!(q.state::<Reminder>("o1-reminder").await.unwrap(), None);
    assert!(matches!(q.state::<Reminder>("o1-later").await.unwrap(), Some(JobState::Waiting { .. })));
    q.shutdown().await.unwrap();
}
