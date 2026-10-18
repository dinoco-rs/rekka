use std::sync::Arc;
use std::time::{Duration, Instant};

use rekka::chrono::Utc;
use rekka::{Enqueued, JobEventKind, JobHandler, JobState, RekkaError, RekkaQueue, RekkaState, Removed};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::common::{Events, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue, Clone, Debug)]
#[rekka(name = "mail")]
struct MailJob {
    task_id: String,
    value: i64,
}

fn mail(id: &str, value: i64) -> MailJob {
    MailJob { task_id: id.into(), value }
}

async fn record(h: JobHandler, job: MailJob, RekkaState(probe): RekkaState<Probe>) {
    probe.record(h.task_id(), h.attempt(), job.value, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_creates_then_upserts() {
    let env = TestEnv::new();
    let q = env.producer().await;

    assert_eq!(q.immediate(mail("a", 1)).await.unwrap(), Enqueued::Created);
    assert!(matches!(q.state::<MailJob>("a").await.unwrap(), Some(JobState::Waiting { run_in, attempts: 0 }) if run_in.is_zero()));

    assert_eq!(q.immediate(mail("a", 2)).await.unwrap(), Enqueued::Updated);
    let stats = q.stats::<MailJob>().await.unwrap();
    assert_eq!((stats.waiting, stats.ready, stats.running), (1, 1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_and_schedule_in_are_not_ready() {
    let env = TestEnv::new();
    let q = env.producer().await;

    q.schedule(mail("at", 1), Utc::now() + chrono::Duration::hours(1)).await.unwrap();
    q.schedule_in(mail("in", 1), Duration::from_secs(600)).await.unwrap();

    let Some(JobState::Waiting { run_in, .. }) = q.state::<MailJob>("at").await.unwrap() else { panic!() };
    assert!(run_in > Duration::from_secs(3500) && run_in <= Duration::from_secs(3600));
    let Some(JobState::Waiting { run_in, .. }) = q.state::<MailJob>("in").await.unwrap() else { panic!() };
    assert!(run_in > Duration::from_secs(590) && run_in <= Duration::from_secs(600));

    let stats = q.stats::<MailJob>().await.unwrap();
    assert_eq!((stats.waiting, stats.ready), (2, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_waiting_job() {
    let env = TestEnv::new();
    let q = env.producer().await;

    q.schedule_in(mail("r", 1), Duration::from_secs(60)).await.unwrap();
    assert_eq!(q.remove::<MailJob>("r").await.unwrap(), Removed::Removed);
    assert_eq!(q.state::<MailJob>("r").await.unwrap(), None);
    assert_eq!(q.remove::<MailJob>("r").await.unwrap(), Removed::NotFound);
    assert!(env.keys("*j:r").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_task_id_is_rejected() {
    let env = TestEnv::new();
    let q = env.producer().await;
    assert!(matches!(q.immediate(mail("", 1)).await, Err(RekkaError::InvalidTaskId)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_many_in_batches() {
    let env = TestEnv::new();
    let q = env.producer().await;

    let out = q.immediate_many((0..600).map(|i| mail(&i.to_string(), i))).await.unwrap();
    assert_eq!(out.len(), 600);
    assert!(out.iter().all(|s| *s == Enqueued::Created));
    assert_eq!(q.stats::<MailJob>().await.unwrap().waiting, 600);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_job_runs_only_when_due() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .register::<MailJob>(|w| w.concurrency(2).build_fn(record))
        .start()
        .await
        .unwrap();

    let start = Instant::now();
    q.schedule_in(mail("in", 1), ms(700)).await.unwrap();
    q.schedule(mail("at", 2), Utc::now() + chrono::Duration::milliseconds(700)).await.unwrap();

    tokio::time::sleep(ms(350)).await;
    assert_eq!(probe.count(), 0, "scheduled jobs ran too early");

    assert!(eventually(Duration::from_secs(5), || probe.count() == 2).await);
    for run in probe.runs() {
        assert!(run.at - start >= ms(600), "{} ran after {:?}", run.task_id, run.at - start);
    }
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_of_waiting_job_runs_latest_payload_once() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .register::<MailJob>(|w| w.build_fn(record))
        .start()
        .await
        .unwrap();

    q.schedule_in(mail("u", 1), ms(400)).await.unwrap();
    assert_eq!(q.immediate(mail("u", 2)).await.unwrap(), Enqueued::Updated);

    assert!(eventually(Duration::from_secs(5), || probe.count() == 1).await);
    tokio::time::sleep(ms(800)).await;
    let runs = probe.runs_of("u");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].value, 2);
    q.shutdown().await.unwrap();
}

struct Gate {
    started: Notify,
    release: Notify,
}

async fn gated(h: JobHandler, job: MailJob, RekkaState(probe): RekkaState<Probe>, RekkaState(gate): RekkaState<Gate>) {
    let _running = probe.gauge.enter();
    probe.record(h.task_id(), h.attempt(), job.value, 0);
    if job.value == 1 {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_while_running_runs_after_current_and_never_in_parallel() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let gate = Arc::new(Gate { started: Notify::new(), release: Notify::new() });
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .register::<MailJob>(|w| w.concurrency(4).build_fn(gated))
        .start()
        .await
        .unwrap();

    q.immediate(mail("same", 1)).await.unwrap();
    gate.started.notified().await;

    assert_eq!(q.immediate(mail("same", 2)).await.unwrap(), Enqueued::AfterCurrentRun);
    assert_eq!(q.immediate(mail("same", 3)).await.unwrap(), Enqueued::AfterCurrentRun);
    tokio::time::sleep(ms(300)).await;
    assert_eq!(probe.count(), 1, "the new version must wait for the running one");

    gate.release.notify_one();
    assert!(eventually(Duration::from_secs(5), || probe.count() == 2).await);
    tokio::time::sleep(ms(300)).await;

    let values: Vec<i64> = probe.runs_of("same").iter().map(|r| r.value).collect();
    assert_eq!(values, vec![1, 3], "only the latest pending version runs");
    assert_eq!(probe.gauge.max(), 1);
    assert_eq!(q.state::<MailJob>("same").await.unwrap(), None);
    q.shutdown().await.unwrap();
}

async fn fail_slowly(h: JobHandler, job: MailJob, RekkaState(probe): RekkaState<Probe>, RekkaState(gate): RekkaState<Gate>) -> Result<(), String> {
    probe.record(h.task_id(), h.attempt(), job.value, 0);
    gate.started.notify_one();
    gate.release.notified().await;
    Err("boom".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_running_job_discards_its_result() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let events = Events::default();
    let gate = Arc::new(Gate { started: Notify::new(), release: Notify::new() });
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .on_event(events.handler())
        .register::<MailJob>(|w| w.retry(rekka::RetryPolicy::RetryImmediate).build_fn(fail_slowly))
        .start()
        .await
        .unwrap();

    q.immediate(mail("rm", 1)).await.unwrap();
    gate.started.notified().await;
    assert_eq!(q.remove::<MailJob>("rm").await.unwrap(), Removed::DiscardAfterRun);
    gate.release.notify_one();

    assert!(eventually(Duration::from_secs(5), || events.kinds("rm").contains(&JobEventKind::Discarded)).await);
    tokio::time::sleep(ms(400)).await;
    assert_eq!(probe.count(), 1, "removed job must not be retried");
    assert_eq!(q.state::<MailJob>("rm").await.unwrap(), None);
    q.shutdown().await.unwrap();
}
