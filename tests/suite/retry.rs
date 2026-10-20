use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rekka::{JobEventKind, JobHandler, RekkaQueue, RekkaData, RekkaWorker, RetryPolicy};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Probe, TestEnv, eventually, ms};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "flaky")]
struct FlakyJob {
    task_id: String,
    /// Quantas tentativas falham antes do sucesso.
    fail_times: u32,
}

#[derive(Default)]
struct LastAttemptSeen(AtomicBool);

async fn flaky(
    h: JobHandler,
    job: FlakyJob,
    RekkaData(probe): RekkaData<Probe>,
    RekkaData(seen): RekkaData<LastAttemptSeen>,
) -> Result<(), String> {
    probe.record(h.task_id(), h.attempt(), i64::from(h.is_last_attempt()), 0);
    if h.is_last_attempt() {
        seen.0.store(true, Ordering::SeqCst);
    }
    if h.attempt() <= job.fail_times { Err(format!("attempt {} failed", h.attempt())) } else { Ok(()) }
}

async fn start(
    env: &TestEnv,
    policy: RetryPolicy,
    configure: impl FnOnce(RekkaWorker<FlakyJob>) -> RekkaWorker<FlakyJob>,
) -> (rekka::Queues, Arc<Probe>, Events, Arc<LastAttemptSeen>) {
    let (probe, events, seen) = (Probe::new(), Events::default(), Arc::new(LastAttemptSeen::default()));
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(seen.clone())
        .on_event(events.handler())
        .register::<FlakyJob>(|w| configure(w.retry(policy).concurrency(4)).build_fn(flaky))
        .start()
        .await
        .unwrap();
    (q, probe, events, seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_immediate_until_success() {
    let env = TestEnv::new();
    let (q, probe, events, _) = start(&env, RetryPolicy::RetryImmediate, |w| w).await;

    q.immediate(FlakyJob { task_id: "f".into(), fail_times: 2 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("f").contains(&JobEventKind::Succeeded)).await);

    let attempts: Vec<u32> = probe.runs_of("f").iter().map(|r| r.attempt).collect();
    assert_eq!(attempts, vec![1, 2, 3]);
    let finals: Vec<JobEventKind> = events.kinds("f").into_iter().filter(|k| *k != JobEventKind::Started).collect();
    assert_eq!(finals, vec![JobEventKind::Failed, JobEventKind::Failed, JobEventKind::Succeeded]);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_after_range_waits_within_range() {
    let env = TestEnv::new();
    let (q, probe, events, _) = start(&env, RetryPolicy::RetryAfterRange(ms(300), ms(500)), |w| w).await;

    q.immediate(FlakyJob { task_id: "r".into(), fail_times: 1 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || probe.count_of("r") == 2).await);

    let runs = probe.runs_of("r");
    let gap = runs[1].at - runs[0].at;
    assert!(gap >= ms(280), "retried after {gap:?}");
    assert!(gap < ms(1500), "retried after {gap:?}");
    let failed = events.all().into_iter().find(|e| e.kind() == JobEventKind::Failed).unwrap();
    let retry_in = failed.retry_in().unwrap();
    assert!((ms(300)..=ms(500)).contains(&retry_in), "{retry_in:?}");
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_policy_never_retries() {
    let env = TestEnv::new();
    let (q, probe, events, seen) = start(&env, RetryPolicy::Cancel, |w| w).await;

    q.immediate(FlakyJob { task_id: "c".into(), fail_times: 5 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("c").contains(&JobEventKind::Dead)).await);
    tokio::time::sleep(ms(300)).await;

    assert_eq!(probe.count_of("c"), 1);
    assert!(seen.0.load(Ordering::SeqCst), "is_last_attempt must be true with RetryPolicy::Cancel");
    assert_eq!(q.state::<FlakyJob>("c").await.unwrap(), None);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_attempts_marks_job_dead() {
    let env = TestEnv::new();
    let (q, probe, events, _) = start(&env, RetryPolicy::RetryImmediate, |w| w.max_attempts(3)).await;

    q.immediate(FlakyJob { task_id: "m".into(), fail_times: 100 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("m").contains(&JobEventKind::Dead)).await);
    tokio::time::sleep(ms(300)).await;

    let runs = probe.runs_of("m");
    assert_eq!(runs.iter().map(|r| r.attempt).collect::<Vec<_>>(), vec![1, 2, 3]);
    // `value` guarda o is_last_attempt() visto pelo handler.
    assert_eq!(runs.iter().map(|r| r.value).collect::<Vec<_>>(), vec![0, 0, 1]);
    let dead = events.last("m").unwrap();
    assert_eq!(dead.kind(), JobEventKind::Dead);
    assert_eq!(dead.error(), Some("attempt 3 failed"));
    assert_eq!(q.state::<FlakyJob>("m").await.unwrap(), None);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_limit_is_ten_attempts() {
    let env = TestEnv::new();
    let (q, probe, events, _) = start(&env, RetryPolicy::RetryImmediate, |w| w).await;

    q.immediate(FlakyJob { task_id: "ten".into(), fail_times: 100 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("ten").contains(&JobEventKind::Dead)).await);
    tokio::time::sleep(ms(300)).await;

    let attempts: Vec<u32> = probe.runs_of("ten").iter().map(|r| r.attempt).collect();
    assert_eq!(attempts, (1..=10).collect::<Vec<_>>());
    assert_eq!(probe.runs_of("ten").last().unwrap().value, 1, "the 10th attempt is the last one");
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unlimited_attempts_retry_until_success() {
    let env = TestEnv::new();
    let (q, probe, events, seen) = start(&env, RetryPolicy::RetryImmediate, RekkaWorker::unlimited_attempts).await;

    q.immediate(FlakyJob { task_id: "u".into(), fail_times: 15 }).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || events.kinds("u").contains(&JobEventKind::Succeeded)).await);
    assert_eq!(probe.count_of("u"), 16);
    assert!(!seen.0.load(Ordering::SeqCst), "no attempt is the last one without a limit");
    q.shutdown().await.unwrap();
}
