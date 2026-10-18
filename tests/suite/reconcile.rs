use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rekka::{Enqueued, EventSource, JobEventKind, JobHandler, Queues, ReconcileHandler, RekkaQueue, RekkaState};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Probe, TestEnv, eventually, eventually_async, ms};

#[derive(Serialize, Deserialize, RekkaQueue, Clone)]
#[rekka(name = "invoice")]
struct InvoiceJob {
    task_id: String,
    sleep_ms: u64,
}

/// Simula o banco de dados com as tasks pendentes.
#[derive(Default)]
struct Db {
    pending: Mutex<Vec<InvoiceJob>>,
    runs: AtomicUsize,
    outcomes: Mutex<Vec<Enqueued>>,
}

impl Db {
    fn with(jobs: impl IntoIterator<Item = InvoiceJob>) -> Arc<Self> {
        Arc::new(Self { pending: Mutex::new(jobs.into_iter().collect()), ..Default::default() })
    }
}

async fn reconcile(handler: ReconcileHandler, RekkaState(db): RekkaState<Db>) -> rekka::Result<()> {
    db.runs.fetch_add(1, Ordering::SeqCst);
    let pending = db.pending.lock().unwrap().clone();
    for job in pending {
        let outcome = handler.add(job).await?;
        db.outcomes.lock().unwrap().push(outcome);
    }
    Ok(())
}

async fn process(h: JobHandler, job: InvoiceJob, RekkaState(probe): RekkaState<Probe>) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
    tokio::time::sleep(ms(job.sleep_ms)).await;
}

fn invoice(id: &str, sleep_ms: u64) -> InvoiceJob {
    InvoiceJob { task_id: id.into(), sleep_ms }
}

async fn start(env: &TestEnv, probe: &Arc<Probe>, db: &Arc<Db>, interval: Duration, completed_ttl: Duration) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data_arc(db.clone())
        .completed_ttl(completed_ttl)
        .with_reconcile(reconcile, interval)
        .register::<InvoiceJob>(|w| w.concurrency(4).build_fn(process))
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_runs_on_start_and_restores_jobs() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([invoice("i1", 0), invoice("i2", 0), invoice("i3", 0)]);
    let q = start(&env, &probe, &db, Duration::from_secs(3600), Duration::from_secs(60)).await;

    assert!(eventually(Duration::from_secs(5), || probe.count() == 3).await);
    assert_eq!(db.runs.load(Ordering::SeqCst), 1);
    q.shutdown().await.unwrap();
}

/// O reconcile roda repetidamente, mas nunca duplica: jobs aguardando ou executando são
/// ignorados e jobs recém-concluídos não são recriados.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_never_duplicates_jobs() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([invoice("slow", 600), invoice("fast", 0)]);
    let q = start(&env, &probe, &db, ms(100), Duration::from_secs(60)).await;

    tokio::time::sleep(ms(1500)).await;
    q.shutdown().await.unwrap();

    assert!(db.runs.load(Ordering::SeqCst) >= 5, "reconcile ran {} times", db.runs.load(Ordering::SeqCst));
    assert_eq!(probe.count_of("slow"), 1);
    assert_eq!(probe.count_of("fast"), 1);
    let outcomes = db.outcomes.lock().unwrap().clone();
    assert_eq!(outcomes.iter().filter(|o| **o == Enqueued::Created).count(), 2);
    assert!(outcomes.contains(&Enqueued::AlreadyExists));
    assert!(outcomes.contains(&Enqueued::RecentlyCompleted));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_marker_expires() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([invoice("again", 0)]);
    let q = start(&env, &probe, &db, ms(150), ms(300)).await;

    // Concluído, depois de `completed_ttl` o reconcile recria o job.
    assert!(eventually(Duration::from_secs(5), || probe.count_of("again") >= 2).await);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_enqueue_ignores_completed_marker() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([]);
    let q = start(&env, &probe, &db, Duration::from_secs(3600), Duration::from_secs(60)).await;

    q.immediate(invoice("x", 0)).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("x") == 1).await);
    assert!(
        eventually_async(Duration::from_secs(3), || async { q.state::<InvoiceJob>("x").await.unwrap().is_none() }).await
    );
    tokio::time::sleep(ms(100)).await;
    assert_eq!(q.immediate(invoice("x", 0)).await.unwrap(), Enqueued::Created);
    assert!(eventually(Duration::from_secs(3), || probe.count_of("x") == 2).await);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn periodic_reconcile_is_coordinated_across_instances() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([]);
    let mut instances = Vec::new();
    for _ in 0..3 {
        instances.push(start(&env, &probe, &db, ms(400), Duration::from_secs(60)).await);
    }

    tokio::time::sleep(ms(1300)).await;
    for q in &instances {
        q.shutdown().await.unwrap();
    }
    let runs = db.runs.load(Ordering::SeqCst);
    // ~4 ciclos coordenados (t = 0, 400, 800, 1200ms), e não 3 por ciclo nem um por instância na subida.
    assert!((3..=5).contains(&runs), "reconcile ran {runs} times");
}

/// Várias instâncias subindo juntas executam o reconcile uma única vez, e reiniciar todas
/// (um deploy) não executa de novo antes do próximo ciclo: igual a um worker recorrente.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_reconcile_runs_once_across_instances() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let db = Db::with([invoice("once", 0)]);
    let interval = Duration::from_secs(3600);
    let ttl = Duration::from_secs(60);

    let (a, b, c) = tokio::join!(
        start(&env, &probe, &db, interval, ttl),
        start(&env, &probe, &db, interval, ttl),
        start(&env, &probe, &db, interval, ttl),
    );
    assert!(eventually(Duration::from_secs(5), || probe.count_of("once") == 1).await);
    tokio::time::sleep(ms(500)).await;
    assert_eq!(db.runs.load(Ordering::SeqCst), 1, "only one instance runs the startup reconcile");
    for q in [&a, &b, &c] {
        q.shutdown().await.unwrap();
    }

    let restarted = start(&env, &probe, &db, interval, ttl).await;
    tokio::time::sleep(ms(500)).await;
    assert_eq!(db.runs.load(Ordering::SeqCst), 1, "a restart follows the shared schedule");
    restarted.shutdown().await.unwrap();
}

async fn add_many_reconcile(handler: ReconcileHandler, RekkaState(count): RekkaState<AtomicUsize>) -> rekka::Result<()> {
    let created = handler.add_many((0..500).map(|i| invoice(&format!("bulk{i}"), 0))).await?;
    count.fetch_add(created, Ordering::SeqCst);
    Ok(())
}

async fn failing_reconcile() -> Result<(), String> {
    Err("db offline".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_many_and_errors() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let events = Events::default();
    let created = Arc::new(AtomicUsize::new(0));
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(created.clone())
        .on_event(events.handler())
        .with_reconcile(add_many_reconcile, Duration::from_secs(3600))
        .with_reconcile(failing_reconcile, Duration::from_secs(3600))
        .register::<InvoiceJob>(|w| w.concurrency(50).build_fn(process))
        .start()
        .await
        .unwrap();

    assert!(eventually(Duration::from_secs(10), || probe.count() == 500).await, "ran {}", probe.count());
    assert_eq!(created.load(Ordering::SeqCst), 500);
    assert!(probe.per_task().values().all(|c| *c == 1));
    assert!(
        eventually(Duration::from_secs(2), || events.all().iter().any(|e| e.source() == EventSource::Reconcile
            && e.kind() == JobEventKind::Failed
            && e.error() == Some("db offline")))
        .await
    );
    q.shutdown().await.unwrap();
}
