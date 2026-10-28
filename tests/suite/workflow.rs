use std::sync::Arc;
use std::time::Duration;

use rekka::{
    FromContext, JobEventKind, JobHandler, Queues, RekkaContext, RekkaData, RekkaError, RekkaQueue, Removed,
    RetryPolicy, StepResult,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::common::{Events, Probe, TestEnv, eventually, eventually_async, ms};

/// Job de entrada: o workflow só começa por ele.
#[derive(Serialize, Deserialize, RekkaQueue, Clone)]
#[rekka(name = "signup")]
struct Signup {
    task_id: String,
    account: i64,
    value: i64,
    mode: String,
}

fn signup(id: &str, account: i64, mode: &str) -> Signup {
    Signup { task_id: id.into(), account, value: 1, mode: mode.into() }
}

/// Passa de um passo para o outro.
#[derive(Serialize, Deserialize)]
struct Account {
    account: i64,
    value: i64,
    mode: String,
    trail: Vec<String>,
}

struct Gate {
    started: Notify,
    release: Notify,
}

fn gate() -> Arc<Gate> {
    Arc::new(Gate { started: Notify::new(), release: Notify::new() })
}

/// Cada execução de passo fica no probe como `<fila>@<task_id>`.
fn record(probe: &Probe, h: &JobHandler, value: i64) {
    probe.record(&format!("{}@{}", h.queue(), h.task_id()), h.attempt(), value, 0);
}

/// `JobHandler` é um extractor como os outros: aqui no meio dos argumentos.
async fn create(job: Signup, h: JobHandler, RekkaData(probe): RekkaData<Probe>) -> Result<Account, String> {
    record(&probe, &h, job.value);
    Ok(Account { account: job.account, value: job.value + 1, mode: job.mode, trail: vec![h.queue().to_owned()] })
}

async fn validate(
    mut acc: Account,
    RekkaData(probe): RekkaData<Probe>,
    h: JobHandler,
) -> Result<StepResult<Account>, String> {
    record(&probe, &h, acc.value);
    match acc.mode.as_str() {
        "fail_once" if h.attempt() == 1 => return Err("validation service unavailable".into()),
        "cancel" => return Ok(h.cancel().into()),
        "finish" => return Ok(h.success().into()),
        _ => {}
    }
    acc.value *= 10;
    acc.trail.push(h.queue().to_owned());
    Ok(StepResult::Next(acc))
}

/// Extractor próprio que lê a execução atual do contexto.
struct TaskId(String);

impl FromContext for TaskId {
    fn from_context(cx: &RekkaContext) -> rekka::Result<Self> {
        let job = cx.job().ok_or_else(|| RekkaError::InvalidConfig("TaskId outside a job".into()))?;
        Ok(TaskId(job.task_id().to_owned()))
    }
}

async fn notify(acc: Account, TaskId(id): TaskId, RekkaData(probe): RekkaData<Probe>, h: JobHandler) {
    assert_eq!(id, h.task_id());
    assert_eq!(acc.trail, ["signup", "signup.1"]);
    record(&probe, &h, acc.value);
}

async fn start(env: &TestEnv, probe: &Arc<Probe>, events: &Events) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .on_event(events.handler())
        .register_workflow(|w| {
            w.concurrency(4)
                .retry(RetryPolicy::RetryAfterRange(ms(100), ms(100)))
                .start_with(create)
                .then(validate)
                .then(notify)
        })
        .start()
        .await
        .unwrap()
}

/// Jobs e listas de espera de qualquer passo (marcas `d:` têm TTL e ficam de fora).
async fn leftovers(env: &TestEnv) -> Vec<String> {
    env.keys("{signup}:*").await.into_iter().filter(|k| k.contains("j:") || k.contains("g:")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_step_receives_the_previous_output() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate_many([signup("r1", 1, "ok"), signup("r2", 2, "ok")]).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || probe.count_of("signup.2@r1") == 1 && probe.count_of("signup.2@r2") == 1)
            .await,
        "{:?}",
        probe.per_task()
    );

    for id in ["r1", "r2"] {
        let steps = ["signup", "signup.1", "signup.2"].map(|q| probe.runs_of(&format!("{q}@{id}")));
        assert!(steps.iter().all(|runs| runs.len() == 1 && runs[0].attempt == 1));
        assert_eq!(steps.each_ref().map(|runs| runs[0].value), [1, 2, 20], "{id}");
        assert!(steps[0][0].at <= steps[1][0].at && steps[1][0].at <= steps[2][0].at, "steps ran out of order");
    }

    // Cada passo é uma fila própria, com os próprios eventos e o `task_id` da execução.
    assert!(eventually(Duration::from_secs(3), || events.kinds("r1").len() == 6).await);
    let succeeded: Vec<_> = events
        .all()
        .into_iter()
        .filter(|e| e.task_id() == Some("r1") && e.is_success())
        .map(|e| e.job_name())
        .collect();
    assert_eq!(succeeded.len(), 3);
    for name in ["signup", "signup.1", "signup.2"] {
        assert!(succeeded.contains(&name), "{succeeded:?}");
    }

    assert!(eventually_async(Duration::from_secs(3), || async { leftovers(&env).await.is_empty() }).await);
    // Só a fila de entrada recebe jobs do reconcile: os outros passos não gravam a marca.
    assert!(env.keys("{signup}:*:d:*").await.is_empty(), "{:?}", env.keys("{signup}:*:d:*").await);
    assert_eq!(env.keys("{signup}:d:*").await.len(), 2);
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_retries_only_that_step() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate(signup("f1", 1, "fail_once")).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || probe.count_of("signup.2@f1") == 1).await, "{:?}", probe.per_task());

    assert_eq!(probe.count_of("signup@f1"), 1);
    assert_eq!(probe.runs_of("signup.1@f1").iter().map(|r| r.attempt).collect::<Vec<_>>(), [1, 2]);
    // Tentativas contam por passo: o passo seguinte começa da primeira.
    assert_eq!(probe.runs_of("signup.2@f1")[0].attempt, 1);
    let failed = events.all().into_iter().find(|e| e.kind() == JobEventKind::Failed).unwrap();
    assert_eq!((failed.job_name(), failed.error()), ("signup.1", Some("validation service unavailable")));
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_or_success_ends_the_run_at_that_step() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = start(&env, &probe, &events).await;

    q.immediate_many([signup("c1", 1, "cancel"), signup("s1", 2, "finish")]).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || events.kinds("c1").contains(&JobEventKind::Cancelled)
            && events.kinds("s1").iter().filter(|k| **k == JobEventKind::Succeeded).count() == 2)
        .await
    );
    tokio::time::sleep(ms(500)).await;
    assert_eq!((probe.count_of("signup.1@c1"), probe.count_of("signup.2@c1")), (1, 0));
    assert_eq!((probe.count_of("signup.1@s1"), probe.count_of("signup.2@s1")), (1, 0));
    assert!(leftovers(&env).await.is_empty());
    q.shutdown().await.unwrap();
}

async fn quick_create(job: Signup, RekkaData(probe): RekkaData<Probe>, h: JobHandler) -> Result<Account, String> {
    let _key = probe.keys.enter(format!("{}:{}", h.queue(), job.account));
    record(&probe, &h, job.value);
    Ok(Account { account: job.account, value: job.value, mode: job.mode, trail: Vec::new() })
}

async fn slow_step(acc: Account, RekkaData(probe): RekkaData<Probe>, h: JobHandler) -> Result<Account, String> {
    let _key = probe.keys.enter(format!("{}:{}", h.queue(), acc.account));
    record(&probe, &h, acc.value);
    tokio::time::sleep(ms(200)).await;
    Ok(acc)
}

/// A key é calculada do job de entrada e acompanha a execução: em cada passo, no máximo um job
/// por key. O passo de entrada é rápido, então os três jobs da conta 1 chegam juntos ao passo 1:
/// sem a key, rodariam ao mesmo tempo.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_key_follows_the_run_through_every_step() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .register_workflow::<Signup>(|w| {
            w.concurrency(4)
                .concurrency_by_key(|job| job.account)
                .start_with(quick_create)
                .then(slow_step)
                .then(slow_step)
        })
        .start()
        .await
        .unwrap();

    q.immediate_many([signup("k1", 1, "ok"), signup("k2", 1, "ok"), signup("k3", 1, "ok"), signup("k4", 2, "ok")])
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(10), || ["k1", "k2", "k3", "k4"]
            .iter()
            .all(|id| probe.count_of(&format!("signup.2@{id}")) == 1))
        .await,
        "{:?}",
        probe.per_task()
    );
    assert_eq!(probe.keys.violations(), 0, "two jobs with the same key ran at once in a step");
    assert!(eventually_async(Duration::from_secs(3), || async { leftovers(&env).await.is_empty() }).await);
    let stats = q.stats::<Signup>().await.unwrap();
    assert_eq!(stats.locked_keys, 0);
    q.shutdown().await.unwrap();
}

async fn gated_create(
    job: Signup,
    RekkaData(probe): RekkaData<Probe>,
    RekkaData(gate): RekkaData<Gate>,
    h: JobHandler,
) -> StepResult<Account> {
    record(&probe, &h, job.value);
    gate.started.notify_one();
    gate.release.notified().await;
    StepResult::Next(Account { account: job.account, value: job.value, mode: job.mode, trail: Vec::new() })
}

async fn gated_step(
    acc: Account,
    RekkaData(probe): RekkaData<Probe>,
    RekkaData(gate): RekkaData<Gate>,
    h: JobHandler,
) -> StepResult<Account> {
    record(&probe, &h, acc.value);
    if acc.mode == "gate" {
        gate.started.notify_one();
        gate.release.notified().await;
    }
    StepResult::Next(acc)
}

async fn last(acc: Account, RekkaData(probe): RekkaData<Probe>, h: JobHandler) {
    record(&probe, &h, acc.value);
}

/// `remove` durante o passo de entrada descarta o resultado: o próximo passo nunca começa.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_the_entry_job_while_it_runs_stops_the_run() {
    let env = TestEnv::new();
    let (probe, gate, events) = (Probe::new(), gate(), Events::default());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .on_event(events.handler())
        .register_workflow(|w| w.start_with(gated_create).then(last))
        .start()
        .await
        .unwrap();

    q.immediate(signup("rm1", 1, "ok")).await.unwrap();
    gate.started.notified().await;
    assert_eq!(q.remove::<Signup>("rm1").await.unwrap(), Removed::DiscardAfterRun);
    gate.release.notify_one();

    assert!(eventually(Duration::from_secs(3), || events.kinds("rm1").contains(&JobEventKind::Discarded)).await);
    tokio::time::sleep(ms(500)).await;
    assert_eq!(probe.count_of("signup.1@rm1"), 0);
    assert!(leftovers(&env).await.is_empty(), "{:?}", leftovers(&env).await);
    q.shutdown().await.unwrap();
}

/// `purge` com o job de entrada apaga todos os passos e conta os jobs de todos eles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purging_the_entry_queue_purges_every_step() {
    let env = TestEnv::new();
    let (probe, gate) = (Probe::new(), gate());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .register_workflow(|w| w.concurrency(1).start_with(create).then(gated_step).then(last))
        .start()
        .await
        .unwrap();

    // Passo 1: um executando (preso no gate) e dois esperando. Entrada: cinco agendados.
    q.immediate(signup("p0", 1, "gate")).await.unwrap();
    gate.started.notified().await;
    q.immediate_many([signup("p1", 1, "ok"), signup("p2", 1, "ok")]).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || probe.count_of("signup@p1") + probe.count_of("signup@p2") == 2).await
    );
    for i in 0..5 {
        q.schedule_in(signup(&format!("later{i}"), 1, "ok"), Duration::from_secs(3600)).await.unwrap();
    }

    assert_eq!(q.purge::<Signup>().await.unwrap(), 8);
    assert!(env.keys("{signup}:*").await.is_empty(), "{:?}", env.keys("{signup}:*").await);

    // O passo que estava executando termina, mas não avança nem recria nada.
    gate.release.notify_one();
    tokio::time::sleep(ms(500)).await;
    assert_eq!(probe.count_of("signup.2@p0"), 0);
    assert!(leftovers(&env).await.is_empty(), "{:?}", leftovers(&env).await);
    q.shutdown().await.unwrap();
}

/// O janitor também cuida das filas dos passos: um job perdido no passo 1 volta e segue o fluxo.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn janitor_repairs_step_queues() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let mut redis = env.redis().await;
    let acc = Account { account: 1, value: 7, mode: "ok".into(), trail: vec!["signup".into()] };
    let _: () = redis::cmd("HSET")
        .arg(format!("{}:{{signup}}:1:j:lost", env.prefix))
        .arg(&["p", &serde_json::to_string(&acc).unwrap(), "a", "0", "s", "w"])
        .query_async(&mut redis)
        .await
        .unwrap();

    let q = env
        .builder()
        .cleanup_interval(ms(200))
        .data_arc(probe.clone())
        .register_workflow(|w| w.start_with(create).then(validate).then(notify))
        .start()
        .await
        .unwrap();

    assert!(
        eventually(Duration::from_secs(15), || probe.count_of("signup.2@lost") == 1).await,
        "{:?}",
        probe.per_task()
    );
    assert_eq!(probe.runs_of("signup.2@lost")[0].value, 70);
    assert_eq!(probe.count_of("signup@lost"), 0);
    q.shutdown().await.unwrap();
}
