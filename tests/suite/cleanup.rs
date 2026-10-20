use std::sync::Arc;
use std::time::Duration;

use rekka::chrono::Utc;
use rekka::{JobHandler, RekkaError, RekkaExecutionResult, RekkaQueue, RekkaData, RetryPolicy};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::common::{Probe, TestEnv, eventually, eventually_async, ms};

#[derive(Serialize, Deserialize, RekkaQueue, Clone)]
#[rekka(name = "tidy")]
struct TidyJob {
    task_id: String,
    key: u32,
    mode: String,
}

fn tidy_job(id: &str, key: u32, mode: &str) -> TidyJob {
    TidyJob { task_id: id.into(), key, mode: mode.into() }
}

struct Gate {
    started: Notify,
    release: Notify,
}

async fn tidy(h: JobHandler, job: TidyJob, RekkaData(probe): RekkaData<Probe>, RekkaData(gate): RekkaData<Gate>) -> RekkaExecutionResult {
    probe.record(h.task_id(), h.attempt(), 0, 0);
    match job.mode.as_str() {
        "cancel" => h.cancel(),
        "dead" => h.err("always fails"),
        "retry" if h.attempt() == 1 => h.err("first attempt fails"),
        "later" if h.attempt() == 1 => h.retry_after(ms(100)),
        "gate" => {
            gate.started.notify_one();
            gate.release.notified().await;
            h.success()
        }
        _ => h.success(),
    }
}

/// Depois de todo tipo de fluxo (sucesso, cancelamento, morte, retry, reagendamento, keys,
/// remoção, re-enfileirar durante a execução), nada permanente sobra no Redis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_work_leaves_nothing_behind() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let gate = Arc::new(Gate { started: Notify::new(), release: Notify::new() });
    let q = env
        .builder()
        .completed_ttl(ms(300))
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .register::<TidyJob>(|w| {
            w.concurrency(4)
                .retry(RetryPolicy::RetryImmediate)
                .max_attempts(2)
                .concurrency_by_key(|job| job.key)
                .build_fn(tidy)
        })
        .start()
        .await
        .unwrap();

    q.immediate(tidy_job("gated", 1, "gate")).await.unwrap();
    gate.started.notified().await;
    q.immediate(tidy_job("gated", 1, "ok")).await.unwrap(); // roda depois da atual
    q.immediate(tidy_job("parked", 1, "ok")).await.unwrap(); // bloqueado pela key 1
    q.schedule_in(tidy_job("removed", 2, "ok"), Duration::from_secs(60)).await.unwrap();
    q.remove::<TidyJob>("removed").await.unwrap();
    let mut jobs = Vec::new();
    for (i, mode) in ["ok", "cancel", "dead", "retry", "later"].iter().enumerate() {
        for n in 0..5 {
            jobs.push(tidy_job(&format!("{mode}-{n}"), 10 + (i as u32 * 5 + n) % 4, mode));
        }
    }
    q.immediate_many(jobs).await.unwrap();
    gate.release.notify_one();

    let expected = 2 + 1 + 5 + 5 + 10 + 10 + 10; // gated x2, parked, ok, cancel, dead x2, retry x2, later x2
    assert!(eventually(Duration::from_secs(10), || probe.count() == expected).await, "ran {}", probe.count());
    assert!(
        eventually_async(Duration::from_secs(5), || async { q.stats::<TidyJob>().await.unwrap() == Default::default() }).await
    );
    q.shutdown().await.unwrap();

    let permanent = env.permanent_keys().await;
    assert!(permanent.is_empty(), "keys without TTL left behind: {permanent:?}");
    // As marcas de conclusão expiram; da fila não sobra nada.
    assert!(eventually_async(Duration::from_secs(3), || async { env.keys("{tidy}:*").await.is_empty() }).await);
}

#[derive(Serialize, Deserialize, RekkaQueue, Clone)]
#[rekka(name = "fix")]
struct FixJob {
    task_id: String,
    key: u32,
}

async fn fix(h: JobHandler, _job: FixJob, RekkaData(probe): RekkaData<Probe>) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
}

/// Estado inconsistente criado "na mão": o janitor recupera os jobs que ainda dá para
/// executar e apaga o lixo — sem tocar em filas de outros serviços.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn janitor_repairs_and_removes_inconsistent_state() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let mut redis = env.redis().await;
    let base = format!("{}:{{fix}}:", env.prefix);
    let future = Utc::now().timestamp_millis() + 3_600_000;
    let payload = |id: &str, key: u32| serde_json::to_string(&FixJob { task_id: id.into(), key }).unwrap();

    let mut setup = redis::pipe();
    // Entradas de índice sem job.
    setup.cmd("ZADD").arg(format!("{base}queue")).arg(future).arg("ghost-q").ignore();
    setup.cmd("ZADD").arg(format!("{base}active")).arg(future).arg("ghost-a").ignore();
    // Jobs fora de qualquer índice (ficariam presos para sempre).
    setup.cmd("HSET").arg(format!("{base}j:lost-w")).arg(&["p", &payload("lost-w", 1), "a", "0", "s", "w"]).ignore();
    setup.cmd("HSET").arg(format!("{base}j:lost-r")).arg(&["p", &payload("lost-r", 2), "a", "1", "s", "r"]).ignore();
    setup.cmd("HSET").arg(format!("{base}j:lost-g")).arg(&["p", &payload("lost-g", 5), "a", "0", "s", "g", "k", "5"]).ignore();
    // Hash corrompido.
    setup.cmd("HSET").arg(format!("{base}j:corrupt")).arg("a").arg("0").ignore();
    // Lista de espera sem dono, com entradas obsoletas.
    setup.cmd("RPUSH").arg(format!("{base}g:7")).arg("missing-1").arg("missing-2").ignore();
    // Fila de outro serviço (não registrada aqui): não pode ser tocada.
    setup.cmd("ZADD").arg(format!("{}:{{other}}:queue", env.prefix)).arg(future).arg("ghost-o").ignore();
    let _: () = setup.query_async(&mut redis).await.unwrap();

    let q = env
        .builder()
        .cleanup_interval(ms(200))
        .data_arc(probe.clone())
        .register::<FixJob>(|w| w.lease(ms(300)).concurrency(4).concurrency_by_key(|job| job.key).build_fn(fix))
        .start()
        .await
        .unwrap();

    assert!(
        eventually(Duration::from_secs(15), || ["lost-w", "lost-r", "lost-g"].iter().all(|id| probe.count_of(id) == 1)).await,
        "recovered jobs did not run: {:?}",
        probe.per_task()
    );
    // `lost-r` estava "executando" (tentativa 1) sem lease: volta como tentativa 2.
    assert_eq!(probe.runs_of("lost-r")[0].attempt, 2);

    let leftovers = || async {
        env.keys("{fix}:*").await.into_iter().filter(|k| !k.contains("}:d:")).collect::<Vec<_>>()
    };
    assert!(
        eventually_async(Duration::from_secs(15), || async { leftovers().await.is_empty() }).await,
        "garbage left: {:?}",
        leftovers().await
    );
    let other: Option<f64> = redis::cmd("ZSCORE")
        .arg(format!("{}:{{other}}:queue", env.prefix))
        .arg("ghost-o")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(other.is_some(), "janitor touched a queue it does not own");
    q.shutdown().await.unwrap();
}

/// Muitas keys presas por donos inexistentes: a varredura incremental libera todas e o
/// cursor auxiliar desaparece no fim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_locks_are_swept_incrementally() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let mut redis = env.redis().await;
    let locks = format!("{}:{{fix}}:locks", env.prefix);
    let mut setup = redis::cmd("HSET");
    setup.arg(&locks);
    for i in 0..500 {
        setup.arg(format!("k{i}")).arg(format!("ghost-{i}"));
    }
    let _: () = setup.query_async(&mut redis).await.unwrap();

    let q = env
        .builder()
        .data_arc(probe.clone())
        .register::<FixJob>(|w| w.lease(ms(200)).concurrency_by_key(|job| job.key).build_fn(fix))
        .start()
        .await
        .unwrap();

    assert!(
        eventually_async(Duration::from_secs(10), || async { env.keys("{fix}:*").await.is_empty() }).await,
        "left: {:?}",
        env.keys("{fix}:*").await
    );
    q.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "legacy")]
struct LegacyJob {
    task_id: u32,
}

async fn hold(h: JobHandler, job: TidyJob, RekkaData(probe): RekkaData<Probe>, RekkaData(gate): RekkaData<Gate>) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
    if job.mode == "gate" {
        gate.started.notify_one();
        gate.release.notified().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_removes_a_whole_queue() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let gate = Arc::new(Gate { started: Notify::new(), release: Notify::new() });
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data_arc(gate.clone())
        .register::<TidyJob>(|w| w.concurrency(1).concurrency_by_key(|job| job.key).build_fn(hold))
        .start()
        .await
        .unwrap();

    q.immediate(tidy_job("running", 1, "gate")).await.unwrap();
    gate.started.notified().await;
    q.immediate(tidy_job("blocked", 1, "ok")).await.unwrap();
    q.immediate_many((0..100).map(|i| tidy_job(&format!("w{i}"), 2 + i, "ok"))).await.unwrap();
    for i in 0..20 {
        q.schedule_in(tidy_job(&format!("s{i}"), 500 + i, "ok"), Duration::from_secs(3600)).await.unwrap();
    }
    q.immediate_many((0..10).map(|task_id| LegacyJob { task_id })).await.unwrap();

    assert_eq!(q.purge::<TidyJob>().await.unwrap(), 122);
    assert!(env.keys("{tidy}:*").await.is_empty());

    // O job que estava executando termina, mas não recria nada.
    gate.release.notify_one();
    tokio::time::sleep(ms(500)).await;
    assert!(env.keys("{tidy}:*").await.is_empty(), "{:?}", env.keys("{tidy}:*").await);
    assert_eq!(probe.count(), 1);

    // Fila cujo tipo "não existe mais" no serviço: removida pelo nome.
    assert_eq!(q.purge_queue("legacy").await.unwrap(), 10);
    assert!(env.keys("{legacy}:*").await.is_empty());
    assert!(matches!(q.purge_queue("bad{name").await, Err(RekkaError::InvalidConfig(_))));
    q.shutdown().await.unwrap();
}

/// O janitor segue o agendamento compartilhado de um worker recorrente: instâncias com as
/// mesmas filas usam um único agendamento (e um único lock) no Redis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn janitor_schedule_is_shared_across_instances() {
    let env = TestEnv::new();
    let mut instances = Vec::new();
    for _ in 0..3 {
        let probe = Probe::new();
        let gate = Gate { started: Notify::new(), release: Notify::new() };
        instances.push(
            env.builder()
                .data_arc(probe)
                .data(gate)
                .cleanup_interval(ms(200))
                .register::<TidyJob>(|w| w.build_fn(tidy))
                .start()
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(ms(500)).await;
    let schedules = env.keys("{rec:__janitor_*}:next").await;
    assert_eq!(schedules.len(), 1, "{schedules:?}");
    for q in &instances {
        q.shutdown().await.unwrap();
    }
}
