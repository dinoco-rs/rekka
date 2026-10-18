use std::sync::Arc;
use std::time::Duration;

use rekka::{JobHandler, JobState, Queues, RekkaQueue, RekkaState, RetryPolicy};
use serde::{Deserialize, Serialize};

use crate::common::{Instance, Probe, TestEnv, eventually, eventually_async, ms};

#[derive(Serialize, Deserialize, RekkaQueue, Clone)]
#[rekka(name = "mail")]
struct MailJob {
    task_id: String,
    business_id: i64,
    sleep_ms: u64,
    #[serde(default)]
    fail_first: bool,
}

fn mail(id: &str, business_id: i64, sleep_ms: u64) -> MailJob {
    MailJob { task_id: id.into(), business_id, sleep_ms, fail_first: false }
}

async fn send(
    h: JobHandler,
    job: MailJob,
    RekkaState(probe): RekkaState<Probe>,
    RekkaState(instance): RekkaState<Instance>,
) -> Result<(), String> {
    let _total = probe.gauge.enter();
    let _key = probe.keys.enter(job.business_id.to_string());
    probe.record(h.task_id(), h.attempt(), job.business_id, instance.0);
    tokio::time::sleep(ms(job.sleep_ms)).await;
    if job.fail_first && h.attempt() == 1 { Err("first attempt fails".into()) } else { Ok(()) }
}

async fn keyed_instance(env: &TestEnv, probe: &Arc<Probe>, id: u32, concurrency: usize) -> Queues {
    env.builder()
        .data_arc(probe.clone())
        .data(Instance(id))
        .register::<MailJob>(|w| {
            w.concurrency(concurrency)
                .retry(RetryPolicy::RetryAfterRange(ms(400), ms(400)))
                .concurrency_by_key(|job| job.business_id)
                .build_fn(send)
        })
        .start()
        .await
        .unwrap()
}

async fn assert_no_leftovers(env: &TestEnv, q: &Queues) {
    let stats = q.stats::<MailJob>().await.unwrap();
    assert_eq!((stats.waiting, stats.running, stats.locked_keys), (0, 0, 0), "{stats:?}");
    assert!(env.keys("*g:*").await.is_empty(), "group lists left behind");
    assert!(env.keys("*j:*").await.is_empty(), "job hashes left behind");
}

/// O exemplo do enunciado: concorrência 5, 5 jobs, 3 da mesma empresa.
/// Executam 3 em paralelo (um por empresa); os outros 2 da empresa 1 esperam.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_job_per_key_at_a_time() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = keyed_instance(&env, &probe, 0, 5).await;

    q.immediate_many([
        mail("a1", 1, 300),
        mail("a2", 1, 300),
        mail("a3", 1, 300),
        mail("b1", 2, 300),
        mail("c1", 3, 300),
    ])
    .await
    .unwrap();

    assert!(eventually(Duration::from_secs(3), || probe.gauge.current() == 3).await);
    tokio::time::sleep(ms(100)).await;
    assert_eq!(probe.gauge.current(), 3, "one job per business must run in parallel");
    assert!(
        matches!(q.state::<MailJob>("a2").await.unwrap(), Some(JobState::Blocked { .. }))
            || matches!(q.state::<MailJob>("a3").await.unwrap(), Some(JobState::Blocked { .. }))
    );

    assert!(eventually(Duration::from_secs(5), || probe.count() == 5).await);
    q.shutdown().await.unwrap();
    assert_eq!(probe.keys.violations(), 0);
    assert_eq!(probe.gauge.max(), 3);
    // A mesma empresa roda em sequência e na ordem de chegada.
    let order: Vec<String> = probe.runs().into_iter().filter(|r| r.value == 1).map(|r| r.task_id).collect();
    assert_eq!(order, vec!["a1", "a2", "a3"]);
    assert_no_leftovers(&env, &q).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_exclusivity_holds_across_instances() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let instances = [
        keyed_instance(&env, &probe, 1, 8).await,
        keyed_instance(&env, &probe, 2, 8).await,
        keyed_instance(&env, &probe, 3, 8).await,
    ];

    let jobs: Vec<MailJob> = (0..400).map(|i| mail(&format!("m{i}"), i % 10, 5 + (i % 3) as u64)).collect();
    instances[0].immediate_many(jobs).await.unwrap();

    assert!(eventually(Duration::from_secs(30), || probe.count() >= 400).await, "ran {}", probe.count());
    tokio::time::sleep(ms(300)).await;
    for q in &instances {
        q.shutdown().await.unwrap();
    }

    assert_eq!(probe.keys.violations(), 0, "two jobs with the same key ran at the same time");
    assert_eq!(probe.count(), 400);
    assert!(probe.per_task().values().all(|c| *c == 1));
    assert!(probe.gauge.max() <= 10, "at most one job per key (10 keys): {}", probe.gauge.max());
    assert!(probe.gauge.max() > 1);
    assert_no_leftovers(&env, &instances[0]).await;
}

/// Quem enfileira não registrou o worker, então não conhece a função de key:
/// a key é calculada pelo worker no momento do claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_without_worker_config_still_respects_keys() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let producer = env.producer().await;
    let workers = [keyed_instance(&env, &probe, 1, 6).await, keyed_instance(&env, &probe, 2, 6).await];

    let jobs: Vec<MailJob> = (0..120).map(|i| mail(&format!("p{i}"), i % 4, 10)).collect();
    producer.immediate_many(jobs).await.unwrap();

    assert!(eventually(Duration::from_secs(20), || probe.count() >= 120).await, "ran {}", probe.count());
    tokio::time::sleep(ms(300)).await;
    for q in &workers {
        q.shutdown().await.unwrap();
    }
    assert_eq!(probe.keys.violations(), 0);
    assert!(probe.per_task().values().all(|c| *c == 1));
    assert!(probe.gauge.max() <= 4);
    assert_no_leftovers(&env, &producer).await;
}

/// A key só fica presa durante a execução: enquanto um job espera o retry,
/// outro job da mesma key pode executar.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_for_retry_releases_the_key() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = keyed_instance(&env, &probe, 0, 4).await;

    q.immediate(MailJob { fail_first: true, ..mail("first", 7, 50) }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    q.immediate(mail("second", 7, 10)).await.unwrap();

    assert!(eventually(Duration::from_secs(5), || probe.count() == 3).await);
    let order: Vec<(String, u32)> = probe.runs().into_iter().map(|r| (r.task_id, r.attempt)).collect();
    assert_eq!(order, vec![("first".into(), 1), ("second".into(), 1), ("first".into(), 2)]);
    q.shutdown().await.unwrap();
    assert_eq!(probe.keys.violations(), 0);
    assert_no_leftovers(&env, &q).await;
}

/// Uma key presa por um job que não existe mais (ex.: chave apagada manualmente)
/// é liberada pelo reaper.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_key_lock_is_released() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let mut redis = env.redis().await;
    let locks = format!("{}:{{mail}}:locks", env.prefix);
    let _: () = redis::cmd("HSET").arg(&locks).arg("9").arg("ghost").query_async(&mut redis).await.unwrap();

    // Enfileirado antes do worker subir: ele estaciona atrás do dono fantasma.
    env.producer().await.immediate(mail("stuck", 9, 0)).await.unwrap();
    let q = env
        .builder()
        .data_arc(probe.clone())
        .data(Instance(0))
        .register::<MailJob>(|w| w.lease(ms(400)).concurrency_by_key(|job| job.business_id).build_fn(send))
        .start()
        .await
        .unwrap();

    assert!(eventually(Duration::from_secs(5), || probe.count() == 1).await, "orphan lock was not released");
    q.shutdown().await.unwrap();
    assert_no_leftovers(&env, &q).await;
}

/// Atualizar (upsert) um job bloqueado pela key move-o para a nova key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_of_blocked_job_moves_it_to_the_new_key() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let q = keyed_instance(&env, &probe, 0, 4).await;

    q.immediate(mail("long", 1, 1000)).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count() == 1).await);
    q.immediate(mail("moved", 1, 0)).await.unwrap();
    assert!(
        eventually_async(Duration::from_secs(3), || async {
            matches!(q.state::<MailJob>("moved").await.unwrap(), Some(JobState::Blocked { .. }))
        })
        .await
    );

    q.immediate(mail("moved", 2, 0)).await.unwrap();
    assert!(eventually(Duration::from_secs(2), || probe.count() == 2).await);
    let moved = &probe.runs_of("moved")[0];
    assert_eq!(moved.value, 2);
    assert!(eventually(Duration::from_secs(1), || probe.gauge.current() == 1).await, "`long` is still running");
    q.shutdown().await.unwrap();
    assert_eq!(probe.keys.violations(), 0);
    assert_no_leftovers(&env, &q).await;
}
