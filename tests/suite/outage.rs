//! Redis fora do ar: nada derruba o processo, chamadas falham rápido e, quando o Redis volta,
//! filas, workers recorrentes, reconcile e cache voltam a funcionar sozinhos.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rekka::{Enqueued, JobEventKind, JobHandler, Queues, ReconcileHandler, RekkaCache, RekkaData, RekkaQueue};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::common::{Events, Probe, TestEnv, eventually, eventually_async, ms, redis_url};

/// Proxy TCP na frente do Redis dos testes. `down()` fecha a porta e derruba as conexões
/// abertas (como um Redis parado); `up()` volta a aceitar na mesma porta. `stall()` deixa as
/// conexões abertas mudas, sem fechar (failover para outro IP, partição de rede); conexões
/// novas funcionam.
pub struct RedisProxy {
    port: u16,
    upstream: String,
    stop: Mutex<Option<watch::Sender<()>>>,
    stall: watch::Sender<u64>,
}

impl RedisProxy {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_, authority, _) = split_url(&redis_url());
        let upstream = if authority.contains(':') { authority } else { format!("{authority}:6379") };
        let port = listener.local_addr().unwrap().port();
        let proxy = Self { port, upstream, stop: Mutex::new(None), stall: watch::channel(0).0 };
        proxy.serve(listener);
        proxy
    }

    /// URL do Redis dos testes apontando para o proxy.
    pub fn url(&self) -> String {
        let (head, _, tail) = split_url(&redis_url());
        format!("{head}127.0.0.1:{}{tail}", self.port)
    }

    pub fn down(&self) {
        // Fechar o canal encerra o accept e todas as conexões desta geração.
        self.stop.lock().unwrap().take();
    }

    pub fn stall(&self) {
        self.stall.send_modify(|n| *n += 1);
    }

    pub async fn up(&self) {
        // O listener anterior é liberado de forma assíncrona: tenta até a porta ficar livre.
        for _ in 0..200 {
            if let Ok(listener) = TcpListener::bind(("127.0.0.1", self.port)).await {
                return self.serve(listener);
            }
            tokio::time::sleep(ms(10)).await;
        }
        panic!("proxy could not listen on port {} again", self.port);
    }

    fn serve(&self, listener: TcpListener) {
        let (stop, stopped) = watch::channel(());
        *self.stop.lock().unwrap() = Some(stop);
        let upstream = self.upstream.clone();
        let stall = self.stall.clone();
        tokio::spawn(async move {
            let mut closed = stopped.clone();
            loop {
                let mut inbound = tokio::select! {
                    _ = closed.changed() => return,
                    accepted = listener.accept() => match accepted {
                        Ok((inbound, _)) => inbound,
                        Err(_) => continue,
                    },
                };
                let upstream = upstream.clone();
                let mut closed = stopped.clone();
                let mut stalled = stall.subscribe();
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(&upstream).await else { return };
                    tokio::select! {
                        _ = closed.changed() => {}
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        // Para de encaminhar, mas mantém os sockets abertos até o `down()`.
                        _ = stalled.changed() => { let _ = closed.changed().await; }
                    }
                });
            }
        });
    }
}

/// `redis://user:pass@host:port/db` -> (`redis://user:pass@`, `host:port`, `/db`).
fn split_url(url: &str) -> (String, String, String) {
    let (scheme, rest) = url.split_once("://").unwrap_or(("redis", url));
    let (authority, tail) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (auth, host) = match authority.rsplit_once('@') {
        Some((auth, host)) => (format!("{auth}@"), host),
        None => (String::new(), authority),
    };
    (format!("{scheme}://{auth}"), host.to_owned(), tail.to_owned())
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "outage")]
struct OutageJob {
    task_id: String,
    sleep_ms: u64,
}

#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq)]
#[rekka(key = id, ttl = "1m")]
struct Item {
    id: i64,
}

#[derive(Default)]
struct Ticks {
    recurring: AtomicUsize,
    reconcile: AtomicUsize,
}

/// Registra `sleep_ms` como valor, para os testes distinguirem versões do mesmo job.
async fn work(h: JobHandler, job: OutageJob, RekkaData(probe): RekkaData<Probe>) {
    probe.record(h.task_id(), h.attempt(), job.sleep_ms as i64, 0);
    tokio::time::sleep(ms(job.sleep_ms)).await;
}

async fn tick(RekkaData(ticks): RekkaData<Ticks>) {
    ticks.recurring.fetch_add(1, Ordering::SeqCst);
}

async fn reconcile(RekkaData(ticks): RekkaData<Ticks>) {
    ticks.reconcile.fetch_add(1, Ordering::SeqCst);
}

fn job(id: &str, sleep_ms: u64) -> OutageJob {
    OutageJob { task_id: id.into(), sleep_ms }
}

async fn start(env: &TestEnv, proxy: &RedisProxy, probe: &Arc<Probe>, ticks: &Arc<Ticks>, events: &Events) -> Queues {
    env.builder_at(proxy.url())
        .data_arc(probe.clone())
        .data_arc(ticks.clone())
        .on_event(events.handler())
        .with_reconcile(reconcile, ms(100))
        .register_recursive("outage-tick", |w| w.interval(ms(100)).build_fn(tick))
        .register::<OutageJob>(|w| w.concurrency(4).build_fn(work))
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn everything_resumes_after_a_redis_outage() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, ticks, events) = (Probe::new(), Arc::new(Ticks::default()), Events::default());
    let q = start(&env, &proxy, &probe, &ticks, &events).await;
    let cache = q.cache();

    q.immediate(job("before", 0)).await.unwrap();
    cache.insert(&Item { id: 1 }).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("before") == 1).await);
    // Executando quando o Redis cai: termina e é concluído quando o Redis volta, sem repetir.
    q.immediate(job("inflight", 800)).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("inflight") == 1).await);

    proxy.down();
    let started = Instant::now();
    assert_eq!(q.immediate(job("during", 0)).await.unwrap(), Enqueued::Buffered);
    let enqueue_took = started.elapsed();
    let started = Instant::now();
    assert_eq!(cache.get::<Item>(1).await, None);
    let get_took = started.elapsed();
    eprintln!("during outage: immediate took {enqueue_took:?}, cache.get took {get_took:?}");

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(q.is_running());
    let (recurring, reconciled) = (ticks.recurring.load(Ordering::SeqCst), ticks.reconcile.load(Ordering::SeqCst));

    proxy.up().await;
    let back = Instant::now();
    assert!(
        eventually_async(Duration::from_secs(10), || async { q.immediate(job("after", 0)).await.is_ok() }).await,
        "enqueue did not recover"
    );
    eprintln!("enqueue recovered after {:?}", back.elapsed());
    assert!(eventually(Duration::from_secs(10), || probe.count_of("after") == 1).await, "queue did not resume");
    eprintln!("queue resumed after {:?}", back.elapsed());
    assert!(eventually(Duration::from_secs(5), || probe.count_of("during") == 1).await, "buffered job was not sent");
    assert!(
        eventually(Duration::from_secs(10), || events.kinds("inflight").contains(&JobEventKind::Succeeded)).await,
        "the job running during the outage was not completed"
    );
    assert_eq!(probe.count_of("inflight"), 1);
    assert!(eventually(Duration::from_secs(10), || ticks.recurring.load(Ordering::SeqCst) > recurring).await);
    assert!(eventually(Duration::from_secs(10), || ticks.reconcile.load(Ordering::SeqCst) > reconciled).await);
    eprintln!("recurring + reconcile resumed after {:?}", back.elapsed());
    assert_eq!(cache.get::<Item>(1).await, Some(Item { id: 1 }));

    q.shutdown().await.unwrap();
}

/// O Redis some sem fechar as conexões: os comandos dão timeout até o sistema operacional
/// desistir do socket (minutos). A conexão precisa ser refeita antes disso.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_connections_are_replaced() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, ticks, events) = (Probe::new(), Arc::new(Ticks::default()), Events::default());
    let q = env
        .builder_at(proxy.url())
        .data_arc(probe.clone())
        .data_arc(ticks.clone())
        .on_event(events.handler())
        .response_timeout(ms(300))
        // Sem polling: o job novo só é pego se o canal de notificação também se recuperar.
        .poll_interval(Duration::from_secs(60))
        .register_recursive("outage-tick", |w| w.interval(ms(100)).build_fn(tick))
        .register::<OutageJob>(|w| w.concurrency(4).build_fn(work))
        .start()
        .await
        .unwrap();
    let cache = q.cache();
    cache.insert(&Item { id: 1 }).await.unwrap();
    q.immediate(job("before", 0)).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("before") == 1).await);

    proxy.stall();
    let stalled = Instant::now();
    assert!(
        eventually_async(Duration::from_secs(10), || async { q.immediate(job("after", 0)).await.is_ok() }).await,
        "enqueue never recovered from a silent connection"
    );
    eprintln!("enqueue recovered after {:?}", stalled.elapsed());
    let recurring = ticks.recurring.load(Ordering::SeqCst);
    assert!(eventually(Duration::from_secs(10), || probe.count_of("after") == 1).await, "queue did not resume");
    eprintln!("queue resumed after {:?}", stalled.elapsed());
    assert!(eventually(Duration::from_secs(10), || ticks.recurring.load(Ordering::SeqCst) > recurring).await);
    assert_eq!(cache.get::<Item>(1).await, Some(Item { id: 1 }));

    q.shutdown().await.unwrap();
}

async fn restore(handler: ReconcileHandler) -> rekka::Result<()> {
    handler.add(job("restored", 0)).await?;
    Ok(())
}

/// Redis reiniciado sem persistência: chaves e scripts somem. Os scripts são recarregados, os
/// agendamentos recriados e o reconcile devolve o job para a fila.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumes_after_a_restart_that_lost_all_data() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, ticks) = (Probe::new(), Arc::new(Ticks::default()));
    let q = env
        .builder_at(proxy.url())
        .data_arc(probe.clone())
        .data_arc(ticks.clone())
        .with_reconcile(restore, ms(200))
        .register_recursive("outage-tick", |w| w.interval(ms(100)).build_fn(tick))
        .register::<OutageJob>(|w| w.concurrency(4).build_fn(work))
        .start()
        .await
        .unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("restored") == 1).await);

    proxy.down();
    let mut conn = env.redis().await;
    let keys = env.keys("*").await;
    if !keys.is_empty() {
        let _: () = redis::cmd("DEL").arg(&keys).query_async(&mut conn).await.unwrap();
    }
    let _: () = redis::cmd("SCRIPT").arg("FLUSH").query_async(&mut conn).await.unwrap();
    let recurring = ticks.recurring.load(Ordering::SeqCst);
    proxy.up().await;

    // A marca de "concluído" sumiu junto: o reconcile recria o job.
    assert!(eventually(Duration::from_secs(10), || probe.count_of("restored") == 2).await, "reconcile did not restore");
    assert!(eventually(Duration::from_secs(10), || ticks.recurring.load(Ordering::SeqCst) > recurring).await);
    q.immediate(job("new", 0)).await.unwrap();
    assert!(eventually(Duration::from_secs(5), || probe.count_of("new") == 1).await);
    q.shutdown().await.unwrap();
}

/// Encerrar com o Redis fora do ar não trava: os loops param e, com `shutdown_timeout`, o job
/// que não consegue reportar o resultado é abandonado (volta pela lease). Sem `shutdown_timeout`,
/// o encerramento espera o Redis voltar para reportar os jobs em execução.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_an_outage_finishes() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, ticks, events) = (Probe::new(), Arc::new(Ticks::default()), Events::default());
    let busy = env
        .builder_at(proxy.url())
        .data_arc(probe.clone())
        .shutdown_timeout(ms(500))
        .register::<OutageJob>(|w| w.build_fn(work))
        .start()
        .await
        .unwrap();
    busy.immediate(job("slow", 300)).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || probe.count_of("slow") == 1).await);
    // Sobe depois do job já estar executando em `busy`: nada em execução, só loops.
    let idle = start(&env, &proxy, &probe, &ticks, &events).await;

    proxy.down();
    let started = Instant::now();
    let (idle_done, busy_done) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), idle.shutdown()),
        tokio::time::timeout(Duration::from_secs(5), busy.shutdown()),
    );
    assert!(matches!(idle_done, Ok(Ok(()))), "idle instance: {idle_done:?}");
    assert!(busy_done.is_ok(), "busy instance did not finish its shutdown");
    eprintln!("shutdown during outage took {:?}", started.elapsed());
}

/// Enfileirar com o Redis fora: os jobs vão para o buffer local, nada executa por conta
/// própria (o Redis é a fonte da verdade) e tudo é enviado quando o Redis volta. O cache não
/// tem buffer: `get` lê `None` e `insert` falha sem gravar nada.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_enqueued_while_redis_is_down_are_buffered_and_sent_later() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, ticks, events) = (Probe::new(), Arc::new(Ticks::default()), Events::default());
    let q = start(&env, &proxy, &probe, &ticks, &events).await;
    let cache = q.cache();

    proxy.down();
    assert_eq!(q.immediate(job("offline-1", 0)).await.unwrap(), Enqueued::Buffered);
    assert_eq!(q.schedule_in(job("offline-later", 0), ms(200)).await.unwrap(), Enqueued::Buffered);
    let many = q.immediate_many([job("offline-2", 0), job("offline-3", 0)]).await.unwrap();
    assert_eq!(many, [Enqueued::Buffered, Enqueued::Buffered]);
    assert!(cache.insert(&Item { id: 7 }).await.is_err());
    assert_eq!(cache.get::<Item>(7).await, None);
    assert_eq!(env.buffered(), ["offline-1", "offline-later", "offline-2", "offline-3"]);

    tokio::time::sleep(ms(500)).await;
    assert_eq!(probe.count(), 0, "buffered jobs must only run after reaching redis");

    proxy.up().await;
    for id in ["offline-1", "offline-later", "offline-2", "offline-3"] {
        assert!(eventually(Duration::from_secs(10), || probe.count_of(id) == 1).await, "{id} was not sent");
    }
    assert!(env.buffered().is_empty());
    assert_eq!(cache.get::<Item>(7).await, None, "a cache insert made during the outage must not be applied");
    q.shutdown().await.unwrap();
}

/// Enquanto o buffer não foi todo enviado, enqueues novos vão para o fim dele: uma versão
/// antiga de um job nunca chega ao Redis depois de uma nova.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_jobs_keep_their_order_after_redis_returns() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let probe = Probe::new();
    let producer = env.builder_at(proxy.url()).start().await.unwrap();
    let worker = env
        .builder()
        .data_arc(probe.clone())
        .register::<OutageJob>(|w| w.concurrency(64).build_fn(work))
        .start()
        .await
        .unwrap();

    proxy.down();
    // Um backlog grande deixa o envio demorar o suficiente para a versão 2 chegar no meio dele.
    let backlog = (0..1000).map(|i| job(&format!("backlog-{i}"), 0));
    assert!(producer.immediate_many(backlog).await.unwrap().iter().all(|s| *s == Enqueued::Buffered));
    assert_eq!(producer.immediate(job("versioned", 1)).await.unwrap(), Enqueued::Buffered);
    proxy.up().await;
    // A conexão já responde, mas a versão 1 ainda não foi enviada: a 2 vai atrás dela.
    assert!(eventually_async(Duration::from_secs(5), || async { producer.stats::<OutageJob>().await.is_ok() }).await);
    assert_eq!(producer.immediate(job("versioned", 2)).await.unwrap(), Enqueued::Buffered);

    assert!(eventually(Duration::from_secs(10), || env.buffered().is_empty()).await);
    assert!(eventually(Duration::from_secs(10), || probe.count_of("backlog-999") == 1).await);
    tokio::time::sleep(ms(500)).await;
    let runs = probe.runs_of("versioned");
    assert_eq!(runs.last().map(|r| r.value), Some(2), "runs: {runs:?}");
    producer.shutdown().await.unwrap();
    worker.shutdown().await.unwrap();
}

/// O buffer está em disco: se o processo para com o Redis fora, a próxima instância que sobe
/// com o mesmo diretório envia os jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_jobs_survive_a_restart() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let probe = Probe::new();
    let worker =
        env.builder().data_arc(probe.clone()).register::<OutageJob>(|w| w.build_fn(work)).start().await.unwrap();

    let first = env.builder_at(proxy.url()).start().await.unwrap();
    proxy.down();
    assert_eq!(first.immediate(job("survivor", 0)).await.unwrap(), Enqueued::Buffered);
    first.shutdown().await.unwrap();
    drop(first);
    assert_eq!(env.buffered(), ["survivor"]);

    proxy.up().await;
    let second = env.builder_at(proxy.url()).start().await.unwrap();
    assert!(eventually(Duration::from_secs(10), || probe.count_of("survivor") == 1).await, "not sent after restart");
    assert!(env.buffered().is_empty());
    second.shutdown().await.unwrap();
    worker.shutdown().await.unwrap();
}

#[derive(Default)]
struct ReconcileLog {
    runs: AtomicUsize,
    failed: AtomicUsize,
}

async fn slow_restore(handler: ReconcileHandler, RekkaData(log): RekkaData<ReconcileLog>) -> rekka::Result<()> {
    log.runs.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(ms(300)).await;
    if let Err(e) = handler.add(job("reconciled", 0)).await {
        log.failed.fetch_add(1, Ordering::SeqCst);
        return Err(e);
    }
    Ok(())
}

/// O reconcile não usa o buffer: com o Redis fora, `add` falha e o reconcile não roda. Quando o
/// Redis volta, ele roda uma vez (em uma das instâncias), mesmo antes do horário.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_skips_the_buffer_and_runs_once_after_reconnecting() {
    let env = TestEnv::new();
    let proxy = RedisProxy::start().await;
    let (probe, log) = (Probe::new(), Arc::new(ReconcileLog::default()));
    let instance = || {
        env.builder_at(proxy.url())
            .data_arc(probe.clone())
            .data_arc(log.clone())
            .with_reconcile(slow_restore, Duration::from_secs(3600))
            .register::<OutageJob>(|w| w.build_fn(work))
            .start()
    };
    let (a, b, c) = tokio::join!(instance(), instance(), instance());
    let instances = [a.unwrap(), b.unwrap(), c.unwrap()];

    // O reconcile da subida está no meio quando o Redis cai: o `add` falha e não vai para o buffer.
    assert!(eventually(Duration::from_secs(3), || log.runs.load(Ordering::SeqCst) == 1).await);
    proxy.down();
    assert!(eventually(Duration::from_secs(3), || log.failed.load(Ordering::SeqCst) == 1).await);
    assert!(env.buffered().is_empty(), "reconcile must not use the offline buffer");
    tokio::time::sleep(ms(700)).await;
    assert_eq!(log.runs.load(Ordering::SeqCst), 1, "reconcile must not run while redis is down");

    proxy.up().await;
    assert!(
        eventually(Duration::from_secs(10), || probe.count_of("reconciled") == 1).await,
        "no run after reconnecting"
    );
    tokio::time::sleep(ms(1500)).await;
    assert_eq!(log.runs.load(Ordering::SeqCst), 2, "one run after reconnecting, across all instances");
    for q in &instances {
        q.shutdown().await.unwrap();
    }
}
