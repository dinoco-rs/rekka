//! Infraestrutura compartilhada pelos testes.
//!
//! Cada teste usa um prefixo único no Redis (`rekka-test-*`), então os testes rodam em
//! paralelo contra o mesmo servidor e apagam apenas as próprias chaves ao final.
//! URL do Redis: `REKKA_REDIS_URL` (padrão `redis://127.0.0.1:6379`).

#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rekka::{Cache, JobEvent, JobEventKind, Queues, QueuesBuilder};

pub fn redis_url() -> String {
    std::env::var("REKKA_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned())
}

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// Prefixo isolado por teste; limpa as chaves no `Drop`.
pub struct TestEnv {
    pub prefix: String,
}

impl TestEnv {
    pub fn new() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        Self { prefix: format!("rekka-test-{}-{nanos:x}-{seq}", std::process::id()) }
    }

    /// Builder já apontando para o prefixo do teste, sem tratar sinais e com polling curto.
    pub fn builder(&self) -> QueuesBuilder {
        Queues::new(redis_url())
            .prefix(self.prefix.clone())
            .handle_signals(false)
            .poll_interval(ms(250))
            .connection_timeout(Duration::from_secs(2))
    }

    /// Instância que só produz (nenhum worker registrado).
    pub async fn producer(&self) -> Queues {
        self.builder().start().await.expect("start producer")
    }

    pub async fn cache(&self) -> Cache {
        Cache::builder(redis_url()).prefix(self.prefix.clone()).connect().await.expect("cache")
    }

    /// Chaves do teste que não expiram (`PTTL == -1`).
    pub async fn permanent_keys(&self) -> Vec<String> {
        let mut conn = self.redis().await;
        let mut out = Vec::new();
        for key in self.keys("*").await {
            let ttl: i64 = redis::cmd("PTTL").arg(&key).query_async(&mut conn).await.unwrap();
            if ttl == -1 {
                out.push(key);
            }
        }
        out
    }

    pub async fn redis(&self) -> redis::aio::MultiplexedConnection {
        redis::Client::open(redis_url())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection")
    }

    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        let mut conn = self.redis().await;
        let pattern = format!("{}:{pattern}", self.prefix);
        let mut cursor: u64 = 0;
        let mut out = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .unwrap();
            out.extend(batch);
            if next == 0 {
                return out;
            }
            cursor = next;
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let prefix = self.prefix.clone();
        // Roda numa thread própria: o Drop pode acontecer dentro do runtime tokio.
        let _ = std::thread::spawn(move || {
            let Ok(client) = redis::Client::open(redis_url()) else { return };
            let Ok(mut conn) = client.get_connection() else { return };
            let pattern = format!("{prefix}:*");
            let mut cursor: u64 = 0;
            loop {
                let Ok((next, keys)): redis::RedisResult<(u64, Vec<String>)> =
                    redis::cmd("SCAN").arg(cursor).arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query(&mut conn)
                else {
                    return;
                };
                if !keys.is_empty() {
                    let _: redis::RedisResult<()> = redis::cmd("DEL").arg(&keys).query(&mut conn);
                }
                if next == 0 {
                    return;
                }
                cursor = next;
            }
        })
        .join();
    }
}

/// Espera até a condição ser verdadeira ou o tempo acabar.
pub async fn eventually(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ms(10)).await;
    }
}

pub async fn eventually_async<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if cond().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ms(20)).await;
    }
}

/// Coleta todos os eventos emitidos.
#[derive(Clone, Default)]
pub struct Events(Arc<Mutex<Vec<JobEvent>>>);

impl Events {
    pub fn handler(&self) -> impl Fn(JobEvent) + Send + Sync + 'static {
        let events = self.clone();
        move |event| events.0.lock().unwrap().push(event)
    }

    pub fn all(&self) -> Vec<JobEvent> {
        self.0.lock().unwrap().clone()
    }

    pub fn kinds(&self, task_id: &str) -> Vec<JobEventKind> {
        self.all().iter().filter(|e| e.task_id() == Some(task_id)).map(JobEvent::kind).collect()
    }

    pub fn count(&self, kind: JobEventKind) -> usize {
        self.all().iter().filter(|e| e.kind() == kind).count()
    }

    pub fn last(&self, task_id: &str) -> Option<JobEvent> {
        self.all().into_iter().rev().find(|e| e.task_id() == Some(task_id))
    }
}

/// Mede concorrência: atual e pico.
#[derive(Default)]
pub struct Gauge {
    current: AtomicUsize,
    max: AtomicUsize,
}

pub struct GaugeGuard<'a>(&'a Gauge);

impl Gauge {
    pub fn enter(&self) -> GaugeGuard<'_> {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(now, Ordering::SeqCst);
        GaugeGuard(self)
    }

    pub fn max(&self) -> usize {
        self.max.load(Ordering::SeqCst)
    }

    pub fn current(&self) -> usize {
        self.current.load(Ordering::SeqCst)
    }
}

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Detecta dois jobs com a mesma key executando ao mesmo tempo.
#[derive(Default)]
pub struct KeyGauge {
    running: Mutex<HashMap<String, usize>>,
    violations: AtomicUsize,
}

pub struct KeyGuard<'a>(&'a KeyGauge, String);

impl KeyGauge {
    pub fn enter(&self, key: impl Into<String>) -> KeyGuard<'_> {
        let key = key.into();
        let mut running = self.running.lock().unwrap();
        let n = running.entry(key.clone()).or_default();
        *n += 1;
        if *n > 1 {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        KeyGuard(self, key)
    }

    pub fn violations(&self) -> usize {
        self.violations.load(Ordering::SeqCst)
    }
}

impl Drop for KeyGuard<'_> {
    fn drop(&mut self) {
        let mut running = self.0.running.lock().unwrap();
        if let Some(n) = running.get_mut(&self.1) {
            *n -= 1;
        }
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub task_id: String,
    pub attempt: u32,
    pub value: i64,
    pub instance: u32,
    pub at: Instant,
}

/// Registro das execuções, compartilhado entre instâncias via `.data_arc(...)`.
#[derive(Default)]
pub struct Probe {
    runs: Mutex<Vec<Run>>,
    pub gauge: Gauge,
    pub keys: KeyGauge,
}

impl Probe {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record(&self, task_id: &str, attempt: u32, value: i64, instance: u32) {
        self.runs.lock().unwrap().push(Run {
            task_id: task_id.to_owned(),
            attempt,
            value,
            instance,
            at: Instant::now(),
        });
    }

    pub fn runs(&self) -> Vec<Run> {
        self.runs.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.runs.lock().unwrap().len()
    }

    pub fn runs_of(&self, task_id: &str) -> Vec<Run> {
        self.runs().into_iter().filter(|r| r.task_id == task_id).collect()
    }

    pub fn count_of(&self, task_id: &str) -> usize {
        self.runs_of(task_id).len()
    }

    /// Quantas vezes cada `task_id` executou.
    pub fn per_task(&self) -> HashMap<String, usize> {
        let mut out = HashMap::new();
        for run in self.runs() {
            *out.entry(run.task_id).or_default() += 1;
        }
        out
    }
}

/// Identifica a instância (processo simulado) em testes de escala horizontal.
pub struct Instance(pub u32);
