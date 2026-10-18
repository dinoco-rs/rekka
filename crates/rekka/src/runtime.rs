use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use redis::aio::ConnectionManagerConfig;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::cache::Cache;
use crate::client::{Enqueued, JobState, QueueStats, RekkaClient, Removed, Shared};
use crate::context::{DataMap, RekkaContext};
use crate::error::{RekkaError, Result};
use crate::event::{EventHandler, JobEvent};
use crate::handler::TaskFn;
use crate::job::{RekkaQueue, ToTaskId};
use crate::keys::{QueueKeys, validate_name};
use crate::recursive::{ErasedRecursive, RecursiveEnv, RecursiveWorker, RekkaRecursiveWorker};
use crate::scripts;
use crate::util::{Backoff, millis, stable_hash};
use crate::worker::{ErasedWorker, LoopEnv, RekkaWorker, Worker};

/// Configuração das filas. Criado com [`Queues::new`] e iniciado com [`QueuesBuilder::start`].
pub struct QueuesBuilder {
    url: String,
    prefix: String,
    data: DataMap,
    on_event: Option<EventHandler>,
    workers: Vec<Box<dyn ErasedWorker>>,
    key_fns: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    recursive: Vec<Box<dyn ErasedRecursive>>,
    reconciles: usize,
    errors: Vec<String>,
    completed_ttl: Duration,
    cleanup_interval: Duration,
    poll_interval: Duration,
    shutdown_timeout: Option<Duration>,
    handle_signals: bool,
    connection_timeout: Duration,
    response_timeout: Duration,
}

impl QueuesBuilder {
    /// Prefixo de todas as chaves no Redis (padrão `rekka`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Disponibiliza `value` para todos os handlers via `RekkaState<T>`. Pode ser chamado
    /// várias vezes com tipos diferentes.
    pub fn data<T: Send + Sync + 'static>(self, value: T) -> Self {
        self.data_arc(Arc::new(value))
    }

    pub fn data_arc<T: Send + Sync + 'static>(mut self, value: Arc<T>) -> Self {
        self.data.insert(value);
        self
    }

    /// Executa `f` a cada `interval` em uma única instância por ciclo, com o mesmo agendamento
    /// compartilhado de um worker recorrente: com várias instâncias rodando, só uma executa.
    /// Na primeira subida (sem agendamento no Redis), executa na hora.
    ///
    /// Use o extractor [`ReconcileHandler`](crate::ReconcileHandler) para (re)adicionar jobs
    /// sem duplicar nem interromper os que já existem.
    pub fn with_reconcile<F, M>(mut self, f: F, interval: Duration) -> Self
    where
        F: TaskFn<M>,
        M: 'static,
    {
        let worker = RecursiveWorker::reconcile(self.reconciles, interval, f);
        self.reconciles += 1;
        self.recursive.push(worker.runner);
        self
    }

    /// Recebe os eventos de todos os jobs. Deve ser rápido: roda na task do job.
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(JobEvent) + Send + Sync + 'static,
    {
        self.on_event = Some(Box::new(handler));
        self
    }

    pub fn register<J: RekkaQueue>(mut self, build: impl FnOnce(RekkaWorker<J>) -> Worker<J>) -> Self {
        let worker = build(RekkaWorker::new());
        if let Some(key_fn) = worker.key_fn {
            self.key_fns.insert(TypeId::of::<J>(), Box::new(key_fn));
        }
        self.workers.push(worker.runner);
        self
    }

    pub fn register_recursive(
        mut self,
        name: &'static str,
        build: impl FnOnce(RekkaRecursiveWorker) -> RecursiveWorker,
    ) -> Self {
        let worker = build(RekkaRecursiveWorker::new(name));
        self.recursive.push(worker.runner);
        self
    }

    /// Por quanto tempo um job concluído impede o reconcile de recriá-lo (padrão 60s).
    /// `Duration::ZERO` desativa.
    pub fn completed_ttl(mut self, ttl: Duration) -> Self {
        self.completed_ttl = ttl;
        self
    }

    /// Intervalo entre passadas do janitor (padrão 10 min), que repara/apaga estado
    /// inconsistente das filas registradas. `Duration::ZERO` desativa.
    ///
    /// Instâncias com o mesmo conjunto de filas se coordenam: só uma faz cada passada.
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Intervalo máximo entre verificações da fila quando não chega notificação (padrão 1s).
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Tempo máximo esperando jobs em execução no encerramento. Ao estourar, os jobs
    /// são abortados e devolvidos para a fila. Sem limite por padrão.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Se `join()` também encerra com SIGINT/SIGTERM (padrão `true`).
    pub fn handle_signals(mut self, enabled: bool) -> Self {
        self.handle_signals = enabled;
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    fn check(&self) -> std::result::Result<(), String> {
        if let Some(e) = self.errors.first() {
            return Err(e.clone());
        }
        validate_name("prefix", &self.prefix)?;
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be greater than zero".into());
        }
        if !self.cleanup_interval.is_zero() && self.cleanup_interval < Duration::from_millis(10) {
            return Err("cleanup_interval must be at least 10ms (or zero to disable)".into());
        }
        let mut names = HashSet::new();
        for w in &self.workers {
            validate_name("queue", w.name())?;
            if !names.insert(w.name()) {
                return Err(format!("queue `{}` registered more than once", w.name()));
            }
            w.check()?;
        }
        let mut names = HashSet::new();
        for r in &self.recursive {
            if !names.insert(r.key_name()) {
                return Err(format!("recursive worker `{}` registered more than once", r.key_name()));
            }
            r.check()?;
        }
        Ok(())
    }

    /// Conecta ao Redis e inicia os workers. Retorna assim que tudo estiver rodando.
    pub async fn start(mut self) -> Result<Queues> {
        self.check().map_err(RekkaError::InvalidConfig)?;

        if !self.workers.is_empty() && !self.cleanup_interval.is_zero() {
            let mut names: Vec<&'static str> = self.workers.iter().map(|w| w.name()).collect();
            names.sort_unstable();
            let key_name = format!("__janitor_{:016x}", stable_hash(&names));
            let names: Arc<[&'static str]> = names.into();
            let janitor = RecursiveWorker::internal("janitor", key_name, self.cleanup_interval, move |client: RekkaClient| {
                crate::janitor::run(client, Arc::clone(&names))
            });
            self.recursive.push(janitor.runner);
        }

        let client = redis::Client::open(self.url.as_str())?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connection_timeout))
            .set_response_timeout(Some(self.response_timeout))
            .set_max_delay(Duration::from_secs(2));
        let mut conn = client.get_connection_manager_with_config(config).await?;
        for script in scripts::all() {
            script.load_async(&mut conn).await?;
        }

        let shared = Arc::new(Shared {
            conn,
            prefix: self.prefix.into(),
            data: self.data,
            key_fns: self.key_fns,
            on_event: self.on_event,
            completed_ttl_ms: millis(self.completed_ttl),
        });
        let cx = RekkaContext { client: RekkaClient { shared: Arc::clone(&shared) } };
        for w in &self.workers {
            w.validate(&cx)?;
        }
        for r in &self.recursive {
            r.validate(&cx)?;
        }

        let token = CancellationToken::new();
        let tracker = TaskTracker::new();
        let abandoned = Arc::new(AtomicUsize::new(0));
        let mut aborts = Vec::with_capacity(self.workers.len() + self.recursive.len() + 1);

        let mut channels = HashMap::with_capacity(self.workers.len());
        for worker in self.workers {
            let wake = Arc::new(Notify::new());
            channels.insert(QueueKeys::new(&shared.prefix, worker.name()).notify_channel(), Arc::clone(&wake));
            let env = LoopEnv {
                cx: cx.clone(),
                token: token.clone(),
                wake,
                poll_interval: self.poll_interval,
                shutdown_timeout: self.shutdown_timeout,
                abandoned: Arc::clone(&abandoned),
            };
            aborts.push(tracker.spawn(worker.into_future(env)).abort_handle());
        }
        if !channels.is_empty() {
            aborts.push(tracker.spawn(notify_listener(client, channels, token.clone())).abort_handle());
        }
        for recursive in self.recursive {
            let env = RecursiveEnv { cx: cx.clone(), token: token.clone(), shutdown_timeout: self.shutdown_timeout };
            aborts.push(tracker.spawn(recursive.into_future(env)).abort_handle());
        }
        tracker.close();

        Ok(Queues {
            inner: Arc::new(Runtime {
                client: cx.client,
                token,
                tracker,
                aborts: Mutex::new(aborts),
                abandoned,
                handle_signals: self.handle_signals,
            }),
        })
    }
}

/// Assina os canais de notificação das filas para acordar os workers assim que um job
/// fica disponível. Se a conexão cair, os workers continuam por polling.
async fn notify_listener(client: redis::Client, channels: HashMap<String, Arc<Notify>>, token: CancellationToken) {
    let names: Vec<&str> = channels.keys().map(String::as_str).collect();
    let mut backoff = Backoff::new();
    loop {
        let session = async {
            let mut pubsub = client.get_async_pubsub().await?;
            pubsub.subscribe(&names).await?;
            // Mensagens podem ter sido perdidas enquanto estava desconectado.
            for wake in channels.values() {
                wake.notify_one();
            }
            let connected = Instant::now();
            let mut messages = pubsub.on_message();
            while let Some(msg) = messages.next().await {
                if let Some(wake) = channels.get(msg.get_channel_name()) {
                    wake.notify_one();
                }
            }
            Ok::<_, redis::RedisError>(connected.elapsed())
        };
        tokio::select! {
            _ = token.cancelled() => return,
            res = session => match res {
                Ok(lasted) if lasted > Duration::from_secs(10) => backoff.reset(),
                Ok(_) => {}
                Err(e) => warn!(error = %e, "rekka: notification channel unavailable; falling back to polling"),
            },
        }
        tokio::select! {
            _ = token.cancelled() => return,
            _ = sleep(backoff.next_delay()) => {}
        }
    }
}

struct Runtime {
    client: RekkaClient,
    token: CancellationToken,
    tracker: TaskTracker,
    aborts: Mutex<Vec<AbortHandle>>,
    abandoned: Arc<AtomicUsize>,
    handle_signals: bool,
}

/// Handle das filas em execução. Clonável e barato.
#[derive(Clone)]
pub struct Queues {
    inner: Arc<Runtime>,
}

impl Queues {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(redis_url: impl Into<String>) -> QueuesBuilder {
        QueuesBuilder {
            url: redis_url.into(),
            prefix: "rekka".to_owned(),
            data: DataMap::default(),
            on_event: None,
            workers: Vec::new(),
            key_fns: HashMap::new(),
            recursive: Vec::new(),
            reconciles: 0,
            errors: Vec::new(),
            completed_ttl: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(600),
            poll_interval: Duration::from_secs(1),
            shutdown_timeout: None,
            handle_signals: true,
            connection_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
        }
    }

    pub fn client(&self) -> &RekkaClient {
        &self.inner.client
    }

    pub async fn immediate<J: RekkaQueue>(&self, job: J) -> Result<Enqueued> {
        self.inner.client.immediate(job).await
    }

    pub async fn schedule<J: RekkaQueue>(&self, job: J, at: DateTime<Utc>) -> Result<Enqueued> {
        self.inner.client.schedule(job, at).await
    }

    pub async fn schedule_in<J: RekkaQueue>(&self, job: J, delay: Duration) -> Result<Enqueued> {
        self.inner.client.schedule_in(job, delay).await
    }

    pub async fn immediate_many<J: RekkaQueue>(&self, jobs: impl IntoIterator<Item = J>) -> Result<Vec<Enqueued>> {
        self.inner.client.immediate_many(jobs).await
    }

    pub async fn remove<J: RekkaQueue>(&self, task_id: impl ToTaskId) -> Result<Removed> {
        self.inner.client.remove::<J>(task_id).await
    }

    pub async fn state<J: RekkaQueue>(&self, task_id: impl ToTaskId) -> Result<Option<JobState>> {
        self.inner.client.state::<J>(task_id).await
    }

    pub async fn stats<J: RekkaQueue>(&self) -> Result<QueueStats> {
        self.inner.client.stats::<J>().await
    }

    /// Ver [`RekkaClient::purge`].
    pub async fn purge<J: RekkaQueue>(&self) -> Result<u64> {
        self.inner.client.purge::<J>().await
    }

    /// Ver [`RekkaClient::purge_queue`].
    pub async fn purge_queue(&self, name: &str) -> Result<u64> {
        self.inner.client.purge_queue(name).await
    }

    /// Cache usando a mesma conexão e o mesmo prefixo.
    pub fn cache(&self) -> Cache {
        self.inner.client.cache()
    }

    /// Aguarda o encerramento: via [`shutdown`](Self::shutdown) ou, se habilitado, SIGINT/SIGTERM.
    /// Para de pegar jobs novos e espera os que estão em execução terminarem.
    pub async fn join(&self) -> Result<()> {
        if self.inner.handle_signals {
            tokio::select! {
                _ = self.inner.token.cancelled() => {}
                _ = shutdown_signal() => self.inner.token.cancel(),
            }
        } else {
            self.inner.token.cancelled().await;
        }
        self.drained().await
    }

    /// Inicia o encerramento gracioso (imediatamente, mesmo sem `.await`) e retorna um future
    /// que termina quando todos os jobs em execução finalizarem.
    pub fn shutdown(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        self.inner.token.cancel();
        self.drained()
    }

    /// Para tudo imediatamente, como se o processo tivesse morrido: os jobs em execução
    /// são recuperados por outras instâncias quando a lease expirar.
    pub fn abort(&self) {
        self.inner.token.cancel();
        for handle in self.inner.aborts.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            handle.abort();
        }
    }

    pub fn is_running(&self) -> bool {
        !self.inner.token.is_cancelled()
    }

    async fn drained(&self) -> Result<()> {
        self.inner.tracker.wait().await;
        match self.inner.abandoned.load(Ordering::Relaxed) {
            0 => Ok(()),
            abandoned => Err(RekkaError::ShutdownTimeout { abandoned }),
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
