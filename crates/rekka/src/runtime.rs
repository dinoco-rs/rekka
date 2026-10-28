use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::{Instant, interval_at, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::buffer::Buffer;
use crate::cache::Cache;
use crate::client::{Enqueued, JobState, QueueStats, RekkaClient, Removed, Shared};
use crate::conn::RedisConn;
use crate::context::{DataMap, RekkaContext};
use crate::error::{RekkaError, Result};
use crate::event::{EventHandler, EventSource, JobEvent};
use crate::handler::{DEFAULT_MAX_FUTURE_SIZE, FutureSize, TaskFn};
use crate::job::{RekkaQueue, ToTaskId};
use crate::keys::{QueueAddr, validate_name};
use crate::recursive::{ErasedRecursive, RecursiveEnv, RecursiveWorker, RekkaRecursiveWorker};
use crate::scripts;
use crate::util::{Backoff, millis, stable_hash, supervise};
use crate::worker::{ErasedWorker, LoopEnv, RekkaWorker, Worker};
use crate::workflow::BuildWorkflow;

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
    buffer_dir: Option<PathBuf>,
    max_future_size: usize,
    strict_future_size: bool,
}

impl QueuesBuilder {
    /// Prefixo de todas as chaves no Redis (padrão `rekka`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Disponibiliza `value` para todos os handlers via `RekkaData<T>`. Pode ser chamado
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
    /// O `interval` conta do fim da execução anterior: enquanto um reconcile está rodando, o
    /// próximo não é agendado, então execuções lentas nunca emendam umas nas outras.
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

    /// Registra um workflow: uma sequência de passos em que a saída de cada um é a entrada do
    /// próximo. Cada passo é uma fila própria, com as opções configuradas no `RekkaWorker` (retry,
    /// tentativas, timeout, concorrência...), e o workflow só começa pela fila de `J`, o job do
    /// primeiro passo: enfileire um `J` com `immediate`, `schedule` ou o reconcile, como em
    /// qualquer fila.
    ///
    /// ```ignore
    /// .register_workflow::<CreateCustomer>(|w| {
    ///     w.max_attempts(8)
    ///         .concurrency_by_key(|job| job.account_id)
    ///         .start_with(create_customer) // async fn(CreateCustomer, ...) -> Result<Customer, E>
    ///         .then(validate_customer) // async fn(Customer, ...) -> Result<Customer, E>
    ///         .then(send_welcome_email) // async fn(Customer, ...) -> Result<(), E>
    /// })
    /// ```
    ///
    /// O handler de `start_with` recebe um `J` como primeiro argumento. Sem o `::<J>`, `J` vem
    /// desse handler, mas aí o closure do `concurrency_by_key` precisa do tipo anotado. O passo `n`
    /// (a partir de 1) roda na fila `<J::NAME>.<n>`, com o mesmo `task_id` do job de entrada. Os
    /// passos são identificados pela posição: com execuções em andamento, acrescente passos só no fim.
    pub fn register_workflow<J: RekkaQueue>(mut self, build: impl BuildWorkflow<J>) -> Self {
        let (key_fn, workers) = build(RekkaWorker::new()).into_parts();
        if let Some(key_fn) = key_fn {
            self.key_fns.insert(TypeId::of::<J>(), Box::new(key_fn));
        }
        self.workers.extend(workers);
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

    /// Diretório do buffer local onde `immediate`, `schedule`, `schedule_in` e `immediate_many`
    /// guardam os jobs enquanto o Redis está fora do ar (padrão: `<temp>/rekka`). Use um
    /// diretório que sobreviva a restarts (um volume em containers): um job no buffer só existe
    /// nesse arquivo até o Redis voltar.
    pub fn buffer_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.buffer_dir = Some(dir.into());
        self
    }

    /// Desliga o buffer local: com o Redis fora do ar, os enqueues retornam o erro.
    pub fn disable_buffer(mut self) -> Self {
        self.buffer_dir = None;
        self
    }

    /// Tamanho máximo, em bytes, do future de um handler (padrão 16 KiB), para os workers que não
    /// definem o próprio `max_future_size` (e para o reconcile). O tamanho é medido no `start()`:
    /// acima do limite, um aviso; com [`strict_future_size`](Self::strict_future_size), erro.
    /// Os tamanhos medidos ficam em [`Queues::future_sizes`].
    pub fn max_future_size(mut self, bytes: usize) -> Self {
        self.max_future_size = bytes;
        self
    }

    /// Com `true`, um handler cujo future passa do `max_future_size` faz o `start()` falhar
    /// (`RekkaError::InvalidConfig`) em vez de só emitir um aviso.
    pub fn strict_future_size(mut self, strict: bool) -> Self {
        self.strict_future_size = strict;
        self
    }

    fn future_sizes(&self) -> Vec<FutureSize> {
        let workers = self.workers.iter().map(|w| w.future_size(self.max_future_size));
        workers.chain(self.recursive.iter().filter_map(|r| r.future_size(self.max_future_size))).collect()
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
        if self.strict_future_size
            && let Some(size) = self.future_sizes().iter().find(|s| s.exceeds_limit())
        {
            return Err(size.describe());
        }
        Ok(())
    }

    /// Conecta ao Redis e inicia os workers. Retorna assim que tudo estiver rodando.
    pub async fn start(mut self) -> Result<Queues> {
        self.check().map_err(RekkaError::InvalidConfig)?;
        let future_sizes = self.future_sizes();
        warn_oversized(&future_sizes);

        if !self.workers.is_empty() && !self.cleanup_interval.is_zero() {
            let mut queues: Vec<(&'static str, QueueAddr)> =
                self.workers.iter().map(|w| (w.name(), w.addr())).collect();
            queues.sort_unstable_by_key(|(name, _)| *name);
            let names: Vec<&str> = queues.iter().map(|(name, _)| *name).collect();
            let key_name = format!("__janitor_{:016x}", stable_hash(&names));
            let queues: Arc<[QueueAddr]> = queues.into_iter().map(|(_, addr)| addr).collect();
            let janitor = RecursiveWorker::internal("janitor", key_name, self.cleanup_interval, move |client: RekkaClient| {
                crate::janitor::run(client, Arc::clone(&queues))
            });
            self.recursive.push(janitor.runner);
        }

        let buffer = match &self.buffer_dir {
            Some(dir) => Some(Arc::new(
                Buffer::open(dir, &self.prefix)
                    .map_err(|e| RekkaError::InvalidConfig(format!("offline buffer at `{}`: {e}", dir.display())))?,
            )),
            None => None,
        };

        let client = redis::Client::open(self.url.as_str())?;
        let mut conn = RedisConn::connect(client.clone(), self.connection_timeout, self.response_timeout).await?;
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
            buffer,
        });
        let cx = RekkaContext::new(RekkaClient { shared: Arc::clone(&shared) }, EventSource::Queue);
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
            channels.insert(worker.addr().keys(&shared.prefix).notify_channel(), Arc::clone(&wake));
            let env = LoopEnv {
                client: cx.client.clone(),
                token: token.clone(),
                wake,
                poll_interval: self.poll_interval,
                shutdown_timeout: self.shutdown_timeout,
                abandoned: Arc::clone(&abandoned),
            };
            aborts.push(tracker.spawn(worker.into_future(env)).abort_handle());
        }
        if !channels.is_empty() {
            let token = token.clone();
            let timeouts = (self.connection_timeout, self.response_timeout);
            let listener = async move {
                supervise("notify listener", "", &token, || {
                    notify_listener(client.clone(), channels.clone(), token.clone(), timeouts)
                })
                .await;
            };
            aborts.push(tracker.spawn(listener).abort_handle());
        }
        for recursive in self.recursive {
            let env = RecursiveEnv {
                client: cx.client.clone(),
                token: token.clone(),
                shutdown_timeout: self.shutdown_timeout,
            };
            aborts.push(tracker.spawn(recursive.into_future(env)).abort_handle());
        }
        if let Some(buffer) = &shared.buffer {
            let (buffer, conn, prefix, token) =
                (Arc::clone(buffer), shared.conn.clone(), Arc::clone(&shared.prefix), token.clone());
            let sync = async move {
                supervise("offline buffer", "", &token, || {
                    crate::buffer::sync_loop(Arc::clone(&buffer), conn.clone(), Arc::clone(&prefix), token.clone())
                })
                .await;
            };
            aborts.push(tracker.spawn(sync).abort_handle());
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
                future_sizes,
            }),
        })
    }
}

/// Fora do `start()`: em debug, o `warn!` aumentaria o frame de cada poll dele.
fn warn_oversized(sizes: &[FutureSize]) {
    for size in sizes.iter().filter(|s| s.exceeds_limit()) {
        warn!(
            source = ?size.source,
            name = size.name,
            size = size.size,
            limit = size.limit,
            "rekka: {}",
            size.describe()
        );
    }
}

/// Assina os canais de notificação das filas para acordar os workers assim que um job
/// fica disponível. Se a conexão cair, os workers continuam por polling até ela voltar.
async fn notify_listener(
    client: redis::Client,
    channels: HashMap<String, Arc<Notify>>,
    token: CancellationToken,
    (connection_timeout, response_timeout): (Duration, Duration),
) {
    let names: Vec<&str> = channels.keys().map(String::as_str).collect();
    let timed_out = |_| redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
    // A assinatura só recebe: sem um PING periódico, uma conexão que parou de responder sem
    // fechar (failover, partição) nunca seria percebida e as notificações parariam de chegar.
    let ping_every = (response_timeout * 2).clamp(Duration::from_secs(1), Duration::from_secs(30));
    let mut backoff = Backoff::new();
    loop {
        let session = async {
            let pubsub = timeout(connection_timeout, client.get_async_pubsub()).await.map_err(timed_out)??;
            let (mut sink, mut messages) = pubsub.split();
            timeout(response_timeout, sink.subscribe(&names)).await.map_err(timed_out)??;
            // Mensagens podem ter sido perdidas enquanto estava desconectado.
            for wake in channels.values() {
                wake.notify_one();
            }
            let connected = Instant::now();
            let mut ping = interval_at(connected + ping_every, ping_every);
            loop {
                tokio::select! {
                    msg = messages.next() => match msg {
                        Some(msg) => {
                            if let Some(wake) = channels.get(msg.get_channel_name()) {
                                wake.notify_one();
                            }
                        }
                        None => break,
                    },
                    _ = ping.tick() => {
                        timeout(response_timeout, sink.ping::<redis::Value>()).await.map_err(timed_out)??;
                    }
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
    future_sizes: Vec<FutureSize>,
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
            buffer_dir: Some(std::env::temp_dir().join("rekka")),
            max_future_size: DEFAULT_MAX_FUTURE_SIZE,
            strict_future_size: false,
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

    /// Tamanho do future de cada handler registrado (filas, workers recorrentes e reconcile),
    /// medido no `start()` contra o `max_future_size`.
    pub fn future_sizes(&self) -> &[FutureSize] {
        &self.inner.future_sizes
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
