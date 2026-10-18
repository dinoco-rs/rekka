use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures_util::FutureExt;
use redis::{FromRedisValue, RedisError, ScriptInvocation};
use tokio::sync::Notify;
use tokio::task::{Id as TaskId, JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::client::{KeyFn, Shared};
use crate::context::RekkaContext;
use crate::error::Result;
use crate::event::{EventSource, JobEvent, JobEventKind};
use crate::handler::JobFn;
use crate::job::{IntoExecutionResult, JobHandler, RekkaExecutionResult, RekkaQueue, RetryPolicy};
use crate::keys::QueueKeys;
use crate::scripts;
use crate::util::{Backoff, is_transient, millis, panic_message, random_between};

/// Máximo de jobs pegos por chamada do script de claim.
const CLAIM_BATCH: usize = 128;
/// Máximo de leases renovadas por chamada.
const EXTEND_CHUNK: usize = 512;
/// Máximo de jobs recuperados por chamada do reaper.
const REAP_LIMIT: usize = 256;
/// Limite padrão de execuções por job.
pub(crate) const DEFAULT_MAX_ATTEMPTS: u32 = 10;

pub(crate) type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Configuração de um worker de fila. Recebido pronto no closure de `register`.
pub struct RekkaWorker<J> {
    retry: RetryPolicy,
    max_attempts: Option<u32>,
    concurrency: usize,
    key_fn: Option<KeyFn<J>>,
    timeout: Option<Duration>,
    lease: Duration,
}

impl<J: RekkaQueue> Default for RekkaWorker<J> {
    fn default() -> Self {
        Self::new()
    }
}

impl<J: RekkaQueue> RekkaWorker<J> {
    pub fn new() -> Self {
        Self {
            retry: RetryPolicy::default(),
            max_attempts: Some(DEFAULT_MAX_ATTEMPTS),
            concurrency: 1,
            key_fn: None,
            timeout: None,
            lease: Duration::from_secs(30),
        }
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Número máximo de execuções, contando a primeira (padrão 10). Esgotado, o job é
    /// descartado (evento `Dead`).
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    /// Remove o limite de tentativas: o job tenta até dar certo (ou ser cancelado/removido).
    pub fn unlimited_attempts(mut self) -> Self {
        self.max_attempts = None;
        self
    }

    /// Jobs desta fila executando em paralelo **por processo**.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// No máximo um job por key executando ao mesmo tempo, **somando todos os processos**.
    pub fn concurrency_by_key<K, F>(mut self, key: F) -> Self
    where
        K: ToString,
        F: Fn(&J) -> K + Send + Sync + 'static,
    {
        self.key_fn = Some(Arc::new(move |job: &J| key(job).to_string()));
        self
    }

    /// Tempo máximo de uma execução; ao estourar conta como erro.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Se o processo morrer, o job volta para a fila após no máximo `lease`.
    /// A lease é renovada automaticamente enquanto o job executa.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn build_fn<F, M>(self, f: F) -> Worker<J>
    where
        F: JobFn<J, M>,
        M: 'static,
    {
        Worker {
            key_fn: self.key_fn.clone(),
            runner: Box::new(WorkerSpec { cfg: self, f, _m: PhantomData }),
        }
    }
}

/// Worker pronto para ser registrado.
pub struct Worker<J> {
    pub(crate) key_fn: Option<KeyFn<J>>,
    pub(crate) runner: Box<dyn ErasedWorker>,
}

pub(crate) struct LoopEnv {
    pub(crate) cx: RekkaContext,
    pub(crate) token: CancellationToken,
    pub(crate) wake: Arc<Notify>,
    pub(crate) poll_interval: Duration,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) abandoned: Arc<AtomicUsize>,
}

pub(crate) trait ErasedWorker: Send {
    fn name(&self) -> &'static str;
    fn check(&self) -> std::result::Result<(), String>;
    fn validate(&self, cx: &RekkaContext) -> Result<()>;
    fn into_future(self: Box<Self>, env: LoopEnv) -> BoxFuture;
}

struct WorkerSpec<J, F, M> {
    cfg: RekkaWorker<J>,
    f: F,
    _m: PhantomData<fn() -> M>,
}

impl<J, F, M> ErasedWorker for WorkerSpec<J, F, M>
where
    J: RekkaQueue,
    F: JobFn<J, M>,
    M: 'static,
{
    fn name(&self) -> &'static str {
        J::NAME
    }

    fn check(&self) -> std::result::Result<(), String> {
        if self.cfg.concurrency == 0 {
            return Err(format!("queue `{}`: concurrency must be at least 1", J::NAME));
        }
        if self.cfg.lease < Duration::from_millis(100) {
            return Err(format!("queue `{}`: lease must be at least 100ms", J::NAME));
        }
        if self.cfg.max_attempts == Some(0) {
            return Err(format!("queue `{}`: max_attempts must be at least 1", J::NAME));
        }
        Ok(())
    }

    fn validate(&self, cx: &RekkaContext) -> Result<()> {
        F::validate(cx)
    }

    fn into_future(self: Box<Self>, env: LoopEnv) -> BoxFuture {
        let WorkerSpec { cfg, f, .. } = *self;
        let rt = Arc::new(QueueRt {
            keys: QueueKeys::new(&env.cx.client.shared.prefix, J::NAME),
            cx: env.cx.clone(),
            lease_ms: millis(cfg.lease),
            cfg,
            f,
            _m: PhantomData,
        });
        Box::pin(queue_loop(rt, env))
    }
}

struct QueueRt<J, F, M> {
    cx: RekkaContext,
    keys: QueueKeys,
    cfg: RekkaWorker<J>,
    f: F,
    lease_ms: u64,
    _m: PhantomData<fn() -> M>,
}

struct Claimed {
    id: String,
    payload: String,
    attempt: u32,
    key: String,
}

type Inflight = HashMap<TaskId, (String, u32)>;

async fn queue_loop<J, F, M>(rt: Arc<QueueRt<J, F, M>>, env: LoopEnv)
where
    J: RekkaQueue,
    F: JobFn<J, M>,
    M: 'static,
{
    let concurrency = rt.cfg.concurrency;
    let mut jobs: JoinSet<()> = JoinSet::new();
    let mut inflight: Inflight = HashMap::with_capacity(concurrency.min(1024));

    let beat = rt.cfg.lease / 3;
    let mut heartbeat = interval_at(Instant::now() + beat, beat);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let reap_every = (rt.cfg.lease / 2).clamp(Duration::from_millis(100), Duration::from_secs(5));
    let mut reaper = interval_at(Instant::now(), reap_every);
    reaper.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut next_claim = Instant::now();
    // Pode haver mais jobs prontos além dos que couberam no último claim.
    let mut more = true;
    let mut backoff = Backoff::new();
    let mut stopping = false;
    let mut deadline: Option<Instant> = None;

    loop {
        while let Some(res) = jobs.try_join_next_with_id() {
            rt.forget(&mut inflight, res);
            if more {
                next_claim = Instant::now();
            }
        }
        if stopping && jobs.is_empty() {
            break;
        }

        let mut free = concurrency.saturating_sub(jobs.len());
        if !stopping && free > 0 && Instant::now() >= next_claim {
            let want = free.min(CLAIM_BATCH);
            match rt.claim(want).await {
                Ok((delay, claimed)) => {
                    backoff.reset();
                    let got = claimed.len();
                    for (id, payload, attempt, key) in claimed {
                        let entry = (id.clone(), attempt);
                        let handle = jobs.spawn(run_job(Arc::clone(&rt), Claimed { id, payload, attempt, key }));
                        inflight.insert(handle.id(), entry);
                    }
                    more = got == want || delay == 0;
                    next_claim = if more {
                        Instant::now()
                    } else if delay > 0 {
                        Instant::now() + Duration::from_millis(delay as u64).min(env.poll_interval)
                    } else {
                        Instant::now() + env.poll_interval
                    };
                }
                Err(e) => {
                    warn!(queue = J::NAME, error = %e, "rekka: failed to claim jobs");
                    next_claim = Instant::now() + backoff.next_delay();
                }
            }
            free = concurrency.saturating_sub(jobs.len());
        }

        // Toda iteração passa pelo select (mesmo com backlog e `next_claim` vencido), para que
        // shutdown e heartbeat nunca fiquem famintos atrás de claims consecutivos.
        let can_claim = !stopping && free > 0;
        tokio::select! {
            biased;
            _ = env.token.cancelled(), if !stopping => {
                stopping = true;
                deadline = env.shutdown_timeout.map(|t| Instant::now() + t);
            }
            _ = heartbeat.tick() => rt.extend_leases(&inflight).await,
            _ = reaper.tick(), if !stopping => rt.reap().await,
            _ = sleep_until(deadline.unwrap_or(next_claim)), if deadline.is_some() => {
                let abandoned = rt.abandon(&mut jobs, &mut inflight).await;
                env.abandoned.fetch_add(abandoned, Ordering::Relaxed);
                break;
            }
            Some(res) = jobs.join_next_with_id(), if !jobs.is_empty() => {
                rt.forget(&mut inflight, res);
                if more {
                    next_claim = Instant::now();
                }
            }
            _ = env.wake.notified(), if can_claim => {
                more = true;
                next_claim = Instant::now();
            }
            _ = sleep_until(next_claim), if can_claim => {}
        }
    }
}

impl<J, F, M> QueueRt<J, F, M>
where
    J: RekkaQueue,
    F: JobFn<J, M>,
    M: 'static,
{
    #[inline]
    fn shared(&self) -> &Shared {
        &self.cx.client.shared
    }

    fn is_last_attempt(&self, attempt: u32) -> bool {
        self.cfg.retry == RetryPolicy::Cancel || self.cfg.max_attempts.is_some_and(|max| attempt >= max)
    }

    async fn claim(&self, max: usize) -> std::result::Result<(i64, Vec<(String, String, u32, String)>), RedisError> {
        let mut inv = self.keys.invocation(&scripts::CLAIM_SCRIPT);
        inv.arg(max).arg(self.lease_ms).arg(u8::from(self.cfg.key_fn.is_some()));
        inv.invoke_async(&mut self.cx.client.conn()).await
    }

    /// Executa o script repetindo em erros transitórios (Redis fora do ar, timeout).
    /// O segundo valor indica se houve repetição — nesse caso a primeira tentativa
    /// pode ter sido aplicada sem que a resposta chegasse.
    async fn invoke_retrying<T: FromRedisValue>(
        &self,
        inv: &ScriptInvocation<'_>,
    ) -> std::result::Result<(T, bool), RedisError> {
        let mut backoff = Backoff::new();
        let mut retried = false;
        loop {
            match inv.invoke_async(&mut self.cx.client.conn()).await {
                Ok(v) => return Ok((v, retried)),
                Err(e) if is_transient(&e) => {
                    debug!(queue = J::NAME, error = %e, "rekka: transient redis error, retrying");
                    retried = true;
                    sleep(backoff.next_delay()).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn forget(self: &Arc<Self>, inflight: &mut Inflight, res: std::result::Result<(TaskId, ()), JoinError>) {
        match res {
            Ok((id, ())) => {
                inflight.remove(&id);
            }
            Err(e) => {
                let Some((job_id, attempt)) = inflight.remove(&e.id()) else { return };
                if e.is_panic() {
                    // Pânico fora do handler (o do handler já é capturado): devolve o job agora
                    // em vez de esperar a lease expirar.
                    error!(queue = J::NAME, task_id = %job_id, "rekka: job task panicked");
                    let rt = Arc::clone(self);
                    tokio::spawn(async move {
                        rt.requeue(&job_id, attempt).await;
                    });
                }
            }
        }
    }

    async fn requeue(&self, id: &str, attempt: u32) -> bool {
        let mut inv = self.keys.invocation(&scripts::COMPLETE_SCRIPT);
        inv.arg(id).arg(attempt).arg("d").arg(0).arg(0);
        match inv.invoke_async::<String>(&mut self.cx.client.conn()).await {
            Ok(status) => status != "stale",
            Err(e) => {
                warn!(queue = J::NAME, task_id = %id, error = %e, "rekka: failed to requeue job");
                false
            }
        }
    }

    /// Estourou o `shutdown_timeout`: aborta o que ainda executa e devolve para a fila,
    /// para outra instância pegar imediatamente (sem esperar a lease).
    async fn abandon(&self, jobs: &mut JoinSet<()>, inflight: &mut Inflight) -> usize {
        jobs.abort_all();
        while let Some(res) = jobs.join_next_with_id().await {
            if let Ok((id, ())) = res {
                inflight.remove(&id);
            }
        }
        let mut abandoned = 0;
        for (id, attempt) in inflight.values() {
            if self.requeue(id, *attempt).await {
                abandoned += 1;
            }
        }
        inflight.clear();
        abandoned
    }

    async fn extend_leases(&self, inflight: &Inflight) {
        let mut entries = inflight.values().peekable();
        let mut conn = self.cx.client.conn();
        while entries.peek().is_some() {
            let mut inv = self.keys.invocation(&scripts::EXTEND_SCRIPT);
            inv.arg(self.lease_ms);
            for (id, attempt) in entries.by_ref().take(EXTEND_CHUNK) {
                inv.arg(id).arg(*attempt);
            }
            if let Err(e) = inv.invoke_async::<i64>(&mut conn).await {
                warn!(queue = J::NAME, error = %e, "rekka: failed to extend leases");
            }
        }
    }

    async fn reap(&self) {
        let shared = self.shared();
        let mut inv = self.keys.invocation(&scripts::REAP_SCRIPT);
        inv.arg(REAP_LIMIT).arg(self.cfg.max_attempts.unwrap_or(0)).arg(shared.completed_ttl_ms);
        let rows: Vec<(String, String, u32, Option<String>)> = match inv.invoke_async(&mut self.cx.client.conn()).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(queue = J::NAME, error = %e, "rekka: failed to recover stalled jobs");
                return;
            }
        };
        for (id, kind, attempt, payload) in rows {
            let kind = match kind.as_str() {
                "dead" => JobEventKind::Dead,
                "discarded" => JobEventKind::Discarded,
                _ => JobEventKind::Stalled,
            };
            warn!(queue = J::NAME, task_id = %id, ?kind, "rekka: lease expired, job recovered");
            shared.emit(|| JobEvent {
                task_id: Some(id),
                attempt,
                error: Some("lease expired: the worker running this job stopped responding".into()),
                payload: payload.map(Arc::from),
                ..JobEvent::new(EventSource::Queue, J::NAME, kind)
            });
        }
    }

    /// Para jobs cuja key não foi calculada no enqueue (produtor sem o worker registrado).
    async fn bind(&self, id: &str, attempt: u32, key: &str) -> bool {
        let mut inv = self.keys.invocation(&scripts::BIND_SCRIPT);
        inv.arg(id).arg(attempt).arg(key);
        match self.invoke_retrying::<i64>(&inv).await {
            Ok((code, _)) => code == 1,
            Err(e) => {
                error!(queue = J::NAME, task_id = %id, error = %e, "rekka: failed to bind concurrency key");
                false
            }
        }
    }

    async fn execute(&self, fut: F::Future) -> RekkaExecutionResult {
        let fut = AssertUnwindSafe(fut).catch_unwind();
        let res = match self.cfg.timeout {
            Some(limit) => match tokio::time::timeout(limit, fut).await {
                Ok(res) => res,
                Err(_) => return RekkaExecutionResult::Error(format!("job timed out after {limit:?}")),
            },
            None => fut.await,
        };
        match res {
            Ok(out) => out.into_execution_result(),
            Err(panic) => RekkaExecutionResult::Error(panic_message(panic)),
        }
    }

    async fn finalize(
        &self,
        id: String,
        attempt: u32,
        result: RekkaExecutionResult,
        payload: Option<Arc<str>>,
        started: Instant,
    ) {
        let (action, value, mut kind, retry_in, error) = match result {
            RekkaExecutionResult::Success => ("f", 0, JobEventKind::Succeeded, None, None),
            RekkaExecutionResult::Cancel => ("f", 0, JobEventKind::Cancelled, None, None),
            RekkaExecutionResult::RetryAt(at) => {
                let wait = (at - Utc::now()).to_std().unwrap_or_default();
                ("r", at.timestamp_millis().max(0) as u64, JobEventKind::Rescheduled, Some(wait), None)
            }
            RekkaExecutionResult::RetryAfter(delay) => ("d", millis(delay), JobEventKind::Rescheduled, Some(delay), None),
            RekkaExecutionResult::Error(e) => match &self.cfg.retry {
                _ if self.is_last_attempt(attempt) => ("f", 0, JobEventKind::Dead, None, Some(e)),
                RetryPolicy::RetryAfterRange(min, max) => {
                    let delay = random_between(*min, *max);
                    ("d", millis(delay), JobEventKind::Failed, Some(delay), Some(e))
                }
                RetryPolicy::RetryImmediate | RetryPolicy::Cancel => {
                    ("d", 0, JobEventKind::Failed, Some(Duration::ZERO), Some(e))
                }
            },
        };

        let mut inv = self.keys.invocation(&scripts::COMPLETE_SCRIPT);
        inv.arg(&id).arg(attempt).arg(action).arg(value).arg(self.shared().completed_ttl_ms);
        let status = match self.invoke_retrying::<String>(&inv).await {
            Ok((status, retried)) => {
                if status == "stale" && !retried {
                    warn!(queue = J::NAME, task_id = %id, "rekka: lease lost before completion; result ignored");
                    return;
                }
                status
            }
            Err(e) => {
                error!(queue = J::NAME, task_id = %id, error = %e, "rekka: failed to complete job");
                return;
            }
        };
        if status == "removed" {
            kind = JobEventKind::Discarded;
        }

        self.shared().emit(|| JobEvent {
            task_id: Some(id),
            attempt,
            error,
            elapsed: Some(started.elapsed()),
            retry_in,
            payload,
            ..JobEvent::new(EventSource::Queue, J::NAME, kind)
        });
    }
}

async fn run_job<J, F, M>(rt: Arc<QueueRt<J, F, M>>, claimed: Claimed)
where
    J: RekkaQueue,
    F: JobFn<J, M>,
    M: 'static,
{
    let started = Instant::now();
    let Claimed { id, payload, attempt, key } = claimed;
    let parsed = serde_json::from_str::<J>(&payload);
    // O payload só fica em memória durante a execução se alguém consome eventos.
    let payload: Option<Arc<str>> = rt.shared().events_enabled().then(|| Arc::from(payload));

    let job = match parsed {
        Ok(job) => job,
        Err(e) => {
            let result = RekkaExecutionResult::Error(format!("failed to deserialize job: {e}"));
            return rt.finalize(id, attempt, result, payload, started).await;
        }
    };

    if key.is_empty()
        && let Some(key_fn) = &rt.cfg.key_fn
        && !rt.bind(&id, attempt, &key_fn(&job)).await
    {
        // Outro job com a mesma key está executando: este ficou estacionado e
        // volta para a fila quando a key for liberada.
        return;
    }

    rt.shared().emit(|| JobEvent {
        task_id: Some(id.clone()),
        attempt,
        payload: payload.clone(),
        ..JobEvent::new(EventSource::Queue, J::NAME, JobEventKind::Started)
    });

    let handler = JobHandler {
        queue: J::NAME,
        task_id: id.clone(),
        attempt,
        max_attempts: rt.cfg.max_attempts,
        last_attempt: rt.is_last_attempt(attempt),
    };
    let result = match rt.f.call(handler, job, &rt.cx) {
        Ok(fut) => rt.execute(fut).await,
        Err(e) => RekkaExecutionResult::Error(e.to_string()),
    };
    rt.finalize(id, attempt, result, payload, started).await;
}
