use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::context::RekkaContext;
use crate::error::Result;
use crate::event::{EventSource, JobEvent, JobEventKind};
use crate::handler::{IntoTaskResult, TaskFn};
use crate::keys::RecursiveKeys;
use crate::scripts;
use crate::util::{AbortOnDrop, Backoff, millis, panic_message, random_token};
use crate::worker::BoxFuture;

/// Espera máxima entre verificações do agendamento compartilhado.
const MAX_WAIT: Duration = Duration::from_secs(30);

/// Worker executado a cada `interval`. Em escala horizontal, apenas uma instância
/// executa cada ciclo e nunca há duas execuções simultâneas (lock no Redis).
pub struct RekkaRecursiveWorker {
    name: &'static str,
    interval: Duration,
    lease: Duration,
}

impl RekkaRecursiveWorker {
    pub fn new(name: &'static str) -> Self {
        Self { name, interval: Duration::from_secs(60), lease: Duration::from_secs(30) }
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Se a instância que está executando morrer, outra assume após no máximo `lease`.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn build_fn<F, M>(self, f: F) -> RecursiveWorker
    where
        F: TaskFn<M>,
        M: 'static,
    {
        RecursiveWorker {
            runner: Box::new(RecursiveSpec {
                name: self.name,
                key_name: self.name.to_owned(),
                source: EventSource::Recursive,
                internal: false,
                interval: self.interval,
                lease: self.lease,
                f,
                _m: PhantomData,
            }),
        }
    }
}

/// Worker recorrente pronto para ser registrado.
pub struct RecursiveWorker {
    pub(crate) runner: Box<dyn ErasedRecursive>,
}

impl RecursiveWorker {
    /// Reconcile segue o mesmo agendamento compartilhado de um worker recorrente: uma única
    /// instância por ciclo, inclusive na subida (sem agendamento no Redis, roda na hora).
    pub(crate) fn reconcile<F, M>(index: usize, interval: Duration, f: F) -> Self
    where
        F: TaskFn<M>,
        M: 'static,
    {
        Self {
            runner: Box::new(RecursiveSpec {
                name: "reconcile",
                key_name: format!("__reconcile_{index}"),
                source: EventSource::Reconcile,
                internal: false,
                interval,
                lease: Duration::from_secs(30),
                f,
                _m: PhantomData,
            }),
        }
    }
}

impl RecursiveWorker {
    /// Tarefa interna (janitor): sem eventos e interrompida imediatamente no shutdown.
    pub(crate) fn internal<F, M>(name: &'static str, key_name: String, interval: Duration, f: F) -> Self
    where
        F: TaskFn<M>,
        M: 'static,
    {
        Self {
            runner: Box::new(RecursiveSpec {
                name,
                key_name,
                source: EventSource::Recursive,
                internal: true,
                interval,
                lease: Duration::from_secs(30),
                f,
                _m: PhantomData,
            }),
        }
    }
}

pub(crate) struct RecursiveEnv {
    pub(crate) cx: RekkaContext,
    pub(crate) token: CancellationToken,
    pub(crate) shutdown_timeout: Option<Duration>,
}

pub(crate) trait ErasedRecursive: Send {
    fn key_name(&self) -> &str;
    fn check(&self) -> std::result::Result<(), String>;
    fn validate(&self, cx: &RekkaContext) -> Result<()>;
    fn into_future(self: Box<Self>, env: RecursiveEnv) -> BoxFuture;
}

struct RecursiveSpec<F, M> {
    name: &'static str,
    key_name: String,
    source: EventSource,
    internal: bool,
    interval: Duration,
    lease: Duration,
    f: F,
    _m: PhantomData<fn() -> M>,
}

impl<F, M> ErasedRecursive for RecursiveSpec<F, M>
where
    F: TaskFn<M>,
    M: 'static,
{
    fn key_name(&self) -> &str {
        &self.key_name
    }

    fn check(&self) -> std::result::Result<(), String> {
        crate::keys::validate_name("recursive worker", self.name)?;
        if self.interval < Duration::from_millis(10) {
            return Err(format!("`{}`: interval must be at least 10ms", self.name));
        }
        if self.lease < Duration::from_millis(100) {
            return Err(format!("`{}`: lease must be at least 100ms", self.name));
        }
        Ok(())
    }

    fn validate(&self, cx: &RekkaContext) -> Result<()> {
        F::validate(cx)
    }

    fn into_future(self: Box<Self>, env: RecursiveEnv) -> BoxFuture {
        let keys = RecursiveKeys::new(&env.cx.client.shared.prefix, &self.key_name);
        let cx = env.cx.clone();
        Box::pin(recursive_loop(Arc::new(RecursiveRt { spec: *self, keys, cx }), env))
    }
}

struct RecursiveRt<F, M> {
    spec: RecursiveSpec<F, M>,
    keys: RecursiveKeys,
    cx: RekkaContext,
}

async fn recursive_loop<F, M>(rt: Arc<RecursiveRt<F, M>>, env: RecursiveEnv)
where
    F: TaskFn<M>,
    M: 'static,
{
    let instance = random_token();
    let mut seq: u64 = 0;
    let mut backoff = Backoff::new();

    if rt.spec.internal {
        // Tarefas internas não rodam no boot: a primeira passada fica para daqui a um intervalo
        // (sem adiar um agendamento que já exista), para não competir com a subida do serviço.
        if let Err(e) = rt.bump(true).await {
            warn!(name = rt.spec.name, error = %e, "rekka: failed to initialize the shared schedule");
        }
    }

    while !env.token.is_cancelled() {
        seq += 1;
        let token = format!("{instance}:{seq}");
        let wait = match rt.claim(&token).await {
            Ok((1, _)) => {
                backoff.reset();
                rt.run(&token, &env).await;
                continue;
            }
            Ok((_, wait)) => {
                backoff.reset();
                Duration::from_millis(wait.max(1) as u64).min(MAX_WAIT)
            }
            Err(e) => {
                warn!(name = rt.spec.name, error = %e, "rekka: failed to check recursive worker schedule");
                backoff.next_delay()
            }
        };
        tokio::select! {
            _ = sleep(wait) => {}
            _ = env.token.cancelled() => break,
        }
    }
}

impl<F, M> RecursiveRt<F, M>
where
    F: TaskFn<M>,
    M: 'static,
{
    async fn claim(&self, token: &str) -> std::result::Result<(i64, i64), redis::RedisError> {
        let mut inv = self.keys.invocation(&scripts::REC_CLAIM_SCRIPT);
        inv.arg(token).arg(millis(self.spec.interval)).arg(millis(self.spec.lease));
        inv.invoke_async(&mut self.cx.client.conn()).await
    }

    async fn bump(&self, only_if_absent: bool) -> std::result::Result<i64, redis::RedisError> {
        let mut inv = self.keys.invocation(&scripts::REC_BUMP_SCRIPT);
        inv.arg(millis(self.spec.interval)).arg(millis(self.spec.lease)).arg(u8::from(only_if_absent));
        inv.invoke_async(&mut self.cx.client.conn()).await
    }

    /// Executa uma vez, mantendo a lease do lock `token` viva enquanto executa e liberando no fim.
    async fn run(&self, token: &str, env: &RecursiveEnv) {
        let name = self.spec.name;
        let source = self.spec.source;
        let shared = &env.cx.client.shared;
        let internal = self.spec.internal;
        let shutdown_timeout = if internal { Some(Duration::ZERO) } else { env.shutdown_timeout };
        let started = Instant::now();
        if !internal {
            shared.emit(|| JobEvent::new(source, name, JobEventKind::Started));
        }

        let outcome = match self.spec.f.call(&env.cx) {
            Err(e) => Err(e.to_string()),
            Ok(fut) => {
                let mut task = AbortOnDrop(tokio::spawn(async move { fut.await.into_task_result() }));
                let beat = self.spec.lease / 3;
                let mut heartbeat = interval_at(Instant::now() + beat, beat);
                heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                let mut deadline: Option<Instant> = None;
                loop {
                    tokio::select! {
                        biased;
                        res = &mut task.0 => break match res {
                            Ok(out) => out,
                            Err(e) if e.is_panic() => Err(panic_message(e.into_panic())),
                            Err(_) => Err("aborted".to_owned()),
                        },
                        _ = heartbeat.tick() => self.extend(token, env).await,
                        _ = env.token.cancelled(), if deadline.is_none() && shutdown_timeout.is_some() => {
                            deadline = shutdown_timeout.map(|t| Instant::now() + t);
                        }
                        _ = sleep_until(deadline.unwrap_or(started)), if deadline.is_some() => {
                            task.0.abort();
                            break Err("aborted: shutdown timeout".to_owned());
                        }
                    }
                }
            }
        };

        let mut inv = self.keys.invocation(&scripts::REC_RELEASE_SCRIPT);
        inv.arg(token);
        if let Err(e) = inv.invoke_async::<i64>(&mut env.cx.client.conn()).await {
            warn!(name, error = %e, "rekka: failed to release recursive worker lock");
        }

        let kind = if outcome.is_ok() { JobEventKind::Succeeded } else { JobEventKind::Failed };
        if let Err(e) = &outcome
            && !(internal && env.token.is_cancelled())
        {
            warn!(name, error = %e, "rekka: recursive worker failed");
        }
        if internal {
            return;
        }
        shared.emit(|| JobEvent {
            error: outcome.err(),
            elapsed: Some(started.elapsed()),
            ..JobEvent::new(source, name, kind)
        });
    }

    async fn extend(&self, token: &str, env: &RecursiveEnv) {
        let mut inv = self.keys.invocation(&scripts::REC_EXTEND_SCRIPT);
        inv.arg(token).arg(millis(self.spec.lease));
        if let Err(e) = inv.invoke_async::<i64>(&mut env.cx.client.conn()).await {
            warn!(name = self.spec.name, error = %e, "rekka: failed to extend recursive worker lock");
        }
    }
}
