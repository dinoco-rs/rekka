use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::context::RekkaContext;
use crate::cron::CronTime;
use crate::error::Result;
use crate::event::{EventSource, JobEvent, JobEventKind};
use crate::handler::{IntoTaskResult, TaskFn};
use crate::keys::RecursiveKeys;
use crate::scripts;
use crate::util::{AbortOnDrop, Backoff, is_transient, millis, panic_message, random_token, supervise};
use crate::worker::BoxFuture;

/// Espera máxima entre verificações do agendamento compartilhado.
const MAX_WAIT: Duration = Duration::from_secs(30);
/// Uma queda do Redis percebida por várias instâncias dentro desta janela gera uma única
/// execução do reconcile quando ele volta.
const RECOVERY_WINDOW: Duration = Duration::from_secs(60);

/// Calcula a próxima execução de um `cron_time` no fuso configurado.
type NextFn = Arc<dyn Fn(&CronTime, DateTime<Utc>) -> Option<DateTime<Utc>> + Send + Sync>;

/// Quando um worker recorrente executa.
pub(crate) enum Schedule {
    /// A cada `interval`, contado do início da execução anterior. Na primeira subida, executa na hora.
    Interval(Duration),
    /// A cada `interval`, contado do fim da execução anterior: a próxima só é agendada quando a
    /// atual termina (ou, se a instância morrer, `interval` depois do último heartbeat). Na
    /// primeira subida e quando o Redis volta de uma queda, executa na hora.
    Delay(Duration),
    /// Nos instantes de uma expressão cron. Nunca executa na subida, só no próximo instante.
    Cron { expr: String, parsed: std::result::Result<CronTime, String>, next: NextFn },
}

impl Schedule {
    fn check(&self, name: &str) -> std::result::Result<(), String> {
        match self {
            Self::Interval(interval) | Self::Delay(interval) if *interval < Duration::from_millis(10) => {
                Err(format!("`{name}`: interval must be at least 10ms"))
            }
            Self::Interval(_) | Self::Delay(_) => Ok(()),
            Self::Cron { expr, parsed: Err(e), .. } => Err(format!("`{name}`: invalid cron_time `{expr}`: {e}")),
            Self::Cron { expr, parsed: Ok(cron), next } => match next(cron, Utc::now()) {
                Some(_) => Ok(()),
                None => Err(format!("`{name}`: cron_time `{expr}` never matches")),
            },
        }
    }
}

/// Worker executado a cada `interval` ou nos instantes de um `cron_time`. Em escala horizontal,
/// apenas uma instância executa cada ciclo e nunca há duas execuções simultâneas (lock no Redis).
pub struct RekkaRecursiveWorker {
    name: &'static str,
    interval: Duration,
    cron: Option<String>,
    timezone: NextFn,
    lease: Duration,
}

impl RekkaRecursiveWorker {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            interval: Duration::from_secs(60),
            cron: None,
            timezone: Arc::new(|cron, after| cron.next_after(after, &Utc)),
            lease: Duration::from_secs(30),
        }
    }

    /// Executa a cada `interval` (padrão 60s). Substitui um `cron_time` anterior.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self.cron = None;
        self
    }

    /// Executa nos instantes de uma expressão cron, como o `cronTime` do `node-cron`:
    /// `segundo minuto hora dia mês dia-da-semana`, com o segundo opcional.
    ///
    /// ```text
    /// "*/2 * * * *"     a cada 2 minutos
    /// "*/10 * * * * *"  a cada 10 segundos
    /// "0 30 9 * * 1-5"  09:30:00 de segunda a sexta
    /// "0 0 1 * *"       meia-noite do dia 1 de cada mês
    /// ```
    ///
    /// Campos aceitam `*`, `5`, `1-5`, `1,15,30`, `*/2`, `10-30/5` e nomes (`jan`, `mon`);
    /// dia da semana vai de 0 a 7 (0 e 7 = domingo). Ao contrário de `interval`, não executa na
    /// subida: a primeira execução é no próximo instante da expressão. Avaliada em UTC; mude com
    /// [`timezone`](Self::timezone). Expressões inválidas fazem o `start()` falhar.
    /// Substitui um `interval` anterior.
    pub fn cron_time(mut self, expr: impl Into<String>) -> Self {
        self.cron = Some(expr.into());
        self
    }

    /// Fuso em que o `cron_time` é avaliado (padrão UTC): `chrono::FixedOffset`, `chrono::Local`
    /// ou um fuso do `chrono-tz` (`chrono_tz::America::Sao_Paulo`). Use o mesmo fuso em todas as
    /// instâncias.
    pub fn timezone<Tz>(mut self, tz: Tz) -> Self
    where
        Tz: TimeZone + Send + Sync + 'static,
    {
        self.timezone = Arc::new(move |cron, after| cron.next_after(after, &tz));
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
        let schedule = match self.cron {
            Some(expr) => Schedule::Cron { parsed: CronTime::parse(&expr), expr, next: self.timezone },
            None => Schedule::Interval(self.interval),
        };
        RecursiveWorker {
            runner: Box::new(RecursiveSpec {
                name: self.name,
                key_name: self.name.to_owned(),
                source: EventSource::Recursive,
                internal: false,
                schedule,
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
    /// instância por ciclo, inclusive na subida (sem agendamento no Redis, roda na hora). O
    /// `interval` conta do fim da execução anterior, então um reconcile lento nunca emenda no próximo.
    /// Com o Redis fora do ar não roda; quando ele volta, roda uma vez, mesmo fora do horário.
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
                schedule: Schedule::Delay(interval),
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
                schedule: Schedule::Interval(interval),
                lease: Duration::from_secs(30),
                f,
                _m: PhantomData,
            }),
        }
    }
}

#[derive(Clone)]
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
    schedule: Schedule,
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
        self.schedule.check(self.name)?;
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
        let rt = Arc::new(RecursiveRt { spec: *self, keys, cx });
        let token = env.token.clone();
        Box::pin(async move {
            let name = rt.spec.name;
            supervise("recursive worker", name, &token, || recursive_loop(Arc::clone(&rt), env.clone())).await;
        })
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

    // Reconcile: roda de novo assim que o Redis volta de uma queda (uma vez, em uma instância).
    let mut recoveries = matches!(rt.spec.schedule, Schedule::Delay(_)).then(|| rt.cx.client.shared.conn.recoveries());
    let mut recovered = false;

    while !env.token.is_cancelled() {
        if let Some(rx) = &mut recoveries
            && rx.has_changed().unwrap_or(false)
        {
            rx.borrow_and_update();
            recovered = true;
        }
        if recovered {
            match rt.recover().await {
                Ok(_) => recovered = false,
                Err(e) => warn!(name = rt.spec.name, error = %e, "rekka: failed to schedule a run after reconnecting"),
            }
        }
        seq += 1;
        let token = format!("{instance}:{seq}");
        let claimed = match &rt.spec.schedule {
            Schedule::Interval(interval) | Schedule::Delay(interval) => rt.claim(&token, *interval).await,
            Schedule::Cron { parsed: Ok(cron), next, .. } => rt.claim_cron(&token, cron, next).await,
            // Rejeitado no `check`.
            Schedule::Cron { parsed: Err(_), .. } => return,
        };
        let wait = match claimed {
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
            Ok(()) = changed(&mut recoveries) => recovered = true,
        }
    }
}

async fn changed(rx: &mut Option<watch::Receiver<u64>>) -> std::result::Result<(), watch::error::RecvError> {
    match rx {
        Some(rx) => rx.changed().await,
        None => std::future::pending().await,
    }
}

impl<F, M> RecursiveRt<F, M>
where
    F: TaskFn<M>,
    M: 'static,
{
    async fn claim(&self, token: &str, interval: Duration) -> std::result::Result<(i64, i64), redis::RedisError> {
        let window = if self.delay_ms() > 0 { millis(RECOVERY_WINDOW) } else { 0 };
        let mut inv = self.keys.invocation(&scripts::REC_CLAIM_SCRIPT);
        inv.arg(token).arg(millis(interval)).arg(millis(self.spec.lease)).arg(window);
        inv.invoke_async(&mut self.cx.client.conn()).await
    }

    /// Marca uma execução devida pela volta do Redis: uma por queda, somando todas as instâncias.
    async fn recover(&self) -> std::result::Result<i64, redis::RedisError> {
        let keep = self.delay_ms().saturating_mul(2).saturating_add(millis(self.spec.lease) + millis(RECOVERY_WINDOW));
        let mut inv = self.keys.invocation(&scripts::REC_RECOVER_SCRIPT);
        inv.arg(keep);
        inv.invoke_async(&mut self.cx.client.conn()).await
    }

    /// Mesmo contrato de [`claim`](Self::claim) para um `cron_time`: `(1, _)` = executar agora,
    /// `(0, espera_ms)` = verificar de novo depois. A próxima execução é calculada a partir do
    /// relógio do Redis, então todas as instâncias chegam no mesmo instante.
    async fn claim_cron(
        &self,
        token: &str,
        cron: &CronTime,
        next: &NextFn,
    ) -> std::result::Result<(i64, i64), redis::RedisError> {
        let mut conn = self.cx.client.conn();
        let (due, now, pttl): (i64, i64, i64) =
            self.keys.invocation(&scripts::REC_PEEK_SCRIPT).invoke_async(&mut conn).await?;
        if pttl != -2 {
            // Executando em alguma instância.
            return Ok((0, if due > now { due - now } else if pttl > 0 { pttl } else { 1000 }));
        }
        if due > now {
            return Ok((0, due - now));
        }
        let Some(following) = Utc.timestamp_millis_opt(now).single().and_then(|now| next(cron, now)) else {
            warn!(name = self.spec.name, "rekka: cron_time has no future occurrence");
            return Ok((0, millis(MAX_WAIT) as i64));
        };
        let following = following.timestamp_millis();
        let mut inv = self.keys.invocation(&scripts::REC_CRON_CLAIM_SCRIPT);
        inv.arg(token).arg(due).arg(following).arg(millis(self.spec.lease));
        match inv.invoke_async::<i64>(&mut conn).await? {
            1 => Ok((1, 0)),
            // Sem agendamento (primeira subida): agendado para o próximo instante, sem executar.
            _ if due < 0 => Ok((0, following - now)),
            // Outra instância levou: verifica de novo.
            _ => Ok((0, 1)),
        }
    }

    async fn bump(&self, only_if_absent: bool) -> std::result::Result<i64, redis::RedisError> {
        let Schedule::Interval(interval) = &self.spec.schedule else { return Ok(0) };
        let mut inv = self.keys.invocation(&scripts::REC_BUMP_SCRIPT);
        inv.arg(millis(*interval)).arg(millis(self.spec.lease)).arg(u8::from(only_if_absent));
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

        // Com `Delay`, a próxima execução é agendada aqui, no mesmo script que libera o lock. Com
        // o Redis fora, tenta até ele voltar: senão o lock só sairia quando a lease expirasse,
        // atrasando a próxima execução depois da volta.
        let mut inv = self.keys.invocation(&scripts::REC_RELEASE_SCRIPT);
        inv.arg(token).arg(self.delay_ms()).arg(millis(self.spec.lease));
        let mut backoff = Backoff::new();
        loop {
            match inv.invoke_async::<i64>(&mut env.cx.client.conn()).await {
                Ok(_) => break,
                Err(e) if is_transient(&e) && !env.token.is_cancelled() => tokio::select! {
                    _ = sleep(backoff.next_delay()) => {}
                    _ = env.token.cancelled() => {}
                },
                Err(e) => {
                    warn!(name, error = %e, "rekka: failed to release recursive worker lock; it expires with the lease");
                    break;
                }
            }
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

    /// `interval` de um `Delay` (contado do fim da execução), 0 nos outros agendamentos.
    fn delay_ms(&self) -> u64 {
        match &self.spec.schedule {
            Schedule::Delay(interval) => millis(*interval),
            _ => 0,
        }
    }

    async fn extend(&self, token: &str, env: &RecursiveEnv) {
        let mut inv = self.keys.invocation(&scripts::REC_EXTEND_SCRIPT);
        inv.arg(token).arg(millis(self.spec.lease)).arg(self.delay_ms());
        match inv.invoke_async::<i64>(&mut env.cx.client.conn()).await {
            Ok(1) => {}
            // O lock expirou (Redis fora do ar por mais que a lease, restart sem persistência):
            // retomado para nenhuma outra instância começar enquanto esta ainda executa.
            Ok(2) => warn!(name = self.spec.name, "rekka: recursive worker lock expired while running; re-acquired"),
            Ok(_) => warn!(
                name = self.spec.name,
                "rekka: recursive worker lock was taken by another instance while running"
            ),
            Err(e) => warn!(name = self.spec.name, error = %e, "rekka: failed to extend recursive worker lock"),
        }
    }
}
