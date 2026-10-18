use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;

use crate::cache::Cache;
use crate::context::DataMap;
use crate::error::{RekkaError, Result};
use crate::event::{EventHandler, JobEvent};
use crate::job::{RekkaQueue, ToTaskId};
use crate::keys::QueueKeys;
use crate::scripts;
use crate::util::millis;

pub(crate) type KeyFn<J> = Arc<dyn Fn(&J) -> String + Send + Sync>;

/// Máximo de jobs por chamada de script em operações em lote.
const ENQUEUE_CHUNK: usize = 256;

pub(crate) struct Shared {
    pub(crate) conn: ConnectionManager,
    pub(crate) prefix: Arc<str>,
    pub(crate) data: DataMap,
    /// `TypeId` do job -> `KeyFn<J>` (para gravar a key de concorrência já no enqueue).
    pub(crate) key_fns: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    pub(crate) on_event: Option<EventHandler>,
    pub(crate) completed_ttl_ms: u64,
}

impl Shared {
    #[inline]
    pub(crate) fn events_enabled(&self) -> bool {
        self.on_event.is_some()
    }

    pub(crate) fn emit(&self, event: impl FnOnce() -> JobEvent) {
        if let Some(handler) = &self.on_event {
            let event = event();
            // Um pânico no handler de eventos nunca pode derrubar o worker.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event)));
        }
    }

    fn key_fn<J: RekkaQueue>(&self) -> Option<&KeyFn<J>> {
        self.key_fns.get(&TypeId::of::<J>())?.downcast_ref::<KeyFn<J>>()
    }
}

/// Resultado de um enqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Enqueued {
    /// Job novo.
    Created,
    /// Já estava aguardando: payload e horário foram atualizados.
    Updated,
    /// Está em execução agora: a nova versão roda assim que a atual terminar
    /// (nunca em paralelo com o mesmo `task_id`).
    AfterCurrentRun,
    /// (reconcile) Já existe na fila; nada foi alterado.
    AlreadyExists,
    /// (reconcile) Concluído há pouco (dentro do `completed_ttl`); não foi recriado.
    RecentlyCompleted,
}

impl Enqueued {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "created" => Self::Created,
            "updated" => Self::Updated,
            "pending" => Self::AfterCurrentRun,
            "exists" => Self::AlreadyExists,
            "completed" => Self::RecentlyCompleted,
            other => return Err(RekkaError::Protocol(format!("unknown enqueue status `{other}`"))),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Removed {
    NotFound,
    /// Estava aguardando e foi removido.
    Removed,
    /// Está em execução: termina normalmente, mas o resultado é descartado (sem retry).
    DiscardAfterRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Na fila; `run_in` é quanto falta para ficar disponível.
    Waiting { run_in: Duration, attempts: u32 },
    Running { attempt: u32 },
    /// Aguardando outro job com a mesma key de concorrência terminar.
    Blocked { attempts: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueStats {
    /// Jobs na fila (prontos + agendados), sem contar os bloqueados por key.
    pub waiting: u64,
    /// Jobs prontos para executar agora.
    pub ready: u64,
    pub running: u64,
    /// Keys de concorrência ocupadas (ou reservadas).
    pub locked_keys: u64,
}

enum Due {
    In(Duration),
    At(DateTime<Utc>),
}

impl Due {
    fn encode(&self) -> String {
        match self {
            Self::In(d) => format!("+{}", millis(*d)),
            Self::At(at) => at.timestamp_millis().max(0).to_string(),
        }
    }
}

/// Handle leve (clonável) para manipular filas. Também é um extractor.
#[derive(Clone)]
pub struct RekkaClient {
    pub(crate) shared: Arc<Shared>,
}

impl RekkaClient {
    /// Enfileira para execução imediata (upsert pelo `task_id`).
    pub async fn immediate<J: RekkaQueue>(&self, job: J) -> Result<Enqueued> {
        self.enqueue_one(job, Due::In(Duration::ZERO), false).await
    }

    /// Agenda para um instante (upsert pelo `task_id`).
    pub async fn schedule<J: RekkaQueue>(&self, job: J, at: DateTime<Utc>) -> Result<Enqueued> {
        self.enqueue_one(job, Due::At(at), false).await
    }

    /// Agenda para daqui a `delay`, medido pelo relógio do Redis.
    pub async fn schedule_in<J: RekkaQueue>(&self, job: J, delay: Duration) -> Result<Enqueued> {
        self.enqueue_one(job, Due::In(delay), false).await
    }

    /// Enfileira vários jobs para execução imediata usando poucas viagens ao Redis.
    pub async fn immediate_many<J: RekkaQueue>(&self, jobs: impl IntoIterator<Item = J>) -> Result<Vec<Enqueued>> {
        self.enqueue_many(jobs.into_iter().map(|j| (j, Due::In(Duration::ZERO))), false).await
    }

    /// Remove um job. Se estiver executando, ele termina mas o resultado é descartado.
    pub async fn remove<J: RekkaQueue>(&self, task_id: impl ToTaskId) -> Result<Removed> {
        let keys = QueueKeys::new(&self.shared.prefix, J::NAME);
        let mut inv = keys.invocation(&scripts::REMOVE_SCRIPT);
        inv.arg(task_id.to_task_id());
        let code: i64 = inv.invoke_async(&mut self.conn()).await?;
        Ok(match code {
            0 => Removed::NotFound,
            1 => Removed::Removed,
            _ => Removed::DiscardAfterRun,
        })
    }

    pub async fn state<J: RekkaQueue>(&self, task_id: impl ToTaskId) -> Result<Option<JobState>> {
        let keys = QueueKeys::new(&self.shared.prefix, J::NAME);
        let mut inv = keys.invocation(&scripts::STATE_SCRIPT);
        inv.arg(task_id.to_task_id());
        let (state, attempts, delay): (Option<String>, u32, i64) = inv.invoke_async(&mut self.conn()).await?;
        Ok(match state.as_deref() {
            None => None,
            Some("r") => Some(JobState::Running { attempt: attempts }),
            Some("g") => Some(JobState::Blocked { attempts }),
            Some(_) => Some(JobState::Waiting { run_in: Duration::from_millis(delay.max(0) as u64), attempts }),
        })
    }

    pub async fn stats<J: RekkaQueue>(&self) -> Result<QueueStats> {
        let keys = QueueKeys::new(&self.shared.prefix, J::NAME);
        let (waiting, ready, running, locked_keys): (u64, u64, u64, u64) =
            keys.invocation(&scripts::STATS_SCRIPT).invoke_async(&mut self.conn()).await?;
        Ok(QueueStats { waiting, ready, running, locked_keys })
    }

    /// Remove a fila inteira (aguardando, agendados, bloqueados e o estado dos que estão
    /// executando — esses terminam, mas o resultado é ignorado). Retorna quantos jobs existiam.
    pub async fn purge<J: RekkaQueue>(&self) -> Result<u64> {
        crate::janitor::purge(self, J::NAME).await
    }

    /// Como [`purge`](Self::purge), pelo nome — útil para filas cujo tipo não existe mais no código.
    pub async fn purge_queue(&self, name: &str) -> Result<u64> {
        crate::janitor::purge(self, name).await
    }

    /// Cache usando a mesma conexão e o mesmo prefixo.
    pub fn cache(&self) -> Cache {
        Cache::from_parts(self.conn(), Arc::clone(&self.shared.prefix))
    }

    #[inline]
    pub(crate) fn conn(&self) -> ConnectionManager {
        self.shared.conn.clone()
    }

    async fn enqueue_one<J: RekkaQueue>(&self, job: J, due: Due, only_new: bool) -> Result<Enqueued> {
        let mut out = self.enqueue_many(std::iter::once((job, due)), only_new).await?;
        out.pop().ok_or_else(|| RekkaError::Protocol("empty enqueue reply".into()))
    }

    async fn enqueue_many<J: RekkaQueue>(
        &self,
        jobs: impl Iterator<Item = (J, Due)>,
        only_new: bool,
    ) -> Result<Vec<Enqueued>> {
        let keys = QueueKeys::new(&self.shared.prefix, J::NAME);
        let key_fn = self.shared.key_fn::<J>();
        let mode = if only_new { "n" } else { "u" };
        let mut conn = self.conn();
        let mut out = Vec::new();
        let mut jobs = jobs.peekable();

        while jobs.peek().is_some() {
            let mut inv = keys.invocation(&scripts::ENQUEUE_SCRIPT);
            inv.arg(mode);
            for (job, due) in jobs.by_ref().take(ENQUEUE_CHUNK) {
                let id = job.task_id();
                if id.is_empty() {
                    return Err(RekkaError::InvalidTaskId);
                }
                let payload = serde_json::to_string(&job)?;
                let key = key_fn.map(|f| f(&job)).unwrap_or_default();
                inv.arg(id).arg(payload).arg(due.encode()).arg(key);
            }
            let statuses: Vec<String> = inv.invoke_async(&mut conn).await?;
            for s in &statuses {
                out.push(Enqueued::parse(s)?);
            }
        }
        Ok(out)
    }
}

/// Extractor usado no reconcile: adiciona jobs **somente se ainda não existirem**
/// (nem tiverem sido concluídos há menos de `completed_ttl`), sem nunca duplicar
/// ou interromper um job que já está na fila ou executando.
#[derive(Clone)]
pub struct ReconcileHandler {
    pub(crate) client: RekkaClient,
}

impl ReconcileHandler {
    pub async fn add<J: RekkaQueue>(&self, job: J) -> Result<Enqueued> {
        self.client.enqueue_one(job, Due::In(Duration::ZERO), true).await
    }

    pub async fn add_at<J: RekkaQueue>(&self, job: J, at: DateTime<Utc>) -> Result<Enqueued> {
        self.client.enqueue_one(job, Due::At(at), true).await
    }

    /// Adiciona vários jobs em lote; retorna quantos foram criados.
    pub async fn add_many<J: RekkaQueue>(&self, jobs: impl IntoIterator<Item = J>) -> Result<usize> {
        let out = self.client.enqueue_many(jobs.into_iter().map(|j| (j, Due::In(Duration::ZERO))), true).await?;
        Ok(out.iter().filter(|s| **s == Enqueued::Created).count())
    }

    pub fn client(&self) -> &RekkaClient {
        &self.client
    }
}
