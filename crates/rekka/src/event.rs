use std::sync::Arc;
use std::time::Duration;

use crate::job::RekkaQueue;

pub(crate) type EventHandler = Box<dyn Fn(JobEvent) + Send + Sync>;

/// De onde o evento veio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventSource {
    Queue,
    Recursive,
    Reconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobEventKind {
    /// A execução começou.
    Started,
    /// Concluído com sucesso.
    Succeeded,
    /// Falhou e será tentado de novo (de acordo com a `RetryPolicy`).
    Failed,
    /// Falhou e não será mais tentado (`RetryPolicy::Cancel` ou `max_attempts` atingido).
    Dead,
    /// O handler retornou `cancel()`.
    Cancelled,
    /// O handler pediu `retry_at` / `retry_after`.
    Rescheduled,
    /// O worker que executava o job parou de responder (lease expirou) e o job voltou para a fila.
    Stalled,
    /// O job foi removido (`remove`) durante a execução; o resultado foi descartado.
    Discarded,
}

/// Evento entregue ao handler registrado em `on_event`.
#[derive(Debug, Clone)]
pub struct JobEvent {
    pub(crate) source: EventSource,
    pub(crate) name: &'static str,
    pub(crate) kind: JobEventKind,
    pub(crate) task_id: Option<String>,
    pub(crate) attempt: u32,
    pub(crate) error: Option<String>,
    pub(crate) elapsed: Option<Duration>,
    pub(crate) retry_in: Option<Duration>,
    pub(crate) payload: Option<Arc<str>>,
}

impl JobEvent {
    pub(crate) fn new(source: EventSource, name: &'static str, kind: JobEventKind) -> Self {
        Self { source, name, kind, task_id: None, attempt: 0, error: None, elapsed: None, retry_in: None, payload: None }
    }

    pub fn source(&self) -> EventSource {
        self.source
    }

    /// Nome da fila (`RekkaQueue::NAME`) ou do worker recorrente.
    pub fn job_name(&self) -> &'static str {
        self.name
    }

    pub fn kind(&self) -> JobEventKind {
        self.kind
    }

    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Duração da execução (eventos finais).
    pub fn elapsed(&self) -> Option<Duration> {
        self.elapsed
    }

    /// Atraso até a próxima tentativa, quando conhecido.
    pub fn retry_in(&self) -> Option<Duration> {
        self.retry_in
    }

    /// Payload serializado (JSON) do job.
    pub fn payload(&self) -> Option<&str> {
        self.payload.as_deref()
    }

    /// Desserializa o job, se o evento pertence à fila de `T`.
    pub fn get_job<T: RekkaQueue>(&self) -> Option<T> {
        if self.source != EventSource::Queue || self.name != T::NAME {
            return None;
        }
        serde_json::from_str(self.payload.as_deref()?).ok()
    }

    pub fn is<T: RekkaQueue>(&self) -> bool {
        self.source == EventSource::Queue && self.name == T::NAME
    }

    pub fn is_started(&self) -> bool {
        self.kind == JobEventKind::Started
    }

    pub fn is_success(&self) -> bool {
        self.kind == JobEventKind::Succeeded
    }

    /// `Failed` ou `Dead`.
    pub fn is_failure(&self) -> bool {
        matches!(self.kind, JobEventKind::Failed | JobEventKind::Dead)
    }

    pub fn is_dead(&self) -> bool {
        self.kind == JobEventKind::Dead
    }

    pub fn is_cancelled(&self) -> bool {
        self.kind == JobEventKind::Cancelled
    }

    /// O job voltará a executar (`Failed`, `Rescheduled` ou `Stalled`).
    pub fn is_retry(&self) -> bool {
        matches!(self.kind, JobEventKind::Failed | JobEventKind::Rescheduled | JobEventKind::Stalled)
    }

    /// O job saiu da fila definitivamente.
    pub fn is_final(&self) -> bool {
        matches!(
            self.kind,
            JobEventKind::Succeeded | JobEventKind::Dead | JobEventKind::Cancelled | JobEventKind::Discarded
        )
    }
}
