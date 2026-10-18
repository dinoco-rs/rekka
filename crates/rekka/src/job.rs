use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Um tipo que pode ser enfileirado. Normalmente implementado via `#[derive(RekkaQueue)]`.
///
/// O `task_id` identifica o job de forma única dentro da fila: enfileirar de novo o mesmo
/// `task_id` atualiza o job existente em vez de duplicá-lo.
pub trait RekkaQueue: Serialize + DeserializeOwned + Send + 'static {
    /// Nome da fila (e dos eventos) deste job.
    const NAME: &'static str;

    fn task_id(&self) -> String;
}

/// Converte um identificador (`String`, `&str`, inteiros, `Uuid`, ...) para o `task_id`.
pub trait ToTaskId {
    fn to_task_id(&self) -> String;
}

impl<T: Display + ?Sized> ToTaskId for T {
    #[inline]
    fn to_task_id(&self) -> String {
        self.to_string()
    }
}

/// O que fazer quando um job retorna erro (ou entra em pânico / estoura o timeout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Volta para a fila imediatamente.
    RetryImmediate,
    /// Volta para a fila após um atraso aleatório dentro do intervalo.
    RetryAfterRange(Duration, Duration),
    /// Não tenta de novo: o job é descartado (evento `Dead`).
    Cancel,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::RetryAfterRange(Duration::from_secs(1), Duration::from_secs(30))
    }
}

/// Resultado da execução de um job.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum RekkaExecutionResult {
    /// Concluído: o job sai da fila.
    Success,
    /// Falhou: aplica a [`RetryPolicy`] do worker.
    Error(String),
    /// Cancelado: o job sai da fila sem retry.
    Cancel,
    /// Executa de novo no instante indicado (não conta como falha).
    RetryAt(DateTime<Utc>),
    /// Executa de novo após o atraso indicado (não conta como falha).
    RetryAfter(Duration),
}

/// Contexto da execução atual, recebido como primeiro argumento do handler.
#[derive(Debug, Clone)]
pub struct JobHandler {
    pub(crate) queue: &'static str,
    pub(crate) task_id: String,
    pub(crate) attempt: u32,
    pub(crate) max_attempts: Option<u32>,
    pub(crate) last_attempt: bool,
}

impl JobHandler {
    pub fn queue(&self) -> &'static str {
        self.queue
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Tentativa atual, começando em 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn max_attempts(&self) -> Option<u32> {
        self.max_attempts
    }

    /// `true` quando um erro nesta execução não será mais tentado de novo.
    pub fn is_last_attempt(&self) -> bool {
        self.last_attempt
    }

    pub fn success(&self) -> RekkaExecutionResult {
        RekkaExecutionResult::Success
    }

    pub fn err(&self, reason: impl Display) -> RekkaExecutionResult {
        RekkaExecutionResult::Error(reason.to_string())
    }

    pub fn cancel(&self) -> RekkaExecutionResult {
        RekkaExecutionResult::Cancel
    }

    pub fn retry_at(&self, at: DateTime<Utc>) -> RekkaExecutionResult {
        RekkaExecutionResult::RetryAt(at)
    }

    pub fn retry_after(&self, delay: Duration) -> RekkaExecutionResult {
        RekkaExecutionResult::RetryAfter(delay)
    }
}

/// Tipos que um handler de job pode retornar.
///
/// - [`RekkaExecutionResult`]
/// - `()` (equivale a sucesso)
/// - `Result<T, E>` com `T: IntoExecutionResult` e `E: Display` (`Err` aplica o retry)
pub trait IntoExecutionResult {
    fn into_execution_result(self) -> RekkaExecutionResult;
}

impl IntoExecutionResult for RekkaExecutionResult {
    #[inline]
    fn into_execution_result(self) -> RekkaExecutionResult {
        self
    }
}

impl IntoExecutionResult for () {
    #[inline]
    fn into_execution_result(self) -> RekkaExecutionResult {
        RekkaExecutionResult::Success
    }
}

impl<T: IntoExecutionResult, E: Display> IntoExecutionResult for Result<T, E> {
    #[inline]
    fn into_execution_result(self) -> RekkaExecutionResult {
        match self {
            Ok(v) => v.into_execution_result(),
            Err(e) => RekkaExecutionResult::Error(format!("{e:#}")),
        }
    }
}
