use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::client::{ReconcileHandler, RekkaClient};
use crate::error::{RekkaError, Result};
use crate::event::EventSource;
use crate::job::JobHandler;

#[derive(Default)]
pub(crate) struct DataMap(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl DataMap {
    pub(crate) fn insert<T: Send + Sync + 'static>(&mut self, value: Arc<T>) {
        self.0.insert(TypeId::of::<T>(), value);
    }

    pub(crate) fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.0.get(&TypeId::of::<T>()).cloned().and_then(|v| v.downcast().ok())
    }

    pub(crate) fn contains<T: 'static>(&self) -> bool {
        self.0.contains_key(&TypeId::of::<T>())
    }
}

/// Contexto compartilhado entregue aos extractors.
#[derive(Clone)]
pub struct RekkaContext {
    pub(crate) client: RekkaClient,
    /// Quem chama o handler: fila (inclusive passos de workflow), worker recorrente ou reconcile.
    pub(crate) source: EventSource,
    /// Execução atual: presente só ao chamar o handler de um job.
    pub(crate) job: Option<JobHandler>,
}

impl RekkaContext {
    pub(crate) fn new(client: RekkaClient, source: EventSource) -> Self {
        Self { client, source, job: None }
    }

    /// O mesmo contexto, para quem chama handlers de `source`.
    pub(crate) fn for_source(&self, source: EventSource) -> Self {
        Self { client: self.client.clone(), source, job: None }
    }

    /// Execução atual (o mesmo valor do extractor [`JobHandler`]). `None` em workers recorrentes,
    /// no reconcile e no `validate` de um extractor.
    pub fn job(&self) -> Option<&JobHandler> {
        self.job.as_ref()
    }

    /// Dado registrado com `.data(...)`.
    pub fn data<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.client.shared.data.get::<T>()
    }

    pub fn has_data<T: Send + Sync + 'static>(&self) -> bool {
        self.client.shared.data.contains::<T>()
    }

    pub fn client(&self) -> &RekkaClient {
        &self.client
    }
}

/// Argumentos extraídos do contexto para handlers de jobs, workers recorrentes e reconcile.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an extractor",
    label = "this argument is not the job and does not implement `FromContext`",
    note = "after the job, a handler only takes extractors: `JobHandler`, `RekkaData<T>`, `RekkaClient`, `Cache`, \
            `ReconcileHandler` or your own type implementing `FromContext`"
)]
pub trait FromContext: Sized + Send + 'static {
    fn from_context(cx: &RekkaContext) -> Result<Self>;

    /// Chamado em `start()`: permite falhar cedo quando falta configuração.
    fn validate(cx: &RekkaContext) -> Result<()> {
        let _ = cx;
        Ok(())
    }
}

/// Acesso ao dado registrado com `.data(value)`.
///
/// ```ignore
/// async fn handler(job: MailJob, RekkaData(db): RekkaData<Db>) { ... }
/// ```
pub struct RekkaData<T>(pub Arc<T>);

impl<T> Clone for RekkaData<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for RekkaData<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Send + Sync + 'static> FromContext for RekkaData<T> {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        cx.data::<T>().map(RekkaData).ok_or(RekkaError::MissingData(type_name::<T>()))
    }

    fn validate(cx: &RekkaContext) -> Result<()> {
        if cx.has_data::<T>() { Ok(()) } else { Err(RekkaError::MissingData(type_name::<T>())) }
    }
}

/// Só em handlers de job (filas e passos de workflow). Num worker recorrente ou no reconcile, o
/// `start()` falha.
impl FromContext for JobHandler {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        cx.job.clone().ok_or_else(|| RekkaError::InvalidConfig(JOB_HANDLER_OUTSIDE_JOB.into()))
    }

    fn validate(cx: &RekkaContext) -> Result<()> {
        match cx.source {
            EventSource::Queue => Ok(()),
            EventSource::Recursive | EventSource::Reconcile => {
                Err(RekkaError::InvalidConfig(JOB_HANDLER_OUTSIDE_JOB.into()))
            }
        }
    }
}

const JOB_HANDLER_OUTSIDE_JOB: &str =
    "`JobHandler` is only available to job handlers, not to recurring workers or reconcile functions";

impl FromContext for RekkaClient {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        Ok(cx.client.clone())
    }
}

impl FromContext for ReconcileHandler {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        Ok(ReconcileHandler { client: cx.client.clone() })
    }
}
