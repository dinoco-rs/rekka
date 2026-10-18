use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::client::{ReconcileHandler, RekkaClient};
use crate::error::{RekkaError, Result};

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
}

impl RekkaContext {
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
/// async fn handler(h: JobHandler, job: MailJob, RekkaState(db): RekkaState<Db>) { ... }
/// ```
pub struct RekkaState<T>(pub Arc<T>);

impl<T> Clone for RekkaState<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for RekkaState<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Send + Sync + 'static> FromContext for RekkaState<T> {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        cx.data::<T>().map(RekkaState).ok_or(RekkaError::MissingData(type_name::<T>()))
    }

    fn validate(cx: &RekkaContext) -> Result<()> {
        if cx.has_data::<T>() { Ok(()) } else { Err(RekkaError::MissingData(type_name::<T>())) }
    }
}

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
