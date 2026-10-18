use std::fmt::Display;
use std::future::Future;

use crate::context::{FromContext, RekkaContext};
use crate::error::Result;
use crate::job::{IntoExecutionResult, JobHandler, RekkaQueue};

/// Handler de job: `async fn(JobHandler, J, E1, ..., En) -> impl IntoExecutionResult`,
/// onde cada `Ei` implementa [`FromContext`] (ex.: `RekkaState<T>`, `RekkaClient`).
///
/// O future do handler não é boxeado: cada job vira uma única task tokio.
pub trait JobFn<J, M>: Send + Sync + 'static {
    type Output: IntoExecutionResult;
    type Future: Future<Output = Self::Output> + Send + 'static;

    fn call(&self, handler: JobHandler, job: J, cx: &RekkaContext) -> Result<Self::Future>;

    fn validate(cx: &RekkaContext) -> Result<()>;
}

/// Handler de worker recorrente e de reconcile: `async fn(E1, ..., En) -> impl IntoTaskResult`.
pub trait TaskFn<M>: Send + Sync + 'static {
    type Output: IntoTaskResult;
    type Future: Future<Output = Self::Output> + Send + 'static;

    fn call(&self, cx: &RekkaContext) -> Result<Self::Future>;

    fn validate(cx: &RekkaContext) -> Result<()>;
}

/// Retornos aceitos por [`TaskFn`]: `()` ou `Result<(), E: Display>`.
pub trait IntoTaskResult {
    fn into_task_result(self) -> Result<(), String>;
}

impl IntoTaskResult for () {
    #[inline]
    fn into_task_result(self) -> Result<(), String> {
        Ok(())
    }
}

impl<E: Display> IntoTaskResult for Result<(), E> {
    #[inline]
    fn into_task_result(self) -> Result<(), String> {
        self.map_err(|e| format!("{e:#}"))
    }
}

macro_rules! impl_handlers {
    ($($E:ident),*) => {
        impl<F, Fut, J, $($E,)*> JobFn<J, ($($E,)*)> for F
        where
            F: Fn(JobHandler, J, $($E,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + 'static,
            Fut::Output: IntoExecutionResult,
            J: RekkaQueue,
            $($E: FromContext,)*
        {
            type Output = Fut::Output;
            type Future = Fut;

            #[inline]
            #[allow(non_snake_case, unused_variables)]
            fn call(&self, handler: JobHandler, job: J, cx: &RekkaContext) -> Result<Fut> {
                $(let $E = $E::from_context(cx)?;)*
                Ok((self)(handler, job, $($E,)*))
            }

            #[allow(unused_variables)]
            fn validate(cx: &RekkaContext) -> Result<()> {
                $($E::validate(cx)?;)*
                Ok(())
            }
        }

        impl<F, Fut, $($E,)*> TaskFn<($($E,)*)> for F
        where
            F: Fn($($E,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + 'static,
            Fut::Output: IntoTaskResult,
            $($E: FromContext,)*
        {
            type Output = Fut::Output;
            type Future = Fut;

            #[inline]
            #[allow(non_snake_case, unused_variables)]
            fn call(&self, cx: &RekkaContext) -> Result<Fut> {
                $(let $E = $E::from_context(cx)?;)*
                Ok((self)($($E,)*))
            }

            #[allow(unused_variables)]
            fn validate(cx: &RekkaContext) -> Result<()> {
                $($E::validate(cx)?;)*
                Ok(())
            }
        }
    };
}

impl_handlers!();
impl_handlers!(E1);
impl_handlers!(E1, E2);
impl_handlers!(E1, E2, E3);
impl_handlers!(E1, E2, E3, E4);
impl_handlers!(E1, E2, E3, E4, E5);
impl_handlers!(E1, E2, E3, E4, E5, E6);
impl_handlers!(E1, E2, E3, E4, E5, E6, E7);
impl_handlers!(E1, E2, E3, E4, E5, E6, E7, E8);
