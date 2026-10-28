use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;

use crate::context::{FromContext, RekkaContext};
use crate::error::Result;
use crate::event::EventSource;

/// Limite padrão do tamanho do future de um handler (`max_future_size`).
pub(crate) const DEFAULT_MAX_FUTURE_SIZE: usize = 16 * 1024;

/// Handler de job: `async fn(J, E1, ..., En) -> R`. O job vem sempre primeiro; cada `Ei`
/// implementa [`FromContext`] (ex.: `JobHandler`, `RekkaData<T>`, `RekkaClient`). Numa fila, `R`
/// implementa [`IntoExecutionResult`](crate::IntoExecutionResult); num passo de workflow,
/// [`IntoStepResult`](crate::IntoStepResult).
///
/// O future do handler tem o tamanho que o handler pedir (tudo o que fica vivo através de um
/// `.await`). `call` o boxeia uma única vez, na fronteira: os futures internos do rekka guardam
/// só o ponteiro e têm tamanho constante. `Future` é o tipo do handler, medido no registro
/// contra `max_future_size`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler for `{J}`",
    label = "expected `async fn(job: {J}, extractors...)`",
    note = "the job is always the first argument; `JobHandler` and the other extractors (types implementing \
            `FromContext`, up to 8) come after it"
)]
pub trait JobFn<J, M>: Send + Sync + 'static {
    type Output;
    type Future: Future<Output = Self::Output> + Send + 'static;

    fn call(&self, job: J, cx: &RekkaContext) -> Result<Pin<Box<Self::Future>>>;

    fn validate(cx: &RekkaContext) -> Result<()>;
}

/// Handler de worker recorrente e de reconcile: `async fn(E1, ..., En) -> impl IntoTaskResult`.
/// O future é boxeado em `call`, como em [`JobFn`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a recurring worker or reconcile handler",
    label = "expected `async fn(extractors...)`",
    note = "recurring workers and reconcile functions only take extractors (types implementing `FromContext`, \
            up to 8), and return `()` or `Result<(), E: Display>`"
)]
pub trait TaskFn<M>: Send + Sync + 'static {
    type Output: IntoTaskResult;
    type Future: Future<Output = Self::Output> + Send + 'static;

    fn call(&self, cx: &RekkaContext) -> Result<Pin<Box<Self::Future>>>;

    fn validate(cx: &RekkaContext) -> Result<()>;
}

/// Tamanho do future de um handler registrado, medido no `start()`. Ver
/// [`Queues::future_sizes`](crate::Queues::future_sizes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FutureSize {
    pub(crate) source: EventSource,
    pub(crate) name: &'static str,
    pub(crate) size: usize,
    pub(crate) limit: usize,
}

impl FutureSize {
    pub fn source(&self) -> EventSource {
        self.source
    }

    /// Nome da fila (`RekkaQueue::NAME`), do worker recorrente ou `reconcile`.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// `size_of` do future devolvido pelo handler, em bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// `max_future_size` aplicado a este handler, em bytes.
    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn exceeds_limit(&self) -> bool {
        self.size > self.limit
    }

    pub(crate) fn describe(&self) -> String {
        let kind = match self.source {
            EventSource::Queue => "queue ",
            EventSource::Recursive => "recursive worker ",
            EventSource::Reconcile => "",
        };
        format!(
            "{kind}`{}`: the handler future is {} bytes, above max_future_size ({} bytes); large values are kept \
             alive across `.await`: wrap the heavy part in `Box::pin`",
            self.name, self.size, self.limit
        )
    }
}

/// Retornos aceitos por [`TaskFn`]: `()` ou `Result<(), E: Display>`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid return type for a recurring worker or reconcile handler",
    note = "return `()` or `Result<(), E>` where `E: Display`"
)]
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
            F: Fn(J, $($E,)*) -> Fut + Send + Sync + 'static,
            Fut: Future + Send + 'static,
            $($E: FromContext,)*
        {
            type Output = Fut::Output;
            type Future = Fut;

            #[inline]
            #[allow(non_snake_case, unused_variables)]
            fn call(&self, job: J, cx: &RekkaContext) -> Result<Pin<Box<Fut>>> {
                $(let $E = $E::from_context(cx)?;)*
                Ok(Box::pin((self)(job, $($E,)*)))
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
            fn call(&self, cx: &RekkaContext) -> Result<Pin<Box<Fut>>> {
                $(let $E = $E::from_context(cx)?;)*
                Ok(Box::pin((self)($($E,)*)))
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
