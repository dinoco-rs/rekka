//! Workflows: uma sequência de passos em que a saída de um passo é a entrada do próximo.
//!
//! Cada passo é uma fila própria (com o próprio worker, tentativas, lease e concorrência), e o
//! fluxo só começa pela fila de entrada, enfileirando o job dela. Concluir um passo e enfileirar o
//! próximo acontece num único script (`ADVANCE`): o `task_id` e a key de concorrência do job de
//! entrada acompanham a execução até o último passo.

use std::fmt::Display;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::client::KeyFn;
use crate::handler::JobFn;
use crate::job::{RekkaExecutionResult, RekkaQueue};
use crate::keys::QueueAddr;
use crate::worker::{ErasedWorker, Input, Outcome, QueueOptions, RekkaWorker, WorkerSpec};

/// Resultado de um passo de workflow. Retornar `Result<T, E>` cobre o caso comum (`Ok` passa
/// `T` adiante, `Err` aplica a [`RetryPolicy`](crate::RetryPolicy)); `StepResult` também permite
/// cancelar, reagendar ou encerrar a execução do workflow.
///
/// ```ignore
/// async fn validate(customer: Customer, handler: JobHandler) -> StepResult<Customer> {
///     if customer.email.is_empty() {
///         return handler.cancel().into(); // os próximos passos não executam
///     }
///     StepResult::Next(customer)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum StepResult<T> {
    /// Concluído: `T` é a entrada do próximo passo (no último passo, é descartado).
    Next(T),
    /// Mesmo efeito que num job: `Error` aplica a `RetryPolicy`, `RetryAt`/`RetryAfter` executam
    /// este passo de novo, `Cancel` encerra a execução sem retry e `Success` a encerra como
    /// concluída. Nos dois últimos casos, os próximos passos não executam.
    Outcome(RekkaExecutionResult),
}

impl<T> From<RekkaExecutionResult> for StepResult<T> {
    #[inline]
    fn from(result: RekkaExecutionResult) -> Self {
        Self::Outcome(result)
    }
}

/// Tipos que um passo de workflow pode retornar. `Output` é a entrada do próximo passo, gravada
/// no Redis como JSON.
///
/// - `Result<T, E>` com `E: Display` (`Ok(value)` vai para o próximo passo, `Err` aplica o retry)
/// - [`StepResult<T>`] e `Result<StepResult<T>, E>`
/// - `()` (o próximo passo recebe `()`)
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid return type for a workflow step",
    note = "return `Result<T, E>` (`Ok(value)` becomes the next step's input), `StepResult<T>`, \
            `Result<StepResult<T>, E>` or `()`, where `T: Serialize + DeserializeOwned` and `E: Display`; \
            wrap a `RekkaExecutionResult` with `.into()` to get a `StepResult`"
)]
pub trait IntoStepResult {
    type Output: Serialize + DeserializeOwned + Send + 'static;

    fn into_step_result(self) -> StepResult<Self::Output>;
}

impl IntoStepResult for () {
    type Output = ();

    #[inline]
    fn into_step_result(self) -> StepResult<()> {
        StepResult::Next(())
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static> IntoStepResult for StepResult<T> {
    type Output = T;

    #[inline]
    fn into_step_result(self) -> StepResult<T> {
        self
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static, E: Display> IntoStepResult for Result<T, E> {
    type Output = T;

    #[inline]
    fn into_step_result(self) -> StepResult<T> {
        match self {
            Ok(value) => StepResult::Next(value),
            Err(e) => StepResult::Outcome(RekkaExecutionResult::Error(format!("{e:#}"))),
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static, E: Display> IntoStepResult for Result<StepResult<T>, E> {
    type Output = T;

    #[inline]
    fn into_step_result(self) -> StepResult<T> {
        match self {
            Ok(step) => step,
            Err(e) => StepResult::Outcome(RekkaExecutionResult::Error(format!("{e:#}"))),
        }
    }
}

/// Closure de [`register_workflow`](crate::QueuesBuilder::register_workflow): recebe o
/// `RekkaWorker<J>` e devolve o workflow. A saída do último passo fica num tipo associado (e não
/// num parâmetro do método), então `register_workflow::<J>` só precisa do tipo de entrada.
/// Implementado para todo `FnOnce(RekkaWorker<J>) -> Workflow<J, O>`; você nunca o implementa.
pub trait BuildWorkflow<J>: FnOnce(RekkaWorker<J>) -> Workflow<J, <Self as BuildWorkflow<J>>::Last> {
    type Last;
}

impl<J, O, F: FnOnce(RekkaWorker<J>) -> Workflow<J, O>> BuildWorkflow<J> for F {
    type Last = O;
}

/// Workflow em construção, criado com [`RekkaWorker::start_with`] no closure de
/// [`register_workflow`](crate::QueuesBuilder::register_workflow). `O` é a saída do último passo.
#[must_use]
pub struct Workflow<J, O> {
    opts: QueueOptions,
    key_fn: Option<KeyFn<J>>,
    steps: Vec<StepFactory>,
    _o: PhantomData<fn() -> O>,
}

/// Onde um passo fica, conhecido só com o workflow completo (o último passo não tem próximo).
struct Place {
    addr: QueueAddr,
    name: &'static str,
    opts: QueueOptions,
    keyed: bool,
    next: Option<QueueAddr>,
}

type StepFactory = Box<dyn FnOnce(Place) -> Box<dyn ErasedWorker> + Send>;

impl<J: RekkaQueue> RekkaWorker<J> {
    /// Começa um workflow (só no closure de
    /// [`register_workflow`](crate::QueuesBuilder::register_workflow)): `f` é o primeiro passo e
    /// roda na fila de `J`, que é por onde o workflow começa. Encadeie os próximos com
    /// [`Workflow::then`]. As opções configuradas antes valem para cada passo.
    pub fn start_with<F, M>(self, f: F) -> Workflow<J, <F::Output as IntoStepResult>::Output>
    where
        F: JobFn<J, M>,
        F::Output: IntoStepResult,
        M: 'static,
    {
        let key_fn = self.key_fn.clone();
        let first: StepFactory = Box::new(move |place| step::<J, F, M>(place, key_fn, f));
        Workflow { opts: self.opts, key_fn: self.key_fn, steps: vec![first], _o: PhantomData }
    }
}

impl<J: RekkaQueue, O: Serialize + DeserializeOwned + Send + 'static> Workflow<J, O> {
    /// Próximo passo: `f` recebe a saída do passo anterior como job (`async fn(input: O, extractors...)`)
    /// e roda numa fila própria, `<fila de entrada>.<n>`, com o mesmo `task_id` da execução.
    pub fn then<F, M>(mut self, f: F) -> Workflow<J, <F::Output as IntoStepResult>::Output>
    where
        F: JobFn<O, M>,
        F::Output: IntoStepResult,
        M: 'static,
    {
        self.steps.push(Box::new(move |place| step::<O, F, M>(place, None, f)));
        Workflow { opts: self.opts, key_fn: self.key_fn, steps: self.steps, _o: PhantomData }
    }
}

impl<J: RekkaQueue, O> Workflow<J, O> {
    /// A key de concorrência (para o enqueue do job de entrada) e uma fila por passo.
    pub(crate) fn into_parts(self) -> (Option<KeyFn<J>>, Vec<Box<dyn ErasedWorker>>) {
        let keyed = self.key_fn.is_some();
        let count = self.steps.len();
        let addr = |step| QueueAddr { root: J::NAME, step };
        let workers = self
            .steps
            .into_iter()
            .enumerate()
            .map(|(i, factory)| {
                let next = (i + 1 < count).then(|| addr(i + 1));
                factory(Place { addr: addr(i), name: step_name(J::NAME, i), opts: self.opts.clone(), keyed, next })
            })
            .collect();
        (self.key_fn, workers)
    }
}

fn step<I, F, M>(place: Place, key_fn: Option<KeyFn<I>>, f: F) -> Box<dyn ErasedWorker>
where
    I: Input,
    F: JobFn<I, M>,
    F::Output: IntoStepResult,
    M: 'static,
{
    let convert = if place.next.is_some() { step_outcome::<F::Output> } else { last_step_outcome::<F::Output> };
    Box::new(WorkerSpec {
        addr: place.addr,
        name: place.name,
        opts: place.opts,
        key_fn,
        keyed: place.keyed,
        f,
        convert,
        next: place.next,
        _m: PhantomData,
    })
}

/// `<entrada>.<n>`. Alocado uma vez por passo, no registro: nomes de fila são `&'static str`.
fn step_name(root: &'static str, step: usize) -> &'static str {
    match step {
        0 => root,
        step => Box::leak(format!("{root}.{step}").into_boxed_str()),
    }
}

fn step_outcome<R: IntoStepResult>(out: R) -> Outcome {
    match out.into_step_result() {
        StepResult::Next(value) => match serde_json::to_string(&value) {
            Ok(input) => Outcome::Next(input),
            Err(e) => Outcome::Result(RekkaExecutionResult::Error(format!("failed to serialize the step output: {e}"))),
        },
        StepResult::Outcome(result) => Outcome::Result(result),
    }
}

/// Último passo: a saída não vai para lugar nenhum, então nem é serializada.
fn last_step_outcome<R: IntoStepResult>(out: R) -> Outcome {
    match out.into_step_result() {
        StepResult::Next(_) => Outcome::Result(RekkaExecutionResult::Success),
        StepResult::Outcome(result) => Outcome::Result(result),
    }
}
