//! Handlers com futures grandes em worker threads com stack reduzido. Precisa do Redis da suíte.
//!
//! O future do handler passa pelo stack uma única vez, quando é criado (antes de ir para o heap,
//! no `call`). Fora isso, nada no caminho claim → execute → finalize, nem no spawn, pode depender
//! do tamanho dele. Medido em debug no 0.0.4, antes do boxing: um handler de 64 KiB precisava de
//! ~1 MiB de stack e um de 256 KiB, de ~4 MiB. Depois: ~120 KiB e ~270 KiB.
//!
//! Arquivo separado da suíte porque um stack overflow aborta o binário de teste inteiro.

#[path = "suite/common.rs"]
mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rekka::{JobHandler, RekkaData, RekkaQueue};
use serde::{Deserialize, Serialize};

use common::{TestEnv, eventually};

const KIB: usize = 1024;

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "stack")]
struct StackJob {
    task_id: u32,
}

async fn holds<const N: usize>(_h: JobHandler, _job: StackJob, RekkaData(done): RekkaData<AtomicUsize>) {
    let buf = [0u8; N];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
    done.fetch_add(1, Ordering::SeqCst);
}

async fn task_holds<const N: usize>(RekkaData(done): RekkaData<AtomicUsize>) {
    let buf = [0u8; N];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
    done.fetch_add(1, Ordering::SeqCst);
}

/// Roda `test` numa task de um runtime cujas worker threads têm `stack` bytes. Tudo acontece
/// numa task (o `block_on` rodaria na thread do teste, com o stack padrão).
fn on_small_stack<Fut: Future<Output = ()> + Send + 'static>(stack: usize, test: impl FnOnce() -> Fut) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(stack)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(rt.spawn(test())).unwrap();
}

async fn run_queue<F, M>(handler: F)
where
    F: rekka::JobFn<StackJob, M>,
    M: 'static,
{
    let env = TestEnv::new();
    let done = Arc::new(AtomicUsize::new(0));
    let queues =
        env.builder().data_arc(done.clone()).register::<StackJob>(|w| w.build_fn(handler)).start().await.unwrap();
    queues.immediate(StackJob { task_id: 1 }).await.unwrap();
    assert!(eventually(Duration::from_secs(10), || done.load(Ordering::SeqCst) == 1).await, "the job must run");
    queues.shutdown().await.unwrap();
}

async fn run_recursive<F, M>(handler: F)
where
    F: rekka::TaskFn<M>,
    M: 'static,
{
    let env = TestEnv::new();
    let done = Arc::new(AtomicUsize::new(0));
    let queues = env
        .builder()
        .data_arc(done.clone())
        .register_recursive("stack", |w| w.interval(Duration::from_secs(3600)).build_fn(handler))
        .start()
        .await
        .unwrap();
    assert!(eventually(Duration::from_secs(10), || done.load(Ordering::SeqCst) == 1).await, "the task must run");
    queues.shutdown().await.unwrap();
}

#[test]
fn queue_runs_a_64k_handler_on_a_256k_stack() {
    on_small_stack(256 * KIB, || run_queue(holds::<{ 64 * KIB }>));
}

#[test]
fn queue_runs_a_256k_handler_on_a_512k_stack() {
    on_small_stack(512 * KIB, || run_queue(holds::<{ 256 * KIB }>));
}

#[test]
fn recursive_worker_runs_a_64k_handler_on_a_256k_stack() {
    on_small_stack(256 * KIB, || run_recursive(task_holds::<{ 64 * KIB }>));
}

#[test]
fn recursive_worker_runs_a_256k_handler_on_a_512k_stack() {
    on_small_stack(512 * KIB, || run_recursive(task_holds::<{ 256 * KIB }>));
}
