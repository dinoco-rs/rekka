//! Orçamento dos futures internos do rekka. O future do handler é boxeado uma única vez, no
//! `call`, então nenhum future interno pode crescer com o handler: um handler de 256 KiB tem que
//! dar os mesmos números que um handler mínimo.
//!
//! Passou do orçamento? Leia a seção "Performance" do AGENTS.md. Só aumente um orçamento com o
//! motivo medido na mensagem de commit. Para ver os layouts:
//! `RUSTC_BOOTSTRAP=1 cargo rustc --test future_sizes -- -Zprint-type-sizes`.

use rekka::__private::{JobFutureSizes, TaskFutureSizes, job_future_sizes, task_future_sizes};
use rekka::RekkaQueue;
use serde::{Deserialize, Serialize};

/// Orçamentos em bytes, ~20% acima do medido em 2026-10-25 (rustc 1.95, tokio 1.53, redis 1.7),
/// iguais em debug e release. Os valores medidos abaixo são do 0.0.6 (2026-10-28), depois de
/// `JobHandler` virar extractor e dos passos de workflow (`Outcome` no lugar do resultado).
mod budget {
    /// Medido: 1040.
    pub const RUN_JOB: usize = 1280;
    /// Medido: 144 (o ponteiro de conversão da saída).
    pub const EXECUTE: usize = 192;
    /// Medido: 712.
    pub const FINALIZE: usize = 896;
    /// Medido: 984.
    pub const QUEUE_LOOP: usize = 1152;
    /// Medido: 1208.
    pub const QUEUE_SUPERVISE: usize = 1408;
    /// Medido: 840.
    pub const RUN: usize = 1024;
    /// Medido: 1048.
    pub const RECURSIVE_LOOP: usize = 1280;
    /// Medido: 1240.
    pub const RECURSIVE_SUPERVISE: usize = 1472;
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "sizes")]
struct SizeJob {
    task_id: u32,
}

async fn minimal(_job: SizeJob) {}

/// Mantém `N` bytes vivos através de um `.await`: o future do handler tem pelo menos `N` bytes.
async fn holds<const N: usize>(_job: SizeJob) -> Result<(), String> {
    let buf = [0u8; N];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
    Ok(())
}

async fn task_minimal() {}

async fn task_holds<const N: usize>() -> Result<(), String> {
    let buf = [0u8; N];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
    Ok(())
}

/// `(future, tamanho, orçamento)`.
type Row = (&'static str, usize, usize);
/// `(handler, tamanho do handler, linhas)`.
type Case<'a, const R: usize> = (&'a str, usize, [Row; R]);

fn job_rows(s: &JobFutureSizes) -> [Row; 5] {
    [
        ("run_job", s.run_job, budget::RUN_JOB),
        ("execute", s.execute, budget::EXECUTE),
        ("finalize", s.finalize, budget::FINALIZE),
        ("queue_loop", s.queue_loop, budget::QUEUE_LOOP),
        ("supervise (queue)", s.supervise, budget::QUEUE_SUPERVISE),
    ]
}

fn task_rows(s: &TaskFutureSizes) -> [Row; 3] {
    [
        ("run", s.run, budget::RUN),
        ("recursive_loop", s.recursive_loop, budget::RECURSIVE_LOOP),
        ("supervise (recursive)", s.supervise, budget::RECURSIVE_SUPERVISE),
    ]
}

/// Falha com todas as linhas fora do orçamento ou que mudam com o handler, de uma vez.
fn check<const R: usize>(cases: &[Case<'_, R>]) {
    let mut problems = Vec::new();
    let (_, _, baseline) = &cases[0];
    for (label, handler, rows) in cases {
        for ((name, size, budget), (_, minimal, _)) in rows.iter().zip(baseline) {
            if size > budget {
                problems.push(format!(
                    "`{name}` with the {label} handler ({handler} bytes) is {size} bytes, over its budget of {budget}"
                ));
            }
            if size != minimal {
                problems.push(format!(
                    "`{name}` is {size} bytes with the {label} handler but {minimal} with the minimal one: it embeds the handler future"
                ));
            }
        }
    }
    assert!(
        problems.is_empty(),
        "rekka's internal futures must have a constant size (see AGENTS.md, \"Performance\"):\n  {}",
        problems.join("\n  ")
    );
}

#[test]
fn queue_futures_stay_within_budget_whatever_the_handler() {
    let cases = [
        ("minimal", job_future_sizes(&minimal)),
        ("64 KiB", job_future_sizes(&holds::<{ 64 * 1024 }>)),
        ("256 KiB", job_future_sizes(&holds::<{ 256 * 1024 }>)),
    ];
    assert!(cases[1].1.handler >= 64 * 1024 && cases[2].1.handler >= 256 * 1024, "the probe must measure the handler");
    check(&cases.map(|(label, s)| (label, s.handler, job_rows(&s))));
}

#[test]
fn recursive_futures_stay_within_budget_whatever_the_handler() {
    let cases = [
        ("minimal", task_future_sizes(&task_minimal)),
        ("64 KiB", task_future_sizes(&task_holds::<{ 64 * 1024 }>)),
        ("256 KiB", task_future_sizes(&task_holds::<{ 256 * 1024 }>)),
    ];
    assert!(cases[1].1.handler >= 64 * 1024 && cases[2].1.handler >= 256 * 1024, "the probe must measure the handler");
    check(&cases.map(|(label, s)| (label, s.handler, task_rows(&s))));
}
