//! Suíte de testes do rekka. Precisa de um Redis acessível em `REKKA_REDIS_URL`
//! (padrão `redis://127.0.0.1:6379`).

mod common;

mod cache;
mod cleanup;
mod concurrency;
mod config;
mod derive;
mod enqueue;
mod events;
mod execution;
mod keyed;
mod outage;
mod reconcile;
mod recovery;
mod recursive;
mod retry;
mod shutdown;
mod throughput;
mod workflow;
