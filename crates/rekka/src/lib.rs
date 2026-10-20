//! Filas de jobs sobre Redis, seguras para escala horizontal.

extern crate self as rekka;

mod cache;
mod client;
mod context;
mod cron;
mod error;
mod event;
mod handler;
mod janitor;
mod job;
mod keys;
mod recursive;
mod runtime;
mod scripts;
mod util;
mod worker;

pub use cache::{Cache, CacheBuilder, RekkaCache};
pub use client::{Enqueued, JobState, QueueStats, ReconcileHandler, RekkaClient, Removed};
pub use context::{FromContext, RekkaContext, RekkaState};
pub use error::{RekkaError, Result};
pub use event::{EventSource, JobEvent, JobEventKind};
pub use handler::{IntoTaskResult, JobFn, TaskFn};
pub use job::{IntoExecutionResult, JobHandler, RekkaExecutionResult, RekkaQueue, RetryPolicy, ToTaskId};
pub use recursive::{RecursiveWorker, RekkaRecursiveWorker};
pub use rekka_derives::{RekkaCache, RekkaQueue};
pub use runtime::{Queues, QueuesBuilder};
pub use worker::{RekkaWorker, Worker};

pub use chrono;

#[doc(hidden)]
pub mod __private {
    /// Bounds de [`RekkaQueue`](crate::RekkaQueue), usados pelo derive em structs genéricas.
    pub trait QueueBound: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}

    impl<T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static> QueueBound for T {}

    /// Bounds de [`RekkaCache`](crate::RekkaCache), usados pelo derive em structs genéricas.
    pub trait CacheBound: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}

    impl<T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static> CacheBound for T {}

    /// Se `key` já é uma chave completa do template: começa com o texto fixo antes do primeiro
    /// `{campo}`, termina com o texto fixo depois do último, os trechos do meio aparecem em ordem
    /// e cada `{campo}` ocupa pelo menos um caractere.
    pub fn matches_key_template(key: &str, literals: &[&str]) -> bool {
        let [first, middle @ .., last] = literals else { return false };
        let Some(mut rest) = key.strip_prefix(first) else { return false };
        for literal in middle {
            let Some(c) = rest.chars().next() else { return false };
            let after = &rest[c.len_utf8()..];
            match after.find(literal) {
                Some(at) => rest = &after[at + literal.len()..],
                None => return false,
            }
        }
        rest.len() > last.len() && rest.ends_with(last)
    }
}
