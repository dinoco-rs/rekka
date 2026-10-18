use std::any::Any;
use std::time::Duration;

use redis::{RedisError, RetryMethod};
use tokio::task::JoinHandle;

pub(crate) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Erros que tendem a sumir sozinhos (rede, reconexão, timeout).
pub(crate) fn is_transient(e: &RedisError) -> bool {
    e.is_io_error() || !matches!(e.retry_method(), RetryMethod::NoRetry)
}

pub(crate) fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        format!("job panicked: {s}")
    } else if let Some(s) = panic.downcast_ref::<String>() {
        format!("job panicked: {s}")
    } else {
        "job panicked".to_owned()
    }
}

pub(crate) fn random_between(min: Duration, max: Duration) -> Duration {
    let (lo, hi) = if min <= max { (min, max) } else { (max, min) };
    let (lo, hi) = (millis(lo), millis(hi));
    Duration::from_millis(if lo == hi { lo } else { fastrand::u64(lo..=hi) })
}

pub(crate) fn random_token() -> String {
    format!("{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..))
}

/// Backoff exponencial com jitter, usado quando o Redis está indisponível.
pub(crate) struct Backoff {
    current: Duration,
}

impl Backoff {
    const MIN: Duration = Duration::from_millis(50);
    const MAX: Duration = Duration::from_secs(5);

    pub(crate) fn new() -> Self {
        Self { current: Self::MIN }
    }

    pub(crate) fn reset(&mut self) {
        self.current = Self::MIN;
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(Self::MAX);
        delay + random_between(Duration::ZERO, delay / 2)
    }
}

/// Aborta a task quando sai de escopo (inclusive quando o dono é abortado).
pub(crate) struct AbortOnDrop<T>(pub(crate) JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Escapa os caracteres especiais do glob do Redis (`SCAN MATCH`).
pub(crate) fn glob_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Remove (UNLINK) todas as chaves que casam com `pattern`, de forma incremental.
/// Retorna quantas chaves satisfizeram `count`.
pub(crate) async fn delete_matching(
    conn: &mut redis::aio::ConnectionManager,
    pattern: &str,
    mut count: impl FnMut(&str) -> bool,
) -> crate::Result<u64> {
    let mut cursor: u64 = 0;
    let mut counted = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(conn)
            .await?;
        if !keys.is_empty() {
            counted += keys.iter().filter(|k| count(k)).count() as u64;
            redis::cmd("UNLINK").arg(&keys).query_async::<u64>(conn).await?;
        }
        if next == 0 {
            return Ok(counted);
        }
        cursor = next;
    }
}

/// Hash estável (FNV-1a) — não muda entre versões do Rust, ao contrário do `DefaultHasher`.
pub(crate) fn stable_hash(parts: &[&str]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for byte in part.bytes().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}
