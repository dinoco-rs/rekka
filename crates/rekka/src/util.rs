use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt;
use redis::{RedisError, RetryMethod};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub(crate) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Erros que tendem a sumir sozinhos (rede, reconexão, timeout).
pub(crate) fn is_transient(e: &RedisError) -> bool {
    e.is_io_error() || !matches!(e.retry_method(), RetryMethod::NoRetry)
}

pub(crate) fn panic_message(panic: Box<dyn Any + Send>) -> String {
    match panic_text(&panic) {
        Some(s) => format!("job panicked: {s}"),
        None => "job panicked".to_owned(),
    }
}

fn panic_text(panic: &Box<dyn Any + Send>) -> Option<&str> {
    panic.downcast_ref::<&'static str>().copied().or_else(|| panic.downcast_ref::<String>().map(String::as_str))
}

/// Executa um loop interno da lib e o reinicia se ele entrar em pânico (bug), com backoff.
/// Termina quando o loop retorna normalmente ou quando `token` é cancelado.
pub(crate) async fn supervise<Fut>(kind: &str, name: &str, token: &CancellationToken, mut start: impl FnMut() -> Fut)
where
    Fut: Future<Output = ()>,
{
    let mut backoff = Backoff::new();
    loop {
        let Err(panic) = AssertUnwindSafe(start()).catch_unwind().await else { return };
        error!(kind, name, panic = panic_text(&panic).unwrap_or("<non-string panic>"), "rekka: internal task crashed; restarting");
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

/// `size_of` do valor que `f` devolveria, sem chamá-lo: mede o future de um `async fn` sem criá-lo.
pub(crate) fn output_size<A, R>(_: impl FnOnce(A) -> R) -> usize {
    size_of::<R>()
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
    conn: &mut crate::conn::RedisConn,
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
