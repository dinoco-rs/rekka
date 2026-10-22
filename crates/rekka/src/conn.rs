//! Conexão compartilhada com o Redis, feita para sobreviver a quedas.
//!
//! - Com o Redis fora do ar, cada chamada falha na hora (uma única tentativa de reconexão, sem
//!   a série de tentativas do `ConnectionManager`); a próxima chamada tenta reconectar de novo.
//!   Os loops internos já têm backoff, e o código do usuário recebe o erro sem ficar preso.
//! - Uma conexão que parou de responder sem fechar (failover para outro IP, partição de rede)
//!   só daria timeout até o sistema operacional desistir do socket, o que leva minutos. Depois
//!   de alguns timeouts seguidos, ela é descartada e uma nova é aberta.
//! - Quem precisa reagir à volta do Redis (reconcile, buffer local) assina [`RedisConn::recoveries`].

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use redis::{Cmd, Pipeline, RedisFuture, RedisResult, Value};
use tokio::sync::watch;
use tracing::warn;

/// Timeouts seguidos que fazem a conexão atual ser trocada por uma nova.
const TIMEOUTS_BEFORE_RESET: u32 = 2;
/// Tentativas de conexão no boot (o Redis pode estar subindo junto com o serviço).
const BOOT_RETRIES: u32 = 6;

#[derive(Clone)]
pub(crate) struct RedisConn {
    slot: Arc<Slot>,
}

struct Slot {
    client: redis::Client,
    config: ConnectionManagerConfig,
    /// Conexão atual e sua geração (incrementada a cada troca).
    current: RwLock<(u64, ConnectionManager)>,
    timeouts: AtomicU32,
    /// Uma chamada falhou por falta de conexão e nenhuma respondeu desde então.
    down: AtomicBool,
    /// Incrementado cada vez que o Redis volta a responder depois de `down`.
    recoveries: watch::Sender<u64>,
}

impl RedisConn {
    /// Abre a conexão e espera ela ficar pronta. Com o Redis inacessível por alguns segundos,
    /// falha: o serviço não sobe sem Redis, mas depois de no ar sobrevive às quedas.
    pub(crate) async fn connect(
        client: redis::Client,
        connection_timeout: Duration,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(connection_timeout))
            .set_response_timeout(Some(response_timeout))
            .set_number_of_retries(0);
        let mut delay = Duration::from_millis(100);
        let mut attempt = 0;
        let conn = loop {
            match ConnectionManager::new_with_config(client.clone(), config.clone()).await {
                Ok(conn) => break conn,
                Err(e) if e.is_io_error() && attempt < BOOT_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        };
        let slot = Slot {
            client,
            config,
            current: RwLock::new((0, conn)),
            timeouts: AtomicU32::new(0),
            down: AtomicBool::new(false),
            recoveries: watch::channel(0).0,
        };
        Ok(Self { slot: Arc::new(slot) })
    }

    /// Avisa cada vez que o Redis volta a responder depois de uma queda percebida por esta
    /// instância (um valor novo por volta).
    pub(crate) fn recoveries(&self) -> watch::Receiver<u64> {
        self.slot.recoveries.subscribe()
    }

    fn current(&self) -> (u64, ConnectionManager) {
        self.slot.current.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn observe<T>(&self, generation: u64, res: &RedisResult<T>) {
        match res {
            Err(e) if e.is_timeout() => {
                if self.slot.timeouts.fetch_add(1, Ordering::Relaxed) + 1 >= TIMEOUTS_BEFORE_RESET {
                    self.reset(generation);
                }
            }
            // Sem conexão: o `ConnectionManager` já reconecta na próxima chamada.
            Err(e) if e.is_io_error() => self.slot.down.store(true, Ordering::Release),
            // O Redis respondeu (mesmo que com erro): a conexão está viva.
            _ => {
                self.slot.timeouts.store(0, Ordering::Relaxed);
                if self.slot.down.swap(false, Ordering::AcqRel) {
                    self.slot.recoveries.send_modify(|n| *n += 1);
                }
            }
        }
    }

    /// Troca a conexão `generation` por uma nova, que conecta na próxima chamada. Se outra
    /// chamada já trocou, não faz nada.
    fn reset(&self, generation: u64) {
        let mut current = self.slot.current.write().unwrap_or_else(PoisonError::into_inner);
        if current.0 != generation {
            return;
        }
        match ConnectionManager::new_lazy_with_config(self.slot.client.clone(), self.slot.config.clone()) {
            Ok(conn) => {
                *current = (generation + 1, conn);
                self.slot.timeouts.store(0, Ordering::Relaxed);
                self.slot.down.store(true, Ordering::Release);
                warn!("rekka: redis stopped answering on the current connection; opening a new one");
            }
            Err(e) => warn!(error = %e, "rekka: failed to open a new redis connection"),
        }
    }
}

impl ConnectionLike for RedisConn {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let (generation, mut conn) = self.current();
            let res = conn.req_packed_command(cmd).await;
            self.observe(generation, &res);
            res
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let (generation, mut conn) = self.current();
            let res = conn.req_packed_commands(pipeline, offset, count).await;
            self.observe(generation, &res);
            res
        })
    }

    fn get_db(&self) -> i64 {
        self.current().1.get_db()
    }
}
