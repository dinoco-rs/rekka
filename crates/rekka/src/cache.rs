use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::context::{FromContext, RekkaContext};
use crate::error::{RekkaError, Result};
use crate::util::{delete_matching, glob_escape, millis};

/// Máximo de chaves por comando / pipeline nas operações em lote.
const CHUNK: usize = 1000;

/// Um tipo que pode ser guardado no [`Cache`]. Normalmente via `#[derive(RekkaCache)]`.
pub trait RekkaCache: Serialize + DeserializeOwned + Send + 'static {
    /// Namespace das chaves deste tipo.
    const NAMESPACE: &'static str;
    /// `None` = permanente.
    const TTL: Option<Duration>;

    fn cache_key(&self) -> String;

    /// Chave completa a partir do que foi passado para `get`, `get_many`, `contains`, `remove` e
    /// `remove_many`. Com `#[rekka(key = "session:{session_id}")]`, o derive aceita tanto a chave
    /// completa (`"session:42"`) quanto só o valor do campo (`42`). Padrão: o valor como está.
    fn resolve_key(key: impl Display) -> String {
        key.to_string()
    }
}

/// Cache tipado sobre Redis. Clonável e barato; também é um extractor dentro dos handlers.
#[derive(Clone)]
pub struct Cache {
    conn: ConnectionManager,
    prefix: Arc<str>,
}

pub struct CacheBuilder {
    url: String,
    prefix: String,
    connection_timeout: Duration,
    response_timeout: Duration,
}

impl CacheBuilder {
    /// Prefixo das chaves (padrão `rekka`): `<prefix>:cache:<namespace>:<key>`.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Conecta (aguardando a conexão) e retorna o cache pronto.
    pub async fn connect(self) -> Result<Cache> {
        if self.prefix.is_empty() {
            return Err(RekkaError::InvalidConfig("prefix must not be empty".into()));
        }
        let client = redis::Client::open(self.url.as_str())?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connection_timeout))
            .set_response_timeout(Some(self.response_timeout))
            .set_max_delay(Duration::from_secs(2));
        let conn = client.get_connection_manager_with_config(config).await?;
        Ok(Cache { conn, prefix: self.prefix.into() })
    }
}

impl Cache {
    /// Conecta com as opções padrão.
    pub async fn new(redis_url: impl Into<String>) -> Result<Self> {
        Self::builder(redis_url).connect().await
    }

    pub fn builder(redis_url: impl Into<String>) -> CacheBuilder {
        CacheBuilder {
            url: redis_url.into(),
            prefix: "rekka".to_owned(),
            connection_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
        }
    }

    pub(crate) fn from_parts(conn: ConnectionManager, prefix: Arc<str>) -> Self {
        Self { conn, prefix }
    }

    /// Chave no Redis a partir do que o usuário passou (hidratada por [`RekkaCache::resolve_key`]).
    fn key<T: RekkaCache>(&self, key: impl Display) -> Result<String> {
        self.full_key::<T>(T::resolve_key(key))
    }

    /// Chave no Redis a partir de uma chave já completa (`cache_key`).
    fn full_key<T: RekkaCache>(&self, key: String) -> Result<String> {
        if key.is_empty() {
            return Err(RekkaError::InvalidKey);
        }
        Ok(format!("{}:cache:{}:{key}", self.prefix, T::NAMESPACE))
    }

    /// Grava (ou substitui) o item, aplicando o TTL do tipo.
    pub async fn insert<T: RekkaCache>(&self, item: &T) -> Result<()> {
        self.set(item, T::TTL).await
    }

    /// Grava com um TTL específico, ignorando o do tipo.
    pub async fn insert_with_ttl<T: RekkaCache>(&self, item: &T, ttl: Duration) -> Result<()> {
        self.set(item, Some(ttl)).await
    }

    async fn set<T: RekkaCache>(&self, item: &T, ttl: Option<Duration>) -> Result<()> {
        let key = self.full_key::<T>(item.cache_key())?;
        let value = serde_json::to_vec(item)?;
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value);
        if let Some(ttl) = ttl {
            cmd.arg("PX").arg(millis(ttl).max(1));
        }
        cmd.query_async::<()>(&mut self.conn.clone()).await?;
        Ok(())
    }

    /// Grava vários itens usando pipelines (poucas viagens ao Redis).
    pub async fn insert_many<'a, T: RekkaCache>(&self, items: impl IntoIterator<Item = &'a T>) -> Result<()> {
        let ttl = T::TTL.map(|ttl| millis(ttl).max(1));
        let mut conn = self.conn.clone();
        let mut items = items.into_iter().peekable();
        while items.peek().is_some() {
            let mut pipe = redis::pipe();
            for item in items.by_ref().take(CHUNK) {
                let cmd = pipe.cmd("SET").arg(self.full_key::<T>(item.cache_key())?).arg(serde_json::to_vec(item)?);
                if let Some(ttl) = ttl {
                    cmd.arg("PX").arg(ttl);
                }
                cmd.ignore();
            }
            pipe.query_async::<()>(&mut conn).await?;
        }
        Ok(())
    }

    /// Lê um item. Ausente, chave inválida, formato incompatível ou Redis indisponível: `None`
    /// (o erro vai para o log), então o cache nunca derruba quem o consulta.
    pub async fn get<T: RekkaCache>(&self, key: impl Display) -> Option<T> {
        let key = match self.key::<T>(key) {
            Ok(key) => key,
            Err(e) => {
                warn!(namespace = T::NAMESPACE, error = %e, "rekka: cache get failed");
                return None;
            }
        };
        match redis::cmd("GET").arg(&key).query_async::<Option<Vec<u8>>>(&mut self.conn.clone()).await {
            Ok(raw) => raw.and_then(|raw| decode(&key, &raw)),
            Err(e) => {
                warn!(key, error = %e, "rekka: cache get failed");
                None
            }
        }
    }

    /// Resultado na mesma ordem das chaves; `None` para as ausentes.
    pub async fn get_many<T: RekkaCache>(&self, keys: impl IntoIterator<Item = impl Display>) -> Result<Vec<Option<T>>> {
        let keys = keys.into_iter().map(|k| self.key::<T>(k)).collect::<Result<Vec<_>>>()?;
        let mut out = Vec::with_capacity(keys.len());
        let mut conn = self.conn.clone();
        for chunk in keys.chunks(CHUNK) {
            let raw: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg(chunk).query_async(&mut conn).await?;
            out.extend(chunk.iter().zip(raw).map(|(key, raw)| raw.and_then(|raw| decode(key, &raw))));
        }
        Ok(out)
    }

    pub async fn contains<T: RekkaCache>(&self, key: impl Display) -> Result<bool> {
        let key = self.key::<T>(key)?;
        Ok(redis::cmd("EXISTS").arg(key).query_async::<u64>(&mut self.conn.clone()).await? > 0)
    }

    /// Retorna se havia algo para remover.
    pub async fn remove<T: RekkaCache>(&self, key: impl Display) -> Result<bool> {
        let key = self.key::<T>(key)?;
        Ok(redis::cmd("UNLINK").arg(key).query_async::<u64>(&mut self.conn.clone()).await? > 0)
    }

    /// Retorna quantas chaves existiam.
    pub async fn remove_many<T: RekkaCache>(&self, keys: impl IntoIterator<Item = impl Display>) -> Result<u64> {
        let keys = keys.into_iter().map(|k| self.key::<T>(k)).collect::<Result<Vec<_>>>()?;
        let mut removed = 0;
        let mut conn = self.conn.clone();
        for chunk in keys.chunks(CHUNK) {
            removed += redis::cmd("UNLINK").arg(chunk).query_async::<u64>(&mut conn).await?;
        }
        Ok(removed)
    }

    /// Remove todas as entradas do tipo `T` (varredura incremental, não bloqueia o Redis).
    pub async fn clear<T: RekkaCache>(&self) -> Result<u64> {
        let pattern = format!("{}:cache:{}:*", glob_escape(&self.prefix), glob_escape(T::NAMESPACE));
        delete_matching(&mut self.conn.clone(), &pattern, |_| true).await
    }
}

/// Entrada com formato incompatível (ex.: a struct mudou) é tratada como ausente.
fn decode<T: RekkaCache>(key: &str, raw: &[u8]) -> Option<T> {
    match serde_json::from_slice(raw) {
        Ok(value) => Some(value),
        Err(e) => {
            warn!(key, error = %e, "rekka: ignoring cache entry with an incompatible format");
            None
        }
    }
}

impl FromContext for Cache {
    #[inline]
    fn from_context(cx: &RekkaContext) -> Result<Self> {
        Ok(cx.client.cache())
    }
}
