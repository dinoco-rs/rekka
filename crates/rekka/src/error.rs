use std::fmt;

/// Erros produzidos pelo rekka.
#[derive(Debug)]
#[non_exhaustive]
pub enum RekkaError {
    /// Falha de comunicação com o Redis.
    Redis(redis::RedisError),
    /// Falha ao serializar ou desserializar um job.
    Serialization(serde_json::Error),
    /// Um extractor `RekkaState<T>` foi usado sem registrar `T` com `.data(...)`.
    MissingData(&'static str),
    /// Configuração inválida (nome duplicado, valores fora do intervalo, etc).
    InvalidConfig(String),
    /// O `task_id` do job é inválido (vazio).
    InvalidTaskId,
    /// A chave do cache é inválida (vazia).
    InvalidKey,
    /// O encerramento gracioso estourou o `shutdown_timeout`; os jobs abandonados
    /// foram devolvidos para a fila.
    ShutdownTimeout { abandoned: usize },
    /// O Redis respondeu algo inesperado.
    Protocol(String),
}

pub type Result<T, E = RekkaError> = std::result::Result<T, E>;

impl fmt::Display for RekkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Redis(e) => write!(f, "redis error: {e}"),
            Self::Serialization(e) => write!(f, "serialization error: {e}"),
            Self::MissingData(ty) => write!(f, "missing data `{ty}`: register it with `.data(...)`"),
            Self::InvalidConfig(msg) => write!(f, "invalid configuration: {msg}"),
            Self::InvalidTaskId => f.write_str("invalid task_id: it must not be empty"),
            Self::InvalidKey => f.write_str("invalid cache key: it must not be empty"),
            Self::ShutdownTimeout { abandoned } => {
                write!(f, "shutdown timed out; {abandoned} job(s) were returned to their queues")
            }
            Self::Protocol(msg) => write!(f, "unexpected redis response: {msg}"),
        }
    }
}

impl std::error::Error for RekkaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Redis(e) => Some(e),
            Self::Serialization(e) => Some(e),
            _ => None,
        }
    }
}

impl From<redis::RedisError> for RekkaError {
    fn from(e: redis::RedisError) -> Self {
        Self::Redis(e)
    }
}

impl From<serde_json::Error> for RekkaError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e)
    }
}
