//! Limpeza e reparo de estado no Redis.
//!
//! No fluxo normal nada fica para trás: jobs concluídos são apagados na hora, marcas de
//! conclusão e chaves auxiliares têm TTL e estruturas vazias somem sozinhas. O janitor é a
//! rede de segurança para estados inconsistentes (intervenção manual, restore parcial, bug):
//! repara o que ainda dá para executar e apaga o que é lixo. Roda de forma incremental, em uma
//! instância por vez para cada conjunto de filas.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};

use crate::client::RekkaClient;
use crate::error::Result;
use crate::keys::{QueueKeys, validate_name};
use crate::scripts;
use crate::util::{delete_matching, glob_escape};

const SCAN_COUNT: usize = 500;
const ZSCAN_COUNT: usize = 250;
/// Pausa entre passos: a passada é lenta de propósito para usar só uma fração pequena da CPU
/// do Redis (é uma rede de segurança, não um caminho quente).
const PAUSE: Duration = Duration::from_millis(20);

#[derive(Default)]
struct Report {
    repaired: u64,
    removed: u64,
}

pub(crate) async fn run(client: RekkaClient, queues: Arc<[&'static str]>) -> Result<()> {
    let prefix = &client.shared.prefix;
    let registered: HashMap<&str, QueueKeys> = queues.iter().map(|name| (*name, QueueKeys::new(prefix, name))).collect();
    let mut conn = client.conn();
    let mut report = Report::default();

    // 1. Uma única passada pelo keyspace cobre hashes de jobs e listas de espera de todas as filas.
    let pattern = format!("{}:{{*}}:*", glob_escape(prefix));
    let mut cursor: u64 = 0;
    loop {
        let (next, found): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async(&mut conn)
            .await?;

        let mut jobs: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut groups: HashMap<&str, Vec<&str>> = HashMap::new();
        for key in &found {
            let Some((queue, rest)) = split_queue_key(prefix, key) else { continue };
            if !registered.contains_key(queue) {
                // Filas de outros serviços: não sabemos se estão abandonadas, então não mexemos.
                continue;
            }
            if let Some(id) = rest.strip_prefix("j:") {
                jobs.entry(queue).or_default().push(id);
            } else if let Some(k) = rest.strip_prefix("g:") {
                groups.entry(queue).or_default().push(k);
            }
        }
        for (queue, ids) in jobs {
            let mut inv = registered[queue].invocation(&scripts::JANITOR_JOBS_SCRIPT);
            for id in ids {
                inv.arg(id);
            }
            let (repaired, removed): (u64, u64) = inv.invoke_async(&mut conn).await?;
            report.repaired += repaired;
            report.removed += removed;
        }
        for (queue, ks) in groups {
            let mut inv = registered[queue].invocation(&scripts::JANITOR_GROUPS_SCRIPT);
            for k in ks {
                inv.arg(k);
            }
            report.repaired += inv.invoke_async::<u64>(&mut conn).await?;
        }

        if next == 0 {
            break;
        }
        cursor = next;
        tokio::time::sleep(PAUSE).await;
    }

    // 2. Índices de cada fila: entradas que não apontam para um job no estado esperado.
    for keys in registered.values() {
        for which in ["q", "a"] {
            let mut cursor = "0".to_owned();
            loop {
                let mut inv = keys.invocation(&scripts::JANITOR_INDEX_SCRIPT);
                inv.arg(which).arg(&cursor).arg(ZSCAN_COUNT);
                let (next, removed): (String, u64) = inv.invoke_async(&mut conn).await?;
                report.removed += removed;
                if next == "0" {
                    break;
                }
                cursor = next;
                tokio::time::sleep(PAUSE).await;
            }
        }
    }

    if report.repaired > 0 || report.removed > 0 {
        info!(repaired = report.repaired, removed = report.removed, "rekka: janitor cleaned up inconsistent state");
    } else {
        debug!("rekka: janitor found nothing to clean");
    }
    Ok(())
}

/// `prefix:{fila}:resto` -> `(fila, resto)`.
fn split_queue_key<'a>(prefix: &str, key: &'a str) -> Option<(&'a str, &'a str)> {
    let rest = key.strip_prefix(prefix)?.strip_prefix(":{")?;
    let end = rest.find('}')?;
    Some((&rest[..end], rest[end + 1..].strip_prefix(':')?))
}

/// Apaga todos os dados de uma fila. Retorna quantos jobs existiam.
pub(crate) async fn purge(client: &RekkaClient, queue: &str) -> Result<u64> {
    validate_name("queue", queue).map_err(crate::RekkaError::InvalidConfig)?;
    let keys = QueueKeys::new(&client.shared.prefix, queue);
    let pattern = format!("{}*", glob_escape(&keys.base));
    let jobs_prefix = format!("{}j:", keys.base);
    delete_matching(&mut client.conn(), &pattern, |key| key.starts_with(&jobs_prefix)).await
}

#[cfg(test)]
mod tests {
    use super::split_queue_key;

    #[test]
    fn splits_queue_keys() {
        assert_eq!(split_queue_key("rekka", "rekka:{mail}:j:a:b}"), Some(("mail", "j:a:b}")));
        assert_eq!(split_queue_key("rekka", "rekka:{mail}:queue"), Some(("mail", "queue")));
        assert_eq!(split_queue_key("rekka", "rekka:cache:x"), None);
        assert_eq!(split_queue_key("rekka", "other:{mail}:queue"), None);
    }
}
