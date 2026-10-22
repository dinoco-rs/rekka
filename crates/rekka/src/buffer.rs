//! Buffer local dos enqueues feitos com o Redis fora do ar.
//!
//! `immediate`, `schedule`, `schedule_in` e `immediate_many` que falham por falta de conexão
//! gravam os jobs num arquivo (um JSON por linha) e retornam [`Enqueued::Buffered`](crate::Enqueued).
//! Uma task envia o arquivo, na ordem, assim que o Redis responde. Enquanto houver algo pendente,
//! enqueues novos também vão para o arquivo, para uma versão antiga de um job nunca sobrescrever
//! uma nova. Nada do buffer executa localmente: o Redis continua sendo a fonte da verdade e os
//! workers só executam o que pegam de lá. O reconcile não usa o buffer (roda de novo quando o
//! Redis volta).
//!
//! Cada instância trava (`File::try_lock`) um arquivo `<prefix>.<n>.jsonl` do diretório. Um
//! arquivo destravado é de um processo que morreu: a próxima instância a subir fica com ele, e
//! as instâncias vivas também o enviam (verificação periódica e a cada reconexão). Os bytes já
//! enviados ficam em `<prefix>.<n>.sent`, para um restart no meio do envio não reenviar tudo.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::client::{ENQUEUE_CHUNK, send_jobs};
use crate::conn::RedisConn;
use crate::keys::QueueKeys;
use crate::util::Backoff;

/// Intervalo da verificação de arquivos de instâncias que morreram.
const ORPHAN_SCAN: Duration = Duration::from_secs(60);
/// Arquivos por prefixo (instâncias no mesmo diretório ao mesmo tempo).
const MAX_SLOTS: usize = 1024;

/// Um job no buffer, já no formato do script de enqueue.
#[derive(Serialize, Deserialize)]
pub(crate) struct Entry {
    /// Fila.
    pub(crate) q: String,
    pub(crate) id: String,
    /// Payload (JSON do job).
    pub(crate) p: String,
    /// `+0` (imediato) ou o instante absoluto em ms.
    pub(crate) due: String,
    /// Key de concorrência (vazia sem `concurrency_by_key`).
    pub(crate) k: String,
}

pub(crate) struct Buffer {
    dir: PathBuf,
    stem: String,
    own: Mutex<Slot>,
    /// Acorda a task de envio quando algo é gravado.
    wake: Notify,
}

/// Um arquivo travado por esta instância.
struct Slot {
    path: PathBuf,
    file: File,
    /// Bytes já enviados ao Redis.
    sent: u64,
    /// Tamanho do arquivo.
    len: u64,
}

impl Slot {
    fn open(path: PathBuf) -> io::Result<Option<Self>> {
        let file = OpenOptions::new().read(true).append(true).create(true).open(&path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(e)) => return Err(e),
        }
        let len = file.metadata()?.len();
        let sent = fs::read_to_string(sent_path(&path)).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        Ok(Some(Self { path, file, sent: u64::min(sent, len), len }))
    }

    fn pending(&self) -> bool {
        self.len > self.sent
    }

    fn write(&mut self, entries: &[Entry]) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut buf, entry).map_err(io::Error::other)?;
            buf.push(b'\n');
        }
        let res = self.file.write_all(&buf).and_then(|()| self.file.sync_data());
        self.len = self.file.metadata()?.len();
        res
    }

    fn mark_sent(&mut self, sent: u64) -> io::Result<()> {
        self.sent = sent;
        // Se tudo foi enviado, o arquivo volta a ficar vazio.
        if self.sent >= self.len {
            self.file.set_len(0)?;
            self.sent = 0;
            self.len = 0;
        }
        let tmp = self.path.with_extension("sent.tmp");
        fs::write(&tmp, self.sent.to_string())?;
        fs::rename(tmp, sent_path(&self.path))
    }
}

fn sent_path(path: &Path) -> PathBuf {
    path.with_extension("sent")
}

/// Prefixo usável como nome de arquivo.
fn file_stem(prefix: &str) -> String {
    prefix.chars().map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') { c } else { '_' }).collect()
}

impl Buffer {
    /// Cria o diretório e trava o primeiro arquivo livre do prefixo.
    pub(crate) fn open(dir: &Path, prefix: &str) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let stem = file_stem(prefix);
        for n in 0..MAX_SLOTS {
            if let Some(slot) = Slot::open(dir.join(format!("{stem}.{n}.jsonl")))? {
                return Ok(Self { dir: dir.to_owned(), stem, own: Mutex::new(slot), wake: Notify::new() });
            }
        }
        Err(io::Error::other(format!("all {MAX_SLOTS} buffer files for `{prefix}` are in use")))
    }

    fn own(&self) -> MutexGuard<'_, Slot> {
        self.own.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Grava as entradas se ainda houver algo pendente (mantém a ordem). Retorna se gravou.
    pub(crate) fn append_if_pending(&self, entries: impl FnOnce() -> Vec<Entry>) -> bool {
        let mut own = self.own();
        if !own.pending() {
            return false;
        }
        match own.write(&entries()) {
            Ok(()) => {
                self.wake.notify_one();
                true
            }
            Err(e) => {
                warn!(path = %own.path.display(), error = %e, "rekka: failed to write the offline buffer");
                false
            }
        }
    }

    /// Grava as entradas depois de o Redis falhar.
    pub(crate) fn append(&self, entries: &[Entry]) -> io::Result<()> {
        let mut own = self.own();
        let first = !own.pending();
        own.write(entries)?;
        if first {
            warn!(path = %own.path.display(), "rekka: redis unreachable; saving enqueued jobs to the offline buffer");
        }
        self.wake.notify_one();
        Ok(())
    }

    /// Envia o que estiver pendente no arquivo próprio e, com `orphans`, nos arquivos de
    /// instâncias que morreram. Para no primeiro erro (tenta de novo depois).
    async fn flush(&self, conn: &RedisConn, prefix: &str, orphans: bool) -> crate::Result<()> {
        loop {
            let (path, from, to) = {
                let own = self.own();
                if !own.pending() {
                    break;
                }
                (own.path.clone(), own.sent, own.len)
            };
            send_file(conn, prefix, &path, from, to, |sent| self.own().mark_sent(sent)).await?;
        }
        if orphans {
            for path in self.orphans().map_err(io_error)? {
                let Some(mut slot) = Slot::open(path).map_err(io_error)? else { continue };
                if slot.pending() {
                    let (path, from, to) = (slot.path.clone(), slot.sent, slot.len);
                    send_file(conn, prefix, &path, from, to, |sent| slot.mark_sent(sent)).await?;
                }
            }
        }
        Ok(())
    }

    /// Outros arquivos do prefixo neste diretório.
    fn orphans(&self) -> io::Result<Vec<PathBuf>> {
        let own = self.own().path.clone();
        let start = format!("{}.", self.stem);
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            let slot = name.strip_prefix(&start).and_then(|rest| rest.strip_suffix(".jsonl"));
            if path != own && slot.is_some_and(|n| n.parse::<usize>().is_ok()) {
                out.push(path);
            }
        }
        Ok(out)
    }
}

/// Envia as linhas completas entre `from` e `to`, em ordem, agrupando jobs seguidos da mesma
/// fila. Depois de cada chamada aceita, `mark_sent` registra até onde foi.
async fn send_file(
    conn: &RedisConn,
    prefix: &str,
    path: &Path,
    from: u64,
    to: u64,
    mut mark_sent: impl FnMut(u64) -> io::Result<()>,
) -> crate::Result<()> {
    let mut raw = Vec::new();
    let mut file = File::open(path).map_err(io_error)?;
    file.seek(SeekFrom::Start(from)).map_err(io_error)?;
    file.take(to - from).read_to_end(&mut raw).map_err(io_error)?;

    let mut entries = Vec::new();
    let mut offset = from;
    for line in raw.split_inclusive(|b| *b == b'\n') {
        offset += line.len() as u64;
        if line.last() != Some(&b'\n') {
            break;
        }
        match serde_json::from_slice::<Entry>(line) {
            Ok(entry) => entries.push((entry, offset)),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "rekka: skipping an unreadable line in the offline buffer")
            }
        }
    }

    let mut conn = conn.clone();
    let mut rest = entries.as_slice();
    let mut sent = 0;
    while let Some((first, _)) = rest.first() {
        let n = rest.iter().take(ENQUEUE_CHUNK).take_while(|(e, _)| e.q == first.q).count();
        let (chunk, tail) = rest.split_at(n);
        let keys = QueueKeys::new(prefix, &first.q);
        let jobs = chunk.iter().map(|(e, _)| (e.id.as_str(), e.p.as_str(), e.due.as_str(), e.k.as_str()));
        send_jobs(&mut conn, &keys, "u", jobs).await?;
        mark_sent(chunk[n - 1].1).map_err(io_error)?;
        sent += n;
        rest = tail;
    }
    if entries.is_empty() {
        // Só linhas ilegíveis: descarta para não tentar de novo para sempre.
        mark_sent(to).map_err(io_error)?;
    }
    if sent > 0 {
        info!(path = %path.display(), jobs = sent, "rekka: sent buffered jobs to redis");
    }
    Ok(())
}

fn io_error(e: io::Error) -> crate::RekkaError {
    crate::RekkaError::Buffer(e)
}

/// Task que envia o buffer ao Redis: na subida (sobras de um processo anterior), a cada gravação
/// e a cada reconexão, com backoff enquanto o Redis não responde.
pub(crate) async fn sync_loop(buffer: Arc<Buffer>, conn: RedisConn, prefix: Arc<str>, token: CancellationToken) {
    let mut recoveries = conn.recoveries();
    let mut backoff = Backoff::new();
    let mut failing = false;
    let mut next_scan = Instant::now();
    loop {
        let scan = Instant::now() >= next_scan;
        match buffer.flush(&conn, &prefix, scan).await {
            Ok(()) => {
                backoff.reset();
                failing = false;
                if scan {
                    next_scan = Instant::now() + ORPHAN_SCAN;
                }
            }
            Err(e) => {
                if !failing {
                    warn!(error = %e, "rekka: failed to send the offline buffer; retrying");
                }
                failing = true;
            }
        }
        let wait = if failing { backoff.next_delay() } else { next_scan.saturating_duration_since(Instant::now()) };
        tokio::select! {
            _ = token.cancelled() => break,
            _ = buffer.wake.notified(), if !failing => {}
            Ok(()) = recoveries.changed() => next_scan = Instant::now(),
            _ = sleep(wait) => {}
        }
    }
    // Encerrando: última tentativa. O que sobrar fica no arquivo e vai na próxima subida.
    if let Err(e) = buffer.flush(&conn, &prefix, false).await {
        let path = buffer.own().path.clone();
        warn!(path = %path.display(), error = %e, "rekka: buffered jobs kept on disk; they are sent on the next start");
    }
}
