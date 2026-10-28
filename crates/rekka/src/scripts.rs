//! Scripts Lua executados atomicamente no Redis.
//!
//! Modelo de dados de uma fila (todas as chaves compartilham a hash tag `{fila}`,
//! então caem no mesmo slot em Redis Cluster):
//!
//! - `P:{q}:queue`  ZSET  id -> instante (ms) em que o job fica disponível
//! - `P:{q}:active` ZSET  id -> prazo da lease (ms) do job em execução
//! - `P:{q}:locks`  HASH  key de concorrência -> id que detém (ou tem reservada) a key
//! - `P:{q}:j:<id>` HASH  p=payload a=tentativas s=estado(w|r|g) k=key rm=removido;
//!   np/nd/nk = próxima versão (enfileirada durante a execução)
//! - `P:{q}:g:<key>` LIST jobs aguardando a key ser liberada
//! - `P:{q}:d:<id>`  STR  marca de "concluído recentemente" (TTL), usada pelo reconcile
//!
//! O passo `n` (a partir de 1) de um workflow cuja fila de entrada é `q` é uma fila com o mesmo
//! modelo sob `P:{q}:n:` (`P:{q}:n:queue`, `P:{q}:n:j:<id>`, ...): mesma hash tag, então
//! `ADVANCE` conclui um passo e enfileira o próximo num único script.
//!
//! O relógio é sempre o do Redis (`TIME`), então servidores com relógios diferentes
//! não afetam agendamentos, leases e retries.

use std::sync::LazyLock;

use redis::Script;

const COMPAT: &str = r"
if redis.replicate_commands then redis.replicate_commands() end
";

const QUEUE_PRELUDE: &str = r"
local Q, A, L = KEYS[1], KEYS[2], KEYS[3]
local PFX = ARGV[1]
local T = redis.call('TIME')
local NOW = tonumber(T[1]) * 1000 + math.floor(tonumber(T[2]) / 1000)
local NOTIFIED = false

local function jkey(id) return PFX .. 'j:' .. id end
local function gkey(k) return PFX .. 'g:' .. k end

local function notify()
  if not NOTIFIED then
    NOTIFIED = true
    redis.call('PUBLISH', PFX .. 'notify', '')
  end
end

local function push_waiting(id, jk, due)
  redis.call('HSET', jk, 's', 'w')
  redis.call('ZADD', Q, due, id)
  if due <= NOW then
    notify()
  else
    local head = redis.call('ZRANGE', Q, 0, 0)
    if head[1] == id then notify() end
  end
end

-- Libera a key: entrega (reserva) para o próximo job estacionado, se houver.
local function release_key(k)
  local g = gkey(k)
  while true do
    local nxt = redis.call('LPOP', g)
    if not nxt then
      redis.call('HDEL', L, k)
      return
    end
    local njk = jkey(nxt)
    if redis.call('HGET', njk, 's') == 'g' then
      redis.call('HSET', L, k, nxt)
      push_waiting(nxt, njk, NOW)
      return
    end
  end
end

local function release_group(id, jk)
  local k = redis.call('HGET', jk, 'k')
  if k and redis.call('HGET', L, k) == id then release_key(k) end
end

-- Se o job foi enfileirado de novo durante a execução, a nova versão substitui a atual.
local function apply_rerun(id, jk)
  local f = redis.call('HMGET', jk, 'np', 'nd', 'nk')
  if not f[1] then return false end
  redis.call('DEL', jk)
  if f[3] and f[3] ~= '' then
    redis.call('HSET', jk, 'p', f[1], 'a', 0, 'k', f[3])
  else
    redis.call('HSET', jk, 'p', f[1], 'a', 0)
  end
  push_waiting(id, jk, tonumber(f[2]))
  return true
end

local function finish(id, jk, tomb)
  release_group(id, jk)
  redis.call('ZREM', A, id)
  if apply_rerun(id, jk) then return 'rerun' end
  redis.call('DEL', jk)
  if tomb > 0 then redis.call('SET', PFX .. 'd:' .. id, '1', 'PX', tomb) end
  return 'done'
end

local function retry(id, jk, due)
  release_group(id, jk)
  redis.call('ZREM', A, id)
  if apply_rerun(id, jk) then return 'rerun' end
  push_waiting(id, jk, due)
  return 'retry'
end

local function discard(id, jk)
  release_group(id, jk)
  redis.call('ZREM', A, id)
  redis.call('DEL', jk)
  return 'removed'
end

-- mode: u=upsert | n=só cria se não existir nem tiver concluído há pouco.
-- Retorna created | updated | pending | exists | completed.
local function upsert(id, p, due, k, mode)
  local jk = jkey(id)
  local dk = PFX .. 'd:' .. id
  local s = redis.call('HGET', jk, 's')
  if mode == 'n' then
    if s then return 'exists' end
    if redis.call('EXISTS', dk) == 1 then return 'completed' end
  else
    redis.call('DEL', dk)
  end
  if not s then
    if k ~= '' then
      redis.call('HSET', jk, 'p', p, 'a', 0, 'k', k)
    else
      redis.call('HSET', jk, 'p', p, 'a', 0)
    end
    push_waiting(id, jk, due)
    return 'created'
  end
  if s == 'r' then
    redis.call('HSET', jk, 'np', p, 'nd', due, 'nk', k)
    redis.call('HDEL', jk, 'rm')
    return 'pending'
  end
  if s == 'g' then
    local old = redis.call('HGET', jk, 'k')
    if old then redis.call('LREM', gkey(old), 1, id) end
  else
    release_group(id, jk)
  end
  redis.call('HSET', jk, 'p', p, 'a', 0)
  if k ~= '' then redis.call('HSET', jk, 'k', k) else redis.call('HDEL', jk, 'k') end
  push_waiting(id, jk, due)
  return 'updated'
end
";

/// ARGV: prefix, max, lease_ms, keyed(0|1)
/// Retorna `{delay_ms_até_o_próximo (-1 = vazio), {{id, payload, attempt, key}, ...}}`.
const CLAIM: &str = r"
local max = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local keyed = ARGV[4] == '1'
local out = {}
local n = 0
local budget = max * 8 + 32
while n < max and budget > 0 do
  local ids = redis.call('ZRANGEBYSCORE', Q, '-inf', NOW, 'LIMIT', 0, max - n)
  if #ids == 0 then break end
  for i = 1, #ids do
    local id = ids[i]
    budget = budget - 1
    redis.call('ZREM', Q, id)
    local jk = jkey(id)
    local f = redis.call('HMGET', jk, 'p', 'k', 's')
    if f[1] and f[3] == 'w' then
      local k = f[2]
      local go = true
      if keyed and k then
        local holder = redis.call('HGET', L, k)
        if not holder then
          redis.call('HSET', L, k, id)
        elseif holder ~= id then
          redis.call('HSET', jk, 's', 'g')
          redis.call('RPUSH', gkey(k), id)
          go = false
        end
      end
      if go then
        local a = redis.call('HINCRBY', jk, 'a', 1)
        redis.call('HSET', jk, 's', 'r')
        redis.call('ZADD', A, NOW + lease, id)
        n = n + 1
        out[n] = {id, f[1], a, k or ''}
      end
    end
  end
end
local delay = -1
local head = redis.call('ZRANGE', Q, 0, 0, 'WITHSCORES')
if head[2] then delay = math.max(0, tonumber(head[2]) - NOW) end
return {delay, out}
";

/// ARGV: prefix, id, attempt, key. Retorna 1 = pode executar, 2 = estacionado, 0 = obsoleto.
const BIND: &str = r"
local id = ARGV[2]
local k = ARGV[4]
local jk = jkey(id)
local f = redis.call('HMGET', jk, 's', 'a')
if f[1] ~= 'r' or tonumber(f[2]) ~= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', jk, 'k', k)
local holder = redis.call('HGET', L, k)
if (not holder) or holder == id then
  redis.call('HSET', L, k, id)
  return 1
end
redis.call('ZREM', A, id)
redis.call('HINCRBY', jk, 'a', -1)
redis.call('HSET', jk, 's', 'g')
redis.call('RPUSH', gkey(k), id)
return 2
";

/// ARGV: prefix, id, attempt, action(f=finalizar | r=retry em ms absoluto | d=retry após ms), valor, tomb_ms
const COMPLETE: &str = r"
local id = ARGV[2]
local jk = jkey(id)
local f = redis.call('HMGET', jk, 's', 'a', 'rm')
if f[1] ~= 'r' or tonumber(f[2]) ~= tonumber(ARGV[3]) then return 'stale' end
if f[3] then return discard(id, jk) end
local act = ARGV[4]
if act == 'f' then return finish(id, jk, tonumber(ARGV[6])) end
local due = tonumber(ARGV[5])
if act == 'd' then due = NOW + due end
return retry(id, jk, due)
";

/// Devolve para a fila, sem contar tentativa, jobs pegos que não chegaram a executar (o
/// encerramento começou durante o claim). ARGV: prefix, (id, attempt)... Retorna quantos voltaram.
const RELEASE: &str = r"
local n = 0
for i = 2, #ARGV, 2 do
  local id = ARGV[i]
  local jk = jkey(id)
  local f = redis.call('HMGET', jk, 's', 'a', 'rm')
  if f[1] == 'r' and tonumber(f[2]) == tonumber(ARGV[i + 1]) then
    if f[3] then
      discard(id, jk)
    else
      redis.call('HINCRBY', jk, 'a', -1)
      retry(id, jk, NOW)
      n = n + 1
    end
  end
end
return n
";

/// ARGV: prefix, lease_ms, (id, attempt)...
const EXTEND: &str = r"
local lease = tonumber(ARGV[2])
local n = 0
for i = 3, #ARGV, 2 do
  local id = ARGV[i]
  local f = redis.call('HMGET', jkey(id), 's', 'a')
  if f[1] == 'r' and tonumber(f[2]) == tonumber(ARGV[i + 1]) then
    redis.call('ZADD', A, 'XX', NOW + lease, id)
    n = n + 1
  end
end
return n
";

/// Recupera jobs cuja lease expirou (worker morto) e libera keys órfãs.
/// ARGV: prefix, limit, max_attempts(0 = ilimitado), tomb_ms
/// Retorna `{{id, kind(stalled|dead|discarded), attempt, payload}, ...}`.
const REAP: &str = r"
local limit = tonumber(ARGV[2])
local maxa = tonumber(ARGV[3])
local tomb = tonumber(ARGV[4])
local out = {}
local ids = redis.call('ZRANGEBYSCORE', A, '-inf', NOW, 'LIMIT', 0, limit)
for i = 1, #ids do
  local id = ids[i]
  local jk = jkey(id)
  local f = redis.call('HMGET', jk, 's', 'a', 'rm', 'p')
  if f[1] ~= 'r' then
    redis.call('ZREM', A, id)
  else
    local a = tonumber(f[2])
    local kind
    if f[3] then
      discard(id, jk)
      kind = 'discarded'
    elseif maxa > 0 and a >= maxa then
      kind = (finish(id, jk, tomb) == 'rerun') and 'stalled' or 'dead'
    else
      retry(id, jk, NOW)
      kind = 'stalled'
    end
    out[#out + 1] = {id, kind, a, f[4]}
  end
end
local ckey = PFX .. 'lcur'
local cur = redis.call('GET', ckey) or '0'
local scan = redis.call('HSCAN', L, cur, 'COUNT', 64)
if scan[1] ~= cur then
  if scan[1] == '0' then redis.call('DEL', ckey) else redis.call('SET', ckey, scan[1], 'PX', 3600000) end
end
local kv = scan[2]
for i = 1, #kv, 2 do
  local s = redis.call('HGET', jkey(kv[i + 1]), 's')
  if s ~= 'r' and s ~= 'w' then release_key(kv[i]) end
end
return out
";

/// Upsert em lote.
/// ARGV: prefix, mode(u=upsert | n=só cria se não existir nem tiver concluído há pouco),
///       (id, payload, due, key)...  — due: '+<ms>' relativo ao relógio do Redis ou ms absoluto.
/// Retorna um status por job: created | updated | pending | exists | completed.
const ENQUEUE: &str = r"
local mode = ARGV[2]
local res = {}
for i = 3, #ARGV, 4 do
  local d = ARGV[i + 2]
  local due
  if string.sub(d, 1, 1) == '+' then due = NOW + tonumber(string.sub(d, 2)) else due = tonumber(d) end
  res[#res + 1] = upsert(ARGV[i], ARGV[i + 1], due, ARGV[i + 3], mode)
end
return res
";

/// Passo de workflow concluído: finaliza o job no passo atual e enfileira a saída como entrada do
/// próximo passo (mesmo `task_id`, mesma key de concorrência), atomicamente. Os passos de um
/// workflow compartilham a hash tag da fila de entrada, então as chaves dos dois cabem no script.
/// KEYS: queue, active, locks do passo atual, depois os do próximo.
/// ARGV: prefix, id, attempt, prefix_do_próximo, payload, tomb_ms
/// Retorna o mesmo que `COMPLETE` com `f`: done | rerun | removed | stale.
const ADVANCE: &str = r"
local id = ARGV[2]
local jk = jkey(id)
local f = redis.call('HMGET', jk, 's', 'a', 'rm', 'k')
if f[1] ~= 'r' or tonumber(f[2]) ~= tonumber(ARGV[3]) then return 'stale' end
if f[3] then return discard(id, jk) end
local status = finish(id, jk, tonumber(ARGV[6]))
Q, A, L, PFX, NOTIFIED = KEYS[4], KEYS[5], KEYS[6], ARGV[4], false
upsert(id, ARGV[5], NOW, f[4] or '', 'u')
return status
";

/// ARGV: prefix, id. Retorna 0 = não existe, 1 = removido, 2 = em execução (resultado será descartado).
const REMOVE: &str = r"
local id = ARGV[2]
local jk = jkey(id)
local f = redis.call('HMGET', jk, 's', 'k')
local s = f[1]
if not s then return 0 end
if s == 'r' then
  redis.call('HSET', jk, 'rm', 1)
  redis.call('HDEL', jk, 'np', 'nd', 'nk')
  return 2
end
if s == 'g' then
  if f[2] then redis.call('LREM', gkey(f[2]), 1, id) end
else
  redis.call('ZREM', Q, id)
  release_group(id, jk)
end
redis.call('DEL', jk)
return 1
";

/// ARGV: prefix, id. Retorna `{estado|false, tentativas, delay_ms_até_executar|-1}`.
const STATE: &str = r"
local jk = jkey(ARGV[2])
local f = redis.call('HMGET', jk, 's', 'a')
if not f[1] then return {false, 0, -1} end
local delay = -1
if f[1] == 'w' then
  local sc = redis.call('ZSCORE', Q, ARGV[2])
  if sc then delay = math.max(0, tonumber(sc) - NOW) end
end
return {f[1], tonumber(f[2]), delay}
";

/// ARGV: prefix. Retorna `{aguardando, prontos, executando, keys_ocupadas}`.
const STATS: &str = r"
return {redis.call('ZCARD', Q), redis.call('ZCOUNT', Q, '-inf', NOW), redis.call('ZCARD', A), redis.call('HLEN', L)}
";

/// Janitor: confere hashes de jobs encontrados no SCAN. Repara o que dá para recuperar
/// (job fora de qualquer índice volta para a fila) e apaga o que é lixo (hash corrompido).
/// ARGV: prefix, id... Retorna `{reparados, removidos}`.
const JANITOR_JOBS: &str = r"
local fixed, removed = 0, 0
for i = 2, #ARGV do
  local id = ARGV[i]
  local jk = jkey(id)
  if redis.call('EXISTS', jk) == 1 then
    local f = redis.call('HMGET', jk, 's', 'p', 'k')
    local s, k = f[1], f[3]
    if (s ~= 'w' and s ~= 'r' and s ~= 'g') or not f[2] then
      redis.call('ZREM', Q, id)
      redis.call('ZREM', A, id)
      if k and redis.call('HGET', L, k) == id then release_key(k) end
      redis.call('DEL', jk)
      removed = removed + 1
    elseif s == 'w' then
      if not redis.call('ZSCORE', Q, id) then
        push_waiting(id, jk, NOW)
        fixed = fixed + 1
      end
    elseif s == 'r' then
      if not redis.call('ZSCORE', A, id) then
        redis.call('ZADD', A, NOW, id)
        fixed = fixed + 1
      end
    else
      local holder = k and redis.call('HGET', L, k)
      if not holder then
        push_waiting(id, jk, NOW)
        fixed = fixed + 1
      else
        local pos = redis.pcall('LPOS', gkey(k), id)
        if pos == false then
          redis.call('RPUSH', gkey(k), id)
          fixed = fixed + 1
        end
      end
    end
  end
end
return {fixed, removed}
";

/// Janitor: remove entradas de índice (`queue`/`active`) que não apontam para um job no estado
/// esperado. ARGV: prefix, which(q|a), cursor, count. Retorna `{próximo_cursor, removidos}`.
const JANITOR_INDEX: &str = r"
local key, want = Q, 'w'
if ARGV[2] == 'a' then key, want = A, 'r' end
local scan = redis.call('ZSCAN', key, ARGV[3], 'COUNT', tonumber(ARGV[4]))
local items = scan[2]
local removed = 0
for i = 1, #items, 2 do
  local id = items[i]
  if redis.call('HGET', jkey(id), 's') ~= want then
    redis.call('ZREM', key, id)
    removed = removed + 1
  end
end
return {scan[1], removed}
";

/// Janitor: listas de espera de key sem um dono válido são liberadas (o próximo job válido
/// assume a key; entradas obsoletas são descartadas). ARGV: prefix, key... Retorna liberadas.
const JANITOR_GROUPS: &str = r"
local fixed = 0
for i = 2, #ARGV do
  local k = ARGV[i]
  local holder = redis.call('HGET', L, k)
  local s = holder and redis.call('HGET', jkey(holder), 's')
  if s ~= 'r' and s ~= 'w' then
    release_key(k)
    fixed = fixed + 1
  end
end
return fixed
";

const RECURSIVE_PRELUDE: &str = r"
local NEXT, LOCK, RECOVERY = KEYS[1], KEYS[2], KEYS[3]
local T = redis.call('TIME')
local NOW = tonumber(T[1]) * 1000 + math.floor(tonumber(T[2]) / 1000)
";

/// ARGV: token, interval_ms, lease_ms, window_ms. Retorna `{1, interval}` (executar) ou
/// `{0, espera_ms}`. Com `window_ms > 0` (reconcile), uma execução devida por uma reconexão
/// (`REC_RECOVER`) roda antes do horário; ao ser consumida, a marca vira `0` por `window_ms`,
/// para as outras instâncias que perceberam a mesma queda não rodarem de novo.
const REC_CLAIM: &str = r"
local window = tonumber(ARGV[4])
local owed = window > 0 and redis.call('GET', RECOVERY) == '1'
local nx = tonumber(redis.call('GET', NEXT) or '0')
local pttl = redis.call('PTTL', LOCK)
if pttl ~= -2 then
  local w = nx - NOW
  if owed or w <= 0 then w = (pttl > 0) and pttl or 1000 end
  return {0, w}
end
if nx > NOW and not owed then return {0, nx - NOW} end
local interval = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
redis.call('SET', LOCK, ARGV[1], 'PX', lease)
redis.call('SET', NEXT, NOW + interval, 'PX', interval * 2 + lease)
if owed then redis.call('SET', RECOVERY, '0', 'PX', window) end
return {1, interval}
";

/// O Redis voltou depois de uma queda: marca uma execução devida, a menos que já exista uma
/// marca (devida, ou `0` de uma execução já feita por esta queda). ARGV: keep_ms. Retorna 1 se marcou.
const REC_RECOVER: &str = r"
if redis.call('EXISTS', RECOVERY) == 1 then return 0 end
redis.call('SET', RECOVERY, '1', 'PX', ARGV[1])
return 1
";

/// ARGV: token, lease_ms, delay_ms. Retorna 1 = renovado, 2 = tinha expirado e foi retomado,
/// 0 = outra instância detém o lock. Com `delay_ms > 0`, mantém a próxima execução pelo menos
/// `delay_ms` à frente enquanto esta executa: se o lock se perder (Redis fora por mais que a
/// lease), nenhuma outra instância começa antes de `delay_ms` depois do último sinal de vida.
const REC_EXTEND: &str = r"
local cur = redis.call('GET', LOCK)
local res = 0
if cur == ARGV[1] then
  redis.call('PEXPIRE', LOCK, ARGV[2])
  res = 1
elseif not cur then
  redis.call('SET', LOCK, ARGV[1], 'PX', ARGV[2])
  res = 2
else
  return 0
end
local delay = tonumber(ARGV[3])
if delay > 0 and NOW + delay > tonumber(redis.call('GET', NEXT) or '0') then
  redis.call('SET', NEXT, NOW + delay, 'PX', delay * 2 + tonumber(ARGV[2]))
end
return res
";

/// Retorna `{próxima_execução_ms (-1 = sem agendamento), agora_ms, pttl_do_lock (-2 = livre)}`.
const REC_PEEK: &str = r"
local nx = redis.call('GET', NEXT)
return {nx and tonumber(nx) or -1, NOW, redis.call('PTTL', LOCK)}
";

/// `cron_time`: se o agendamento ainda é `expected` (lido no `REC_PEEK`), está vencido e o lock
/// está livre, pega o lock e agenda `next`. Com `expected = -1`, só cria o agendamento (a
/// primeira execução é no próximo instante da expressão, nunca na subida).
/// ARGV: token, expected_ms, next_ms, lease_ms. Retorna 1 (executar) ou 0.
const REC_CRON_CLAIM: &str = r"
local expected = tonumber(ARGV[2])
local nx = tonumber(redis.call('GET', NEXT) or '-1')
if nx ~= expected then return 0 end
local nxt = tonumber(ARGV[3])
local lease = tonumber(ARGV[4])
local keep = math.max(nxt - NOW, 1) * 2 + lease
if expected == -1 then
  redis.call('SET', NEXT, nxt, 'PX', keep)
  return 0
end
if nx > NOW or redis.call('EXISTS', LOCK) == 1 then return 0 end
redis.call('SET', LOCK, ARGV[1], 'PX', lease)
redis.call('SET', NEXT, nxt, 'PX', keep)
return 1
";

/// ARGV: token, delay_ms, lease_ms. Com `delay_ms > 0`, agenda a próxima execução para
/// `agora + delay_ms`, exceto se outra instância detém o lock (ela agenda ao terminar).
const REC_RELEASE: &str = r"
local cur = redis.call('GET', LOCK)
if cur == ARGV[1] then redis.call('DEL', LOCK) end
local delay = tonumber(ARGV[2])
if delay > 0 and (cur == ARGV[1] or not cur) then
  redis.call('SET', NEXT, NOW + delay, 'PX', delay * 2 + tonumber(ARGV[3]))
end
return 1
";

/// Empurra a próxima execução para pelo menos `agora + interval`.
/// ARGV: interval_ms, lease_ms, only_if_absent(0|1) — com 1, não altera um agendamento existente.
const REC_BUMP: &str = r"
local interval = tonumber(ARGV[1])
if ARGV[3] == '1' and redis.call('EXISTS', NEXT) == 1 then return 0 end
local nx = tonumber(redis.call('GET', NEXT) or '0')
local want = NOW + interval
if want > nx then redis.call('SET', NEXT, want, 'PX', interval * 2 + tonumber(ARGV[2])) end
return 1
";

fn queue_script(body: &str) -> Script {
    Script::new(&[COMPAT, QUEUE_PRELUDE, body].concat())
}

fn recursive_script(body: &str) -> Script {
    Script::new(&[COMPAT, RECURSIVE_PRELUDE, body].concat())
}

pub(crate) static CLAIM_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(CLAIM));
pub(crate) static BIND_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(BIND));
pub(crate) static COMPLETE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(COMPLETE));
pub(crate) static ADVANCE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(ADVANCE));
pub(crate) static EXTEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(EXTEND));
pub(crate) static RELEASE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(RELEASE));
pub(crate) static REAP_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(REAP));
pub(crate) static ENQUEUE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(ENQUEUE));
pub(crate) static REMOVE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(REMOVE));
pub(crate) static STATE_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(STATE));
pub(crate) static STATS_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(STATS));
pub(crate) static JANITOR_JOBS_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(JANITOR_JOBS));
pub(crate) static JANITOR_INDEX_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(JANITOR_INDEX));
pub(crate) static JANITOR_GROUPS_SCRIPT: LazyLock<Script> = LazyLock::new(|| queue_script(JANITOR_GROUPS));
pub(crate) static REC_CLAIM_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_CLAIM));
pub(crate) static REC_EXTEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_EXTEND));
pub(crate) static REC_RELEASE_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_RELEASE));
pub(crate) static REC_BUMP_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_BUMP));
pub(crate) static REC_PEEK_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_PEEK));
pub(crate) static REC_CRON_CLAIM_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_CRON_CLAIM));
pub(crate) static REC_RECOVER_SCRIPT: LazyLock<Script> = LazyLock::new(|| recursive_script(REC_RECOVER));

/// Todos os scripts, carregados no `start()` para falhar cedo.
pub(crate) fn all() -> [&'static Script; 21] {
    [
        &CLAIM_SCRIPT,
        &BIND_SCRIPT,
        &COMPLETE_SCRIPT,
        &ADVANCE_SCRIPT,
        &EXTEND_SCRIPT,
        &RELEASE_SCRIPT,
        &REAP_SCRIPT,
        &ENQUEUE_SCRIPT,
        &REMOVE_SCRIPT,
        &STATE_SCRIPT,
        &STATS_SCRIPT,
        &JANITOR_JOBS_SCRIPT,
        &JANITOR_INDEX_SCRIPT,
        &JANITOR_GROUPS_SCRIPT,
        &REC_CLAIM_SCRIPT,
        &REC_EXTEND_SCRIPT,
        &REC_RELEASE_SCRIPT,
        &REC_BUMP_SCRIPT,
        &REC_PEEK_SCRIPT,
        &REC_CRON_CLAIM_SCRIPT,
        &REC_RECOVER_SCRIPT,
    ]
}
