#!/usr/bin/env node
// Publica os crates do workspace no crates.io, na ordem das dependências internas.
//
// Uso:
//   node scripts/publish-crates.js --dry-run   # empacota e compila como o crates.io faria, sem enviar
//   node scripts/publish-crates.js             # publica (pede confirmação)
//
// Opções:
//   --dry-run      não envia nada; valida o empacotamento de todos os crates pendentes
//   --allow-dirty  permite publicar com alterações não commitadas
//   --yes, -y      não pede confirmação (CI)
//   --help, -h     mostra esta ajuda
//
// Autenticação: `cargo login` ou a variável de ambiente CARGO_REGISTRY_TOKEN.
// Versões já publicadas são puladas, então o script pode ser executado de novo após uma falha.

'use strict';

const { execFileSync, spawnSync } = require('node:child_process');
const path = require('node:path');
const readline = require('node:readline/promises');

const ROOT = path.resolve(__dirname, '..');
const REQUIRED_FIELDS = ['description', 'license'];
const RECOMMENDED_FIELDS = ['repository'];
// Publicar vários crates do workspace num único comando (necessário para o dry-run de crates
// que dependem de outros ainda não publicados) existe a partir do cargo 1.90.
const MULTI_PUBLISH_CARGO = [1, 90];

function usage() {
  const lines = require('node:fs').readFileSync(__filename, 'utf8').split('\n').slice(1);
  const header = lines.slice(0, lines.findIndex((line) => !line.startsWith('//')));
  return header.map((line) => line.replace(/^\/\/ ?/, '')).join('\n');
}

function parseArgs(argv) {
  const opts = { dryRun: false, allowDirty: false, yes: false };
  for (const arg of argv) {
    if (arg === '--dry-run') opts.dryRun = true;
    else if (arg === '--allow-dirty') opts.allowDirty = true;
    else if (arg === '--yes' || arg === '-y') opts.yes = true;
    else if (arg === '--help' || arg === '-h') {
      console.log(usage());
      process.exit(0);
    } else {
      fail(`opção desconhecida: ${arg}\n\n${usage()}`);
    }
  }
  return opts;
}

function fail(message) {
  console.error(`\n✖ ${message}`);
  process.exit(1);
}

function run(cmd, args, { capture = false } = {}) {
  if (capture) {
    return execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  }
  console.log(`\n$ ${cmd} ${args.join(' ')}`);
  const res = spawnSync(cmd, args, { cwd: ROOT, stdio: 'inherit' });
  if (res.error) fail(`não foi possível executar ${cmd}: ${res.error.message}`);
  if (res.status !== 0) fail(`\`${cmd} ${args.join(' ')}\` terminou com código ${res.status}`);
}

function cargoVersion() {
  const out = run('cargo', ['--version'], { capture: true });
  const match = out.match(/cargo (\d+)\.(\d+)/);
  return match ? [Number(match[1]), Number(match[2])] : [0, 0];
}

/** Crates do workspace que podem ir para o crates.io (`publish` ausente ou incluindo crates-io). */
function publishableCrates() {
  const meta = JSON.parse(run('cargo', ['metadata', '--format-version', '1', '--no-deps'], { capture: true }));
  const members = new Set(meta.workspace_members);
  return meta.packages.filter((pkg) => {
    if (!members.has(pkg.id)) return false;
    return pkg.publish === null || pkg.publish.includes('crates-io');
  });
}

/** Ordena para que cada crate venha depois das dependências internas (dev-dependencies não contam). */
function topologicalOrder(crates) {
  const byName = new Map(crates.map((c) => [c.name, c]));
  const done = new Set();
  const visiting = new Set();
  const out = [];
  const visit = (crate) => {
    if (done.has(crate.name)) return;
    if (visiting.has(crate.name)) fail(`dependência circular envolvendo ${crate.name}`);
    visiting.add(crate.name);
    for (const dep of crate.dependencies) {
      const target = dep.kind !== 'dev' && byName.get(dep.name);
      if (target) visit(target);
    }
    visiting.delete(crate.name);
    done.add(crate.name);
    out.push(crate);
  };
  [...crates].sort((a, b) => a.name.localeCompare(b.name)).forEach(visit);
  return out;
}

/** Erros que fariam o crates.io (ou o cargo) recusar a publicação. */
function validate(crates) {
  const byName = new Map(crates.map((c) => [c.name, c]));
  const errors = [];
  const warnings = [];
  for (const crate of crates) {
    for (const field of REQUIRED_FIELDS) {
      if (!crate[field]) errors.push(`${crate.name}: campo \`${field}\` ausente no Cargo.toml`);
    }
    for (const field of RECOMMENDED_FIELDS) {
      if (!crate[field]) warnings.push(`${crate.name}: campo \`${field}\` recomendado ausente`);
    }
    for (const dep of crate.dependencies) {
      const target = byName.get(dep.name);
      if (!target || dep.kind === 'dev') continue;
      if (dep.req === '*') {
        errors.push(`${crate.name}: a dependência \`${dep.name}\` precisa de \`version\` além de \`path\``);
        continue;
      }
      // Requisitos simples (`=x.y.z`, `^x.y.z`, `x.y.z`) devem apontar para a versão do workspace.
      const simple = dep.req.match(/^[=^]?\s*(\d+\.\d+\.\d+\S*)$/);
      if (simple && simple[1] !== target.version) {
        errors.push(
          `${crate.name}: depende de \`${dep.name} ${dep.req}\`, mas o workspace está em ${target.version}`,
        );
      }
    }
  }
  return { errors, warnings };
}

async function isPublished(name, version) {
  const url = `https://crates.io/api/v1/crates/${encodeURIComponent(name)}/${encodeURIComponent(version)}`;
  let res;
  try {
    res = await fetch(url, { headers: { 'User-Agent': 'rekka-publish-crates-script' } });
  } catch (e) {
    fail(`não foi possível consultar o crates.io: ${e.message}`);
  }
  if (res.status === 200) return true;
  if (res.status === 404) return false;
  fail(`crates.io respondeu ${res.status} ao consultar ${name}@${version}`);
}

function dirtyFiles() {
  try {
    return run('git', ['status', '--porcelain'], { capture: true }).split('\n').filter(Boolean);
  } catch {
    return [];
  }
}

async function confirm(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  try {
    const answer = await rl.question(`${question} [y/N] `);
    return /^(y|yes|s|sim)$/i.test(answer.trim());
  } finally {
    rl.close();
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const crates = topologicalOrder(publishableCrates());
  if (crates.length === 0) fail('nenhum crate publicável no workspace');

  const { errors, warnings } = validate(crates);
  warnings.forEach((w) => console.warn(`⚠ ${w}`));
  if (errors.length) fail(`corrija antes de publicar:\n  - ${errors.join('\n  - ')}`);

  console.log('\nCrates (ordem de publicação):');
  const pending = [];
  for (const crate of crates) {
    const published = await isPublished(crate.name, crate.version);
    console.log(`  ${published ? '✔' : '•'} ${crate.name}@${crate.version}${published ? '  (já publicado, pulando)' : ''}`);
    if (!published) pending.push(crate);
  }
  if (pending.length === 0) {
    console.log('\nNada a publicar: todas as versões já estão no crates.io.');
    return;
  }

  const dirty = dirtyFiles();
  let allowDirty = opts.allowDirty;
  if (dirty.length && !allowDirty) {
    if (!opts.dryRun) {
      fail(`há alterações não commitadas (use --allow-dirty para publicar mesmo assim):\n  ${dirty.join('\n  ')}`);
    }
    console.warn(`\n⚠ ${dirty.length} arquivo(s) não commitado(s); o dry-run segue com --allow-dirty`);
    allowDirty = true;
  }
  const extra = allowDirty ? ['--allow-dirty'] : [];
  const packages = pending.flatMap((c) => ['-p', c.name]);

  if (opts.dryRun) {
    const [major, minor] = cargoVersion();
    const [needMajor, needMinor] = MULTI_PUBLISH_CARGO;
    if (pending.length > 1 && (major < needMajor || (major === needMajor && minor < needMinor))) {
      fail(`o dry-run de vários crates exige cargo >= ${needMajor}.${needMinor} (atual ${major}.${minor})`);
    }
    run('cargo', ['publish', '--dry-run', ...packages, ...extra]);
    console.log(`\n✔ dry-run ok: ${pending.map((c) => `${c.name}@${c.version}`).join(', ')}`);
    return;
  }

  if (!opts.yes) {
    const list = pending.map((c) => `${c.name}@${c.version}`).join(', ');
    const ok = await confirm(`\nPublicar ${list} no crates.io? Uma versão publicada não pode ser apagada (só yank).`);
    if (!ok) fail('cancelado');
  }

  // Um crate por vez: o cargo espera cada um aparecer no índice antes de seguir, e uma falha
  // no meio deixa os anteriores publicados (rodar de novo continua de onde parou).
  for (const crate of pending) {
    run('cargo', ['publish', '-p', crate.name, ...extra]);
    console.log(`✔ ${crate.name}@${crate.version} publicado`);
  }
  console.log('\n✔ todos os crates foram publicados');
}

main().catch((e) => fail(e.stack || String(e)));
