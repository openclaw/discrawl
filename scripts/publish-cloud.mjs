import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdtemp, open, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createGzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';

class PublishError extends Error {}

export const tables = {
  guilds: { columns: ['guild_id', 'name', 'updated_at'], keys: ['guild_id'], sourceKeys: ['id'] },
  channels: { columns: ['channel_id', 'guild_id', 'name', 'type', 'parent_id', 'updated_at'], keys: ['channel_id'], sourceKeys: ['id'] },
  members: { columns: ['guild_id', 'user_id', 'username', 'display_name', 'updated_at'], keys: ['guild_id', 'user_id'], sourceKeys: ['guild_id', 'user_id'] },
  messages: { columns: ['message_id', 'channel_id', 'guild_id', 'author_id', 'author_username', 'content', 'created_at', 'edited_at'], keys: ['message_id'], sourceKeys: ['id'] },
};
const digest = value => createHash('sha256').update(value).digest('hex');
const rowKey = (row, table) => tables[table].keys.map(key => row[tables[table].columns.indexOf(key)]);
function selectRange(db, table, after, through) {
  const { keys, columns } = tables[table];
  const tuple = keys.length === 1 ? keys[0] : `(${keys.join(',')})`;
  const placeholders = keys.length === 1 ? '?' : `(${keys.map(() => '?').join(',')})`;
  return { statement: db.prepare(`select ${columns.join(',')} from ${table} where 1=1${after ? ` and ${tuple}>${placeholders}` : ''}${through ? ` and ${tuple}<=${placeholders}` : ''} order by ${keys.join(',')}`), values: [...(after ?? []), ...(through ?? [])] };
}
const project = (row, table) => tables[table].columns.map(c => row[c] == null ? '' : String(row[c]));
export function buildBatches(db, previous = []) {
  const batches = [];
  const counts = {};
  for (const table of Object.keys(tables)) {
    counts[table] = 0;
    // Keep earlier key boundaries, so one deletion does not shift every later batch.
    const ranges = previous.filter(b => b.table === table);
    if (!ranges.length) ranges.push({ after: null, through: null });
    for (const range of ranges) {
      const selected = selectRange(db, table, range.after, range.through);
      let after = range.after;
      let rows = [];
      let bytes = 2;
      const flush = through => {
        batches.push({ table, after, through, rows: rows.length, sha256: digest(JSON.stringify(rows)) });
        counts[table] += rows.length;
        after = through;
        rows = [];
        bytes = 2;
      };
      for (const value of selected.statement.iterate(...selected.values)) {
        const row = project(value, table);
        const size = Buffer.byteLength(JSON.stringify(row)) + 1;
        if (size > 768 * 1024) throw new PublishError('cloud row exceeds batch limit');
        if (rows.length && (rows.length === 500 || bytes + size > 768 * 1024)) flush(rowKey(rows.at(-1), table));
        rows.push(row);
        bytes += size;
      }
      flush(range.through);
    }
  }
  return { batches, counts };
}
export function batchBody(db, batch) {
  const selected = selectRange(db, batch.table, batch.after, batch.through);
  const rows = selected.statement.all(...selected.values).map(row => project(row, batch.table));
  const raw = JSON.stringify(rows);
  if (rows.length !== batch.rows || digest(raw) !== batch.sha256) throw new PublishError('frozen cloud export changed');
  return raw;
}

async function fileDigest(path) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}
async function gzipParts(snapshot, directory) {
  const compressed = join(directory, 'archive.db.gz');
  await pipeline(createReadStream(snapshot), createGzip({ level: 6 }), createWriteStream(compressed, { flags: 'wx', mode: 0o600 }));
  const parts = [];
  const handle = await open(compressed, 'r');
  try {
    let offset = 0;
    const total = (await handle.stat()).size;
    while (offset < total) {
      const size = Math.min(64 * 1024 ** 2, total - offset);
      const buffer = Buffer.alloc(size);
      let used = 0;
      while (used < size) {
        const read = await handle.read(buffer, used, size - used, offset + used);
        if (!read.bytesRead) throw new PublishError('incomplete snapshot read');
        used += read.bytesRead;
      }
      parts.push({ size, sha256: digest(buffer) });
      offset += size;
    }
    return { compressed, parts, archive_bytes: total, archive_sha256: await fileDigest(compressed) };
  } finally { await handle.close(); }
}

export function cloudRequest(endpoint, headers, fetchImpl = fetch) {
  const base = new URL(endpoint.endsWith('/') ? endpoint : `${endpoint}/`);
  if (base.protocol !== 'https:' && base.hostname !== 'localhost') throw new PublishError('cloud endpoint must use HTTPS');
  return async (path, { method = 'GET', body, generation, binary = false } = {}) => {
    for (let attempt = 0; ; attempt++) {
      let response;
      try {
        response = await fetchImpl(new URL(path, base), { method, redirect: 'error',
          headers: { ...headers, ...(body === undefined ? {} : { 'content-type': binary ? 'application/gzip' : 'application/json' }),
            ...(generation ? { 'x-crawl-publication': generation } : {}) },
          body: body === undefined ? undefined : typeof body === 'string' || binary ? body : JSON.stringify(body),
          signal: AbortSignal.timeout(10 * 60_000) });
        if (response.ok) return await response.json();
      } catch {
        if (attempt >= 3) throw new PublishError('cloud request failed; no private response was logged');
        response = undefined;
      }
      const retryable = !response || response.status === 429 || response.status >= 500;
      if (!retryable || attempt >= 3) throw new PublishError(`cloud request failed (HTTP ${response?.status ?? 'unavailable'})`);
      await response?.body?.cancel();
      await new Promise(r => setTimeout(r, 1000 * 2 ** attempt));
    }
  };
}

export async function verifyAdoption(source, request, path) {
  for (const [table, { sourceKeys }] of Object.entries(tables)) {
    const exists = source.prepare(`select 1 from ${table} where ${sourceKeys.map(c => `${c}=?`).join(' and ')}`);
    let after = null;
    do {
      const page = await request(`${path}?table=${table}${after ? `&after=${encodeURIComponent(JSON.stringify(after))}` : ''}`);
      for (const key of page.keys) {
        // Source tombstones prove intentional deletion. Unknown remote IDs need owner recovery.
        if (!exists.get(...key)) throw new PublishError('cloud archive has rows absent from the source; adoption stopped without changing data');
      }
      after = page.next;
    } while (after);
  }
}

export async function publishCloud({ snapshot, sourceDB, sourceCommit, request, archive, exportedAt = new Date().toISOString() }) {
  const path = `v1/apps/discrawl/archives/${encodeURIComponent(archive)}/current-state`;
  const db = new DatabaseSync(snapshot, { readOnly: true });
  const directory = await mkdtemp(join(tmpdir(), 'discrawl-cloud-publish-'));
  try {
    let state = await request(path);
    const sha256 = await fileDigest(snapshot);
    if (state.current?.plan.sha256 === sha256 && state.current.generation === state.generation) return { status: 'unchanged', counts: state.current.plan.counts };
    const { batches, counts } = buildBatches(db, state.manifest?.batches);
    const compressed = await gzipParts(snapshot, directory);
    const plan = { exported_at: exportedAt, source_commit: sourceCommit, sha256, bytes: (await stat(snapshot)).size,
      archive_sha256: compressed.archive_sha256, archive_bytes: compressed.archive_bytes, parts: compressed.parts, counts, batches };
    // Reuse an interrupted generation, including its immutable object names.
    const samePending = state.manifest?.sha256 === sha256 && state.manifest?.archive_sha256 === plan.archive_sha256
      && JSON.stringify(state.manifest.batches) === JSON.stringify(batches);
    if (!state.current && !state.generation) {
      const source = new DatabaseSync(sourceDB, { readOnly: true });
      try { await verifyAdoption(source, request, path); } finally { source.close(); }
    }
    if (!samePending) await request(path, { method: 'POST', body: { operation: 'begin', expected_generation: state.generation, manifest: plan } });
    state = await request(path);
    const generation = state.generation;
    if (!state.current) {
      const source = new DatabaseSync(sourceDB, { readOnly: true });
      try { await verifyAdoption(source, request, path); } finally { source.close(); }
    }
    const handle = await open(compressed.compressed, 'r');
    try {
      let offset = 0;
      for (const [index, part] of compressed.parts.entries()) {
        if (state.uploaded_parts[index] !== 1) {
          const buffer = Buffer.alloc(part.size);
          let used = 0;
          while (used < part.size) {
            const read = await handle.read(buffer, used, part.size - used, offset + used);
            if (!read.bytesRead) throw new PublishError('incomplete snapshot read');
            used += read.bytesRead;
          }
          await request(`${path}?part=${index}`, { method: 'PUT', body: buffer, binary: true, generation });
        }
        offset += part.size;
      }
    } finally { await handle.close(); }
    if (!state.snapshot_verified) await request(path, { method: 'POST', body: { operation: 'verify', generation } });
    let uploaded = 0;
    for (const [index, batch] of batches.entries()) {
      if (state.completed_batches[index] === 1) continue;
      const raw = batchBody(db, batch);
      let complete = false;
      for (let attempt = 0; !complete && attempt < 10000; attempt++) {
        ({ complete } = await request(`${path}?batch=${index}`, { method: 'POST', body: raw, generation }));
      }
      if (!complete) throw new PublishError('cloud deletion did not finish within its bound');
      uploaded++;
    }
    const result = await request(path, { method: 'POST', body: { operation: 'complete', generation } });
    return { status: result.status, batches_uploaded: uploaded, batches_skipped: batches.length - uploaded, counts };
  } finally { db.close(); await rm(directory, { recursive: true, force: true }); }
}

async function main() {
  const env = process.env;
  for (const key of ['DISCRAWL_CLOUD_ENDPOINT', 'DISCRAWL_CLOUD_ARCHIVE', 'DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN', 'DISCRAWL_CLOUD_SNAPSHOT', 'DISCRAWL_CLOUD_SOURCE_DB', 'DISCRAWL_CLOUD_SOURCE_COMMIT']) {
    if (!env[key]) throw new PublishError(`missing ${key}`);
  }
  const headers = {};
  if (env.DISCRAWL_CLOUD_ACCESS_CLIENT_ID || env.DISCRAWL_CLOUD_ACCESS_CLIENT_SECRET) {
    if (!env.DISCRAWL_CLOUD_ACCESS_CLIENT_ID || !env.DISCRAWL_CLOUD_ACCESS_CLIENT_SECRET) throw new PublishError('both Access service credentials are required');
    headers['CF-Access-Client-Id'] = env.DISCRAWL_CLOUD_ACCESS_CLIENT_ID;
    headers['CF-Access-Client-Secret'] = env.DISCRAWL_CLOUD_ACCESS_CLIENT_SECRET;
  }
  const login = await cloudRequest(env.DISCRAWL_CLOUD_ENDPOINT, headers)('v1/auth/github/token', { method: 'POST', body: { token: env.DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN } });
  delete env.DISCRAWL_CLOUD_AUTH_GITHUB_TOKEN;
  if (login.status !== 'complete' || login.org !== 'openclaw' || !/^[A-Za-z0-9_-]+\.[a-f0-9]{64}$/.test(login.token ?? '')) throw new PublishError('cloud login did not return a valid session');
  const result = await publishCloud({ snapshot: env.DISCRAWL_CLOUD_SNAPSHOT, sourceDB: env.DISCRAWL_CLOUD_SOURCE_DB,
    sourceCommit: env.DISCRAWL_CLOUD_SOURCE_COMMIT, archive: env.DISCRAWL_CLOUD_ARCHIVE,
    request: cloudRequest(env.DISCRAWL_CLOUD_ENDPOINT, { ...headers, authorization: `Bearer ${login.token}` }) });
  // Only aggregate counts are safe in public workflow logs.
  console.log(JSON.stringify(result));
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(error => {
    console.error(error instanceof PublishError ? error.message : 'Cloud publication failed; private diagnostics were suppressed.');
    console.error('The Git snapshot and runtime cache remain available.');
    process.exitCode = 1;
  });
}
