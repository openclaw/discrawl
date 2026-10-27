import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { tables, buildBatches, batchBody, verifyAdoption, cloudRequest, publishCloud } from './publish-cloud.mjs';

function fixture(t) {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  for (const [table, { columns }] of Object.entries(tables)) db.exec(`create table ${table}(${columns.map(c => `${c} text`).join(',')})`);
  return db;
}
test('stable key ranges skip unrelated rows after an edit or deletion and propagate empty tables', t => {
  const db = fixture(t);
  const insert = db.prepare('insert into messages values(?,?,?,?,?,?,?,?)');
  for (let i=0;i<1200;i++) insert.run(String(i).padStart(5,'0'),'channel','guild','user','<&\u2028name>','body','','');
  const first = buildBatches(db);
  assert.equal(first.counts.messages,1200);
  const before = first.batches.filter(b=>b.table==='messages');
  assert.deepEqual(before.map(b=>b.rows),[500,500,200]);
  db.exec("delete from messages where message_id='00020'; update messages set content='edited' where message_id='00021'");
  const after = buildBatches(db,first.batches).batches.filter(b=>b.table==='messages');
  assert.notEqual(after[0].sha256,before[0].sha256);
  assert.deepEqual(after.slice(1),before.slice(1));
  for (const batch of after) assert.equal(JSON.parse(batchBody(db,batch)).length,batch.rows);
  db.exec('delete from messages');
  const empty = buildBatches(db,after);
  assert.equal(empty.counts.messages,0);
  assert.ok(empty.batches.every(b=>b.rows===0));
});
test('a changed frozen export is detected before upload', t => {
  const db=fixture(t); db.exec("insert into guilds values('g','original','')");
  const batch=buildBatches(db).batches[0]; db.exec("update guilds set name='changed'");
  assert.throws(()=>batchBody(db,batch), /frozen cloud export changed/);
});
test('adoption accepts source tombstones and refuses unknown remote rows', async t => {
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());
  for(const [table,{sourceKeys}] of Object.entries(tables)) db.exec(`create table ${table}(${sourceKeys.map(k=>`${k} text`).join(',')},deleted_at text)`);
  db.exec("insert into messages values('deleted-message','2026-09-19')");
  const request=async path=>({keys:path.includes('table=messages')?[['deleted-message']]:[],next:null});
  await verifyAdoption(db,request,'current-state');
  const requestUnknown = async path => ({ keys: path.includes('table=members') ? [['guild', 'unknown-member']] : path.includes('table=messages') ? [['unknown-one'], ['unknown-two'], ['deleted-message']] : [], next: null });
  await assert.rejects(verifyAdoption(db, requestUnknown, 'current-state'), error => error.message ===
    'cloud archive has rows absent from the source; adoption stopped without changing data; missing_counts={"guilds":0,"channels":0,"members":1,"messages":2}');
});
test('HTTP failures never expose private server responses', async () => {
  const request=cloudRequest('https://fixture.invalid/',{authorization:'fixture-token'},async()=>new Response('private backend detail',{status:403}));
  await assert.rejects(request('current-state'),error=>error.message==='cloud request failed (HTTP 403)');
});

test('a denied publisher team identifies cloud login without retrying or leaking credentials', async () => {
  let calls = 0;
  const request = cloudRequest('https://fixture.invalid/private-endpoint/', {}, async () => {
    calls++;
    return Response.json({ error: 'github_org_denied', message: 'private membership detail', token: 'private-token' }, { status: 403 });
  });
  await assert.rejects(request('v1/auth/github/token', { method: 'POST', body: { token: 'private-token' } }), error =>
    error.message === 'cloud login failed (HTTP 403); GitHub organization/team authorization denied: check the deployed cloud service team policy and token membership permissions');
  assert.equal(calls, 1);
});

test('authorization diagnostics recognize only fixed codes with their expected status', async () => {
  for (const [code, status, hint] of [
    ['github_user_failed', 403, 'GitHub identity lookup failed without a confirmed rate-limit signal: check DISCORD_BACKUP_TOKEN validity and GitHub API availability'],
    ['forbidden', 403, 'cloud authorization denied: check the session publisher role'],
    ['missing_access_jwt', 401, 'Cloudflare Access authentication required: check the endpoint and Access service credentials'],
  ]) {
    const request = cloudRequest('https://fixture.invalid/', {}, async () => Response.json({ error: code, message: 'private' }, { status }));
    await assert.rejects(request('private-archive?private-query'), error => error.message === `cloud request failed (HTTP ${status}); ${hint}`);
  }
  for (const body of [{ error: 'private-backend-detail' }, { error: '__proto__' }, { error: { token: 'private' } }, { error: 'missing_access_jwt' }, null]) {
    const request = cloudRequest('https://fixture.invalid/', {}, async () => Response.json(body, { status: 403 }));
    await assert.rejects(request('current-state'), error => error.message === 'cloud request failed (HTTP 403)');
  }
});

test('invalid, oversized, and non-JSON authorization bodies stay private and are released', async () => {
  for (const [contentType, body] of [
    ['application/json', '{private malformed response'],
    ['application/json', JSON.stringify({ error: 'github_org_denied', private: 'x'.repeat(4096) })],
    ['text/html', '<html>private Access rejection</html>'],
  ]) {
    let cancelled = false;
    const request = cloudRequest('https://fixture.invalid/', {}, async () => new Response(new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode(body)); controller.close(); },
      cancel() { cancelled = true; },
    }), { status: 403, headers: { 'content-type': contentType } }));
    await assert.rejects(request('current-state'), error => error.message === 'cloud request failed (HTTP 403)');
    if (contentType === 'text/html') assert.equal(cancelled, true);
  }
});

test('a stalled authorization body cannot delay failure or trigger a retry', async () => {
  let cancelled = false;
  let calls = 0;
  const request = cloudRequest('https://fixture.invalid/', {}, async () => {
    calls++;
    return new Response(new ReadableStream({
      cancel() { cancelled = true; return new Promise(() => {}); },
    }), { status: 403, headers: { 'content-type': 'application/json' } });
  });
  await assert.rejects(request('current-state'), error => error.message === 'cloud request failed (HTTP 403)');
  assert.equal(cancelled, true);
  assert.equal(calls, 1);
});

test('the diagnostic size bound cancels a streaming body before reading its remainder', async () => {
  let cancelled = false;
  let reads = 0;
  const request = cloudRequest('https://fixture.invalid/', {}, async () => new Response(new ReadableStream({
    pull(controller) {
      reads++;
      controller.enqueue(new TextEncoder().encode('x'.repeat(2048)));
    },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 }), { status: 403, headers: { 'content-type': 'application/json' } }));
  await assert.rejects(request('current-state'), error => error.message === 'cloud request failed (HTTP 403)');
  assert.equal(cancelled, true);
  assert.equal(reads, 3);
});

test('authorization response stream errors preserve the terminal HTTP status', async () => {
  const request = cloudRequest('https://fixture.invalid/', {}, async () => new Response(new ReadableStream({
    start(controller) { controller.error(new Error('private stream error')); },
  }), { status: 403, headers: { 'content-type': 'application/json; charset=utf-8' } }));
  await assert.rejects(request('v1/auth/github/token'), error => error.message === 'cloud login failed (HTTP 403)');
});


test('a connection drop after successful headers retries the identical mutation', async () => {
  const requests = [];
  const request = cloudRequest('https://fixture.invalid/', { authorization: 'fixture-token' }, async (_url, options) => {
    requests.push({ method: options.method, body: options.body, generation: options.headers['x-crawl-publication'] });
    if (requests.length === 1) return new Response(new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('{"complete":'));
      controller.error(new Error('connection dropped after headers'));
    } }), { status: 200 });
    return Response.json({ complete: true, skipped: true });
  });
  assert.deepEqual(await request('current-state?batch=3', { method: 'POST', body: '[["fixture"]]', generation: 'fixture-generation' }), { complete: true, skipped: true });
  assert.equal(requests.length, 2);
  assert.deepEqual(requests[1], requests[0]);
});

test('adoption stops when the server repeats a pagination cursor', async t => {
  const db = new DatabaseSync(':memory:');
  t.after(() => db.close());
  for (const [table, { sourceKeys }] of Object.entries(tables)) {
    db.exec(`create table ${table}(${sourceKeys.map(key => `${key} text`).join(',')})`);
  }
  db.exec("insert into guilds values('known-guild')");
  let calls = 0;
  await assert.rejects(verifyAdoption(db, async () => {
    if (++calls > 2) throw new Error('unexpected third request');
    return { keys: [['known-guild']], next: ['known-guild'] };
  }, 'current-state'), /cloud adoption pagination did not advance/);
  assert.equal(calls, 2);
});

test('terminal HTTP failures release the unread response body', async () => {
  let cancelled = false;
  const request = cloudRequest('https://fixture.invalid/', {}, async () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  }), { status: 403 }));
  await assert.rejects(request('current-state'), /cloud request failed \(HTTP 403\)/);
  assert.equal(cancelled, true);
});

test('errored response bodies preserve HTTP failures and retries', async () => {
  const response = status => new Response(new ReadableStream({
    start(controller) { controller.error(new Error('private stream failure')); },
  }), { status });
  const terminal = cloudRequest('https://fixture.invalid/', {}, async () => response(403));
  await assert.rejects(terminal('current-state'), error => error.message === 'cloud request failed (HTTP 403)');
  let calls = 0;
  const retry = cloudRequest('https://fixture.invalid/', {}, async () =>
    ++calls === 1 ? response(503) : Response.json({ complete: true }));
  assert.deepEqual(await retry('current-state'), { complete: true });
  assert.equal(calls, 2);
});


test('cloud login waits for primary and secondary rate-limit headers before retrying', async () => {
  const now = Date.parse('2026-10-15T23:00:00Z');
  for (const [headers, delay] of [
    [{ 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': String(now / 1000 + 720) }, 720_000],
    [{ 'retry-after': '600', 'x-ratelimit-remaining': '4999' }, 600_000],
    [{ 'retry-after': new Date(now + 600_000).toUTCString() }, 600_000],
    [{ 'x-ratelimit-remaining': '0' }, 600_000],
  ]) {
    const waits = [];
    const requests = [];
    const request = cloudRequest('https://fixture.invalid/', {}, async (_url, options) => {
      requests.push(options.body);
      if (requests.length > 1) {
        return Response.json({ status: 'complete' });
      }
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode('{"error":"github_rate_limited"}')); controller.close(); },
      }), { status: 403, headers: { ...headers, 'content-type': 'application/json' } });
    }, { now: () => now, sleep: async ms => waits.push(ms) });
    assert.deepEqual(await request('v1/auth/github/token', { method: 'POST', body: { token: 'synthetic' } }), { status: 'complete' });
    assert.deepEqual(waits, [delay]);
    assert.deepEqual(requests, ['{"token":"synthetic"}', '{"token":"synthetic"}']);
  }
});

test('rate limits stop after three retries or the wait budget without shortening a cooldown', async () => {
  for (const [headers, expectedWaits, expectedCalls] of [
    [{ 'retry-after': '600' }, [600_000, 600_000, 600_000], 4],
    [{ 'retry-after': '1801' }, [], 1],
    [{}, [600_000, 600_000, 600_000], 4],
  ]) {
    const waits = [];
    let calls = 0;
    const request = cloudRequest('https://fixture.invalid/', {}, async () => {
      calls++;
      return Response.json({ error: 'github_rate_limited', private: 'do-not-log' }, { status: 429, headers });
    }, { sleep: async ms => waits.push(ms) });
    await assert.rejects(request('v1/auth/github/token'), error => error.message.includes('retry budget exhausted') && !error.message.includes('do-not-log'));
    assert.deepEqual(waits, expectedWaits);
    assert.equal(calls, expectedCalls);
  }
});

test('ambiguous identity failures, invalid rate headers, and credential denials remain terminal', async () => {
  for (const [code, status, headers] of [
    ['github_user_failed', 403, {}],
    ['github_user_failed', 403, { 'retry-after': 'private-invalid-header' }],
    ['github_org_denied', 403, { 'retry-after': '600' }],
    ['github_token_rejected', 401, { 'retry-after': '600' }],
  ]) {
    let calls = 0;
    const request = cloudRequest('https://fixture.invalid/', {}, async () => {
      calls++;
      return Response.json({ error: code }, { status, headers });
    }, { sleep: async () => assert.fail('must not retry unconfirmed rate limit or authorization failure') });
    await assert.rejects(request('v1/auth/github/token'), error => !error.message.includes('private-invalid-header'));
    assert.equal(calls, 1);
  }
});


async function diskFixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'discrawl-publisher-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const snapshot = join(directory, 'export.db');
  const sourceDB = join(directory, 'native.db');
  const exported = new DatabaseSync(snapshot);
  const source = new DatabaseSync(sourceDB);
  for (const [table, { columns, sourceKeys }] of Object.entries(tables)) {
    exported.exec(`create table ${table}(${columns.map(c => `${c} text`).join(',')})`);
    source.exec(`create table ${table}(${sourceKeys.map(c => `${c} text`).join(',')},deleted_at text)`);
  }
  exported.exec("insert into messages values('new-message','channel','guild','user','name','current content','2026-10-27T12:00:00Z','')");
  source.exec("insert into messages values('new-message',null),('deleted-message','2026-10-26')");
  exported.close(); source.close();
  return { snapshot, sourceDB, archive: 'fixture', sourceCommit: 'a'.repeat(40) };
}

test('a failed cloud refresh resumes its frozen native export without repeating completed uploads', async t => {
  const options = await diskFixture(t);
  let state = { current: null, generation: 0, manifest: null, completed_batches: [], uploaded_parts: [], snapshot_verified: false };
  const writes = [];
  const uploaded = new Map();
  const rows = new Map();
  let fail = true;
  const request = async (path, { method = 'GET', body, generation } = {}) => {
    const url = new URL(path, 'https://fixture.invalid/');
    assert.equal(url.pathname, '/v1/apps/discrawl/archives/fixture/current-state');
    if (method === 'GET') {
      if (url.searchParams.has('table')) return { keys: url.searchParams.get('table') === 'messages' ? [['deleted-message']] : [], next: null };
      return structuredClone(state);
    }
    writes.push({ method, query: url.search, operation: body?.operation });
    if (body?.operation === 'begin') {
      assert.equal(body.expected_generation, 0);
      assert.equal(body.manifest.source_commit, options.sourceCommit);
      state.manifest = body.manifest; state.generation = 1;
      return {};
    }
    if (method === 'PUT') {
      assert.equal(generation, 1);
      const index = Number(url.searchParams.get('part'));
      assert.equal(createHash('sha256').update(body).digest('hex'), state.manifest.parts[index].sha256);
      uploaded.set(index, body); state.uploaded_parts[index] = 1;
      return {};
    }
    if (body?.operation === 'verify') {
      assert.equal(body.generation, 1);
      const uncompressed = gunzipSync(Buffer.concat([...uploaded.values()]));
      assert.deepEqual(uncompressed, await readFile(options.snapshot));
      state.snapshot_verified = true; return {};
    }
    if (url.searchParams.has('batch')) {
      assert.equal(generation, 1);
      const index = Number(url.searchParams.get('batch'));
      if (state.manifest.batches[index].table === 'messages' && fail) { fail = false; throw new Error('fixture transport failure'); }
      rows.set(state.manifest.batches[index].table, JSON.parse(body));
      state.completed_batches[index] = 1;
      return { complete: true };
    }
    assert.equal(body.operation, 'complete');
    assert.equal(body.generation, 1);
    assert.equal(state.completed_batches.filter(Boolean).length, state.manifest.batches.length);
    state.current = { generation: 1, plan: state.manifest };
    return { status: 'complete' };
  };
  await assert.rejects(publishCloud({ ...options, request }), /fixture transport failure/);
  assert.equal(state.current, null, 'failed refresh does not claim completion');
  const before = writes.length;
  const result = await publishCloud({ ...options, request });
  assert.deepEqual(result, { status: 'complete', batches_uploaded: 1, batches_skipped: 3, counts: { guilds: 0, channels: 0, members: 0, messages: 1 } });
  assert.deepEqual(writes.slice(before).map(w => w.query || w.operation), ['?batch=3', 'complete']);
  assert.deepEqual(rows.get('messages'), [['new-message','channel','guild','user','name','current content','2026-10-27T12:00:00Z','']]);
  const completedWrites = writes.length;
  assert.equal((await publishCloud({ ...options, request })).status, 'unchanged');
  assert.equal(writes.length, completedWrites);
});

test('initial unknown remote records stop publication before any begin, upload or row write', async t => {
  const options = await diskFixture(t);
  const request = async (path, { method = 'GET' } = {}) => {
    assert.equal(method, 'GET', 'adoption must stay read-only on refusal');
    const table = new URL(path, 'https://fixture.invalid/').searchParams.get('table');
    if (table) return { keys: table === 'messages' ? [['unknown-private-id']] : [], next: null };
    return { current: null, generation: 0, manifest: null };
  };
  await assert.rejects(publishCloud({ ...options, request }), error => error.message.includes('missing_counts=') && !error.message.includes('unknown-private-id'));
  // A pending initial adoption is not proof that a changed source still owns all keys.
  await assert.rejects(publishCloud({ ...options, request: async (path, settings) => {
    const response = await request(path, settings);
    return 'generation' in response ? { ...response, generation: 1 } : response;
  } }), error => error.message.includes('missing_counts='));
});
