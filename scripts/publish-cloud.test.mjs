import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { tables, buildBatches, batchBody, verifyAdoption, cloudRequest } from './publish-cloud.mjs';

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
  await assert.rejects(verifyAdoption(db,async()=>({keys:[['unknown']],next:null}),'current-state'),/absent from the source/);
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
    ['github_user_failed', 403, 'GitHub identity lookup failed: check the publishing token and GitHub API availability'],
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
