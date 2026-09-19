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
