// Runs pigeonhole (crates/web, wasm32-wasip1) in a Web Worker, its files in
// the site's OPFS (wasi.js: a pool of sync access handles, seen as /data).
// Without OPFS (a file:// page, an old browser) the pool is in memory and a
// reload starts from scratch.
//
// Messages: {id, op, …, body: ArrayBuffer} in, {id, events: [...], reply, ms}
// out; id 0 is the start ({reply: {config, demo, saved, persistent}}).
// {op: "reset"} drops everything kept (the page reloads after it).
importScripts("wasi.js");
const {Wasi, Pool, MemoryHandle} = self.PHWasi;

const POOL_DIR = "pigeonhole";
const POOL_START = 64;   // files opened at start
const POOL_LOW = 24;     // fewer free than this after a call: open POOL_MORE
const POOL_MORE = 32;

let wasi, exports, pool, dir = null, opened = 0, persistent = false;
const enc = new TextEncoder();

async function openFiles(n) {
  const handles = [];
  for (let i = 0; i < n; i++) {
    const fh = await dir.getFileHandle(`file-${String(opened++).padStart(5, "0")}`, {create: true});
    handles.push(await fh.createSyncAccessHandle());
  }
  return handles;
}

async function openPool() {
  try {
    const root = await navigator.storage.getDirectory();
    dir = await root.getDirectoryHandle(POOL_DIR, {create: true});
    let existing = 0;
    for await (const [name] of dir.entries()) if (name.startsWith("file-")) existing++;
    const handles = await openFiles(Math.max(existing, POOL_START));
    persistent = true;
    return new Pool(handles);
  } catch (e) {
    // Another tab holds the files (sync access handles are exclusive).
    if (e && e.name === "NoModificationAllowedError") throw new Error("busy: pigeonhole is open in another tab");
    console.warn("no OPFS, keeping files in memory:", e);
    return new Pool(Array.from({length: 256}, () => new MemoryHandle()));
  }
}

function call(fn, request, body) {
  const head = enc.encode(JSON.stringify(request) + "\n");
  const input = new Uint8Array(head.length + (body ? body.byteLength : 0));
  input.set(head);
  if (body) input.set(new Uint8Array(body), head.length);
  wasi.setStdin(input);
  const t0 = performance.now();
  fn();
  const ms = performance.now() - t0;
  const parsed = wasi.takeLines().map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  const reply = parsed.find(l => "reply" in l)?.reply;
  return {events: parsed.filter(l => !("reply" in l)), reply, ms};
}

async function reset() {
  pool?.close();
  if (dir) {
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(POOL_DIR, {recursive: true});
  }
}

const ready = (async () => {
  pool = await openPool();
  wasi = new Wasi({pool, preopen: "/data"});
  const {instance} = await WebAssembly.instantiateStreaming(fetch("pigeonhole.wasm"), {wasi_snapshot_preview1: wasi.imports()});
  exports = instance.exports;
  wasi.memory = exports.memory;
  const out = call(exports.ph_open, {op: "start"});
  out.reply = {...out.reply, persistent};
  postMessage({id: 0, ...out});
})().catch(e => postMessage({id: 0, reply: {error: String(e.message || e)}, events: []}));

onmessage = async ({data}) => {
  await ready;
  const {id, body, ...request} = data;
  if (request.op === "reset") {
    await reset();
    postMessage({id, reply: {ok: true}, events: []});
    return;
  }
  request.len = body ? body.byteLength : 0;
  try {
    postMessage({id, ...call(exports.ph_call, request, body)});
  } catch (e) {
    postMessage({id, reply: {error: String(e)}, events: []});
  }
  // More files for the next calls, while nothing runs.
  if (persistent && pool.free() < POOL_LOW) pool.add(await openFiles(POOL_MORE));
};
