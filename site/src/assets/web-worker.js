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

let wasi, exports, pool, dir = null, opened = 0, persistent = false, inFlight = false;
const enc = new TextEncoder();

async function openFiles(n) {
  const handles = [];
  try {
    for (let i = 0; i < n; i++) {
      const fh = await dir.getFileHandle(`file-${String(opened + i).padStart(5, "0")}`, {create: true});
      handles.push(await fh.createSyncAccessHandle());
    }
  } catch (e) {
    for (const h of handles) h.close();
    throw e;
  }
  opened += n;
  return handles;
}

// A sync access handle is exclusive: the page just left (a reload, the
// other language) may still hold the files for a moment. Wait for them,
// then give up as busy (another tab); only without OPFS keep files in memory.
const LOCK_WAIT_MS = 5000;

async function openPool() {
  let root;
  try {
    root = await navigator.storage.getDirectory();
    dir = await root.getDirectoryHandle(POOL_DIR, {create: true});
  } catch (e) {
    console.warn("no OPFS, keeping files in memory:", e);
    return new Pool(Array.from({length: 256}, () => new MemoryHandle()));
  }
  let existing = 0;
  for await (const [name] of dir.entries()) if (name.startsWith("file-")) existing++;
  const deadline = performance.now() + LOCK_WAIT_MS;
  for (;;) {
    try {
      opened = 0;
      const handles = await openFiles(Math.max(existing, POOL_START));
      persistent = true;
      return new Pool(handles);
    } catch (e) {
      if (performance.now() > deadline) throw new Error(`busy: pigeonhole is open in another tab (${e.name || e})`);
      await new Promise(r => setTimeout(r, 100));
    }
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

// Between calls nothing runs on this thread: once a second, while idle, let
// the node's background work go on (a snapshot being built, timers) and pass
// on what it logged ({id: -1, events}).
const TICK_MS = 1000;
setInterval(() => {
  if (inFlight || !exports?.ph_tick) return;
  wasi.setStdin(new Uint8Array(0));
  exports.ph_tick();
  const events = wasi.takeLines().map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  if (events.length) postMessage({id: -1, events});
}, TICK_MS);

onmessage = async ({data}) => {
  inFlight = true;
  try {
    await handle(data);
  } finally {
    inFlight = false;
  }
};

async function handle(data) {
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
}
