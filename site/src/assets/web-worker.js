// Runs pigeonhole (crates/web, wasm32-wasip1) in a Web Worker (a module
// worker), its files in the site's OPFS (wasi.js: a pool of sync access
// handles, seen as /data). Without OPFS (a file:// page, an old browser) the
// pool is in memory and a reload starts from scratch.
//
// In: {id, op, …, body?: ArrayBuffer}; {op: "reset"} drops everything kept
// (the page reloads after it). Out:
//   {type: "waiting", why: "files"}  the page just left still holds the files
//   {type: "started", reply: {config, demo, saved, persistent, why} | {error}, events}
//   {type: "events", id, events}  what the node logged during call `id`, as it went
//   {type: "reply", id, reply, events: [], ms}
//   {type: "tick", events}        what the node logged on its own between calls
//
// The module never blocks: a call is queued (ph_call), then the node runs
// (ph_run) until it has nothing to do but wait; the shim notes its next
// timer (wasi.nextWake) and this worker sleeps until then, free meanwhile.
import {frame, unframe} from "./lib/ops.js";
import {MemoryHandle, Pool, Wasi} from "./wasi.js";

// The assets' version (?v=…), the same for the wasm as for this script.
const V = new URL(import.meta.url).search;

const POOL_DIR = "pigeonhole";
// The demo's cluster seals its Raft frames with this key ([cluster]
// key_env): no secret, every visitor's is the same.
const DEMO_CLUSTER_KEY = "706967656f6e686f6c652064656d6f20636c75737465723a206e6f206b657900";
const POOL_START = 64; // files opened at start
const POOL_LOW = 24; // fewer free than this after a call: open POOL_MORE
const POOL_MORE = 32;

let wasi,
  exports,
  pool,
  dir = null,
  opened = 0,
  persistent = false,
  growing = false,
  timer = null,
  why = "";
// The module's call number → the page's {id, t0}.
const calls = new Map();

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
  try {
    const root = await navigator.storage.getDirectory();
    dir = await root.getDirectoryHandle(POOL_DIR, {create: true});
  } catch (e) {
    console.warn("no OPFS, keeping files in memory:", e);
    why = String(e?.name || e?.message || e);
    return new Pool(Array.from({length: 256}, () => new MemoryHandle()));
  }
  let existing = 0;
  for await (const [name] of dir.entries()) if (name.startsWith("file-")) existing++;
  const deadline = performance.now() + LOCK_WAIT_MS;
  let said = false;
  for (;;) {
    try {
      opened = 0;
      const handles = await openFiles(Math.max(existing, POOL_START));
      persistent = true;
      return new Pool(handles);
    } catch (e) {
      if (opened === 0 && !said) {
        // Held by the page just left: say so while waiting.
        postMessage({type: "waiting", why: "files"});
        said = true;
      }
      if (performance.now() > deadline) {
        console.warn("the files stay locked:", e);
        throw new Error("other-tab");
      }
      await new Promise(r => setTimeout(r, 100));
    }
  }
}

// One export of the module on the framed request: {reply, events, ms}.
function run(fn, request = {}, body = null) {
  wasi.setStdin(frame(request, body));
  const t0 = performance.now();
  fn();
  const ms = performance.now() - t0;
  return {...unframe(wasi.takeLines()), ms};
}

// The module's output, in order: log lines go with the call in flight
// (else as the node's own), an answer ends its call.
function deliver(lines) {
  let events = [];
  const flush = () => {
    if (!events.length) return;
    const call = calls.values().next().value;
    postMessage(call ? {type: "events", id: call.id, events} : {type: "tick", events});
    events = [];
  };
  for (const line of lines) {
    let l;
    try {
      l = JSON.parse(line);
    } catch {
      continue; // a stray print: not the protocol's
    }
    if (!("call" in l)) {
      events.push(l);
      continue;
    }
    flush();
    const call = calls.get(l.call);
    calls.delete(l.call);
    if (call?.op === "close") end();
    else if (call) postMessage({type: "reply", id: call.id, reply: l.reply, events: [], ms: performance.now() - call.t0});
  }
  flush();
}

// Runs the node until it would wait, then sleeps until its next timer.
function pump() {
  clearTimeout(timer);
  timer = null;
  wasi.nextWake = Infinity;
  wasi.setStdin(new Uint8Array(0));
  exports.ph_run();
  deliver(wasi.takeLines());
  if (wasi.nextWake !== Infinity) timer = setTimeout(pump, Math.max(0, wasi.nextWake - performance.now()));
  grow();
}

// The page is gone and the node closed: let go of the files for the next
// page, and stop.
function end() {
  clearTimeout(timer);
  pool?.close();
  close();
}

// More files for what comes, between runs.
async function grow() {
  if (!persistent || growing || pool.free() >= POOL_LOW) return;
  growing = true;
  try {
    pool.add(await openFiles(POOL_MORE));
  } finally {
    growing = false;
  }
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
  wasi = new Wasi({pool, preopen: "/data", env: {PH_DEMO_CLUSTER_KEY: DEMO_CLUSTER_KEY}});
  const {instance} = await WebAssembly.instantiateStreaming(fetch(`pigeonhole.wasm${V}`), {wasi_snapshot_preview1: wasi.imports()});
  exports = instance.exports;
  wasi.memory = exports.memory;
  wasi.yieldFlag = exports.ph_yield_flag();
  const out = run(exports.ph_open, {op: "start"});
  postMessage({type: "started", reply: {...out.reply, persistent, why}, events: out.events});
})().catch(e => postMessage({type: "started", reply: {error: String(e.message || e)}, events: []}));

addEventListener("message", ({data}) => handle(data));

async function handle({id, body, ...request}) {
  await ready;
  if (request.op === "reset") {
    clearTimeout(timer);
    await reset();
    postMessage({type: "reply", id, reply: {ok: true}, events: []});
    return;
  }
  try {
    wasi.setStdin(frame(request, body));
    calls.set(exports.ph_call(), {id, op: request.op, t0: performance.now()});
    deliver(wasi.takeLines());
    pump();
  } catch (e) {
    postMessage({type: "reply", id, reply: {error: String(e)}, events: []});
  }
}
