// Runs pigeonhole (crates/web, wasm32-wasip1) in a Web Worker (a module
// worker), its files in the site's OPFS (wasi.js: a pool of sync access
// handles, seen as /data). Without OPFS (a file:// page, an old browser) the
// pool is in memory and a reload starts from scratch.
//
// In: {id, op, …, body?: ArrayBuffer}; {op: "reset"} drops everything kept
// (the page reloads after it). Out:
//   {type: "started", reply: {config, demo, saved, persistent, why} | {error}, events}
//   {type: "reply", id, reply, events, ms}
//   {type: "tick", events}      what the node logged on its own while idle
import {frame, unframe} from "./lib/ops.js";
import {MemoryHandle, Pool, Wasi} from "./wasi.js";

// The assets' version (?v=…), the same for the wasm as for this script.
const V = new URL(import.meta.url).search;

const POOL_DIR = "pigeonhole";
const POOL_START = 64; // files opened at start
const POOL_LOW = 24; // fewer free than this after a call: open POOL_MORE
const POOL_MORE = 32;

let wasi,
  exports,
  pool,
  dir = null,
  opened = 0,
  persistent = false,
  inFlight = false,
  why = "";

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
  for (;;) {
    try {
      opened = 0;
      const handles = await openFiles(Math.max(existing, POOL_START));
      persistent = true;
      return new Pool(handles);
    } catch (e) {
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
  const {instance} = await WebAssembly.instantiateStreaming(fetch(`pigeonhole.wasm${V}`), {wasi_snapshot_preview1: wasi.imports()});
  exports = instance.exports;
  wasi.memory = exports.memory;
  const out = run(exports.ph_open, {op: "start"});
  postMessage({type: "started", reply: {...out.reply, persistent, why}, events: out.events});
})().catch(e => postMessage({type: "started", reply: {error: String(e.message || e)}, events: []}));

// Between calls nothing runs on this thread: once a second, while idle, let
// the node's background work go on (a snapshot being built, timers) and pass
// on what it logged.
const TICK_MS = 1000;
setInterval(() => {
  if (inFlight || !exports?.ph_tick) return;
  wasi.setStdin(new Uint8Array(0));
  exports.ph_tick();
  const {events} = unframe(wasi.takeLines());
  if (events.length) postMessage({type: "tick", events});
}, TICK_MS);

addEventListener("message", async ({data}) => {
  inFlight = true;
  try {
    await handle(data);
  } finally {
    inFlight = false;
  }
});

async function handle({id, body, ...request}) {
  await ready;
  if (request.op === "reset") {
    await reset();
    postMessage({type: "reply", id, reply: {ok: true}, events: []});
    return;
  }
  try {
    postMessage({type: "reply", id, ...run(exports.ph_call, request, body)});
  } catch (e) {
    postMessage({type: "reply", id, reply: {error: String(e)}, events: []});
  }
  // More files for the next calls, while nothing runs.
  if (persistent && pool.free() < POOL_LOW) pool.add(await openFiles(POOL_MORE));
}
