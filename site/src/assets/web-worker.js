// Runs pigeonhole (crates/web, wasm32-wasip1) in a Web Worker (a module
// worker), its files in the site's OPFS (wasi.js: a pool of sync access
// handles, seen as /data). Without OPFS (a file:// page, an old browser) the
// pool is in memory and a reload starts from scratch.
//
// In: {id, ...request, body?: ArrayBuffer} (lib/ops.js `ops`); `reset`
// drops everything kept (the page reloads after it), `close` ends the
// worker once the node closed. Out: lib/ops.js `MSG`.
//
// The module never blocks: a call is queued (ph_call), then the node runs
// (ph_run) until it has nothing to do but wait; the shim notes its next
// timer (wasi.nextWake) and this worker sleeps until then, free meanwhile.
//
// The node's time is the shim's clock, held while the page draws what the
// node did: each batch of its events goes out with a `seq`, and the clock
// stands until the page has sent `go` for every batch out. Meanwhile a call
// still runs, at that one instant; only the node's timers wait. The page
// can stop the clock (`pause`) and move it on by hand, one timer at a time
// (`step`: to the node's next timer, the node run once).
import {deliver, frame, MSG, msg, OP_GO, OP_PAUSE, OP_RESET, OP_STEP, unframe} from "./lib/ops.js";
import {Clock, MemoryHandle, Pool, Wasi} from "./wasi.js";

// The assets' version (?v=…), the same for the wasm as for this script.
const V = new URL(import.meta.url).search;
// The module downloads and compiles while the files open (below).
const compiled = WebAssembly.compileStreaming(fetch(`pigeonhole.wasm${V}`));
compiled.catch(() => {}); // awaited in `ready`, which reports a failure

const POOL_DIR = "pigeonhole";
// The demo's cluster seals its Raft frames with this key ([cluster]
// key_env): no secret, every visitor's is the same.
const DEMO_CLUSTER_KEY = "706967656f6e686f6c652064656d6f20636c75737465723a206e6f206b657900";
// A call can't wait for a file (the module is synchronous): a pool that
// runs out fails it (ENOSPC). A three-node cluster's boot takes ~70.
const POOL_START = 128; // files opened at start
const POOL_LOW = 64; // fewer free than this before or after a call: open POOL_MORE
const POOL_MORE = 64;

let wasi,
  exports,
  pool,
  dir = null,
  opened = 0,
  persistent = false,
  growing = null, // the files being opened, a promise
  timer = null,
  why = "";
// The module's call number → the page's {id, t0}.
const calls = new Map();
const clock = new Clock();
// The batches of events the page has not drawn yet (their seq).
const drawing = new Set();
let seq = 0,
  paused = false; // the page stopped the clock

async function openFiles(n) {
  const open = async i => (await dir.getFileHandle(`file-${String(opened + i).padStart(5, "0")}`, {create: true})).createSyncAccessHandle();
  const got = await Promise.allSettled(Array.from({length: n}, (_, i) => open(i)));
  const failed = got.find(g => g.status === "rejected");
  if (failed) {
    for (const g of got) if (g.status === "fulfilled") g.value.close();
    throw failed.reason;
  }
  opened += n;
  return got.map(g => g.value);
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
        postMessage(msg.waiting());
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

// The module's output, in order (lib/ops.js deliver): to the page; the
// worker ends once the node closed. A batch of events holds the clock
// until the page has drawn it.
function post(lines) {
  const {messages, closed} = deliver(lines, calls);
  for (const m of messages) {
    if (m.type === MSG.events || m.type === MSG.tick) {
      m.seq = ++seq;
      drawing.add(m.seq);
      clock.hold();
    }
    postMessage(m);
  }
  if (closed) end();
}

// Runs the node until it would wait, then sleeps until its next timer.
function pump() {
  clearTimeout(timer);
  timer = null;
  wasi.nextWake = Infinity;
  wasi.setStdin(new Uint8Array(0));
  exports.ph_run();
  post(wasi.takeLines());
  sleep();
  grow().catch(e => console.warn("no more files:", e));
}

// Until the node's next timer, on its clock; while held, until the page lets it go.
function sleep() {
  clearTimeout(timer);
  timer = null;
  if (clock.held || wasi.nextWake === Infinity) return;
  timer = setTimeout(pump, Math.max(0, wasi.nextWake - clock.now()));
}

// The page drew batch `n`: once it has drawn every one out, the clock goes
// on (unless stopped).
function drawn(n) {
  drawing.delete(n);
  if (!drawing.size && !paused) {
    clock.release();
    sleep();
  }
}

// The page stops the clock, or lets it go.
function pause(on) {
  paused = on;
  if (on) clock.hold();
  else drawn(null);
  postMessage(msg.clock(paused, clock.now(), wasi.nextWake));
}
// Stopped: on to the node's next timer (a ms past it: tokio rounds a
// deadline up to its ms), the node run once.
function step() {
  if (!paused) return;
  if (wasi.nextWake !== Infinity) clock.advance(wasi.nextWake + 1);
  pump();
  postMessage(msg.clock(paused, clock.now(), wasi.nextWake));
}

// The page is gone and the node closed: let go of the files for the next
// page, and stop.
function end() {
  clearTimeout(timer);
  pool?.close();
  close();
}

// More files for what comes, between runs; a call waits for those being
// opened.
function grow() {
  if (!growing && persistent && pool.free() < POOL_LOW)
    growing = openFiles(POOL_MORE)
      .then(handles => pool.add(handles))
      .finally(() => {
        growing = null;
      });
  return growing || Promise.resolve();
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
  wasi = new Wasi({pool, preopen: "/data", env: {PH_DEMO_CLUSTER_KEY: DEMO_CLUSTER_KEY}, clock});
  const instance = await WebAssembly.instantiate(await compiled, {wasi_snapshot_preview1: wasi.imports()});
  exports = instance.exports;
  wasi.memory = exports.memory;
  wasi.yieldFlag = exports.ph_yield_flag();
  // Its tracing set up; the node itself starts with the page's `open`.
  wasi.setStdin(new Uint8Array(0));
  exports.ph_open();
  const out = unframe(wasi.takeLines());
  postMessage(msg.started({...out.reply, persistent, why}, out.events));
})().catch(e => postMessage(msg.started({error: String(e.message || e)}, [])));

// One message at a time, in order: a call may first wait for more files.
let queue = Promise.resolve();
addEventListener("message", ({data}) => {
  queue = queue.then(() => handle(data)).catch(e => console.error(e));
});

async function handle({id, body, ...request}) {
  await ready;
  if (request.op === OP_GO) return drawn(request.seq);
  if (request.op === OP_PAUSE) return pause(!!request.on);
  if (request.op === OP_STEP) return step();
  await grow().catch(e => console.warn("no more files:", e));
  if (request.op === OP_RESET) {
    clearTimeout(timer);
    await reset();
    postMessage(msg.reply(id, {ok: true}));
    return;
  }
  try {
    wasi.setStdin(frame(request, body));
    calls.set(exports.ph_call(), {id, op: request.op, t0: performance.now()});
    post(wasi.takeLines());
    pump();
  } catch (e) {
    postMessage(msg.reply(id, {error: String(e)}));
  }
}
