// The site's WASI shim (site/src/assets/wasi.js) in Node, its pool on
// buffers instead of OPFS:
//
//   node run-shim.mjs smoke    <pigeonhole-web.wasm>   the smoke binary, /tmp in the pool
//   node run-shim.mjs reload   <pigeonhole_web.wasm>   two module instances on one pool:
//                                                      a page load, then a reload
//   node run-shim.mjs snapshot <pigeonhole_web.wasm>   enough puts for a snapshot, a tick
//   node run-shim.mjs events   <pigeonhole_web.wasm>   every event the demo draws, from the node
import {readFile} from "node:fs/promises";
import {frame} from "../../site/src/assets/lib/ops.js";
import {MemoryHandle, Pool, Wasi} from "../../site/src/assets/wasi.js";

const here = new URL(".", import.meta.url);
const [mode, file] = process.argv.slice(2);
const module = await WebAssembly.compile(await readFile(file));
const handles = Array.from({length: 256}, () => new MemoryHandle());
// The demo keeps its channels in /data/channels: the pool, here.
const config = () => readFile(new URL("pigeonhole-web/demo.toml", here), "utf8");

async function instance(preopen, env = {}) {
  const wasi = new Wasi({pool: new Pool(handles), preopen, env});
  const inst = await WebAssembly.instantiate(module, {wasi_snapshot_preview1: wasi.imports()});
  wasi.memory = inst.exports.memory;
  wasi.yieldFlag = inst.exports.ph_yield_flag?.() ?? 0;
  return {wasi, exports: inst.exports};
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const parse = lines =>
  lines.flatMap(l => {
    try {
      return [JSON.parse(l)];
    } catch {
      return [];
    }
  });

// The demo's module, started as the worker starts it, and driven as the
// worker drives it: a call queued, the node run until it would wait, a
// sleep until its next timer.
async function node() {
  const m = await instance("/data");
  m.exports.ph_open();
  m.wasi.takeLines();
  m.events = [];
  // Runs the node once; true if it has a timer to wait for.
  const runOnce = () => {
    m.wasi.nextWake = Infinity;
    m.wasi.setStdin(new Uint8Array(0));
    m.exports.ph_run();
    return m.wasi.nextWake !== Infinity;
  };
  // One op, as the page sends it: {reply, events}; the node's lines meanwhile in m.events too.
  m.op = async (request, body) => {
    m.wasi.setStdin(frame(request, body));
    const n = m.exports.ph_call();
    const events = [];
    for (;;) {
      for (const l of parse(m.wasi.takeLines())) {
        if (!("call" in l)) events.push(l);
        else if (l.call === n || n === 0) return {reply: l.reply, events};
      }
      const waits = runOnce();
      if (!waits && !m.wasi.lines.length) throw new Error(`call ${n} (${request.op}) stuck: nothing to run, no timer`);
      if (waits && !m.wasi.lines.length) await sleep(Math.max(0, m.wasi.nextWake - performance.now()));
    }
  };
  // Its reply, or its error thrown.
  m.call = async (request, body) => {
    const {reply} = await m.op(request, body);
    if (reply.error) throw new Error(reply.error);
    return reply;
  };
  // The node on its own for `ms`, as between the page's calls: its lines.
  m.idle = async ms => {
    const end = performance.now() + ms;
    while (performance.now() < end) {
      if (!runOnce()) break;
      await sleep(Math.max(0, Math.min(end, m.wasi.nextWake) - performance.now()));
    }
    return parse(m.wasi.takeLines());
  };
  return m;
}

if (mode === "smoke") {
  const {wasi, exports} = await instance("/tmp", {TMPDIR: "/tmp"});
  exports._start();
  console.error(
    wasi
      .takeLines()
      .filter(l => !l.startsWith("{"))
      .join("\n"),
  );
} else if (mode === "snapshot") {
  // The demo snapshots every 15 entries: 60 puts, then the log's base.
  const m = await node();
  // A config that does not open is an error, and the module lives on (with
  // nothing left to run it must not park forever: wasm has no threads).
  const bad = await m.op({op: "open", config: "[meta]\nkeep_log_entries = 0\n"});
  if (!bad.reply.error) throw new Error("a bad config opened");
  await m.idle(50);
  await m.call({op: "open", config: await config()});
  for (let i = 0; i < 60; i++) await m.call({op: "put", key: `k${i}`}, new Uint8Array(2000).fill(i));
  // The worker runs the node between calls: it must run (and not panic).
  await m.idle(100);
  const log = await m.call({op: "log", limit: 3});
  const snaps = (await m.call({op: "channels"})).channels.find(c => c.role === "snapshots");
  console.error(`head ${log.head}, base ${JSON.stringify(log.base)}, snapshot channel ${snaps.messages.map(x => x.name).join(" ")}`);
} else if (mode === "events") {
  // The node's log as the demo reads it (demo/events.js): every kind of
  // event the page draws must still come out of the real node's lines.
  const {readEvents} = await import("../../site/src/assets/demo/events.js");
  const seen = new Map();
  const m = await node();
  const read = (events, label) => {
    for (const e of readEvents(events)) seen.set(e.type === "posted" && e.preview ? "posted text" : e.type, label);
  };
  const call = async (request, body) => read((await m.op(request, body)).events, request.op);
  // Repair passes every second, so one comes after the check.
  await call({op: "open", config: (await config()).replace('interval = "10s"', 'interval = "1s"')});
  await call({op: "put", key: "text"}, new TextEncoder().encode("hello ".repeat(100000)));
  await call({op: "put", key: "random"}, crypto.getRandomValues(new Uint8Array(60000)));
  // A copy gone behind the node's back: a read that tries it (on the fakes
  // the Discord copy comes first) fails over; the check queues it for repair.
  await call({op: "wipe", store: "discord"});
  await call({op: "get", key: "random"});
  await call({op: "verify", store: "discord"});
  read(await m.idle(1500), "idle");
  const want = ["posted", "posted text", "claimed", "s3", "call", "call_failed", "repair_pass"];
  const missing = want.filter(k => !seen.has(k));
  console.error(`events: ${[...seen].map(([k, op]) => `${k} (${op})`).join(", ")}`);
  if (missing.length) throw new Error(`the demo no longer reads: ${missing.join(", ")}`);
  console.error("events ok");
} else if (mode === "reload") {
  const body = new Uint8Array(3 << 20).map((_, i) => (i * 7) % 251);

  const first = await node();
  const t0 = performance.now();
  await first.call({op: "open", config: await config()});
  await first.call({op: "put", key: "photo.jpg"}, body);
  await first.call({op: "put", key: "small.bin"}, body.subarray(0, 3000));
  console.error(`first load: 2 puts in ${Math.round(performance.now() - t0)} ms; pool ${handles.filter(h => h.len).length} files`);

  // A reload: a new instance, nothing of the first but the pool's files.
  const second = await node();
  const t1 = performance.now();
  const open = await second.call({op: "open", config: await config()});
  const got = await second.call({op: "get", key: "photo.jpg"});
  const list = await second.call({op: "list"});
  console.error(
    `reload: ${open.replayed} entries replayed, photo.jpg ${got.len} B, objects ${JSON.stringify(list.objects)} in ${Math.round(performance.now() - t1)} ms`,
  );
  if (got.len !== body.length || list.objects.length !== 2) throw new Error("the reload lost data");
  console.error("reload ok");
}
