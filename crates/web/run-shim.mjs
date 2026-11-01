// The site's WASI shim (site/src/assets/wasi.js) in Node, its pool on
// buffers instead of OPFS:
//
//   node run-shim.mjs smoke    <pigeonhole-web.wasm>   the smoke binary, /tmp in the pool
//   node run-shim.mjs reload   <pigeonhole_web.wasm>   two module instances on one pool:
//                                                      a page load, then a reload
//   node run-shim.mjs snapshot <pigeonhole_web.wasm>   enough puts for a snapshot, a tick
//   node run-shim.mjs events   <pigeonhole_web.wasm>   every event the demo draws, from the node
import {readFile} from "node:fs/promises";
import {frame, unframe} from "../../site/src/assets/lib/ops.js";
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
  return {wasi, exports: inst.exports};
}

// The demo's module, started as the worker starts it.
async function node() {
  const m = await instance("/data");
  m.exports.ph_open();
  m.wasi.takeLines();
  // One op, as the page sends it: its reply, or its error thrown.
  m.call = (request, body) => {
    m.wasi.setStdin(frame(request, body));
    m.exports.ph_call();
    const {reply} = unframe(m.wasi.takeLines());
    if (reply.error) throw new Error(reply.error);
    return reply;
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
  // The demo snapshots every 50 entries: 60 puts, then the log's base.
  const m = await node();
  m.call({op: "open", config: await config()});
  for (let i = 0; i < 60; i++) m.call({op: "put", key: `k${i}`}, new Uint8Array(2000).fill(i));
  // The worker ticks while idle: a tick must run (and not panic).
  m.exports.ph_tick();
  m.wasi.takeLines();
  const log = m.call({op: "log", limit: 3});
  const snaps = m.call({op: "channels"}).channels.find(c => c.role === "snapshots");
  console.error(`head ${log.head}, base ${JSON.stringify(log.base)}, snapshot channel ${snaps.messages.map(x => x.name).join(" ")}`);
} else if (mode === "events") {
  // The node's log as the demo reads it (demo/events.js): every kind of
  // event the page draws must still come out of the real node's lines.
  const {readEvents} = await import("../../site/src/assets/demo/events.js");
  const seen = new Map();
  const m = await node();
  const read = (lines, label) => {
    for (const e of readEvents(unframe(lines).events)) seen.set(e.type === "posted" && e.preview ? "posted text" : e.type, label);
  };
  const call = (request, body) => {
    m.wasi.setStdin(frame(request, body));
    m.exports.ph_call();
    read(m.wasi.takeLines(), request.op);
  };
  // Repair passes every second, so one comes after the check.
  call({op: "open", config: (await config()).replace('interval = "10s"', 'interval = "1s"')});
  call({op: "put", key: "text"}, new TextEncoder().encode("hello ".repeat(100000)));
  call({op: "put", key: "random"}, crypto.getRandomValues(new Uint8Array(60000)));
  // A copy gone behind the node's back: a read that tries it (on the fakes
  // the Discord copy comes first) fails over; the check queues it for repair.
  call({op: "wipe", store: "discord"});
  call({op: "get", key: "random"});
  call({op: "verify", store: "discord"});
  await new Promise(r => setTimeout(r, 1200));
  m.wasi.setStdin(new Uint8Array(0));
  m.exports.ph_tick();
  read(m.wasi.takeLines(), "tick");
  const want = ["posted", "posted text", "claimed", "s3", "call", "call_failed", "repair_pass"];
  const missing = want.filter(k => !seen.has(k));
  console.error(`events: ${[...seen].map(([k, op]) => `${k} (${op})`).join(", ")}`);
  if (missing.length) throw new Error(`the demo no longer reads: ${missing.join(", ")}`);
  console.error("events ok");
} else if (mode === "reload") {
  const body = new Uint8Array(3 << 20).map((_, i) => (i * 7) % 251);

  const first = await node();
  const t0 = performance.now();
  first.call({op: "open", config: await config()});
  first.call({op: "put", key: "photo.jpg"}, body);
  first.call({op: "put", key: "small.bin"}, body.subarray(0, 3000));
  console.error(`first load: 2 puts in ${Math.round(performance.now() - t0)} ms; pool ${handles.filter(h => h.len).length} files`);

  // A reload: a new instance, nothing of the first but the pool's files.
  const second = await node();
  const t1 = performance.now();
  const open = second.call({op: "open", config: await config()});
  const got = second.call({op: "get", key: "photo.jpg"});
  const list = second.call({op: "list"});
  console.error(
    `reload: ${open.replayed} entries replayed, photo.jpg ${got.len} B, objects ${JSON.stringify(list.objects)} in ${Math.round(performance.now() - t1)} ms`,
  );
  if (got.len !== body.length || list.objects.length !== 2) throw new Error("the reload lost data");
  console.error("reload ok");
}
