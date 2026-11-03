// The site's WASI shim (site/src/assets/wasi.js) in Node, its pool on
// buffers instead of OPFS:
//
//   node run-shim.mjs smoke    <pigeonhole-web.wasm>   the smoke binary, /tmp in the pool
//   node run-shim.mjs reload   <pigeonhole_web.wasm>   two module instances on one pool:
//                                                      a page load, then a reload
//   node run-shim.mjs snapshot <pigeonhole_web.wasm>   enough puts for a snapshot, a tick
//   node run-shim.mjs events   <pigeonhole_web.wasm>   every event the demo draws, from a
//                                                      cluster (the wizard's), every line it reads
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
async function node(env = {}) {
  const m = await instance("/data", env);
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
  // The node's log as the demo reads it (demo/events.js): every message it
  // reads must still come out of the real node's lines, and every kind of
  // event it draws. A cluster of three as the wizard makes it, its links
  // direct and over the chat; node 2's log on Discord, with a lease.
  const {EVENT_TYPES, MESSAGES, readEvents, S3_OK, S3_TARGET} = await import("../../site/src/assets/demo/events.js");
  const {tomlOf} = await import("../../site/src/assets/demo/toml.js");
  const cluster = tomlOf({
    ...{channels: ["telegram", "discord"], copies: 2, quorum: 2, log: "telegram", snapshots: "telegram", every: 15, bots: 1, pace: "demo"},
    ...{
      nodes: 3,
      links: "mixed",
      others: [
        {log: "discord", snapshots: "discord"},
        {log: "local_disk", snapshots: "local_disk"},
      ],
    },
  })
    // Repair passes every second, so one comes after the check; elections
    // quicker, so that an outbox keeps its frames (twice the longest
    // election timeout) for less, and prunes them while this runs.
    .replace('interval = "10s"', 'interval = "1s"')
    .replace('heartbeat = "1s"', 'heartbeat = "250ms"')
    .replace('election_timeout_min = "4s"', 'election_timeout_min = "1s"')
    .replace('election_timeout_max = "8s"', 'election_timeout_max = "2s"');
  const env = {PH_DEMO_CLUSTER_KEY: "706967656f6e686f6c652064656d6f20636c75737465723a206e6f206b657900"};
  const seen = new Map(),
    said = new Set();
  const read = (lines, label) => {
    for (const l of lines) {
      const msg = l.fields?.message || "";
      if ((l.target || "").startsWith(S3_TARGET) && msg.endsWith(S3_OK)) said.add("<Op> ok");
      else said.add(msg);
    }
    for (const e of readEvents(lines)) {
      const kind = e.type === "posted" && e.preview ? "posted text" : e.type === "raft" ? `raft ${e.via} ${e.at}` : e.type;
      if (!seen.has(kind)) seen.set(kind, label);
    }
  };
  const m = await node(env);
  const call = async (request, body) => {
    const {reply, events} = await m.op(request, body);
    read(events, request.op);
    if (reply.error) throw new Error(`${request.op}: ${reply.error}`);
    return reply;
  };
  await call({op: "open", config: cluster});
  read(await m.idle(3000), "idle"); // heartbeats, over both kinds of links
  await call({op: "put", key: "text"}, new TextEncoder().encode("hello ".repeat(100000)));
  await call({op: "put", key: "random"}, crypto.getRandomValues(new Uint8Array(60000)));
  // A copy gone behind the node's back: a read that tries it fails over;
  // the check queues it for repair.
  await call({op: "wipe", store: "dc-1"});
  await call({op: "get", key: "random"});
  await call({op: "verify", store: "dc-1"});
  read(await m.idle(5000), "idle"); // the repair; the outboxes pruned
  // A reload whose page did not close: node 2's Discord log is still leased.
  const again = await node(env);
  const {events} = await again.op({op: "open", config: cluster});
  read(events, "reopen");
  const want = [...EVENT_TYPES.filter(k => k !== "raft"), "posted text", "raft chat sent", "raft chat read", "raft grpc sent", "raft chat answered"];
  // Every message, but a stream failing mid-read (a wiped copy fails as it
  // opens: "store call failed"), which nothing here can time.
  const lines = [...Object.values(MESSAGES).filter(m => m !== MESSAGES.failover), "<Op> ok"];
  const missing = [...want.filter(k => !seen.has(k)), ...lines.filter(m => !said.has(m)).map(m => `"${m}"`)];
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
