// The site's WASI shim (site/src/assets/wasi.js) in Node, its pool on
// buffers instead of OPFS:
//
//   node run-shim.mjs smoke  <pigeonhole-web.wasm>   the smoke binary, /tmp in the pool
//   node run-shim.mjs reload <pigeonhole_web.wasm>   two module instances on one pool:
//                                                    a page load, then a reload
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const here = fileURLToPath(new URL(".", import.meta.url));
vm.runInThisContext(await readFile(here + "../../site/src/assets/wasi.js", "utf8"));
const { Wasi, Pool, MemoryHandle } = globalThis.PHWasi;
const [mode, file] = process.argv.slice(2);
const module = await WebAssembly.compile(await readFile(file));
const handles = Array.from({ length: 256 }, () => new MemoryHandle());

async function instance(preopen, env = {}) {
  const wasi = new Wasi({ pool: new Pool(handles), preopen, env });
  const inst = await WebAssembly.instantiate(module, { wasi_snapshot_preview1: wasi.imports() });
  wasi.memory = inst.exports.memory;
  return { wasi, exports: inst.exports };
}

if (mode === "smoke") {
  const { wasi, exports } = await instance("/tmp", { TMPDIR: "/tmp" });
  exports._start();
  console.error(wasi.takeLines().filter(l => !l.startsWith("{")).join("\n"));
} else if (mode === "snapshot") {
  // The demo snapshots every 50 entries: 60 puts, then the log's base.
  const enc = new TextEncoder();
  const m = await instance("/data");
  const call = (request, body = new Uint8Array(0)) => {
    const head = enc.encode(JSON.stringify({ ...request, len: body.length }) + "\n");
    const input = new Uint8Array(head.length + body.length);
    input.set(head);
    input.set(body, head.length);
    m.wasi.setStdin(input);
    m.exports.ph_call();
    const reply = m.wasi.takeLines().map(l => JSON.parse(l)).find(l => "reply" in l).reply;
    if (reply.error) throw new Error(reply.error);
    return reply;
  };
  m.exports.ph_open();
  m.wasi.takeLines();
  call({ op: "open", config: await readFile(here + "pigeonhole-web/demo.toml", "utf8") });
  for (let i = 0; i < 60; i++) call({ op: "put", key: `k${i}` }, new Uint8Array(2000).fill(i));
  // The worker ticks while idle: a tick must run (and not panic).
  m.exports.ph_tick();
  m.wasi.takeLines();
  const log = call({ op: "log", limit: 3 });
  const snaps = call({ op: "channels" }).channels.find(c => c.role === "snapshots");
  console.error(`head ${log.head}, base ${JSON.stringify(log.base)}, snapshot channel ${snaps.messages.map(x => x.name).join(" ")}`);
} else if (mode === "reload") {
  const enc = new TextEncoder();
  const call = (m, request, body = new Uint8Array(0)) => {
    const head = enc.encode(JSON.stringify({ ...request, len: body.length }) + "\n");
    const input = new Uint8Array(head.length + body.length);
    input.set(head);
    input.set(body, head.length);
    m.wasi.setStdin(input);
    m.exports.ph_call();
    const lines = m.wasi.takeLines().map(l => JSON.parse(l));
    const reply = lines.find(l => "reply" in l).reply;
    if (reply.error) throw new Error(reply.error);
    return reply;
  };
  // The demo keeps its channels in /data/channels: the pool, here.
  const config = await readFile(here + "pigeonhole-web/demo.toml", "utf8");
  const body = new Uint8Array(3 << 20).map((_, i) => (i * 7) % 251);

  const first = await instance("/data");
  first.exports.ph_open();
  first.wasi.takeLines();
  const t0 = performance.now();
  call(first, { op: "open", config });
  call(first, { op: "put", key: "photo.jpg" }, body);
  call(first, { op: "put", key: "small.bin" }, body.subarray(0, 3000));
  console.error(`first load: 2 puts in ${Math.round(performance.now() - t0)} ms; pool ${handles.filter(h => h.len).length} files`);

  // A reload: a new instance, nothing of the first but the pool's files.
  const second = await instance("/data");
  second.exports.ph_open();
  second.wasi.takeLines();
  const t1 = performance.now();
  const open = call(second, { op: "open", config });
  const got = call(second, { op: "get", key: "photo.jpg" });
  const list = call(second, { op: "list" });
  console.error(`reload: ${open.replayed} entries replayed, photo.jpg ${got.len} B, objects ${JSON.stringify(list.objects)} in ${Math.round(performance.now() - t1)} ms`);
  if (got.len !== body.length || list.objects.length !== 2) throw new Error("the reload lost data");
  console.error("reload ok");
}
