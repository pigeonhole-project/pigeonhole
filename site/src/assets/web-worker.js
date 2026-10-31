// Runs pigeonhole (crates/web, wasm32-wasip1) in a Web Worker.
//
// The module needs ten WASI calls; they are here, over memory buffers:
// stdin is the request the page sent, stdout collects the log events and
// the answer. Messages: {op, key, to, range, body: ArrayBuffer} in,
// {events: [...], reply} out.

let mem, exports;
let stdin = new Uint8Array(0), stdinPos = 0;
let stdout = [];          // bytes of the current line
let lines = [];           // finished stdout lines
const dec = new TextDecoder(), enc = new TextEncoder();

const view = () => new DataView(mem.buffer);
const bytes = () => new Uint8Array(mem.buffer);
const now = id => id === 0 ? BigInt(Date.now()) * 1000000n : BigInt(Math.round(performance.now() * 1e6));

function iovs(ptr, n) {
  const v = view(), out = [];
  for (let i = 0; i < n; i++) out.push([v.getUint32(ptr + 8 * i, true), v.getUint32(ptr + 8 * i + 4, true)]);
  return out;
}

const wasi = {
  args_get: () => 0,
  args_sizes_get: (argc, size) => { view().setUint32(argc, 0, true); view().setUint32(size, 0, true); return 0; },
  environ_get: () => 0,
  environ_sizes_get: (count, size) => { view().setUint32(count, 0, true); view().setUint32(size, 0, true); return 0; },
  clock_time_get: (id, _precision, out) => { view().setBigUint64(out, now(id), true); return 0; },
  random_get: (ptr, len) => {
    for (let off = 0; off < len; off += 65536) crypto.getRandomValues(bytes().subarray(ptr + off, ptr + Math.min(len, off + 65536)));
    return 0;
  },
  fd_fdstat_get: (fd, out) => {
    if (fd > 2) return 8; // EBADF
    const v = view();
    for (let i = 0; i < 24; i++) v.setUint8(out + i, 0);
    v.setUint8(out, 2); // character device
    return 0;
  },
  fd_write: (fd, ptr, n, written) => {
    let total = 0;
    for (const [buf, len] of iovs(ptr, n)) {
      const chunk = bytes().slice(buf, buf + len);
      total += len;
      if (fd === 1) {
        for (const b of chunk) {
          if (b === 10) { lines.push(dec.decode(new Uint8Array(stdout))); stdout = []; } else stdout.push(b);
        }
      } else console.warn(dec.decode(chunk));
    }
    view().setUint32(written, total, true);
    return 0;
  },
  fd_read: (fd, ptr, n, read) => {
    if (fd !== 0) return 8;
    let total = 0;
    for (const [buf, len] of iovs(ptr, n)) {
      const take = Math.min(len, stdin.length - stdinPos);
      bytes().set(stdin.subarray(stdinPos, stdinPos + take), buf);
      stdinPos += take; total += take;
      if (take < len) break;
    }
    view().setUint32(read, total, true);
    return 0;
  },
  // Sleeps: the module waits on a clock only. A worker may block, so spin.
  poll_oneoff: (inPtr, outPtr, nsubs, nevents) => {
    const v = view();
    let wake = Infinity;
    for (let i = 0; i < nsubs; i++) {
      const s = inPtr + 48 * i;
      if (v.getUint8(s + 8) !== 0) continue; // not a clock
      const id = v.getUint32(s + 16, true), t = v.getBigUint64(s + 24, true), abs = v.getUint16(s + 40, true) & 1;
      const ns = abs ? Number(t - now(id)) : Number(t);
      wake = Math.min(wake, performance.now() + Math.max(0, ns) / 1e6);
    }
    if (wake !== Infinity) while (performance.now() < wake) { /* spin */ }
    for (let i = 0; i < nsubs; i++) {
      const s = inPtr + 48 * i, e = outPtr + 32 * i;
      for (let k = 0; k < 32; k++) v.setUint8(e + k, 0);
      v.setBigUint64(e, v.getBigUint64(s, true), true);
      v.setUint8(e + 10, v.getUint8(s + 8));
    }
    v.setUint32(nevents, nsubs, true);
    return 0;
  },
  sched_yield: () => 0,
  proc_exit: code => { throw new Error(`proc_exit(${code})`); },
};

function call(fn, request, body) {
  const head = enc.encode(JSON.stringify(request) + "\n");
  stdin = new Uint8Array(head.length + (body ? body.byteLength : 0));
  stdin.set(head);
  if (body) stdin.set(new Uint8Array(body), head.length);
  stdinPos = 0; lines = [];
  const t0 = performance.now();
  fn();
  const ms = performance.now() - t0;
  const parsed = lines.map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  const reply = parsed.find(l => "reply" in l)?.reply;
  return {events: parsed.filter(l => !("reply" in l)), reply, ms};
}

const ready = (async () => {
  const {instance} = await WebAssembly.instantiateStreaming(fetch("pigeonhole.wasm"), {wasi_snapshot_preview1: wasi});
  exports = instance.exports;
  mem = exports.memory;
  const out = call(exports.ph_open, {op: "open"});
  postMessage({id: 0, ...out});
})().catch(e => postMessage({id: 0, reply: {error: String(e)}, events: []}));

onmessage = async ({data}) => {
  await ready;
  const {id, body, ...request} = data;
  request.len = body ? body.byteLength : 0;
  try {
    postMessage({id, ...call(exports.ph_call, request, body)});
  } catch (e) {
    postMessage({id, reply: {error: String(e)}, events: []});
  }
};
