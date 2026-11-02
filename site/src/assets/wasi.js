// WASI preview1 for pigeonhole's wasm build, without the browser in it:
// web-worker.js runs it on OPFS, crates/web/run-shim.mjs on buffers in Node.
//
// Files live in a Pool: a set of handles with a synchronous read / write /
// getSize / truncate / flush (OPFS's FileSystemSyncAccessHandle, or a buffer).
// Each pool file starts with a 4 KiB header naming the file it holds (empty:
// free); the data follows. Creating, removing and opening a file are then
// synchronous, as WASI needs; only adding pool files is not (the worker does
// it between calls). Pool file 0 holds the list of directories.
export const HEADER = 4096;
const DIRS = "\u0000dirs";
const E = {SUCCESS: 0, BADF: 8, EXIST: 20, INVAL: 28, ISDIR: 31, NOENT: 44, NOSPC: 51, NOSYS: 52, NOTDIR: 54, NOTEMPTY: 55};
const enc = new TextEncoder(),
  dec = new TextDecoder();

export class Pool {
  constructor(handles) {
    this.slots = [];
    this.files = new Map(); // path → slot index
    this.dirs = new Set();
    this.add(handles);
    const d = this.slots[0];
    if (d.path === DIRS) {
      const n = this.size(0);
      if (n > 0) {
        const buf = new Uint8Array(n);
        this.read(0, buf, 0);
        for (const p of JSON.parse(dec.decode(buf))) this.dirs.add(p);
      }
    } else {
      this.claim(0, DIRS);
    }
  }

  add(handles) {
    for (const h of handles) {
      const i = this.slots.length;
      const head = new Uint8Array(HEADER);
      const got = h.getSize() >= 4 ? h.read(head, {at: 0}) : 0;
      const len = got >= 4 ? new DataView(head.buffer).getUint32(0, true) : 0;
      const path = len > 0 && len <= HEADER - 4 ? dec.decode(head.subarray(4, 4 + len)) : null;
      this.slots.push({h, path});
      if (path && path !== DIRS) this.files.set(path, i);
    }
  }

  free() {
    return this.slots.filter((s, i) => i > 0 && s.path === null).length;
  }

  claim(i, path) {
    const head = new Uint8Array(HEADER);
    const name = enc.encode(path);
    new DataView(head.buffer).setUint32(0, name.length, true);
    head.set(name, 4);
    const h = this.slots[i].h;
    h.truncate(HEADER);
    h.write(head, {at: 0});
    this.slots[i].path = path;
  }

  create(path) {
    const i = this.slots.findIndex((s, k) => k > 0 && s.path === null);
    if (i < 0) return -1;
    this.claim(i, path);
    this.files.set(path, i);
    return i;
  }

  remove(path) {
    const i = this.files.get(path);
    if (i === undefined) return;
    const h = this.slots[i].h;
    h.truncate(0);
    h.flush();
    this.slots[i].path = null;
    this.files.delete(path);
  }

  size(i) {
    return Math.max(0, this.slots[i].h.getSize() - HEADER);
  }
  read(i, dst, at) {
    return this.slots[i].h.read(dst, {at: at + HEADER});
  }
  write(i, src, at) {
    return this.slots[i].h.write(src, {at: at + HEADER});
  }
  truncate(i, n) {
    this.slots[i].h.truncate(n + HEADER);
  }
  flush(i) {
    this.slots[i].h.flush();
  }

  saveDirs() {
    const data = enc.encode(JSON.stringify([...this.dirs].sort()));
    this.truncate(0, data.length);
    this.write(0, data, 0);
    this.flush(0);
  }

  isDir(path) {
    if (this.dirs.has(path)) return true;
    const pre = `${path}/`;
    for (const p of this.dirs) if (p.startsWith(pre)) return true;
    for (const p of this.files.keys()) if (p.startsWith(pre)) return true;
    return false;
  }

  children(path) {
    const pre = `${path}/`,
      out = new Map();
    const take = (p, dir) => {
      if (!p.startsWith(pre)) return;
      const rest = p.slice(pre.length),
        cut = rest.indexOf("/");
      if (cut < 0) out.set(rest, out.get(rest) || dir);
      else out.set(rest.slice(0, cut), true);
    };
    for (const p of this.files.keys()) take(p, false);
    for (const p of this.dirs) take(p, true);
    return [...out].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  }

  close() {
    for (const s of this.slots) s.h.close?.();
  }
}

// A pool handle over memory: FileSystemSyncAccessHandle's shape.
export class MemoryHandle {
  constructor() {
    this.buf = new Uint8Array(0);
    this.len = 0;
  }
  getSize() {
    return this.len;
  }
  grow(n) {
    if (n <= this.buf.length) return;
    const b = new Uint8Array(Math.max(n, this.buf.length * 2, 8192));
    b.set(this.buf.subarray(0, this.len));
    this.buf = b;
  }
  read(dst, {at}) {
    const n = Math.max(0, Math.min(dst.length, this.len - at));
    dst.set(this.buf.subarray(at, at + n));
    return n;
  }
  write(src, {at}) {
    this.grow(at + src.length);
    this.buf.set(src, at);
    this.len = Math.max(this.len, at + src.length);
    return src.length;
  }
  truncate(n) {
    this.grow(n);
    if (n > this.len) this.buf.fill(0, this.len, n);
    this.len = n;
  }
  flush() {}
  close() {}
}

function concat(parts) {
  const out = new Uint8Array(parts.reduce((a, p) => a + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function normalize(base, rel) {
  const parts = (rel.startsWith("/") ? rel : `${base}/${rel}`).split("/");
  const out = [];
  for (const p of parts) {
    if (!p || p === ".") continue;
    if (p === "..") out.pop();
    else out.push(p);
  }
  return `/${out.join("/")}`;
}

// The WASI imports, with stdin given per call and stdout cut into lines.
export class Wasi {
  constructor({pool = null, preopen = "/data", env = {}} = {}) {
    this.pool = pool;
    this.preopen = preopen;
    this.env = Object.entries(env).map(([k, v]) => `${k}=${v}`);
    this.fds = new Map();
    if (pool) this.fds.set(3, {dir: preopen});
    this.nextFd = 4;
    this.stdin = new Uint8Array(0);
    this.stdinPos = 0;
    this.partial = []; // stdout after its last newline
    this.lines = [];
    this.memory = null;
    // Where the module's yield flag is (ph_yield_flag), and when its next
    // timer is due (performance.now(); Infinity: none) as of its last park.
    this.yieldFlag = 0;
    this.nextWake = Infinity;
  }

  setStdin(bytes) {
    this.stdin = bytes;
    this.stdinPos = 0;
  }
  takeLines() {
    const l = this.lines;
    this.lines = [];
    return l;
  }
  writeStdout(chunk) {
    let start = 0;
    for (let nl = chunk.indexOf(10); nl >= 0; nl = chunk.indexOf(10, start)) {
      this.partial.push(chunk.subarray(start, nl));
      this.lines.push(dec.decode(concat(this.partial)));
      this.partial = [];
      start = nl + 1;
    }
    if (start < chunk.length) this.partial.push(chunk.subarray(start));
  }

  imports() {
    const v = () => new DataView(this.memory.buffer);
    const b = () => new Uint8Array(this.memory.buffer);
    const str = (ptr, len) => dec.decode(b().slice(ptr, ptr + len));
    const iovs = (ptr, n) => {
      const out = [],
        dv = v();
      for (let i = 0; i < n; i++) out.push([dv.getUint32(ptr + 8 * i, true), dv.getUint32(ptr + 8 * i + 4, true)]);
      return out;
    };
    const now = id => (id === 0 ? BigInt(Date.now()) * 1000000n : BigInt(Math.round(performance.now() * 1e6)));
    const full = (fd, rel) => {
      const d = this.fds.get(fd);
      return d && d.dir !== undefined ? normalize(d.dir, rel) : null;
    };
    const stat = (buf, type, size) => {
      const dv = v();
      for (let k = 0; k < 64; k++) dv.setUint8(buf + k, 0);
      dv.setUint8(buf + 16, type);
      dv.setBigUint64(buf + 24, 1n, true);
      dv.setBigUint64(buf + 32, BigInt(size), true);
    };
    const exists = p => p === this.preopen || this.pool.files.has(p) || this.pool.isDir(p);
    const parentOk = p => {
      const parent = p.slice(0, p.lastIndexOf("/")) || "/";
      return parent === this.preopen || this.pool.isDir(parent);
    };
    return {
      args_get: () => 0,
      args_sizes_get: (c, s) => {
        v().setUint32(c, 0, true);
        v().setUint32(s, 0, true);
        return 0;
      },
      environ_sizes_get: (c, s) => {
        v().setUint32(c, this.env.length, true);
        v().setUint32(
          s,
          this.env.reduce((a, e) => a + enc.encode(e).length + 1, 0),
          true,
        );
        return 0;
      },
      environ_get: (ptrs, buf) => {
        let off = buf;
        this.env.forEach((e, i) => {
          const bytes = enc.encode(`${e}\0`);
          v().setUint32(ptrs + 4 * i, off, true);
          b().set(bytes, off);
          off += bytes.length;
        });
        return 0;
      },
      clock_time_get: (id, _p, out) => {
        v().setBigUint64(out, now(id), true);
        return 0;
      },
      random_get: (ptr, len) => {
        for (let o = 0; o < len; o += 65536) crypto.getRandomValues(b().subarray(ptr + o, ptr + Math.min(len, o + 65536)));
        return 0;
      },
      sched_yield: () => 0,
      proc_exit: code => {
        throw new Error(`proc_exit(${code})`);
      },
      poll_oneoff: (inPtr, outPtr, nsubs, nevents) => {
        const dv = v();
        let wake = Infinity;
        for (let i = 0; i < nsubs; i++) {
          const s = inPtr + 48 * i;
          if (dv.getUint8(s + 8) !== 0) continue;
          const id = dv.getUint32(s + 16, true),
            t = dv.getBigUint64(s + 24, true),
            abs = dv.getUint16(s + 40, true) & 1;
          const ns = abs ? Number(t - now(id)) : Number(t);
          wake = Math.min(wake, performance.now() + Math.max(0, ns) / 1e6);
        }
        // The module is giving the thread back (ph_run): note when it
        // wants to run again, say so (the flag's second byte), wait for
        // nothing. Else (a module that blocks,
        // as the smoke binary does) spin until the earliest timer.
        if (this.yieldFlag && b()[this.yieldFlag]) {
          this.nextWake = wake;
          b()[this.yieldFlag + 1] = 1;
        } else if (wake !== Infinity)
          while (performance.now() < wake) {
            /* spin */
          }
        for (let i = 0; i < nsubs; i++) {
          const s = inPtr + 48 * i,
            e = outPtr + 32 * i;
          for (let k = 0; k < 32; k++) dv.setUint8(e + k, 0);
          dv.setBigUint64(e, dv.getBigUint64(s, true), true);
          dv.setUint8(e + 10, dv.getUint8(s + 8));
        }
        dv.setUint32(nevents, nsubs, true);
        return 0;
      },

      fd_prestat_get: (fd, buf) => {
        const d = this.fds.get(fd);
        if (fd !== 3 || !d) return E.BADF;
        v().setUint8(buf, 0);
        v().setUint32(buf + 4, enc.encode(this.preopen).length, true);
        return 0;
      },
      fd_prestat_dir_name: (fd, ptr, len) => {
        if (fd !== 3 || !this.fds.has(3)) return E.BADF;
        b().set(enc.encode(this.preopen).subarray(0, len), ptr);
        return 0;
      },
      fd_fdstat_get: (fd, out) => {
        const dv = v();
        for (let k = 0; k < 24; k++) dv.setUint8(out + k, 0);
        if (fd <= 2) dv.setUint8(out, 2);
        else {
          const d = this.fds.get(fd);
          if (!d) return E.BADF;
          dv.setUint8(out, d.dir !== undefined ? 3 : 4);
        }
        dv.setBigUint64(out + 8, 0xffffffffffffffffn, true);
        dv.setBigUint64(out + 16, 0xffffffffffffffffn, true);
        return 0;
      },
      fd_write: (fd, ptr, n, written) => {
        let total = 0;
        for (const [buf, len] of iovs(ptr, n)) {
          const chunk = b().slice(buf, buf + len);
          if (fd === 1) {
            this.writeStdout(chunk);
          } else if (fd === 2) {
            console.warn(dec.decode(chunk));
          } else {
            const f = this.fds.get(fd);
            if (!f || f.slot === undefined) return E.BADF;
            if (f.append) f.pos = this.pool.size(f.slot);
            this.pool.write(f.slot, chunk, f.pos);
            f.pos += len;
          }
          total += len;
        }
        v().setUint32(written, total, true);
        return 0;
      },
      fd_read: (fd, ptr, n, read) => {
        let total = 0;
        for (const [buf, len] of iovs(ptr, n)) {
          let got;
          if (fd === 0) {
            got = Math.min(len, this.stdin.length - this.stdinPos);
            b().set(this.stdin.subarray(this.stdinPos, this.stdinPos + got), buf);
            this.stdinPos += got;
          } else {
            const f = this.fds.get(fd);
            if (!f || f.slot === undefined) return E.BADF;
            const dst = new Uint8Array(len);
            got = this.pool.read(f.slot, dst, f.pos);
            b().set(dst.subarray(0, got), buf);
            f.pos += got;
          }
          total += got;
          if (got < len) break;
        }
        v().setUint32(read, total, true);
        return 0;
      },
      fd_seek: (fd, offset, whence, out) => {
        const f = this.fds.get(fd);
        if (!f || f.slot === undefined) return E.BADF;
        const base = whence === 0 ? 0 : whence === 1 ? f.pos : this.pool.size(f.slot);
        const pos = base + Number(offset);
        if (pos < 0) return E.INVAL;
        f.pos = pos;
        v().setBigUint64(out, BigInt(pos), true);
        return 0;
      },
      fd_tell: (fd, out) => {
        const f = this.fds.get(fd);
        if (!f || f.slot === undefined) return E.BADF;
        v().setBigUint64(out, BigInt(f.pos), true);
        return 0;
      },
      fd_sync: fd => {
        const f = this.fds.get(fd);
        if (f && f.slot !== undefined) this.pool.flush(f.slot);
        return 0;
      },
      fd_datasync: fd => {
        const f = this.fds.get(fd);
        if (f && f.slot !== undefined) this.pool.flush(f.slot);
        return 0;
      },
      fd_close: fd => (this.fds.delete(fd) ? 0 : E.BADF),
      fd_filestat_get: (fd, buf) => {
        const f = this.fds.get(fd);
        if (!f) return E.BADF;
        if (f.dir !== undefined) stat(buf, 3, 0);
        else stat(buf, 4, this.pool.size(f.slot));
        return 0;
      },
      fd_filestat_set_size: (fd, size) => {
        const f = this.fds.get(fd);
        if (!f || f.slot === undefined) return E.BADF;
        this.pool.truncate(f.slot, Number(size));
        return 0;
      },
      path_filestat_get: (fd, _flags, ptr, len, buf) => {
        const p = full(fd, str(ptr, len));
        if (p === null) return E.BADF;
        if (this.pool.files.has(p)) stat(buf, 4, this.pool.size(this.pool.files.get(p)));
        else if (p === this.preopen || this.pool.isDir(p)) stat(buf, 3, 0);
        else return E.NOENT;
        return 0;
      },
      path_open: (fd, _dirflags, ptr, len, oflags, _rb, _ri, fdflags, out) => {
        const p = full(fd, str(ptr, len));
        if (p === null) return E.BADF;
        const creat = oflags & 1,
          directory = oflags & 2,
          excl = oflags & 4,
          trunc = oflags & 8;
        const isDir = p === this.preopen || (!this.pool.files.has(p) && this.pool.isDir(p));
        if (isDir) {
          if (creat && excl) return E.EXIST;
          const id = this.nextFd++;
          this.fds.set(id, {dir: p});
          v().setUint32(out, id, true);
          return 0;
        }
        if (directory) return this.pool.files.has(p) ? E.NOTDIR : E.NOENT;
        let slot = this.pool.files.get(p);
        if (slot === undefined) {
          if (!creat) return E.NOENT;
          if (!parentOk(p)) return E.NOENT;
          slot = this.pool.create(p);
          if (slot < 0) return E.NOSPC;
        } else {
          if (creat && excl) return E.EXIST;
          if (trunc) this.pool.truncate(slot, 0);
        }
        const id = this.nextFd++;
        this.fds.set(id, {path: p, slot, pos: 0, append: !!(fdflags & 1)});
        v().setUint32(out, id, true);
        return 0;
      },
      path_create_directory: (fd, ptr, len) => {
        const p = full(fd, str(ptr, len));
        if (p === null) return E.BADF;
        if (exists(p)) return E.EXIST;
        if (!parentOk(p)) return E.NOENT;
        this.pool.dirs.add(p);
        this.pool.saveDirs();
        return 0;
      },
      path_remove_directory: (fd, ptr, len) => {
        const p = full(fd, str(ptr, len));
        if (p === null) return E.BADF;
        if (this.pool.files.has(p)) return E.NOTDIR;
        if (!this.pool.isDir(p)) return E.NOENT;
        if (this.pool.children(p).length) return E.NOTEMPTY;
        this.pool.dirs.delete(p);
        this.pool.saveDirs();
        return 0;
      },
      path_unlink_file: (fd, ptr, len) => {
        const p = full(fd, str(ptr, len));
        if (p === null) return E.BADF;
        if (!this.pool.files.has(p)) return this.pool.isDir(p) ? E.ISDIR : E.NOENT;
        this.pool.remove(p);
        return 0;
      },
      fd_readdir: (fd, buf, bufLen, cookie, used) => {
        const d = this.fds.get(fd);
        if (!d || d.dir === undefined) return E.BADF;
        const list = this.pool.children(d.dir);
        let off = 0;
        for (let i = Number(cookie); i < list.length; i++) {
          const [name, isDir] = list[i];
          const nm = enc.encode(name);
          const ent = new Uint8Array(24 + nm.length);
          const dv = new DataView(ent.buffer);
          dv.setBigUint64(0, BigInt(i + 1), true);
          dv.setBigUint64(8, BigInt(i + 1), true);
          dv.setUint32(16, nm.length, true);
          dv.setUint8(20, isDir ? 3 : 4);
          ent.set(nm, 24);
          const take = Math.min(ent.length, bufLen - off);
          b().set(ent.subarray(0, take), buf + off);
          off += take;
          if (off >= bufLen) break;
        }
        v().setUint32(used, off, true);
        return 0;
      },
    };
  }
}
