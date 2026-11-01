// The node behind the page: the demo's (pigeonhole compiled to wasm, in a
// worker: web-worker.js), or a real node when the page is served from its
// admin address ([admin] ui_dir), over HTTP to /admin/ops. The ops are the
// same (pigeonhole-admin runs them in both).
//
// Either way: {admin, started, call(request, body?) → {reply, events},
// onTick(fn)}; calls run one at a time, in order.
import {frame} from "../lib/ops.js";

// Served by a node, /admin/config is its API (JSON); on a static site, a 404 page.
async function isAdmin() {
  try {
    const r = await fetch("/admin/config", {cache: "no-store"});
    return r.ok && (r.headers.get("content-type") || "").includes("json");
  } catch {
    return false;
  }
}

export async function connect(workerUrl) {
  return (await isAdmin()) ? httpNode() : workerNode(workerUrl);
}

// fn's calls one after the other: a call starts when the last one is done.
function serial(fn) {
  let queue = Promise.resolve();
  return (...args) => {
    const run = queue.then(() => fn(...args));
    queue = run.catch(() => {});
    return run;
  };
}

function workerNode(url) {
  const worker = new Worker(url, {type: "module"});
  const waiting = new Map();
  let nextId = 1,
    tick = () => {},
    started;
  worker.onmessage = ({data}) => {
    if (data.type === "started") started(data);
    else if (data.type === "tick") tick(data.events);
    else {
      waiting.get(data.id)?.(data);
      waiting.delete(data.id);
    }
  };
  // Let go of the files at once, for the next page (a reload, the other language).
  addEventListener("pagehide", () => worker.terminate());
  return {
    admin: false,
    // {reply: {config, demo, saved, persistent, why} | {error}, events}
    started: new Promise(res => (started = res)),
    call: serial(
      (request, body) =>
        new Promise(res => {
          const id = nextId++;
          waiting.set(id, res);
          worker.postMessage({id, ...request, body}, body ? [body] : []);
        }),
    ),
    // What the node logged on its own between calls (a snapshot, a repair pass).
    onTick: fn => {
      tick = fn;
    },
  };
}

function httpNode() {
  return {
    admin: true,
    started: Promise.resolve({reply: {}, events: []}),
    // A real node's log stays on the server: no events.
    call: serial(async (request, body) => {
      try {
        const r = await fetch("/admin/ops", {method: "POST", body: frame(request, body)});
        return {reply: (await r.json()).reply, events: []};
      } catch (e) {
        return {reply: {error: String(e)}, events: []};
      }
    }),
    onTick: () => {},
  };
}
