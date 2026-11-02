// The node behind the page: the demo's (pigeonhole compiled to wasm, in a
// worker: web-worker.js), or a real node when the page is served from its
// admin address ([admin] ui_dir), over HTTP to /admin/ops. The ops are the
// same (pigeonhole-admin runs them in both).
//
// Either way: {admin, started, call(request, body?, onEvents?) → {reply,
// events}, onTick(fn)}; calls run one at a time, in order. With onEvents, a
// call's events go to it as the node logs them (a slow call's flights fly
// as they happen), and the reply's events are what is left.
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
  // id → {done, onEvents, events}
  const waiting = new Map();
  let nextId = 1,
    tick = () => {},
    onWaiting = () => {},
    started;
  worker.onmessage = ({data}) => {
    const w = waiting.get(data.id);
    if (data.type === "started") started(data);
    else if (data.type === "waiting") onWaiting();
    else if (data.type === "tick") tick(data.events);
    else if (data.type === "events") {
      if (w?.onEvents) w.onEvents(data.events);
      else w?.events.push(...data.events);
    } else if (w) {
      waiting.delete(data.id);
      w.done({...data, events: [...w.events, ...data.events]});
    }
  };
  // Leaving (a reload, the other language): the node closes as a server
  // does on SIGTERM, releasing a lease log, then the worker lets go of the
  // files and ends (web-worker.js), so the next page boots at once.
  addEventListener("pagehide", () => worker.postMessage({id: 0, op: "close"}));
  // Back from the back-forward cache: that node is closed; start anew.
  addEventListener("pageshow", e => e.persisted && location.reload());
  return {
    admin: false,
    // {reply: {config, demo, saved, persistent, why} | {error}, events}
    started: new Promise(res => (started = res)),
    call: serial(
      (request, body, onEvents) =>
        new Promise(done => {
          const id = nextId++;
          waiting.set(id, {done, onEvents, events: []});
          worker.postMessage({id, ...request, body}, body ? [body] : []);
        }),
    ),
    // The worker waits for the files the page just left still holds.
    onWaiting: fn => {
      onWaiting = fn;
    },
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
    onWaiting: () => {},
  };
}
