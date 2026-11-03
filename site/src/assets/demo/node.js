// The node behind the page: the demo's (pigeonhole compiled to wasm, in a
// worker: web-worker.js), or a real node when the page is served from its
// admin address ([admin] ui_dir), over HTTP to /admin/ops. The ops are the
// same (pigeonhole-admin runs them in both; lib/ops.js lists them).
//
// Either way: {admin, call(request, body?, onEvents?) → {reply, events},
// onTick(fn), onWaiting(fn), onDrawn(fn), now(), close()}; calls run one at
// a time, in order.
// With onEvents, a call's events go to it as the node logs them (a slow
// call's flights fly as they happen), and the reply's events are what is
// left. The worker's also has `started`: the module up, {reply, events}.
//
// The worker's node keeps time by the page (web-worker.js): its clock
// stands while a batch of its events is being drawn. A batch handed to
// onEvents or onTick is drawn once onDrawn's fn (asked right after) says
// so: a promise, or nothing for at once; a batch kept for the reply is
// drawn at once. now(): the node's time as the page reckons it, on
// performance.now()'s scale (a real node's: the wall's).
import {frame, MSG, ops} from "../lib/ops.js";
import {Clock} from "../wasi.js";

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
  return (await isAdmin()) ? httpNode() : workerNode(new Worker(workerUrl, {type: "module"}));
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

// The node in `worker` (a Worker, or anything with postMessage and onmessage).
export function workerNode(worker) {
  // id → {done, onEvents, events}
  const waiting = new Map();
  let nextId = 1,
    tick = () => {},
    onWaiting = () => {},
    drawing = () => null,
    started;
  // The node's clock as the page holds it: while a batch is out undrawn.
  const clock = new Clock(),
    out = new Set();
  // Batch `seq` handed over (`shown`: to a view, which draws it): `go` once drawn.
  function batch(seq, shown) {
    if (seq == null) return;
    out.add(seq);
    clock.hold();
    Promise.resolve(shown ? drawing() : null).then(() => {
      worker.postMessage(ops.go(seq));
      out.delete(seq);
      if (!out.size) clock.release();
    });
  }
  // Each message by its type (lib/ops.js MSG); a call's by its id.
  worker.onmessage = ({data}) => {
    const w = waiting.get(data.id);
    switch (data.type) {
      case MSG.started:
        return started({reply: data.reply, events: data.events});
      case MSG.waiting:
        return onWaiting();
      case MSG.tick:
        tick(data.events);
        return batch(data.seq, true);
      case MSG.events:
        if (w?.onEvents) w.onEvents(data.events);
        else w?.events.push(...data.events);
        return batch(data.seq, !!w?.onEvents);
      case MSG.reply:
        if (!w) return;
        waiting.delete(data.id);
        return w.done({reply: data.reply, events: [...w.events, ...data.events]});
      default:
        console.warn("the worker said what the page does not know:", data);
    }
  };
  const send = (request, body) => {
    const id = nextId++;
    worker.postMessage({id, ...request, body}, body ? [body] : []);
    return id;
  };
  return {
    admin: false,
    // {reply: {config, demo, saved, persistent, why} | {error}, events}
    started: new Promise(res => {
      started = res;
    }),
    call: serial(
      (request, body, onEvents) =>
        new Promise(done => {
          // The worker answers later (a message): the id is waited on by then.
          waiting.set(send(request, body), {done, onEvents, events: []});
        }),
    ),
    // Leaving (a reload, the other language): the node closes as a server
    // does on SIGTERM, releasing a lease log, then the worker lets go of the
    // files and ends (web-worker.js), so the next page boots at once. Not
    // queued behind a call: the page will not wait; nobody waits for its answer.
    close: () => {
      send(ops.close());
    },
    // The worker waits for the files the page just left still holds.
    onWaiting: fn => {
      onWaiting = fn;
    },
    // What the node logged on its own between calls (a snapshot, a repair pass).
    onTick: fn => {
      tick = fn;
    },
    // fn(): when what the views just drew of a batch is done (a promise), or null.
    onDrawn: fn => {
      drawing = fn;
    },
    now: () => clock.now(),
  };
}

// A real node: its log stays on the server, so a call has no events.
function httpNode() {
  return {
    admin: true,
    call: serial(async (request, body) => {
      try {
        const r = await fetch("/admin/ops", {method: "POST", body: frame(request, body)});
        return {reply: (await r.json()).reply, events: []};
      } catch (e) {
        return {reply: {error: String(e)}, events: []};
      }
    }),
    close: () => {},
    onTick: () => {},
    onWaiting: () => {},
    onDrawn: () => {},
    now: () => performance.now(),
  };
}
