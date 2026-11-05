// The node behind the page: the demo's (pigeonhole compiled to wasm, in a
// worker: web-worker.js), or a real node when the page is served from its
// admin address ([admin] ui_dir), over HTTP to /admin/ops. The ops are the
// same (pigeonhole-admin runs them in both; lib/ops.js lists them).
//
// Either way: {admin, call(request, body?, onEvents?) → {reply, events},
// onTick(fn), onWaiting(fn), now(), close()}; calls run one at a time, in
// order. The worker's also has speed(value), pause(on), step(one),
// onClock(fn), and onLog(fn): fn(lines) gets every line the node logs, as
// they come (those before it was set first).
// With onEvents, a call's events go to it as the node logs them (a slow
// call's flights fly as they happen), and the reply's events are what is
// left. The worker's also has `started`: the module up, {reply, events}.
//
// The worker's node runs on a clock of its own (web-worker.js): slowed
// down by speed(value), stopped by pause(true), moved on by step(one)
// timer by timer until the node does something drawn (or by `one` timer);
// onClock's fn hears {paused, now, next} after a pause or a step. now():
// the node's time as the page reckons it (a copy of that clock, caught up
// with every batch's `at`), on performance.now()'s scale; a real node's:
// the wall's.
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
    onClock = () => {},
    early = [], // the node's lines before onLog
    logged = lines => early.push(...lines),
    started;
  // The node's clock as the page reckons it.
  const clock = new Clock();
  // Each message by its type (lib/ops.js MSG); a call's by its id.
  worker.onmessage = ({data}) => {
    const w = waiting.get(data.id);
    if (data.events?.length) logged(data.events);
    switch (data.type) {
      case MSG.started:
        return started({reply: data.reply, events: data.events});
      case MSG.waiting:
        return onWaiting();
      case MSG.tick:
        clock.advance(data.at);
        return tick(data.events);
      case MSG.events:
        clock.advance(data.at);
        if (w?.onEvents) w.onEvents(data.events);
        else w?.events.push(...data.events);
        return;
      case MSG.clock:
        clock.advance(data.now);
        return onClock(data);
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
    // queued behind a call: the page will not wait; nobody waits for its
    // answer. At full speed, the fakes answering at once: a close cut short
    // leaves a lease the next page must wait out.
    close: () => {
      send(ops.pause(false));
      send(ops.speed(1));
      send(ops.fake([0, 0], [0, 0]));
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
    now: () => clock.now(),
    speed(value) {
      clock.setSpeed(value);
      worker.postMessage(ops.speed(value));
    },
    pause(on) {
      if (on) clock.hold();
      else clock.release();
      worker.postMessage(ops.pause(on));
    },
    step: one => worker.postMessage(ops.step(one)),
    onClock: fn => {
      onClock = fn;
    },
    onLog(fn) {
      fn(early);
      early = [];
      logged = fn;
    },
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
    now: () => performance.now(),
  };
}
