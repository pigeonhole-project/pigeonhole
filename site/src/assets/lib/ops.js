// The node's op protocol, in one place: the ops the page sends, the
// messages the worker answers with, and the framing, the same over HTTP
// (/admin/ops on a real node) and into the wasm (stdin): a JSON line with
// the body's length, then the body; out come JSON lines, the node's log and
// one {"reply": …} (the wasm's: {"call": n, "reply": …}).
//
// The ops are pigeonhole-admin's (crates/admin), the same on a real node and
// in the demo; the demo's worker adds its own (crates/web, web.rs): `open`,
// `restart` (a config, or the one kept: every node boots on it), `close`
// (the page is going: the node closes, the worker ends), `isolate` (a node
// of the cluster cut off); `reset` is the worker's alone (web-worker.js:
// its files go, no node behind it). A request may name the `node` of a
// cluster it goes to; none: the leader.

const OP_CLOSE = "close";
export const OP_RESET = "reset";

// Each op the page sends, as a request (a body, if any, goes beside it).
export const ops = {
  // The demo's worker (web.rs): a config, or the one the browser keeps.
  open: config => (config == null ? {op: "open"} : {op: "open", config}),
  restart: config => ({op: "restart", config}),
  close: () => ({op: OP_CLOSE}),
  reset: () => ({op: OP_RESET}),
  isolate: (node, down) => ({op: "isolate", node, down}),
  // S3 (the body: the object's bytes).
  put: (bucket, key) => ({op: "put", bucket, key}),
  get: (bucket, key, range) => ({op: "get", bucket, key, range}),
  copy: (bucket, key, to) => ({op: "copy", bucket, key, to}),
  remove: (bucket, key) => ({op: "delete", bucket, key}),
  list: (bucket, node) => ({op: "list", bucket, node}),
  // The node as it is.
  info: () => ({op: "info"}),
  channels: node => ({op: "channels", node}),
  log: limit => ({op: "log", limit}),
  background: node => ({op: "background", node}),
  budgets: node => ({op: "budgets", node}),
  cluster: node => ({op: "cluster", node}),
  // The dynamic config (an entry in the log, through the leader).
  dynamic: node => ({op: "dynamic", node}),
  dynamicSet: (key, value, node) => ({op: "dynamic_set", key, value, node}),
  dynamicUnset: key => ({op: "dynamic_unset", key}),
  runTask: task => ({op: "run_task", task}),
  // A data channel's faults, and what finds and mends them.
  outage: (store, down) => ({op: "outage", store, down}),
  wipe: store => ({op: "wipe", store}),
  deleteMessage: (store, message) => ({op: "delete_message", store, message}),
  verify: store => ({op: "verify", store}),
  repair: () => ({op: "repair"}),
};

// The worker's messages to the page (web-worker.js → demo/node.js); the
// page sends {id, ...request, body?}.
export const MSG = {
  waiting: "waiting", // the page just left still holds the files
  started: "started", // the module is up: {reply: {config, demo, saved, persistent, why} | {error}, events}
  events: "events", // what the node logged during call `id`, as it went: {id, events}
  reply: "reply", // call `id` answered: {id, reply, events: [], ms}
  tick: "tick", // what the node logged on its own between calls: {events}
};
export const msg = {
  waiting: () => ({type: MSG.waiting, why: "files"}),
  started: (reply, events) => ({type: MSG.started, reply, events}),
  events: (id, events) => ({type: MSG.events, id, events}),
  reply: (id, reply, ms = 0) => ({type: MSG.reply, id, reply, events: [], ms}),
  tick: events => ({type: MSG.tick, events}),
};

export function frame(request, body) {
  const len = body ? body.byteLength : 0;
  const head = new TextEncoder().encode(`${JSON.stringify({...request, len})}\n`);
  const out = new Uint8Array(head.length + len);
  out.set(head);
  if (body) out.set(body instanceof Uint8Array ? body : new Uint8Array(body), head.length);
  return out;
}

const parse = line => {
  try {
    return JSON.parse(line);
  } catch {
    return null; // a stray print: not the protocol's
  }
};

// Output lines → {reply, events}; a line that is not JSON is dropped.
export function unframe(lines) {
  const parsed = lines.map(parse).filter(l => l != null);
  const reply = parsed.find(l => "reply" in l)?.reply;
  return {reply, events: parsed.filter(l => !("reply" in l))};
}

// The module's output, in order, as the worker's messages: log lines go
// with the oldest call in flight (else they are the node's own: a tick), an
// answer ({call, reply}) ends its call. `calls`: the module's call number →
// {id, op, t0}, the calls answered taken out. `closed`: `close` was answered
// (the worker ends; nobody waits for that answer).
export function deliver(lines, calls, now = performance.now()) {
  const messages = [];
  let events = [],
    closed = false;
  const flush = () => {
    if (!events.length) return;
    const call = calls.values().next().value;
    messages.push(call ? msg.events(call.id, events) : msg.tick(events));
    events = [];
  };
  for (const l of lines.map(parse)) {
    if (l == null) continue;
    if (!("call" in l)) {
      events.push(l);
      continue;
    }
    flush();
    const call = calls.get(l.call);
    calls.delete(l.call);
    if (call?.op === OP_CLOSE) closed = true;
    else if (call) messages.push(msg.reply(call.id, l.reply, now - call.t0));
  }
  flush();
  return {messages, closed};
}
