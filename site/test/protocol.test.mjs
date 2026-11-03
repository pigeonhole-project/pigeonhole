// The worker protocol (lib/ops.js): the page's side (demo/node.js) routing
// the worker's messages, the worker's side (deliver) attributing the
// module's lines to the calls in flight.
import assert from "node:assert/strict";
import {test} from "node:test";
import {workerNode} from "../src/assets/demo/node.js";
import {deliver, MSG, msg, ops} from "../src/assets/lib/ops.js";

// A worker that keeps what the page sent; `say(m)` is the worker answering.
function fakeWorker() {
  const w = {sent: [], onmessage: null};
  w.postMessage = data => w.sent.push(data);
  w.say = data => w.onmessage({data});
  return w;
}
const tickOnce = () => new Promise(r => setTimeout(r, 0));

test("node: a reply ends its call with the events that came before it", async () => {
  const w = fakeWorker();
  const node = workerNode(w);
  const got = node.call(ops.list("demo"));
  await tickOnce();
  assert.deepEqual(w.sent, [{id: 1, op: "list", bucket: "demo", node: undefined, body: undefined}]);
  w.say(msg.events(1, [{a: 1}]));
  w.say(msg.reply(1, {objects: []}));
  assert.deepEqual(await got, {reply: {objects: []}, events: [{a: 1}]});
});

test("node: calls go one at a time; onEvents gets a call's events as they come", async () => {
  const w = fakeWorker();
  const node = workerNode(w);
  const live = [];
  const first = node.call(ops.background(), null, evs => live.push(...evs));
  const second = node.call(ops.budgets());
  await tickOnce();
  assert.equal(w.sent.length, 1, "the second waits for the first");
  w.say(msg.events(1, [{x: 1}]));
  assert.deepEqual(live, [{x: 1}]);
  w.say(msg.reply(1, {ok: true}));
  assert.deepEqual((await first).events, [], "the events went to onEvents");
  await tickOnce();
  assert.equal(w.sent[1].op, "budgets");
  w.say(msg.reply(2, {budgets: []}));
  assert.deepEqual((await second).reply, {budgets: []});
});

test("node: each message by its type; an unknown type is no call's reply", async () => {
  const w = fakeWorker();
  const node = workerNode(w);
  const ticks = [];
  let waited = 0;
  node.onTick(evs => ticks.push(...evs));
  node.onWaiting(() => waited++);
  w.say(msg.waiting());
  w.say(msg.tick([{t: 1}]));
  w.say(msg.started({config: "", saved: false}, [{s: 1}]));
  assert.deepEqual(await node.started, {reply: {config: "", saved: false}, events: [{s: 1}]});
  assert.equal(waited, 1);
  assert.deepEqual(ticks, [{t: 1}]);

  const call = node.call(ops.info());
  await tickOnce();
  const warn = console.warn;
  console.warn = () => {};
  try {
    w.say({type: "surprise", id: 1, reply: {wrong: true}});
  } finally {
    console.warn = warn;
  }
  w.say(msg.reply(1, {config: "x"}));
  assert.deepEqual((await call).reply, {config: "x"});
});

test("node: close goes out at once, not behind a call, with an id of its own", async () => {
  const w = fakeWorker();
  const node = workerNode(w);
  node.call(ops.log(5));
  await tickOnce();
  node.close();
  assert.deepEqual(
    w.sent.map(m => [m.id, m.op]),
    [
      [1, "log"],
      [2, "close"],
    ],
  );
});

test("deliver: log lines go with the oldest call in flight, else they are a tick", () => {
  const calls = new Map([
    [7, {id: 1, op: "put", t0: 0}],
    [8, {id: 2, op: "list", t0: 5}],
  ]);
  const lines = [
    '{"fields":{"message":"a"}}',
    "not json",
    '{"call":7,"reply":{"ok":true}}',
    '{"fields":{"message":"b"}}',
    '{"call":8,"reply":{"objects":[]}}',
    '{"fields":{"message":"c"}}',
  ];
  const {messages, closed} = deliver(lines, calls, 10);
  assert.equal(closed, false);
  assert.deepEqual(messages, [
    msg.events(1, [{fields: {message: "a"}}]),
    msg.reply(1, {ok: true}, 10),
    msg.events(2, [{fields: {message: "b"}}]),
    msg.reply(2, {objects: []}, 5),
    msg.tick([{fields: {message: "c"}}]),
  ]);
  assert.equal(calls.size, 0);
});

test("deliver: close's answer ends the worker, and is no reply", () => {
  const calls = new Map([[3, {id: 9, op: "close", t0: 0}]]);
  const {messages, closed} = deliver(['{"call":3,"reply":{"closed":true}}'], calls, 1);
  assert.equal(closed, true);
  assert.deepEqual(messages, []);
  assert.deepEqual(Object.values(MSG).sort(), ["clock", "events", "reply", "started", "tick", "waiting"], "the messages node.js routes");
});
