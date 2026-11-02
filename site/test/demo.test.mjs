// The demo's modules that hold no DOM: the node's events, the background
// tasks, the wizard's config, a channel as a chat, the log's lines.
import assert from "node:assert/strict";
import {beforeEach, test} from "node:test";
import {chatModel} from "../src/assets/demo/chat/model.js";
import {readEvents} from "../src/assets/demo/events.js";
import {flightsOf, ownerOf} from "../src/assets/demo/flights.js";
import {summary} from "../src/assets/demo/log-view.js";
import {makeRaftWatch} from "../src/assets/demo/raft.js";
import {describe, ms} from "../src/assets/demo/tasks.js";
import {divisors, sketch, tomlOf} from "../src/assets/demo/toml.js";

beforeEach(() => {
  globalThis.LANG = "en";
  globalThis.I18N = {};
});

const line = (message, fields = {}, spans = [], target = "pigeonhole") => ({target, fields: {message, ...fields}, spans});

test("readEvents: a text post takes the fake channel's preview", () => {
  const events = readEvents([
    line("fake channel: text", {preview: "漢字"}),
    line("posted", {store: "tg-1", message_id: 7, kind: "text", name: "ph-1.bin", bytes: "461"}),
    line("posted", {store: "tg-1", message_id: 8, kind: "document", name: "ph-2.bin", bytes: "4096"}),
  ]);
  assert.deepEqual(events, [
    {type: "posted", store: "tg-1", id: 7, kind: "text", name: "ph-1.bin", bytes: 461, preview: "漢字"},
    {type: "posted", store: "tg-1", id: 8, kind: "document", name: "ph-2.bin", bytes: 4096},
  ]);
});

test("readEvents: deletions, store calls, repair, the claim, S3", () => {
  const store = (op, bytes) => [{name: "store", instance: "dc-1", op, bytes}];
  const events = readEvents([
    line("deleted", {store: "tg-1", messages: "[3, 4]"}),
    line("store call", {}, store("get", "1024")),
    line("store call failed", {}, store("put")),
    line("repair pass complete", {repaired: 3}),
    line("claimed the metadata log", {generation: 2}),
    line("PutObject ok", {key: "k", size: 5}, [], "pigeonhole_gateway_s3::ops"),
  ]);
  assert.deepEqual(events, [
    {type: "deleted", store: "tg-1", ids: ["3", "4"]},
    {type: "call", store: "dc-1", op: "get", bytes: 1024},
    {type: "call_failed", store: "dc-1", op: "put"},
    {type: "repair_pass", repaired: 3},
    {type: "claimed", generation: 2},
    {type: "s3", key: "k", size: 5, op: "PutObject"},
  ]);
});

test("readEvents: a fake channel's own tablet is not the node's", () => {
  assert.deepEqual(readEvents([line("posted", {store: "x"}, [{name: "fake_channel"}])]), []);
});

test("ms reads durations; off is null", () => {
  assert.equal(ms("10s"), 10_000);
  assert.equal(ms("1m 30s"), 90_000);
  assert.equal(ms("250ms"), 250);
  assert.equal(ms("2h"), 7_200_000);
  assert.equal(ms("off"), null);
  assert.equal(ms(""), null);
});

test("describe: a task's clock and its work; the snapshot's entries", () => {
  const bg = {
    tasks: [
      {task: "repair", every: "10s", next_in_ms: 2500, work: [{chunk: 1, store: "tg-1"}]},
      {task: "sweep", every: "off", next_in_ms: null, work: []},
      {task: "snapshot", every: "1m", next_in_ms: 60_000, work: []},
    ],
    snapshot: {head: 70, base: 50, every: 50},
  };
  const repair = describe(bg, "repair");
  assert.equal(repair.toNext, 0.75);
  assert.equal(repair.n, 1);
  assert.equal(describe(bg, "sweep").period, null);
  assert.equal(describe(bg, "sweep").toNext, 0);
  const snap = describe(bg, "snapshot");
  assert.deepEqual([snap.n, snap.of, snap.base], [20, 50, 50]);
  assert.equal(describe(bg, "cool"), null);
});

const setup = (channels, copies, more = {}) => ({
  channels,
  copies,
  quorum: copies,
  log: "telegram",
  snapshots: "discord",
  every: 50,
  bots: 1,
  pace: "demo",
  ...more,
});

test("tomlOf: one channel holds the chunks itself", () => {
  const toml = tomlOf(setup(["telegram"], 1));
  assert.match(toml, /\[chunk\]\nstore = "tg-1"/);
  assert.doesNotMatch(toml, /\[\[replicas\]\]|\[\[shards\]\]/);
});

test("tomlOf: copies in every channel, a quorum", () => {
  const toml = tomlOf(setup(["telegram", "discord"], 2, {quorum: 1}));
  assert.match(toml, /\[\[replicas\]\]\nid = "copies"\nmembers = \["tg-1", "dc-1"\]\nwrite_quorum = 1/);
  assert.match(toml, /\[chunk\]\nstore = "copies"/);
});

test("tomlOf: shards of copies", () => {
  const toml = tomlOf(setup(["telegram", "discord", "telegram", "discord"], 2));
  assert.match(toml, /id = "copies-1"\nmembers = \["tg-1", "dc-1"\]/);
  assert.match(toml, /id = "copies-2"\nmembers = \["tg-2", "dc-2"\]/);
  assert.match(toml, /\[\[shards\]\]\nid = "shards"\nmembers = \["copies-1", "copies-2"\]/);
});

test("tomlOf: the log on Discord takes a lease; the real pace has no fast sends", () => {
  const toml = tomlOf(setup(["telegram"], 1, {log: "discord", pace: "real"}));
  assert.match(toml, /log = "dc-log"/);
  assert.match(toml, /lease = "5s"/);
  assert.doesNotMatch(toml, /rate_per_sec/);
});

test("tomlOf: the log and snapshots on the node's disk, one store for both", () => {
  const toml = tomlOf(setup(["telegram"], 1, {log: "local_disk", snapshots: "local_disk"}));
  assert.match(toml, /id = "node-disk"\nkind = "local_disk"\npath = "\/data\/node"/);
  assert.match(toml, /log = "node-disk"\nsnapshot = "node-disk"/);
  assert.doesNotMatch(toml, /tg-log|lease/);
  const sketched = sketch(setup(["telegram"], 1, {log: "local_disk", snapshots: "telegram"}));
  assert.deepEqual(
    sketched.slice(1).map(c => [c.store, c.kind]),
    [
      ["meta-log", "local_disk"],
      ["tg-snap", "telegram"],
    ],
  );
});

test("tomlOf: one file for a cluster, each [[node]] its own log and snapshots", () => {
  const others = [
    {log: "discord", snapshots: "discord"},
    {log: "local_disk", snapshots: "local_disk"},
  ];
  const toml = tomlOf(setup(["telegram", "discord"], 2, {nodes: 3, links: "mixed", others}));
  assert.match(toml, /\[cluster\]\nid = 1\nkey_env/, "no `node`: every node runs this file");
  assert.match(toml, /\[\[node\]\]\nid = 1\ngrpc = "node-1:7001"\noutbox = -1001000009001\nlog = "tg-log-1"\nsnapshot = "dc-snap-1"/);
  assert.match(toml, /\[\[node\]\]\nid = 2\ngrpc = "node-2:7001"\noutbox = -1001000009002\nlog = "dc-log-2"/);
  assert.match(toml, /\[\[node\]\]\nid = 3\noutbox = -1001000009003\nlog = "node-3-disk"\nsnapshot = "node-3-disk"/, "node 3 over the chat, on its disk");
  assert.match(toml, /id = "node-3-disk"\nkind = "local_disk"\npath = "\/data\/node-3"/);
  assert.match(toml, /\[meta\]\nsnapshot_every_entries = \d+\nsnapshot_every = "off"\n# [^\n]*\nlease = "5s"/, "no log in [meta]; a Discord log takes a lease");
  assert.match(toml, /members = \["tg-1", "dc-1"\]/, "the chunk channels are the same for all");
  assert.doesNotMatch(tomlOf(setup(["telegram"], 1, {nodes: 3, links: "direct", others})), /outbox/);
});

test("sketch: each data channel's place; the log and snapshots last", () => {
  const chans = sketch(setup(["telegram", "discord", "telegram", "discord"], 2));
  assert.deepEqual(
    chans.map(c => [c.store, c.role, c.place && `${c.place.shard}/${c.place.copy}`]),
    [
      ["tg-1", "data", "0/0"],
      ["dc-1", "data", "0/1"],
      ["tg-2", "data", "1/0"],
      ["dc-2", "data", "1/1"],
      ["tg-log", "log", undefined],
      ["dc-snap", "snapshots", undefined],
    ],
  );
  assert.ok(chans.every(c => c.total === undefined));
  // The wizard's sketch grows with its steps.
  const w = setup(["telegram", "discord"], 2, {bots: 3});
  assert.deepEqual(
    sketch(w, "channels").map(c => [c.store, c.place, c.bots]),
    [
      ["tg-1", null, 1],
      ["dc-1", null, 1],
    ],
  );
  assert.equal(sketch(w, "copies")[1].place.copy, 1);
  assert.equal(sketch(w, "copies").length, 2, "no log yet");
  assert.deepEqual(
    sketch(w, "meta").map(c => c.role),
    ["data", "data", "log", "snapshots"],
  );
  assert.ok(sketch(w, "meta").every(c => c.bots === 1) && sketch(w, "bots").every(c => c.bots === 3));
  assert.deepEqual(divisors(4), [1, 2, 4]);
});

test("chatModel: what each message is, and the posts not shown", () => {
  const c = {
    kind: "discord",
    role: "data",
    total: 4,
    pinned: [{id: "11", name: "owner-0a1b2c3d.bin"}],
    messages: [
      {id: "11", kind: "document", name: "owner-0a1b2c3d.bin", size: 59},
      {id: "12", kind: "text", name: "ph-1.bin", size: 461, preview: "漢"},
      {id: "13", kind: "other"},
    ],
  };
  const model = chatModel(c, true);
  assert.equal(model.earlier, 1);
  assert.equal(model.pins, 1);
  const [owner, text, service] = model.messages;
  assert.ok(owner.pinned);
  assert.deepEqual(owner.notes, ["demo.owner_marker"]);
  assert.equal(text.kind, "text");
  assert.equal(text.text, "ph-1.bin 461\n漢…");
  assert.equal(service.kind, "service");
  assert.ok(text.deletable);
  assert.ok(chatModel(c).messages.every(m => !m.deletable));
  // A Discord id carries its time: 2015-01-01 plus the high bits, in ms.
  assert.equal(chatModel({...c, messages: [{id: String(1000n << 22n), kind: "other"}]}).messages[0].at.getTime(), 1420070401000);
});

test("chatModel: the log's own posts are quiet; nothing to delete outside data channels", () => {
  const c = {
    kind: "telegram",
    role: "log",
    total: 2,
    messages: [
      {id: 1, kind: "text", log: {what: "claim", gen: 1}},
      {id: 2, kind: "text", log: {what: "batch", gen: 1, first: 3, last: 4}},
    ],
  };
  const [claim, batch] = chatModel(c, true).messages;
  assert.ok(claim.quiet && !batch.quiet);
  assert.deepEqual(batch.notes, ["demo.post_batch"]);
  assert.ok(!claim.deletable);
});

test("summary: a command's verb and what it touched", () => {
  assert.deepEqual(summary({namespace: "s3", command: {PutObject: {bucket: "b", key: "k", size: 2048}}}), {verb: "PutObject", fields: "b/k, 2.0 KiB"});
  assert.deepEqual(summary({namespace: "chunk", command: {AddReplica: {chunk_id: 3, replica: {}}}}), {verb: "AddReplica", fields: "demo.chunk_n"});
});

test("flightsOf: a node's own channel is its; a data channel the leader's; Raft by its frame", () => {
  const channels = {"tg-log-2": {store: "tg-log-2", node: 2}, "tg-1": {store: "tg-1", node: null}, "outbox-3": {store: "outbox-3", node: 3}};
  const channel = s => channels[s];
  const ctx = {leader: 1, shown: 2};
  assert.deepEqual(flightsOf({type: "posted", store: "tg-log-2", bytes: 300}, channel, ctx), [
    {from: {node: 2}, to: {store: "tg-log-2", at: "new"}, size: 300},
  ]);
  assert.deepEqual(flightsOf({type: "call", store: "tg-1", op: "get", bytes: 9}, channel, ctx)[0].to, {node: 1}, "the leader reads the data");
  assert.equal(flightsOf({type: "call_failed", store: "tg-1"}, channel, ctx)[0].failed, true);
  assert.deepEqual(flightsOf({type: "posted", store: "nope"}, channel, ctx), [], "a channel not drawn: nothing flies");
  const read = flightsOf({type: "raft", from: 3, to: 1, via: "chat", kind: "append", at: "read"}, channel, ctx)[0];
  assert.deepEqual([read.from, read.to, read.lane], [{store: "outbox-3", at: "new"}, {node: 1}, "raft"]);
  assert.equal(flightsOf({type: "raft", from: 1, to: 2, via: "grpc", kind: "heartbeat", at: "sent"}, channel, ctx)[0].spark, true);
  assert.equal(ownerOf({node: null}, {leader: null, shown: 3}), 3, "no leader known: the node on screen");
});

test("raft watch: a heartbeat winds a follower's clock; votes and a candidate are told", () => {
  const w = makeRaftWatch();
  w.view(2, {role: "follower", term: 1}, 0);
  assert.equal(w.clock(2, 0, 1000), null, "not heard from a leader yet");
  w.frame({kind: "heartbeat", via: "grpc", at: "sent", from: 1, to: 2}, 100);
  assert.equal(w.clock(2, 600, 1000), 0.5);
  assert.equal(w.clock(2, 5000, 1000), 0);
  assert.deepEqual(w.view(2, {role: "candidate", term: 2}, 5000), [["raft.candidate", {n: 2, term: 2}]]);
  assert.deepEqual(w.frame({kind: "voted", granted: true, from: 3, to: 2}, 5100), [["raft.vote_for", {from: 3, to: 2}]]);
  assert.deepEqual(w.view(2, {role: "leader", term: 2}, 5200), [["raft.leader", {n: 2, term: 2}]]);
  assert.equal(w.clock(2, 6000, 1000), null, "a leader runs no election clock");
  assert.equal(w.chronicle().length, 3);
});
