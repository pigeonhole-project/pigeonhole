// The demo's modules that hold no DOM: the node's events, the background
// tasks, the wizard's config, a channel as a chat, the log's lines.
import assert from "node:assert/strict";
import {beforeEach, test} from "node:test";
import {chatModel} from "../src/assets/demo/chat/model.js";
import {readEvents} from "../src/assets/demo/events.js";
import {summary} from "../src/assets/demo/log-view.js";
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
      ["dc-snapshots", "snapshots", undefined],
    ],
  );
  assert.ok(chans.every(c => c.total === undefined));
  assert.deepEqual(divisors(4), [1, 2, 4]);
});

test("chatModel: what each message is, and the posts not shown", () => {
  const c = {
    kind: "discord",
    role: "data",
    total: 5,
    pinned: [{id: "11", name: "owner-0a1b2c3d.bin"}],
    messages: [
      {id: "11", kind: "document", name: "owner-0a1b2c3d.bin", size: 59},
      {id: "12", kind: "text", name: "ph-1.bin", size: 461, preview: "漢"},
      {id: "13", kind: "other"},
      {id: "14", kind: "document", name: "ph-2.bin", size: 9, gone: true},
    ],
  };
  const model = chatModel(c);
  assert.equal(model.earlier, 2);
  assert.equal(model.pins, 1);
  const [owner, text, service, gone] = model.messages;
  assert.ok(owner.pinned);
  assert.deepEqual(owner.notes, ["demo.owner_marker"]);
  assert.equal(text.kind, "text");
  assert.equal(text.text, "ph-1.bin 461\n漢…");
  assert.equal(service.kind, "service");
  assert.ok(!gone.deletable && text.deletable);
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
  const [claim, batch] = chatModel(c).messages;
  assert.ok(claim.quiet && !batch.quiet);
  assert.deepEqual(batch.notes, ["demo.post_batch"]);
  assert.ok(!claim.deletable);
});

test("summary: a command's verb and what it touched", () => {
  assert.deepEqual(summary({namespace: "s3", command: {PutObject: {bucket: "b", key: "k", size: 2048}}}), {verb: "PutObject", fields: "b/k, 2.0 KiB"});
  assert.deepEqual(summary({namespace: "chunk", command: {AddReplica: {chunk_id: 3, replica: {}}}}), {verb: "AddReplica", fields: "demo.chunk_n"});
});
