// The page's channels as the node's events change them, and its listings
// merged in (demo/reduce.js).
import assert from "node:assert/strict";
import {beforeEach, test} from "node:test";
import {apply, gone, merge} from "../src/assets/demo/reduce.js";

beforeEach(() => {
  globalThis.LANG = "en";
  globalThis.I18N = {};
});

const at = new Date(0);
const msgs = (...ids) => ids.map(id => ({id, kind: "document", fresh: false}));
const chan = (store, role, ids, total = ids.length) => ({store, role, kind: "telegram", total, messages: msgs(...ids)});

test("apply: a post adds a fresh message; a snapshot's says so", () => {
  const channels = [chan("tg-1", "data", [1, 2]), chan("tg-snap", "snapshots", [], 4)];
  const post = apply(channels, {type: "posted", store: "tg-1", id: 3, kind: "text", name: "ph-1.bin", bytes: 461, preview: "hi"}, at);
  assert.equal(post.channel, channels[0]);
  assert.deepEqual(post.said, []);
  assert.equal(channels[0].total, 3);
  assert.deepEqual(channels[0].messages.at(-1), {id: 3, kind: "text", name: "ph-1.bin", size: 461, preview: "hi", fresh: true, at});
  const snap = apply(channels, {type: "posted", store: "tg-snap", id: 9, kind: "document", name: "meta-60.snap", bytes: 1}, at);
  assert.deepEqual(snap.said, [["say.snapshot", {at: 60}]]);
  assert.equal(channels[1].total, 5);
  assert.deepEqual(apply(channels, {type: "posted", store: "nowhere", id: 1}, at), {channel: null, said: []});
});

test("apply: a deletion takes the messages out, the unlisted older ones counted", () => {
  // 10 messages, the last 3 listed: 8, 9, 10.
  const c = chan("outbox-1", "outbox", [8, 9, 10], 10);
  apply([c], {type: "deleted", store: "outbox-1", ids: ["5", "6", "8"]}, at);
  assert.deepEqual(
    c.messages.map(m => m.id),
    [9, 10],
  );
  assert.equal(c.total, 7);
});

test("gone: listed ids, and ids below the first listed, no more than are unlisted", () => {
  const c = chan("x", "outbox", [8, 9], 4); // two unlisted, older
  assert.equal(gone(c, ["8"]), 1);
  assert.equal(gone(c, ["1", "2", "3", "4", "8"]), 3, "only two unlisted older ones were there");
  assert.equal(gone(c, ["12"]), 0, "newer than any listed, and not listed: not there");
});

test("apply: an S3 write is told with what the first data channel got; repair passes that mended", () => {
  const channels = [chan("tg-1", "data", [])];
  apply(channels, {type: "posted", store: "tg-1", id: 1, kind: "document", bytes: 1000}, at);
  const put = apply(channels, {type: "s3", op: "PutObject", key: "a", size: "4000", extents: "1", inline: "false"}, at);
  assert.equal(put.said.length, 1);
  assert.equal(put.said[0][0], "say.PutObjectZ");
  assert.deepEqual(apply(channels, {type: "s3", op: "CreateBucket", bucket: "demo"}, at).said, [], "nothing to say: no sentence");
  assert.deepEqual(apply(channels, {type: "repair_pass", repaired: 2}, at).said, [["say.repair_pass", {n: 2}]]);
  assert.deepEqual(apply(channels, {type: "repair_pass", repaired: 0}, at).said, []);
});

test("merge: the listing wins; posts seen arriving stay fresh; those newer than it stay", () => {
  const old = [chan("tg-1", "data", [1, 2]), chan("dc-1", "data", ["1000000000000000009"])];
  old[0].messages[1].fresh = true;
  // Posted while the channels were read: not in the listing yet.
  old[0].messages.push({id: 3, kind: "document", fresh: true});
  old[1].messages.push({id: "1000000000000000010", kind: "text", fresh: true});
  old[1].messages[0].fresh = true;
  const listed = [
    {store: "tg-1", role: "data", total: 2, messages: msgs(1, 2)},
    {store: "dc-1", role: "data", total: 1, messages: msgs("1000000000000000009")},
    {store: "new", role: "log", total: 0, messages: []},
  ];
  const now = merge(old, listed);
  assert.deepEqual(
    now[0].messages.map(m => [m.id, m.fresh]),
    [
      [1, false],
      [2, true],
      [3, true],
    ],
  );
  assert.equal(now[0].total, 3);
  assert.deepEqual(
    now[1].messages.map(m => m.id),
    ["1000000000000000009", "1000000000000000010"],
    "snowflakes are compared as numbers too long for one",
  );
  assert.deepEqual(now[2], listed[2]);
  assert.notEqual(now[0], old[0], "new objects: the listing's");
});

test("merge: a fresh message older than the listing's newest and not in it is gone", () => {
  const old = [chan("tg-1", "data", [5, 6])];
  old[0].messages[0].fresh = true; // 5: deleted since
  const now = merge(old, [{store: "tg-1", role: "data", total: 1, messages: msgs(6)}]);
  assert.deepEqual(
    now[0].messages.map(m => m.id),
    [6],
  );
  assert.equal(now[0].total, 1);
});
