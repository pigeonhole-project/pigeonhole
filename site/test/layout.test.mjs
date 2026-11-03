// Where the town's places go (demo/layout.js): one node's town wide (a
// desktop) and tall (a phone), a cluster's wide. The layouts are kept in
// layout.snapshot.json: a change in them shows in the diff; to take it,
// UPDATE_SNAPSHOTS=1 node --test site/test/layout.test.mjs.
import assert from "node:assert/strict";
import {readFileSync, writeFileSync} from "node:fs";
import {test} from "node:test";
import {layout} from "../src/assets/demo/layout.js";

const ch = (store, role, kind, node) => ({store, role, kind, ...(node == null ? {} : {node})});
// The demo's one node: two data channels, its log and snapshots.
const ONE = [ch("telegram", "data", "telegram"), ch("discord", "data", "discord"), ch("tg-log", "log", "telegram"), ch("tg-snap", "snapshots", "telegram")];
// The wizard's quick cluster, as the `channels` op names it.
const CLUSTER = [
  ch("tg-1", "data", "telegram"),
  ch("dc-1", "data", "discord"),
  ch("tg-log-1", "log", "telegram", 1),
  ch("tg-snap-1", "snapshots", "telegram", 1),
  ch("dc-log-2", "log", "discord", 2),
  ch("dc-snap-2", "snapshots", "discord", 2),
  ch("meta-log-3", "log", "local_disk", 3),
  ch("meta-snap-3", "snapshots", "local_disk", 3),
  ch("outbox-1", "outbox", "telegram", 1),
  ch("outbox-2", "outbox", "telegram", 2),
  ch("outbox-3", "outbox", "telegram", 3),
];

const round = v => (typeof v === "number" ? Math.round(v * 10) / 10 : v);
const flat = o => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, round(v)]));
const plain = at => ({
  width: round(at.width),
  height: at.height,
  lift: at.lift,
  client: flat(at.client),
  lofts: Object.fromEntries([...at.lofts].map(([id, L]) => [id, flat(L)])),
  stores: Object.fromEntries([...at.stores].map(([store, b]) => [store, flat(b)])),
});
const LAYOUTS = {
  "one node, wide": layout(ONE, {tall: false, height: 640}),
  "one node, tall": layout(ONE, {tall: true, height: 640}),
  "a cluster, wide": layout(CLUSTER, {tall: false, height: 640}),
};

const file = new URL("layout.snapshot.json", import.meta.url);
test("layout: as kept in layout.snapshot.json", () => {
  const now = Object.fromEntries(Object.entries(LAYOUTS).map(([name, at]) => [name, plain(at)]));
  if (process.env.UPDATE_SNAPSHOTS) writeFileSync(file, `${JSON.stringify(now, null, 2)}\n`);
  assert.deepEqual(now, JSON.parse(readFileSync(file, "utf8")));
});

test("layout: every channel has a block; blocks on one street do not overlap and stay in the view", () => {
  for (const [name, at] of Object.entries(LAYOUTS)) {
    const channels = name.startsWith("a cluster") ? CLUSTER : ONE;
    assert.deepEqual([...at.stores.keys()].sort(), channels.map(c => c.store).sort(), name);
    const streets = Map.groupBy([at.client, ...at.stores.values()], b => b.ground);
    for (const blocks of streets.values()) {
      const row = blocks.toSorted((a, b) => a.x - b.x);
      for (const [i, b] of row.entries()) {
        assert.ok(b.x >= 0 && b.x + b.w <= at.width, `${name}: in the view`);
        if (i) assert.ok(row[i - 1].x + row[i - 1].w <= b.x, `${name}: no overlap at ${b.x}`);
      }
    }
  }
});

test("layout: a node's bridge lies on its own log and snapshots, of one height", () => {
  const at = LAYOUTS["a cluster, wide"];
  for (const id of [1, 2, 3]) {
    const L = at.lofts.get(id);
    const own = CLUSTER.filter(c => c.node === id && c.role !== "outbox").map(c => at.stores.get(c.store));
    assert.equal(own.length, 2);
    for (const b of own) {
      assert.ok(b.x >= L.x && b.x + b.w <= L.x + L.w, `node ${id}: its blocks under its bridge`);
      assert.equal(b.floors, own[0].floors, `node ${id}: as tall as each other`);
    }
  }
});
