// The pigeons' sky (demo/pigeons/model.js): order, caps, landings, motion.
import assert from "node:assert/strict";
import {test} from "node:test";
import {along, arc, makeSky, pointOn} from "../src/assets/demo/pigeons/model.js";
import {set, settings} from "../src/assets/demo/settings.js";

const SPACING = settings.spacing,
  RAFT_CAP = settings.raftCap;

const sky = (dur = 100) => {
  const started = [];
  const s = makeSky({
    start: f => {
      started.push(f.id);
      return f.dur ?? dur;
    },
  });
  return {s, started};
};

test("sky: yours leave one by one, SPACING apart, in order; each lands once", async () => {
  const {s, started} = sky();
  const landed = [];
  const ends = [1, 2, 3].map(id => s.add({id, onLand: () => landed.push(id)}, 0));
  s.tick(0);
  assert.deepEqual(started, [1]);
  s.tick(SPACING);
  assert.deepEqual(started, [1, 2]);
  s.tick(2 * SPACING);
  assert.deepEqual(started, [1, 2, 3]);
  s.tick(2 * SPACING + 100);
  await Promise.all(ends);
  assert.deepEqual(landed, [1, 2, 3]);
  assert.equal(s.busy(), false);
});

test("sky: Raft's go at once, at most RAFT_CAP; the rest are let go, landed all the same", async () => {
  const {s, started} = sky();
  let landed = 0;
  const ends = Array.from({length: RAFT_CAP + 2}, (_, id) => s.add({id, lane: "raft", onLand: () => landed++}, 0));
  assert.equal(started.length, RAFT_CAP);
  assert.equal(landed, 2, "the two over the cap end at once");
  assert.equal(s.tick(50).length, RAFT_CAP);
  s.tick(100);
  await Promise.all(ends);
  assert.equal(landed, RAFT_CAP + 2);
});

test("sky: a Raft cap of 0 is none: every one of Raft's flies", () => {
  set("raftCap", 0);
  try {
    const {s, started} = sky();
    for (let id = 0; id < 20; id++) s.add({id, lane: "raft"}, 0);
    assert.equal(started.length, 20);
  } finally {
    set("raftCap", RAFT_CAP);
  }
});

test("sky: landed() waits for yours, not Raft's; a flight that cannot fly ends at once", async () => {
  const {s} = sky();
  s.add({id: "raft", lane: "raft", dur: 10_000}, 0);
  s.add({id: "mine"}, 0);
  let done = false;
  s.landed().then(() => {
    done = true;
  });
  s.tick(0);
  s.tick(150);
  await new Promise(r => setTimeout(r, 0));
  assert.equal(done, true, "the long Raft flight is still up");
  assert.equal(s.busy(), true);
  let gone = false;
  await s.add({id: "nowhere", lane: "raft", dur: 0, onLand: () => (gone = true)}, 200);
  assert.equal(gone, true, "its places are not on the screen: let go, landed");
});

test("motion: the arc starts and ends at its places; a failed flight turns back short", () => {
  const curve = arc([0, 100], [200, 50], 40);
  const start = pointOn(curve, 0),
    end = pointOn(curve, 1);
  assert.deepEqual([start.x, start.y, end.x, end.y], [0, 100, 200, 50]);
  assert.ok(pointOn(curve, 0.5).y < 50, "up over the higher end");
  assert.deepEqual(along(0), {t: 0, back: false});
  assert.equal(along(1).t, 1);
  const turn = along(0.5, true);
  assert.ok(Math.abs(turn.t - 0.88) < 1e-9 && !turn.back);
  assert.ok(along(0.75, true).back);
  assert.equal(along(1, true).t, 0, "back where it left");
});
