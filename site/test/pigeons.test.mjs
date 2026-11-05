// The pigeons' sky (demo/pigeons/model.js): order, caps, landings, motion.
import assert from "node:assert/strict";
import {test} from "node:test";
import {along, arc, makeSky, pointOn} from "../src/assets/demo/pigeons/model.js";
import {set, settings} from "../src/assets/demo/settings.js";

const RAFT_CAP = settings.raftCap;

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
const ended = () => new Promise(r => setTimeout(r, 0));

test("sky: a moment's flight flies in the wall's time, all at once; each lands once", async () => {
  const {s, started} = sky();
  const landed = [];
  for (const id of [1, 2]) s.add({id, ms: 0}, 0, 0).then(() => landed.push(id));
  assert.deepEqual(started, [1, 2]);
  assert.deepEqual(
    s.tick(50, 0).map(u => u.k),
    [0.5, 0.5],
  );
  s.tick(100, 0);
  await ended();
  assert.deepEqual(landed, [1, 2]);
  assert.equal(s.busy(), false);
});

test("sky: a timed flight goes on the node's clock, waits for its end, lands with it", async () => {
  const {s} = sky();
  let landed = false;
  s.add({id: 1, key: "api:1", ms: 200}, 0, 1000).then(() => {
    landed = true;
  });
  assert.equal(s.tick(5000, 1000)[0].k, 0, "the wall's time alone moves nothing");
  assert.equal(s.tick(5000, 1100)[0].k, 0.5);
  assert.equal(s.tick(6000, 1250)[0].k, 1, "there, its end not logged yet: it waits");
  s.land("api:1", 6000, 1250);
  s.tick(6001, 1250);
  await ended();
  assert.equal(landed, true);
});

test("sky: a timed flight that fails turns back from where it is; one whose end never comes ends", async () => {
  const {s} = sky();
  s.add({id: 1, key: "api:1", ms: 200}, 0, 0);
  s.tick(0, 100);
  s.land("api:1", 0, 100, {failed: true});
  const [u] = s.tick(300, 100);
  assert.equal(u.back, true);
  assert.ok(Math.abs(u.k - 0.25) < 1e-9, "half way out, half way back");
  assert.equal(u.flight.failed, true);
  s.tick(600, 100);
  s.add({id: 2, key: "api:2", ms: 100}, 600, 100);
  assert.equal(s.tick(700, 350).length, 1, "late, still waiting");
  assert.equal(s.tick(700, 400).length, 0, "three times late: given up");
});

test("sky: a vote's answer turns out for or against as it lands", () => {
  const {s} = sky();
  s.add({id: 1, key: "vote:7", ms: 50, lane: "raft", kind: "vote"}, 0, 0);
  s.land("vote:7", 0, 50, {kind: "vote-for"});
  assert.equal(s.tick(0, 49)[0].flight.kind, "vote-for");
});

test("sky: Raft's at most RAFT_CAP in the air; the rest are let go, landed all the same", async () => {
  const {s, started} = sky();
  let landed = 0;
  const ends = Array.from({length: RAFT_CAP + 2}, (_, id) => s.add({id, lane: "raft", ms: 0}, 0, 0).then(() => landed++));
  assert.equal(started.length, RAFT_CAP);
  await ended();
  assert.equal(landed, 2, "the two over the cap end at once");
  s.tick(100, 0);
  await Promise.all(ends);
  assert.equal(landed, RAFT_CAP + 2);
});

test("sky: a Raft cap of 0 is none: every one of Raft's flies", () => {
  set("raftCap", 0);
  try {
    const {s, started} = sky();
    for (let id = 0; id < 20; id++) s.add({id, lane: "raft", ms: 0}, 0, 0);
    assert.equal(started.length, 20);
  } finally {
    set("raftCap", RAFT_CAP);
  }
});

test("sky: landed() waits for yours, not Raft's; a flight that cannot fly ends at once", async () => {
  const {s} = sky();
  s.add({id: "raft", lane: "raft", dur: 10_000, ms: 0}, 0, 0);
  s.add({id: "mine", ms: 0}, 0, 0);
  let done = false;
  s.landed().then(() => {
    done = true;
  });
  s.tick(150, 0);
  await ended();
  assert.equal(done, true, "the long Raft flight is still up");
  assert.equal(s.busy(), true);
  await s.add({id: "nowhere", lane: "raft", dur: 0, ms: 0}, 200, 0);
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
