// The pigeons' sky without a screen: which flights are in the air and how
// far each has come. The renderer (render.js) asks it each frame and draws;
// time is passed in (the wall's and the node's), so the caps and the motion
// can be tested without a browser.
//
// A flight (flights.js) is timed one of two ways:
//   ms > 0   on the node's clock: it leaves as the node starts the thing it
//            stands for and is where the node's time says, landing as the
//            thing ends (land(key)), at `ms` if all goes as the log said; a
//            clock slowed down or stopped slows or stops it
//   ms = 0   a moment of the node's: flown in the wall's time, as long as
//            `start` says
// It goes in a lane: "raft" (the cluster's own: at most `settings.raftCap`
// in the air, 0: no cap; more are let go: they never fly, but count as
// landed) or none (every one flies). One that fails turns back from where
// it is (land(key, {failed})). Every flight ends once, landing or let go:
// then the promise `add` returned resolves.

import {settings} from "../settings.js";

const LOST = 3; // a timed flight whose end never comes ends at LOST × its ms
const BACK_MS = 600; // a failed flight's way back, in the wall's ms

// `start(flight)`: the flight's duration in the wall's ms as it leaves (a
// moment's; a timed one's is a guess, for its wing beats), or 0 when it
// cannot fly (its places are not on the screen): it is let go.
export function makeSky({start}) {
  const flying = new Set(); // {flight, ended, done, t0: node, w0: wall, dur, endAt?: node, back?: {from, w0}}
  const byKey = new Map(); // flight.key → its entry
  const cap = () => settings.raftCap || Infinity;

  function end(entry) {
    flying.delete(entry);
    if (byKey.get(entry.flight.key) === entry) byKey.delete(entry.flight.key);
    entry.done();
  }
  // How far a timed flight has come at node time `node` (0…1).
  const along = (entry, node) => {
    const span = entry.endAt == null ? entry.flight.ms : Math.max(1e-6, entry.endAt - entry.t0);
    return Math.min(1, Math.max(0, (node - entry.t0) / span));
  };

  return {
    // A flight into the sky at wall time `wall`, node time `node`; resolves when it ends.
    add(flight, wall, node) {
      const entry = {flight, t0: node, w0: wall};
      entry.ended = new Promise(done => {
        entry.done = done;
      });
      const full = flight.lane === "raft" && [...flying].filter(x => x.flight.lane === "raft").length >= cap();
      const dur = full ? 0 : start(flight);
      if (!dur) {
        entry.done();
        return entry.ended;
      }
      entry.dur = dur;
      flying.add(entry);
      if (flight.key) byKey.set(flight.key, entry);
      return entry.ended;
    },
    // What flight `key` stands for ended at node time `node`: it lands, or
    // (failed) turns back from where it is; `kind` what it turned out to be.
    land(key, wall, node, {failed = false, kind} = {}) {
      const entry = byKey.get(key);
      if (!entry) return;
      if (kind) entry.flight.kind = kind;
      if (!failed) entry.endAt = node;
      else if (!entry.back) {
        entry.flight.failed = true;
        entry.back = {from: entry.flight.ms > 0 ? along(entry, node) : 1, w0: wall};
      }
    },
    // Moves time to `wall` and `node`: the arrived land. Returns the flights
    // in the air, each with how far it has come (0…1), whether on its way
    // back, and for how long it has flown and will (the wall's ms).
    tick(wall, node) {
      const up = [];
      for (const entry of [...flying]) {
        const f = entry.flight;
        let k;
        if (entry.back) {
          const b = (wall - entry.back.w0) / BACK_MS;
          if (b >= 1) {
            end(entry);
            continue;
          }
          k = entry.back.from * (1 - b);
        } else if (f.ms > 0) {
          const late = node - entry.t0 >= f.ms * LOST;
          k = along(entry, node);
          if (k >= 1 && (entry.endAt != null || late)) {
            end(entry);
            continue;
          }
        } else {
          k = (wall - entry.w0) / entry.dur;
          if (k >= 1) {
            end(entry);
            continue;
          }
        }
        up.push({flight: f, k: Math.max(0, k), back: !!entry.back, ms: wall - entry.w0, dur: entry.dur});
      }
      return up;
    },
    // Something is in the air.
    busy: () => flying.size > 0,
    // Resolves when every flight of yours (not Raft's) added so far has ended.
    landed: () => Promise.all([...flying].filter(e => e.flight.lane !== "raft").map(e => e.ended)),
  };
}

// ---- the motion, along an arc up over the roofs and down

// The arc's control points: from `a` to `b`, `lift` over the higher end.
export function arc([x1, y1], [x2, y2], lift) {
  const top = Math.min(y1, y2) - lift;
  return [
    [x1, y1],
    [x1 + (x2 - x1) * 0.25, top],
    [x1 + (x2 - x1) * 0.75, top],
    [x2, y2],
  ];
}
// A point on the arc at t (0…1), and its direction there.
export function pointOn([p0, p1, p2, p3], t) {
  const u = 1 - t;
  const at = i => u * u * u * p0[i] + 3 * u * u * t * p1[i] + 3 * u * t * t * p2[i] + t * t * t * p3[i];
  const d = i => 3 * u * u * (p1[i] - p0[i]) + 6 * u * t * (p2[i] - p1[i]) + 3 * t * t * (p3[i] - p2[i]);
  return {x: at(0), y: at(1), dx: d(0), dy: d(1)};
}
// About how long the arc is: its control polygon and chord, averaged.
export function lengthOf([p0, p1, p2, p3]) {
  const dist = (a, b) => Math.hypot(b[0] - a[0], b[1] - a[1]);
  return (dist(p0, p1) + dist(p1, p2) + dist(p2, p3) + dist(p0, p3)) / 2;
}
// A flight's duration for an arc that long.
export const durationOf = len => Math.min(settings.flightMax, settings.flightMin + len * settings.flightPerPx);

// Where along its arc a flight is at k (0…1 of its time): a soft start and
// landing; a failed one reaches 88% of the way and turns back.
export function along(k, failed = false) {
  const e = k - (Math.sin(2 * Math.PI * k) / (2 * Math.PI)) * 0.5;
  return failed ? {t: 0.88 * (1 - Math.abs(1 - 2 * e)), back: e > 0.5} : {t: e, back: false};
}
