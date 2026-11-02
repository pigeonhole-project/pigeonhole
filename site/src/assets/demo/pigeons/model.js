// The pigeons' sky without a screen: which flights are in the air, which
// wait, how far each has come. The renderer (render.js) asks it each frame
// and draws; time is passed in, so the order, the caps and the motion can be
// tested without a browser.
//
// A flight (flights.js) goes in a lane:
//   "yours"  your requests and the node's work: one leaves every SPACING ms,
//            in the order they came; every one flies
//   "raft"   the cluster's own traffic: at once, at most RAFT_CAP in the air;
//            more are let go (they never fly, but still count as landed)
// Every flight ends once, landing or let go: then its `onLand` runs and the
// promise `add` returned resolves.

export const SPACING = 70;
export const RAFT_CAP = 6;
const LANES = {yours: {spacing: SPACING, cap: Infinity}, raft: {spacing: 0, cap: RAFT_CAP}};

// `start(flight)`: the flight's duration in ms as it leaves, or 0 when it
// cannot fly (its places are not on the screen): it is let go.
export function makeSky({start}) {
  const waiting = []; // [{flight, ended, done}] in the order they came, yours only
  const flying = new Set(); // {flight, ended, done, t0, dur}
  let nextAt = 0; // when the next of yours may leave

  const lane = f => LANES[f.lane ?? "yours"];
  function end(entry) {
    flying.delete(entry);
    entry.flight.onLand?.();
    entry.done();
  }
  function leave(entry, now) {
    const dur = start(entry.flight);
    if (!dur) return end(entry);
    Object.assign(entry, {t0: now, dur});
    flying.add(entry);
  }

  return {
    // A flight into the sky at `now`; resolves when it ends.
    add(flight, now) {
      const entry = {flight};
      entry.ended = new Promise(done => {
        entry.done = done;
      });
      const l = lane(flight);
      if (l.spacing > 0) waiting.push(entry);
      else if ([...flying].filter(x => lane(x.flight) === l).length >= l.cap) end(entry);
      else leave(entry, now);
      return entry.ended;
    },
    // Moves time to `now`: the waiting leave, the arrived land. Returns the
    // flights in the air, each with how far it has come (0…1) and for how long.
    tick(now) {
      while (waiting.length && now >= nextAt) {
        leave(waiting.shift(), Math.max(now, nextAt));
        nextAt = Math.max(now, nextAt) + SPACING;
      }
      const up = [];
      for (const entry of [...flying]) {
        const k = (now - entry.t0) / entry.dur;
        if (k >= 1) end(entry);
        else up.push({flight: entry.flight, k: Math.max(0, k), ms: now - entry.t0, dur: entry.dur});
      }
      return up;
    },
    // Something is in the air or waits to leave.
    busy: () => flying.size > 0 || waiting.length > 0,
    // Resolves when every flight of yours (not Raft's) added so far has ended.
    landed: () => Promise.all([...waiting, ...flying].filter(e => lane(e.flight) === LANES.yours).map(e => e.ended)),
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
export const durationOf = len => Math.min(1400, 450 + len * 1.1);

// Where along its arc a flight is at k (0…1 of its time): a soft start and
// landing; a failed one reaches 88% of the way and turns back.
export function along(k, failed = false) {
  const e = k - (Math.sin(2 * Math.PI * k) / (2 * Math.PI)) * 0.5;
  return failed ? {t: 0.88 * (1 - Math.abs(1 - 2 * e)), back: e > 0.5} : {t: e, back: false};
}
