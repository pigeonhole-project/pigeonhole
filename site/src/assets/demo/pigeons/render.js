// The pigeons on the screen: one animation loop for every flight in the sky
// (model.js). Each frame a flight finds its places again, so a town drawn
// anew under it (a resize, a panel opened, a new leader) does not end it:
// it flies on to where its places are now.
import {reducedMotion, svg} from "../../lib/dom.js";
import {beat, flying, glide} from "../art.js";
import {settings} from "../settings.js";
import {along, arc, durationOf, lengthOf, makeSky, pointOn} from "./model.js";

// town: {spot(place): where a place is, chosen once (a window: which one)
//        or null; point(spot): that spot on the screen now, [x, y] or null;
//        lift(): how high a pigeon climbs; small(): a phone's town; now():
//        the node's time (its flights are timed on it)}
export function makePigeons(town) {
  const reduced = reducedMotion();
  let layer = null;
  const shown = new Map(); // flight → {from, to, el, bird?, mark?}
  const sky = makeSky({start});
  let raf = 0;

  const liftOf = f => town.lift() * (f.lane === "raft" ? settings.raftLift : 1);
  const classOf = f => ["flight", f.failed && "failed", f.lane === "raft" && `raft ${f.kind}`].filter(Boolean).join(" ");
  const curveOf = (f, d) => {
    const a = town.point(d.from),
      b = town.point(d.to);
    return a && b ? arc(a, b, liftOf(f)) : null;
  };

  // A flight leaves: its places chosen, its pigeon (or spark) made; its
  // duration, or 0 when a place is not on the screen.
  function start(f) {
    const d = {from: town.spot(f.from), to: town.spot(f.to)};
    const curve = d.from && d.to && layer ? curveOf(f, d) : null;
    if (!curve) return 0;
    if (f.spark) d.el = svg("circle", {class: "spark", r: 2.6}, layer);
    else {
      d.cls = classOf(f);
      d.bird = flying(layer, {class: d.cls});
      d.el = d.bird.g;
    }
    shown.set(f, d);
    return f.spark ? settings.sparkMs : durationOf(lengthOf(curve));
  }

  // One flight's pigeon at k (0…1) of its way (`home`: on its way back).
  function draw(f, d, k, ms, dur, home) {
    const curve = curveOf(f, d);
    d.el.style.display = curve ? "" : "none";
    if (!curve) return;
    if (f.spark) {
      const p = pointOn(curve, k);
      d.el.setAttribute("cx", p.x.toFixed(1));
      d.el.setAttribute("cy", p.y.toFixed(1));
      return;
    }
    // What it turned out to be (a vote's answer: for or against).
    if (classOf(f) !== d.cls) {
      d.cls = classOf(f);
      d.el.setAttribute("class", d.cls);
    }
    // A moment's failed flight turns round at the far end; a timed one where it failed.
    const {t, back} = home ? {t: k, back: true} : f.ms > 0 ? {t: along(k).t, back: false} : along(k, f.failed);
    const p = pointOn(curve, t);
    // Facing the way it goes; a failed one turns round at the far end.
    const face = (curve[3][0] < curve[0][0] ? -1 : 1) * (back ? -1 : 1);
    const dir = back ? -1 : 1;
    const tilt = Math.max(-35, Math.min(35, (Math.atan2(p.dy * dir, p.dx * dir * face) * 180) / Math.PI));
    const scale = (0.5 + Math.min(0.35, Math.log2(1 + (f.size ?? 4096) / 4096) / 12)) * (town.small() ? 0.8 : 1);
    d.el.setAttribute("transform", `translate(${p.x.toFixed(1)},${p.y.toFixed(1)}) scale(${face * scale},${scale}) rotate(${tilt.toFixed(1)})`);
    // Flapping off and in to land, on a long way gliding a while between.
    const phase = ms / 450;
    if (dur > 1000 && t > 0.3 && t < 0.72 && Math.floor(phase) % 3) glide(d.bird);
    else beat(d.bird, phase % 1);
    if (back && !d.mark) d.mark = svg("circle", {class: "failed-mark", cx: 14, cy: -16, r: 3.4}, d.el);
  }

  function frame() {
    const up = sky.tick(performance.now(), town.now());
    const live = new Set(up.map(u => u.flight));
    for (const [f, d] of shown)
      if (!live.has(f)) {
        d.el.remove();
        shown.delete(f);
      }
    for (const u of up) {
      const d = shown.get(u.flight);
      if (d) draw(u.flight, d, u.k, u.ms, u.dur, u.back);
    }
    raf = sky.busy() ? requestAnimationFrame(frame) : 0;
  }

  return {
    // A flight (flights.js) leaves now; resolves when it lands or is let
    // go. With reduced motion nothing flies: it lands at once.
    fly(f) {
      if (reduced) return Promise.resolve();
      const ended = sky.add(f, performance.now(), town.now());
      if (!raf) raf = requestAnimationFrame(frame);
      return ended;
    },
    // What flight `key` stands for ended now: it lands, or turns back ({failed, kind}).
    land(key, how) {
      sky.land(key, performance.now(), town.now(), how);
    },
    // Every flight of yours in the air or waiting has landed.
    landed: () => sky.landed(),
    // The town was drawn anew: the pigeons fly on in its sky.
    attach(g) {
      layer = g;
      for (const d of shown.values()) g.append(d.el);
    },
  };
}
