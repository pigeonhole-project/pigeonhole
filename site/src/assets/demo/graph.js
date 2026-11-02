// The graph, drawn as a town of Soviet blocks of flats. Each channel is a
// block and each of its windows a message, filled from the ground floor up:
// a chunk's window lit warm, a small text's cool, a deleted message's
// shuttered, the pinned owner marker's flagged; the block grows a floor as
// it fills (up to a height, then "+n" older ones under). You live on a
// block whose windows are your objects; the node is a dovecote on its
// block's roof. Pigeons fly from the dovecote into the windows.
//
// A node of the graph is a target: {kind: "client"}, {kind: "loft"} or
// {kind: "store", store}. Clicking one calls onOpen(target).
import {activate, reducedMotion, svg} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {beat, birdDefs, block, dovecote, FLOOR, flying, glide, perched, roofLetters, tower} from "./art.js";
import {appOf, placeNote} from "./channels.js";

const QUEUE_SHOWN = 12; // repair jobs drawn over a block; the rest as "+n"
const COL = 26; // a window's column
const MIN_FLOORS = 6,
  HEADROOM = 2; // a block's floors at least; empty floors over its last message
// A phone's screen: the town top to bottom, scrolled.
const NARROW = "(max-width: 640px)";

const sleep = ms => new Promise(r => setTimeout(r, ms));
// A pigeon's way from one place to another: up over the roofs and down.
const arc = ([x1, y1], [x2, y2], lift) => {
  const top = Math.min(y1, y2) - lift;
  return `M${x1},${y1} C${x1 + (x2 - x1) * 0.25},${top} ${x1 + (x2 - x1) * 0.75},${top} ${x2},${y2}`;
};
// The same windows lit on every drawing.
const seeded = seed => () => {
  seed = (seed * 9301 + 49297) % 233280;
  return seed / 233280;
};

// What a channel's windows hold, oldest first: as many unnamed messages as
// its total has beyond the ones it lists, then those (the deleted too).
function contents(c) {
  const list = c.messages || [];
  const older = Math.max(0, (c.total ?? 0) - list.filter(m => !m.gone).length);
  return {older, list, count: older + list.length};
}
// A block's floors for so many windows, `cols` to a floor.
const floorsFor = (count, cols, most) => Math.max(MIN_FLOORS, Math.min(most, Math.ceil(count / cols) + HEADROOM));

// Where everything goes: the view's size, the town's streets, and on them
// (left to right) you, the node's block, then each channel's block:
// {x, w, cols, ground, most: floors at most, deep: its wall under the ground}.
// Wide: one street, the view as tall as the screen's shape asks…
function wide(channels, height) {
  const own = channels.filter(c => c.role !== "data"),
    data = channels.filter(c => c.role === "data");
  const ground = height - 6,
    most = Math.floor((ground - 12 - 70 - height * 0.12) / FLOOR);
  const at = new Map();
  let x = 390;
  for (const c of own) {
    const cols = c.role === "log" ? 3 : 4;
    at.set(c.store, {x, w: cols * COL, cols, ground, most, deep: 2000});
    x += cols * COL + 18;
  }
  x += 24;
  const gap = 14,
    each = (1180 - x - (data.length - 1) * gap) / Math.max(1, data.length);
  for (const c of data) {
    const cols = Math.max(2, Math.min(6, Math.floor(each / COL)));
    at.set(c.store, {x: x + (each - cols * COL) / 2, w: cols * COL, cols, ground, most, deep: 2000});
    x += each + gap;
  }
  return {
    width: 1200,
    height,
    client: {x: 30, w: 3 * COL, cols: 3, ground, most, deep: 2000},
    loft: {x: 150, w: 230, floors: 9, ground, scale: 0.8, deep: 2000, cote: 60, letters: 54, size: 13},
    stores: at,
    lift: 80,
  };
}

// …or two streets top to bottom, for a phone: you, the node and its own
// channels on the first, the data channels on the one in front of it.
function tall(channels) {
  const own = channels.filter(c => c.role !== "data"),
    data = channels.filter(c => c.role === "data");
  const first = 400,
    most = 12;
  const at = new Map();
  let x = 226;
  for (const c of own) {
    const cols = c.role === "log" ? 2 : 3;
    at.set(c.store, {x, w: cols * COL, cols, ground: first, most, deep: 0});
    x += cols * COL + 10;
  }
  const gap = 10,
    each = (388 - (data.length - 1) * gap) / Math.max(1, data.length);
  const cols = data.map(() => Math.max(2, Math.min(8, Math.floor(each / COL))));
  const floors = Math.max(MIN_FLOORS, ...data.map((c, i) => floorsFor(contents(c).count, cols[i], most)));
  const second = first + 70 + floors * FLOOR + 12;
  x = 6;
  for (const [i, c] of data.entries()) {
    at.set(c.store, {x: x + (each - cols[i] * COL) / 2, w: cols[i] * COL, cols: cols[i], ground: second, most, deep: 2000});
    x += each + gap;
  }
  return {
    width: 400,
    height: second + 8,
    client: {x: 6, w: 2 * COL, cols: 2, ground: first, most, deep: 0},
    loft: {x: 66, w: 150, floors: 7, ground: first, scale: 0.5, deep: 0, cote: 38, letters: 38, size: 10},
    stores: at,
    lift: 40,
  };
}

// onLoft(g, windows): the loft was drawn, its entrances are free to dress.
// onRelayout(): the screen turned narrow or wide, or a block needs another
// floor: draw again.
export function makeGraph(el, {onOpen, onLoft, onRelayout}) {
  const reduced = reducedMotion();
  const narrow = matchMedia(NARROW);
  narrow.addEventListener("change", onRelayout);
  // A wide view as tall as the screen's shape asks, at least 640; drawn
  // again when that changes much (a panel opened beside it, a resize).
  const wideHeight = () => {
    const r = el.getBoundingClientRect();
    return r.width ? Math.round(Math.min(1100, Math.max(640, (1200 * r.height) / r.width))) : 640;
  };
  let lastHeight = 640;
  new ResizeObserver(() => {
    if (!narrow.matches && client && Math.abs(wideHeight() - lastHeight) > 40) onRelayout();
  }).observe(el);

  let viewWidth = 1200;
  let client = null,
    loft = null,
    air = null,
    lift = 80;
  let cards = new Map(); // store id → its block
  let edges = new Map(); // store id → the way to its block; "" → the client's

  const nodeOf = target => (target?.kind === "client" ? client : target?.kind === "loft" ? loft : target?.kind === "store" ? cards.get(target.store) : null);

  // A clickable node: a group, named for screen readers.
  function node(target, parent, label) {
    const g = svg("g", {class: "node", tabindex: 0, role: "button", "aria-label": label}, parent);
    activate(g, () => onOpen(target));
    return {g};
  }
  // A frame round a block, shown when it is hovered, focused or open.
  const ring = (n, x, top, w, ground) => svg("rect", {class: "ring", x: x - 5, y: top - 5, width: w + 10, height: ground - top + 10, rx: 4}, n.g);

  // A channel's block: its windows, the letters on its roof.
  function card(c, at, parent) {
    const role = placeNote(c) + (c.bots > 1 ? `, ${t("demo.bots", {n: c.bots})}` : "") + (c.down ? `, ${t("demo.down")}` : "");
    const n = node({kind: "store", store: c.store}, parent, `${c.store}: ${role}`);
    n.g.classList.toggle("down", !!c.down);
    n.c = c;
    n.cols = at.cols;
    n.most = at.most;
    n.floors = floorsFor(contents(c).count, at.cols, at.most);
    const b = tower(n.g, at.x, at.ground, at.w, n.floors, at.cols, {shade: c.role === "data" ? 1 : 3, deep: at.deep});
    n.slots = b.slots;
    n.win = svg("g", {}, n.g);
    // The letters on its roof, kept inside the view.
    const size = at.w < 70 ? 12 : 15,
      half = (c.store.length * size * 0.62 + size * 2.6 + 4) / 2,
      cx = Math.max(half + 4, Math.min(viewWidth - half - 4, at.x + at.w / 2));
    n.count = roofLetters(n.g, cx, b.roof, c.store, {kind: c.kind, app: appOf(c.kind), size});
    n.sign = [cx, b.roof - 26];
    n.tip = svg("title", {}, n.g);
    n.tip.textContent = role;
    n.older = svg("text", {class: "older", x: at.x + at.w / 2, y: at.ground - 4, "text-anchor": "middle"}, n.g);
    n.queue = svg("g", {class: "queue", transform: `translate(${cx},${b.roof - 40})`}, n.g);
    ring(n, at.x, b.roof - 50, at.w, at.ground);
    n.shown = 0;
    fill(n, false);
    return n;
  }

  // A channel's windows, from what it holds now; `fresh`: light the new ones up.
  function fill(n, fresh) {
    const {older, list, count} = contents(n.c);
    const room = n.slots.length - (count > n.slots.length ? n.cols : 0);
    const shown = Math.min(count, room),
      skip = count - shown;
    const pins = new Set((n.c.pinned || []).map(p => String(p.id)));
    n.win.replaceChildren();
    for (const [i, s] of n.slots.entries()) {
      const m = i < shown && skip + i >= older ? list[skip + i - older] : null;
      let cls = i >= shown ? "pane" : m?.gone ? "pane gone" : m?.kind === "text" ? "pane lit text" : m?.kind === "other" ? "pane lit svc" : "pane lit";
      if (fresh && i >= n.shown && i < shown) cls += " new";
      svg("rect", {class: cls, x: s.x, y: s.y, width: s.w, height: s.h}, n.win);
      if (m?.gone) svg("path", {class: "pane-shut", d: `M${s.x + s.w / 2},${s.y} L${s.x + s.w / 2},${s.y + s.h}`}, n.win);
      if (m && pins.has(String(m.id)))
        svg("path", {class: "pin-flag", d: `M${s.x + s.w},${s.y + 1} L${s.x + s.w},${s.y - 9} L${s.x + s.w + 8},${s.y - 6} L${s.x + s.w},${s.y - 3}`}, n.win);
    }
    n.shown = shown;
    n.older.textContent = skip ? `+${skip}` : "";
    n.count.textContent = String(n.c.total ?? count);
    // Full to its last free floor: draw again with a floor more.
    if (count + n.cols > n.floors * n.cols && n.floors < n.most) onRelayout();
  }
  // Where a window is, by its place among the shown.
  const windowAt = (n, i) => {
    const s = n.slots[Math.max(0, Math.min(n.slots.length - 1, i))];
    return [s.x + s.w / 2, s.y + s.h / 2];
  };

  // You: a block whose windows are your objects, a pigeon on its roof.
  function you(at, objects, parent) {
    const n = node({kind: "client"}, parent, "");
    n.floors = floorsFor(objects, at.cols, at.most);
    n.at = at;
    const b = tower(n.g, at.x, at.ground, at.w, n.floors, at.cols, {shade: 2, deep: at.deep});
    n.slots = b.slots;
    n.win = svg("g", {}, n.g);
    n.sub = roofLetters(n.g, at.x + at.w / 2, b.roof, t("demo.you"), {size: 13});
    n.perch = [at.x + at.w - 14, b.roof - 6];
    perched(n.g, {transform: `translate(${at.x + at.w + 2},${b.roof - 12}) scale(1.3)`});
    ring(n, at.x, b.roof - 56, at.w, at.ground);
    return n;
  }
  function setObjects(count) {
    if (!client) return;
    client.objects = count;
    client.sub.textContent = String(count);
    client.g.setAttribute("aria-label", `${t("demo.you")}: ${t("demo.objects", {n: count})}`);
    client.win.replaceChildren();
    for (const [i, s] of client.slots.entries()) svg("rect", {class: i < count ? "pane lit" : "pane", x: s.x, y: s.y, width: s.w, height: s.h}, client.win);
    if (count + client.at.cols > client.floors * client.at.cols && client.floors < client.at.most) onRelayout();
  }

  // The sky, and the town far off: wider and taller than the view, so that
  // on a screen of another shape they fill it still.
  function backdrop(at, rnd, tallNow) {
    const sky = svg("linearGradient", {id: "sky", x1: 0, y1: 0, x2: 0, y2: at.height, gradientUnits: "userSpaceOnUse"}, el.querySelector("defs"));
    svg("stop", {offset: 0, class: "sky-top"}, sky);
    svg("stop", {offset: 1, class: "sky-low"}, sky);
    svg("rect", {x: -2000, y: -2000, width: at.width + 4000, height: at.height + 4000, fill: "url(#sky)"}, el);
    for (const [cls, base] of [
      ["far", at.height * (tallNow ? 0.12 : 0.4)],
      ["far-2", at.height * (tallNow ? 0.2 : 0.56)],
    ]) {
      let d = `M-2000,${at.height + 2000} L-2000,${base}`;
      for (let x = -2000; x < at.width + 2000; ) {
        const w = 40 + Math.floor(rnd() * 90),
          top = base - Math.floor(rnd() * 7) * 9 - (rnd() < 0.08 ? 50 : 0);
        d += ` L${x},${top} L${x + w},${top}`;
        x += w + Math.floor(rnd() * 3) * 6;
        d += ` L${x},${base + 30}`;
      }
      svg("path", {class: cls, d: `${d} L${at.width + 2000},${at.height + 2000} Z`}, el);
    }
  }

  // Draws it all anew for these channels (no count on one: nothing known yet,
  // as in the wizard's sketch) and this many objects.
  function draw(channels, objects) {
    el.replaceChildren();
    const tallNow = narrow.matches;
    lastHeight = wideHeight();
    const at = tallNow ? tall(channels) : wide(channels, lastHeight);
    lift = at.lift;
    viewWidth = at.width;
    el.classList.toggle("tall", tallNow);
    el.setAttribute("viewBox", `0 0 ${at.width} ${at.height}`);
    el.setAttribute("preserveAspectRatio", tallNow ? "xMidYMin meet" : "xMidYMax meet");
    birdDefs(el);
    const rnd = seeded(11);
    backdrop(at, rnd, tallNow);
    const routes = svg("g", {}, el),
      town = svg("g", {}, el);
    air = svg("g", {}, el);

    client = you(at.client, objects, town);
    setObjects(objects);

    // The node: the dovecote on its block's roof, its plaque on the wall.
    loft = node({kind: "loft"}, town, `pigeonhole: ${t("demo.loft_role")}`);
    const L = at.loft,
      roof = L.ground - L.floors * 26 - 14;
    block(loft.g, L.x, roof, L.w, L.ground - roof + L.deep, rnd, {shade: 2, loggias: [2, 5], aerials: 1});
    const cx = L.x + L.cote;
    const cote = svg("g", {transform: `translate(${cx},${roof - 4}) scale(${L.scale})`}, loft.g);
    onLoft(cote, dovecote(cote));
    loft.door = [cx, roof - 4 - 150 * L.scale];
    roofLetters(loft.g, L.x + L.w - L.letters, roof, "pigeonhole", {size: L.size}).remove();
    ring(loft, L.x, roof - 4 - 175 * L.scale, L.w, L.ground);

    cards = new Map(channels.map(c => [c.store, card(c, at.stores.get(c.store), town)]));

    // The ways: you to the node, the node to each channel's roof.
    edges = new Map([["", svg("path", {class: "edge live", d: arc(client.perch, loft.door, lift * 0.8)}, routes)]]);
    for (const c of channels)
      edges.set(c.store, svg("path", {class: `edge${c.role === "data" ? "" : " own"}`, d: arc(loft.door, cards.get(c.store).sign, lift * 0.6)}, routes));
  }

  const markOpen = target => {
    const open = nodeOf(target);
    for (const n of [client, loft, ...cards.values()]) n?.g.classList.toggle("open", n === open);
  };
  const focus = target => nodeOf(target)?.g.focus();

  // A channel's count changed (a post landed, one was deleted): its windows anew.
  function setTotal(store) {
    const n = cards.get(store);
    if (n) fill(n, true);
  }
  // The channels as the node has them now (the same ones, read again).
  function setChannels(channels) {
    for (const c of channels) {
      const n = cards.get(c.store);
      if (!n) continue;
      n.c = c;
      fill(n, false);
    }
  }

  // Repair jobs waiting for a channel ({chunk}), a square each over its roof; `title(job)` names one.
  function setQueue(store, jobs, title) {
    const q = cards.get(store)?.queue;
    if (!q) return;
    q.replaceChildren();
    const shown = Math.min(jobs.length, QUEUE_SHOWN),
      left = -((shown - 1) * 9) / 2 - 3;
    jobs.slice(0, QUEUE_SHOWN).forEach((j, i) => {
      const sq = svg("rect", {class: "job", x: left + i * 9, y: 0, width: 6, height: 6, rx: 1.5}, q);
      svg("title", {}, sq).textContent = title(j);
    });
    if (jobs.length > QUEUE_SHOWN) svg("text", {class: "job-more", x: left + QUEUE_SHOWN * 9 + 2, y: 6}, q).textContent = `+${jobs.length - QUEUE_SHOWN}`;
  }

  // ---- pigeons
  const flights = new Set();
  let lastStart = Promise.resolve();
  // A pigeon from the dovecote into a channel's window (the newest
  // message's), or out of a lit one back; bigger for a bigger payload. A
  // failed call reaches the block's next free window and turns back.
  // Pigeons leave 70 ms apart, in the order the node logged them; resolves
  // when this one lands. "" is the client's: between your roof and the node.
  function fly(store, {back = false, size = 4096, failed = false} = {}) {
    const n = cards.get(store);
    if (!(n || (store === "" && client)) || reduced) return Promise.resolve();
    const start = lastStart.then(() => sleep(70));
    lastStart = start;
    const flight = start.then(() => flight1(n, back, size, failed));
    flights.add(flight);
    flight.finally(() => flights.delete(flight));
    return flight;
  }
  // Every pigeon in the air has landed.
  const landed = () => Promise.all([...flights]);

  // The way for one pigeon: to the window it is after.
  function way(n, back, failed) {
    if (!n) return edges.get("");
    const {count} = contents(n.c);
    const shown = Math.min(count, n.slots.length - (count > n.slots.length ? n.cols : 0));
    const i = failed ? shown : back ? Math.floor(Math.random() * Math.max(1, shown)) : shown - 1;
    return svg("path", {class: "way", d: arc(loft.door, windowAt(n, i), lift)}, air);
  }

  function flight1(n, back, size, failed) {
    return new Promise(done => {
      const path = way(n, back, failed),
        own = n ? path : null;
      const end = () => {
        own?.remove();
        bird.g.remove();
        done();
      };
      const len = path.getTotalLength();
      const scale = (0.5 + Math.min(0.35, Math.log2(1 + size / 4096) / 12)) * (el.classList.contains("tall") ? 0.8 : 1);
      const bird = flying(air, {class: `flight${failed ? " failed" : ""}`});
      const t0 = performance.now(),
        dur = Math.min(1400, 450 + len * 1.1);
      // Which way it faces, once for the flight; a failed one turns round at the far end.
      const a = path.getPointAtLength(0),
        b = path.getPointAtLength(len);
      const facing = (b.x - a.x) * (back ? -1 : 1) < 0 ? -1 : 1;
      let mark = null;
      const step = now => {
        // Redrawn under it: its place is gone, so is the pigeon.
        if (!path.isConnected || !air.isConnected) {
          end();
          return;
        }
        // A soft start and landing.
        const k = Math.max(0, Math.min(1, (now - t0) / dur)),
          e = k - (Math.sin(2 * Math.PI * k) / (2 * Math.PI)) * 0.5;
        const pos = failed ? 0.88 * (1 - Math.abs(1 - 2 * e)) : e;
        const along = len * (back ? 1 - pos : pos),
          returning = failed && e > 0.5,
          ahead = back !== returning ? -3 : 3;
        const pt = path.getPointAtLength(along),
          p0 = path.getPointAtLength(Math.max(0, Math.min(len, along - ahead))),
          p1 = path.getPointAtLength(Math.max(0, Math.min(len, along + ahead)));
        const face = returning ? -facing : facing;
        const tilt = Math.max(-35, Math.min(35, (Math.atan2(p1.y - p0.y, (p1.x - p0.x) * face) * 180) / Math.PI));
        bird.g.setAttribute("transform", `translate(${pt.x.toFixed(1)},${pt.y.toFixed(1)}) scale(${face * scale},${scale}) rotate(${tilt.toFixed(1)})`);
        // Flapping off and in to land, on a long way gliding a while between.
        const phase = Math.max(0, now - t0) / 450;
        if (dur > 1000 && e > 0.3 && e < 0.72 && Math.floor(phase) % 3) glide(bird);
        else beat(bird, phase % 1);
        if (returning && !mark) mark = svg("circle", {class: "failed-mark", cx: 14, cy: -16, r: 3.4}, bird.g);
        if (k < 1) requestAnimationFrame(step);
        else end();
      };
      requestAnimationFrame(step);
    });
  }

  // A pigeon landed: its window lights up as the count comes (setTotal).
  function bump() {}

  // ---- the first boot: nodes and ways come up one by one
  function hideAll() {
    el.classList.add("booting");
  }
  function reveal(target) {
    nodeOf(target)?.g.classList.add("up");
    const e = edges.get(target.kind === "store" ? target.store : target.kind === "client" ? "" : null);
    if (!e) return;
    if (reduced) {
      e.classList.add("up");
      return;
    }
    const len = e.getTotalLength();
    e.style.strokeDasharray = `${len}`;
    e.style.strokeDashoffset = `${len}`;
    requestAnimationFrame(() => e.classList.add("up"));
  }
  function showAll() {
    el.classList.remove("booting");
    for (const e of edges.values()) {
      e.style.strokeDasharray = "";
      e.style.strokeDashoffset = "";
    }
  }

  return {draw, markOpen, focus, setTotal, setChannels, setObjects, setQueue, fly, landed, bump, hideAll, reveal, showAll, drawn: () => !!loft};
}
