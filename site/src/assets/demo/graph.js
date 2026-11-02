// The graph, drawn as a town of Soviet blocks of flats: you on yours, the
// node a dovecote on its block's roof with its own channels (its log, its
// snapshots) in hutches beside it, the data channels in hutches on another
// roof; pigeons fly between the roofs.
//
// A node of the graph is a target: {kind: "client"}, {kind: "loft"} or
// {kind: "store", store}. Clicking one calls onOpen(target).
import {activate, reducedMotion, svg} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {beat, birdDefs, block, dovecote, flying, glide, hutch, perched, plaque} from "./art.js";
import {appOf, placeNote} from "./channels.js";

const QUEUE_SHOWN = 12; // repair jobs drawn under a hutch; the rest as "+n"
// A phone's screen: the town top to bottom, scrolled.
const NARROW = "(max-width: 640px)";

const sleep = ms => new Promise(r => setTimeout(r, ms));
// A pigeon's way from one roof to another: up over the roofs and down.
const arc = ([x1, y1], [x2, y2], lift) => {
  const top = Math.min(y1, y2) - lift;
  return `M${x1},${y1} C${x1 + (x2 - x1) * 0.25},${top} ${x1 + (x2 - x1) * 0.75},${top} ${x2},${y2}`;
};
// The same windows lit on every drawing.
const seeded = seed => () => {
  seed = (seed * 9301 + 49297) % 233280;
  return seed / 233280;
};

// Where everything goes. Each layout gives the view's size, the blocks
// (back to front: {x, roof, w, shade, loggias, room, aerials}), where you
// are (`client`: your plaque, `perch`: where your pigeons leave from,
// `bird`: one sitting on your roof), the loft (its foot, its scale, its
// plaque), each store's hutch ({x, roof, scale, short: its label without
// the role}), the data's sign, and how high the pigeons' arcs rise.
// Left to right: you, the node, the data channels on the tallest block;
// `height`: the view's, from the screen's shape (the roofs a little lower
// on a taller one, the blocks taller)…
function wide(channels, height) {
  const data = channels.filter(c => c.role === "data"),
    own = channels.filter(c => c.role !== "data");
  const n = data.length,
    step = n <= 3 ? 140 : Math.max(64, 520 / n),
    scale = n <= 3 ? 1.3 : n <= 5 ? 1 : 0.8;
  const dw = Math.max(320, n * step + 40),
    dx = 1170 - dw,
    down = (height - 640) * 0.3;
  const dataRoof = 250 + down,
    yours = 290 + down,
    nodes = 400 + down;
  const stores = new Map();
  for (const [i, c] of data.entries()) stores.set(c.store, {x: dx + 20 + step / 2 + i * step, roof: dataRoof, scale, short: n > 3});
  for (const [i, c] of own.entries()) stores.set(c.store, {x: 530 + i * 140, roof: nodes, scale: 1, short: false});
  return {
    width: 1200,
    height,
    blocks: [
      {x: dx, roof: dataRoof, w: dw, shade: 1, loggias: [2, 6], room: dw - 60, aerials: 0},
      {x: 30, roof: yours, w: 170, shade: 1, loggias: [1, 4], room: 100, aerials: 1},
      {x: 270, roof: nodes, w: 450, shade: 2, loggias: [2, 5, 11], aerials: 1},
    ],
    client: {plaque: [44, yours + 40], perch: [150, yours - 26], bird: [60, yours - 4]},
    loft: {foot: [380, nodes - 4], scale: 0.95, plaque: [300, nodes + 40]},
    stores,
    sign: {x: dx + 16, y: dataRoof + 80},
    lift: 90,
  };
}

// …or top to bottom, for a phone: you and the node, the data channels on
// the roofs in front, two or three to a roof.
function tall(channels) {
  const data = channels.filter(c => c.role === "data"),
    own = channels.filter(c => c.role !== "data");
  const per = data.length > 4 ? 3 : 2,
    rows = Math.max(1, Math.ceil(data.length / per)),
    step = 388 / per;
  const roofOf = r => 500 + r * 150;
  const stores = new Map();
  for (const [i, c] of data.entries())
    stores.set(c.store, {x: 6 + step / 2 + (i % per) * step, roof: roofOf(Math.floor(i / per)), scale: per === 3 ? 0.8 : 0.95, short: per === 3});
  for (const [i, c] of own.entries()) stores.set(c.store, {x: 262 + i * 92, roof: 300, scale: 0.65, short: true});
  const blocks = [
    {x: 8, roof: 120, w: 130, shade: 1, loggias: [1], room: 70, aerials: 0},
    {x: 4, roof: 300, w: 392, shade: 2, loggias: [2, 6, 11], aerials: 0},
  ];
  for (let r = 0; r < rows; r++) blocks.push({x: 4, roof: roofOf(r), w: 392, shade: r % 2 ? 3 : 1, loggias: [3, 9], aerials: 0});
  return {
    width: 400,
    height: roofOf(rows - 1) + 170,
    blocks,
    client: {plaque: [18, 140], perch: [104, 94], bird: [116, 116]},
    loft: {foot: [118, 296], scale: 0.62, plaque: [20, 340]},
    stores,
    sign: {x: 16, y: roofOf(rows - 1) + 76},
    lift: 50,
  };
}

// onLoft(g, windows): the loft was drawn, its entrances are free to dress.
// onRelayout(): the screen turned narrow or wide: draw again.
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
  let client = null,
    loft = null,
    air = null,
    tallNow = false;
  let cards = new Map(); // store id → its hutch
  let edges = new Map(); // store id → the way to it; "" → the client's

  const nodeOf = target => (target?.kind === "client" ? client : target?.kind === "loft" ? loft : target?.kind === "store" ? cards.get(target.store) : null);

  // A clickable node: a group, named for screen readers.
  function node(target, parent, label) {
    const g = svg("g", {class: "node", tabindex: 0, role: "button", "aria-label": label}, parent);
    activate(g, () => onOpen(target));
    return {g};
  }

  // A channel's hutch on its roof, its name (and what it is for) on the
  // wall under it, repair jobs under that.
  function card(c, at, parent) {
    const role = placeNote(c) + (c.bots > 1 ? `, ${t("demo.bots", {n: c.bots})}` : "") + (c.down ? `, ${t("demo.down")}` : "");
    const n = node({kind: "store", store: c.store}, parent, `${c.store}: ${role}`);
    n.g.classList.toggle("down", !!c.down);
    const place = svg("g", {transform: `translate(${at.x},${at.roof - 4})`}, n.g);
    n.body = svg("g", {class: "hutch"}, svg("g", {transform: `scale(${at.scale})`}, place));
    svg("path", {class: "ring", d: "M-30,4 L-30,-34 L0,-56 L30,-34 L30,4 Z"}, n.body);
    const parts = hutch(n.body, c.kind, appOf(c.kind), c.down);
    n.door = [at.x, at.roof - 4 + parts.door * at.scale];
    n.count = svg("text", {class: "count", x: 0, y: parts.top - 8, "text-anchor": "middle"}, n.body);
    n.tip = svg("title", {}, n.g);
    svg("text", {class: "sign", x: 0, y: 22, "text-anchor": "middle"}, place).textContent = c.store;
    const sub = at.short ? (c.down ? t("demo.down") : "") : role;
    if (sub) svg("text", {class: "sign-sub", x: 0, y: 37, "text-anchor": "middle"}, place).textContent = sub;
    n.queue = svg("g", {class: "queue", transform: `translate(0,${sub ? 44 : 30})`}, place);
    return n;
  }

  // The sky, and the town far off: wider and taller than the view, so that
  // on a screen of another shape they fill it still.
  function backdrop(at, rnd) {
    const sky = svg("linearGradient", {id: "sky", x1: 0, y1: 0, x2: 0, y2: at.height, gradientUnits: "userSpaceOnUse"}, el.querySelector("defs"));
    svg("stop", {offset: 0, class: "sky-top"}, sky);
    svg("stop", {offset: 1, class: "sky-low"}, sky);
    svg("rect", {x: -2000, y: -2000, width: at.width + 4000, height: at.height + 4000, fill: "url(#sky)"}, el);
    for (const [cls, base] of [
      ["far", at.height * (tallNow ? 0.12 : 0.36)],
      ["far-2", at.height * (tallNow ? 0.2 : 0.5)],
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
    tallNow = narrow.matches;
    lastHeight = wideHeight();
    const at = tallNow ? tall(channels) : wide(channels, lastHeight);
    el.classList.toggle("tall", tallNow);
    el.setAttribute("viewBox", `0 0 ${at.width} ${at.height}`);
    el.setAttribute("preserveAspectRatio", tallNow ? "xMidYMin meet" : "xMidYMax meet");
    birdDefs(el);
    const rnd = seeded(11);
    backdrop(at, rnd);
    const routes = svg("g", {}, el),
      town = svg("g", {}, el);
    air = svg("g", {}, el);
    // The blocks, back to front, down past the view's bottom.
    for (const b of at.blocks) block(town, b.x, b.roof, b.w, at.height - b.roof + 2000, rnd, b);

    // You: a plaque on your block, a pigeon on its roof.
    client = node({kind: "client"}, town, `${t("demo.you")}: ${t("demo.objects", {n: objects})}`);
    client.sub = plaque(client.g, ...at.client.plaque, t("demo.you"), t("demo.objects", {n: objects}));
    perched(town, {transform: `translate(${at.client.bird[0]},${at.client.bird[1] - 8}) scale(1.4)`});

    // The node: the dovecote on its roof, its plaque under it.
    loft = node({kind: "loft"}, town, `pigeonhole: ${t("demo.loft_role")}`);
    const [lx, ly] = at.loft.foot,
      s = at.loft.scale;
    const cote = svg("g", {transform: `translate(${lx},${ly}) scale(${s})`}, loft.g);
    svg("rect", {class: "ring", x: -80, y: -180, width: 160, height: 186, rx: 10}, cote);
    onLoft(cote, dovecote(cote));
    loft.door = [lx, ly - 150 * s];
    plaque(loft.g, ...at.loft.plaque, "pigeonhole", t("demo.loft_role"));

    // The channels' hutches; the data's sign on its block.
    if (channels.some(c => c.role === "data")) plaque(town, at.sign.x, at.sign.y, t("demo.group_data"), null);
    cards = new Map(channels.map(c => [c.store, card(c, at.stores.get(c.store), town)]));
    for (const c of channels) setTotal(c.store, c.total);

    // The ways: you to the node, the node to each channel.
    edges = new Map([["", svg("path", {class: "edge live", d: arc(at.client.perch, loft.door, at.lift * 0.8)}, routes)]]);
    for (const c of channels) {
      const own = c.role !== "data";
      edges.set(c.store, svg("path", {class: `edge${own ? " own" : ""}`, d: arc(loft.door, cards.get(c.store).door, own ? at.lift * 0.4 : at.lift)}, routes));
    }
  }

  const markOpen = target => {
    const open = nodeOf(target);
    for (const n of [client, loft, ...cards.values()]) n?.g.classList.toggle("open", n === open);
  };
  const focus = target => nodeOf(target)?.g.focus();

  function setTotal(store, total) {
    const n = cards.get(store);
    if (!n || total == null) return;
    n.count.textContent = String(total);
    n.tip.textContent = t("demo.messages", {n: total});
  }
  const setObjects = n => {
    if (!client) return;
    client.sub.textContent = t("demo.objects", {n});
    client.g.setAttribute("aria-label", `${t("demo.you")}: ${t("demo.objects", {n})}`);
  };

  // Repair jobs waiting for a channel ({chunk}), a square each; `title(job)` names one.
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
  // A pigeon from the loft to a channel (or back), bigger for a bigger
  // payload; a failed call reaches the channel and turns back. Pigeons
  // leave 70 ms apart, in the order the node logged them; resolves when
  // this one lands.
  function fly(store, {back = false, size = 4096, failed = false} = {}) {
    const path = edges.get(store);
    if (!path || reduced) return Promise.resolve();
    const start = lastStart.then(() => sleep(70));
    lastStart = start;
    const flight = start.then(() => flight1(path, back, size, failed));
    flights.add(flight);
    flight.finally(() => flights.delete(flight));
    return flight;
  }
  // Every pigeon in the air has landed.
  const landed = () => Promise.all([...flights]);

  function flight1(path, back, size, failed) {
    return new Promise(done => {
      const len = path.getTotalLength();
      const scale = (0.5 + Math.min(0.35, Math.log2(1 + size / 4096) / 12)) * (tallNow ? 0.8 : 1);
      const bird = flying(air, {class: `flight${failed ? " failed" : ""}`});
      const t0 = performance.now(),
        dur = Math.min(1400, 450 + len * 1.1);
      // Which way it faces, once for the flight; a failed one turns round at the far end.
      const a = path.getPointAtLength(0),
        b = path.getPointAtLength(len);
      const facing = (b.x - a.x) * (back ? -1 : 1) < 0 ? -1 : 1;
      let mark = null;
      const step = now => {
        // Redrawn under it: the way is gone, so is the pigeon.
        if (!path.isConnected) {
          bird.g.remove();
          done();
          return;
        }
        // A soft start and landing.
        const k = Math.min(1, (now - t0) / dur),
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
        const phase = (now - t0) / 300;
        if (dur > 1000 && e > 0.3 && e < 0.72 && Math.floor(phase) % 3) glide(bird);
        else beat(bird, phase % 1);
        if (returning && !mark) mark = svg("circle", {class: "failed-mark", cx: 14, cy: -16, r: 3.4}, bird.g);
        if (k < 1) requestAnimationFrame(step);
        else {
          bird.g.remove();
          done();
        }
      };
      requestAnimationFrame(step);
    });
  }

  // A hutch's little jump where a pigeon landed (demo.css: .landed), from
  // the start again for each pigeon.
  function bump(store) {
    const n = cards.get(store);
    if (!n || reduced) return;
    n.g.classList.add("landed");
    for (const a of n.body.getAnimations()) {
      a.cancel();
      a.play();
    }
  }

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

  return {draw, markOpen, focus, setTotal, setObjects, setQueue, fly, landed, bump, hideAll, reveal, showAll, drawn: () => !!loft};
}
