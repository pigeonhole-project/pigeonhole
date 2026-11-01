// The graph: you (the S3 client), the node (a dovecote) and its channels,
// the data channels in one group and the node's own (its log, its
// snapshots) in another; pigeons fly along the edges.
//
// A node of the graph is a target: {kind: "client"}, {kind: "loft"} or
// {kind: "store", store}. Clicking one calls onOpen(target).
import {activate, reducedMotion, svg} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {dovecote, FLY_BODY, FLY_WING, logo} from "./art.js";
import {appOf, placeNote} from "./channels.js";

const W = 1000;
const STEP = 96,
  GAP = 64,
  HEAD = 34; // a channel's row, between groups, a group's title
const QUEUE_SHOWN = 12; // repair jobs drawn under a card; the rest as "+n"

const sleep = ms => new Promise(r => setTimeout(r, ms));
const curve = ([x1, y1], [x2, y2]) => {
  const mx = (x1 + x2) / 2;
  return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
};

// Where everything goes for these channels: the view's height, each node's
// centre (client, loft, then stores by id) and the groups' boxes.
function layout(channels) {
  const data = channels.filter(c => c.role === "data");
  const meta = channels.filter(c => c.role !== "data");
  const groups = [data, meta].filter(g => g.length);
  const tall = groups.reduce((a, g) => a + HEAD + g.length * STEP, 0) + GAP * (groups.length - 1);
  const height = Math.max(480, tall + 80);
  const client = [120, height / 2],
    loft = [440, height / 2],
    stores = new Map(),
    boxes = [];
  let y = (height - tall) / 2;
  for (const g of groups) {
    boxes.push({role: g === data ? "data" : "meta", top: y, height: HEAD + g.length * STEP});
    for (const [i, c] of g.entries()) stores.set(c.store, [790, y + HEAD + STEP / 2 + i * STEP]);
    y += HEAD + g.length * STEP + GAP;
  }
  return {height, client, loft, stores, boxes};
}

// onLoft(g, windows): the loft was drawn, its entrances are free to dress.
export function makeGraph(el, {onOpen, onLoft}) {
  const reduced = reducedMotion();
  let client = null,
    loft = null;
  let cards = new Map(); // store id → its card
  let edges = new Map(); // store id → the edge to it; "" → the client's

  const nodeOf = target => (target?.kind === "client" ? client : target?.kind === "loft" ? loft : target?.kind === "store" ? cards.get(target.store) : null);

  // A clickable node: a group at `p` with its name and role under (or beside) it.
  function node(target, p, name, role) {
    const g = svg("g", {class: "node", transform: `translate(${p[0]},${p[1]})`, tabindex: 0, role: "button", "aria-label": `${name}: ${role}`}, el);
    activate(g, () => onOpen(target));
    const n = {g, p, role: null};
    n.label = (x, y, anchor = "middle") => {
      const top = anchor === "start" ? y - 6 : y;
      svg("text", {class: "name", x, y: top, "text-anchor": anchor}, g).textContent = name;
      n.role = svg("text", {class: "role", x, y: top + 18, "text-anchor": anchor}, g);
      n.role.textContent = role;
    };
    n.setRole = text => {
      n.role.textContent = text;
      g.setAttribute("aria-label", `${name}: ${text}`);
    };
    return n;
  }

  function card(c, p) {
    const role = placeNote(c) + (c.bots > 1 ? `, ${t("demo.bots", {n: c.bots})}` : "") + (c.down ? `, ${t("demo.down")}` : "");
    const n = node({kind: "store", store: c.store}, p, c.store, role);
    n.g.classList.toggle("down", !!c.down);
    svg("rect", {class: `shape${c.role === "data" ? "" : " meta"}`, x: -130, y: -34, width: 260, height: 68, rx: 14}, n.g);
    logo(c.kind, n.g, -106, 0, appOf(c.kind));
    n.label(-88, 0, "start");
    // The channel's message count, top right; repair jobs waiting for it, under the card.
    n.count = svg("text", {class: "count", x: 116, y: -10, "text-anchor": "end"}, n.g);
    n.tip = svg("title", {}, n.g);
    n.queue = svg("g", {class: "queue", transform: "translate(-118,46)"}, n.g);
    return n;
  }

  // Draws it all anew for these channels (no count on one: nothing known yet,
  // as in the wizard's sketch) and this many objects.
  function draw(channels, objects) {
    el.replaceChildren();
    const at = layout(channels);
    el.setAttribute("viewBox", `0 0 ${W} ${at.height}`);
    el.setAttribute("preserveAspectRatio", "xMidYMid meet");
    // The two groups of channels, under everything else.
    for (const b of at.boxes) {
      svg("rect", {class: `group ${b.role}`, x: 642, y: b.top, width: 296, height: b.height - 8, rx: 18}, el);
      svg("text", {class: "group-label", x: 660, y: b.top + 22}, el).textContent = t(`demo.group_${b.role}`);
    }
    const iris = svg("linearGradient", {id: "iris", x1: "0", y1: "0", x2: "1", y2: "1"}, svg("defs", {}, el));
    svg("stop", {offset: "0", "stop-color": "var(--teal)"}, iris);
    svg("stop", {offset: "1", "stop-color": "var(--amethyst)"}, iris);

    const edgeLayer = svg("g", {}, el);
    edges = new Map([["", svg("path", {class: "edge live", d: curve([166, at.client[1]], [345, at.loft[1]])}, edgeLayer)]]);
    for (const c of channels) {
      const d = curve([535, at.loft[1]], [660, at.stores.get(c.store)[1]]);
      edges.set(c.store, svg("path", {class: `edge${c.role === "data" ? "" : " meta"}`, d}, edgeLayer));
    }

    client = node({kind: "client"}, at.client, t("demo.you"), t("demo.objects", {n: objects}));
    svg("circle", {class: "shape", cx: 0, cy: 0, r: 46}, client.g);
    client.label(0, 72);

    loft = node({kind: "loft"}, at.loft, "pigeonhole", t("demo.loft_role"));
    onLoft(loft.g, dovecote(loft.g));
    loft.label(0, 100);

    cards = new Map(channels.map(c => [c.store, card(c, at.stores.get(c.store))]));
    for (const c of channels) setTotal(c.store, c.total);
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
  const setObjects = n => client?.setRole(t("demo.objects", {n}));

  // Repair jobs waiting for a channel ({chunk}), a square each; `title(job)` names one.
  function setQueue(store, jobs, title) {
    const q = cards.get(store)?.queue;
    if (!q) return;
    q.replaceChildren();
    jobs.slice(0, QUEUE_SHOWN).forEach((j, i) => {
      const sq = svg("rect", {class: "job", x: i * 14, y: 0, width: 10, height: 10, rx: 2}, q);
      svg("title", {}, sq).textContent = title(j);
    });
    if (jobs.length > QUEUE_SHOWN) svg("text", {class: "job-more", x: QUEUE_SHOWN * 14 + 2, y: 9}, q).textContent = `+${jobs.length - QUEUE_SHOWN}`;
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
      const scale = 0.9 + Math.min(1, Math.log2(1 + size / 4096) / 10);
      const bird = svg("g", {class: `flight${failed ? " failed" : ""}`}, el);
      svg("path", {fill: "url(#iris)", d: FLY_BODY}, bird);
      const wing = svg("path", {fill: "url(#iris)", d: FLY_WING}, bird);
      const len = path.getTotalLength(),
        t0 = performance.now(),
        dur = 680;
      const step = now => {
        // Redrawn under it: the edge is gone, so is the pigeon.
        if (!path.isConnected) {
          bird.remove();
          done();
          return;
        }
        const k = Math.min(1, (now - t0) / dur),
          e = k < 0.5 ? 2 * k * k : 1 - (-2 * k + 2) ** 2 / 2;
        const at = failed ? len * 0.92 * (1 - Math.abs(1 - 2 * e)) : len * (back ? 1 - e : e);
        const pt = path.getPointAtLength(at),
          ahead = path.getPointAtLength(Math.min(len, Math.max(0, at + (back ? -1 : 1))));
        const flip = ahead.x < pt.x ? -1 : 1;
        const flap = 0.55 + 0.45 * Math.abs(Math.sin((now - t0) / 55));
        bird.setAttribute("transform", `translate(${pt.x},${pt.y}) scale(${flip * scale},${scale})`);
        wing.setAttribute("transform", `translate(2.8,-1.2) scale(1,${flap}) translate(-2.8,1.2)`);
        if (k < 1) requestAnimationFrame(step);
        else {
          bird.remove();
          done();
        }
      };
      requestAnimationFrame(step);
    });
  }

  // A flash of a card's outline where a pigeon landed (demo.css: .landed),
  // from the start again for each pigeon.
  function bump(store) {
    const n = cards.get(store);
    if (!n || reduced) return;
    n.g.classList.add("landed");
    for (const a of n.g.querySelector(".shape").getAnimations()) {
      a.cancel();
      a.play();
    }
  }

  // ---- the first boot: nodes and edges come up one by one
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
