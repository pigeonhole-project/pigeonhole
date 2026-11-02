// The graph, drawn as a town of Soviet blocks of flats. Each channel is a
// block and each of its windows a message, filled from the ground floor up:
// a chunk's window lit warm, a small text's cool, a deleted message's
// shuttered, the pinned owner marker's flagged; the block grows a floor as
// it fills (up to a height, then "+n" older ones under). You live on a
// block whose windows are your objects; the node is a dovecote on its
// block's roof. Pigeons fly from the dovecote into the windows.
//
// A cluster is a dovecote per node, each on its own block, its log and
// snapshots beside it; the leader's flies a flag, one cut off is dark. Raft's
// pigeons fly between them, or through their outboxes over the chat.
//
// A node of the graph is a target: {kind: "client"}, {kind: "loft", node}
// (node: a cluster's, else none) or {kind: "store", store}. Clicking one
// calls onOpen(target).
import {activate, reducedMotion, svg} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {beat, birdDefs, block, boarded, dovecote, flag, flying, glide, perched, roofLetters, tower} from "./art.js";
import {appOf, placeNote} from "./channels.js";
import {ownerOf} from "./flights.js";
import {contents, floorsFor, layout} from "./layout.js";

const QUEUE_SHOWN = 12; // repair jobs drawn over a block; the rest as "+n"
const RAFT_SHOWN = 6; // Raft's pigeons in the air at most; the rest are let go
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

// onLoft(g, windows): the node on screen was drawn, its dovecote's
// entrances are free to dress (the other nodes' are shuttered).
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
    air = null,
    lift = 80;
  let lofts = new Map(); // node id (0: the one node) → its block and dovecote
  // A cluster as the page knows it: who leads, who is cut off, which node is on screen.
  // `every`: the entries between two snapshots (the log's block has room for them).
  let scene = {leader: null, cut: new Set(), shown: null, links: [], every: 0};
  // A node's block: node `id`'s, else the one on screen, else the only one.
  // The one node's dovecote is every flight's; a cluster's, by its id.
  const loftOf = id => (lofts.has(0) ? lofts.get(0) : (lofts.get(id ?? scene.shown) ?? lofts.get(scene.leader) ?? lofts.values().next().value));
  // The dovecote a channel's way leads from (flights.js: whose traffic it is).
  const homeOf = c => loftOf(ownerOf(c, scene));
  let cards = new Map(); // store id → its block
  let edges = new Map(); // store id → the way to its block; "" → the client's

  const nodeOf = target =>
    target?.kind === "client" ? client : target?.kind === "loft" ? loftOf(target.node) : target?.kind === "store" ? cards.get(target.store) : null;

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
    // A node's own two blocks come as tall as each other (its bridge lies on both).
    n.floors = at.floors ?? floorsFor(contents(c).count, at.cols, at.most);
    const b = tower(n.g, at.x, at.ground, at.w, n.floors, at.cols, {shade: c.role === "data" ? 1 : 3, deep: at.deep});
    n.slots = b.slots;
    n.win = svg("g", {}, n.g);
    // The letters on its roof, kept inside the view.
    const size = at.size ?? (at.w < 70 ? 12 : 15),
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
  function draw(channels, objects, cluster = null) {
    if (cluster) scene = cluster;
    el.replaceChildren();
    const tallNow = narrow.matches;
    lastHeight = wideHeight();
    const at = layout(channels, {tall: tallNow, height: lastHeight, every: scene.every});
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

    const onScreen = at.lofts.has(0) ? 0 : (scene.shown ?? scene.leader ?? [...at.lofts.keys()][0]);
    // The blocks first: a node's bridge lies on its own two.
    cards = new Map(channels.map(c => [c.store, card(c, at.stores.get(c.store), town)]));
    lofts = new Map([...at.lofts].map(([id, L]) => [id, loftBlock(id, L, town, rnd, id === onScreen)]));

    // The ways: you to the node you talk to, each node to its own channels,
    // the leader to the data channels; between nodes with a direct link, theirs.
    edges = new Map([["", svg("path", {class: "edge live", d: arc(client.perch, loftOf().door, lift * 0.8)}, routes)]]);
    for (const c of channels)
      edges.set(c.store, svg("path", {class: `edge${c.role === "data" ? "" : " own"}`, d: arc(homeOf(c).door, cards.get(c.store).sign, lift * 0.6)}, routes));
    for (const l of scene.links.filter(l => l.via === "grpc" && lofts.has(l.a) && lofts.has(l.b)))
      edges.set(`raft:${l.a}-${l.b}`, svg("path", {class: "edge raft", d: arc(lofts.get(l.a).door, lofts.get(l.b).door, lift * 0.4)}, routes));
  }

  // A node: the dovecote on its block's roof, the name on the roof. In a
  // cluster, what it is now under the name; the leader's flies a flag. The
  // node on screen's entrances are its background tasks (onLoft).
  function loftBlock(id, L, parent, rnd, onScreen) {
    const cluster = id !== 0;
    const role = !cluster
      ? t("demo.loft_role")
      : scene.cut.has(id)
        ? t("nodes.role_cut")
        : id === scene.leader
          ? t("nodes.role_leader")
          : t("nodes.role_follower");
    const name = cluster ? t("nodes.node", {n: id}) : "pigeonhole";
    const n = node(cluster ? {kind: "loft", node: id} : {kind: "loft"}, parent, `${name}: ${role}`);
    n.g.classList.toggle("cut", scene.cut.has(id));
    // No log or snapshots of its own to stand on (a real node's in memory): a block of its own.
    if (L.alone) block(n.g, L.x, L.roof, L.w, L.ground - L.roof + 2000, rnd, {shade: 2, aerials: 1});
    // The bridge: a slab high over the blocks' letters, on two posts.
    const deck = L.roof - L.size - 44,
      cx = L.x + L.w / 2;
    // Its posts stand beside the blocks, clear of their letters, down to the ground.
    for (const px of [L.x - 5, L.x + L.w + 1]) svg("rect", {class: "post", x: px, y: deck, width: 4, height: L.ground - deck + 2000}, n.g);
    svg("rect", {class: "parapet", x: L.x - 8, y: deck - 5, width: L.w + 16, height: 6}, n.g);
    // Its name (and in a cluster what it is now) on a sign under the slab.
    const sign = svg("text", {class: "sign node-sign", x: cx, y: deck + 15, "text-anchor": "middle", style: `font-size: ${Math.min(12, L.size + 2)}px`}, n.g);
    sign.textContent = name;
    if (cluster) {
      const r = svg("tspan", {class: `node-role${id === scene.leader ? " leading" : ""}`}, sign);
      r.textContent = ` · ${role}`;
    }
    const cote = svg("g", {transform: `translate(${cx},${deck - 4}) scale(${L.scale})`}, n.g);
    const entrances = dovecote(cote);
    if (onScreen) onLoft(cote, entrances);
    else for (const w of entrances) boarded(cote, w);
    if (cluster && id === scene.leader && !scene.cut.has(id)) flag(cote);
    n.door = [cx, deck - 4 - 150 * L.scale];
    ring(n, L.x - 4, deck - 4 - 175 * L.scale, L.w + 8, deck + 20);
    return n;
  }

  const markOpen = target => {
    const open = nodeOf(target);
    for (const n of [client, ...lofts.values(), ...cards.values()]) n?.g.classList.toggle("open", n === open);
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

  // ---- pigeons: flights (flights.js) from place to place
  const flights = new Set();
  let lastStart = Promise.resolve();
  let raftInAir = 0;

  // Where a place is on the town now; null: not drawn.
  function anchor(p) {
    if (p.client) return client?.perch ?? null;
    if ("node" in p) return loftOf(p.node)?.door ?? null;
    const n = cards.get(p.store);
    if (!n) return null;
    if (p.at === "sign") return n.sign;
    const {count} = contents(n.c);
    const shown = Math.min(count, n.slots.length - (count > n.slots.length ? n.cols : 0));
    const i = p.at === "free" ? shown : p.at === "any" ? Math.floor(Math.random() * Math.max(1, shown)) : shown - 1;
    return windowAt(n, i);
  }

  // One flight; resolves when it lands. Yours and the node's leave 70 ms
  // apart, in the order the node logged them; Raft's own traffic goes at
  // once, a few in the air at most (the rest are let go), and is not waited for.
  function fly(f) {
    if (reduced) return Promise.resolve();
    const raft = f.traffic === "raft";
    if (raft && (raftInAir >= RAFT_SHOWN || [f.from, f.to].some(p => "node" in p && scene.cut.has(p.node)))) return Promise.resolve();
    const go = () => {
      const a = anchor(f.from),
        b = anchor(f.to);
      if (!a || !b || !air) return Promise.resolve();
      const path = svg("path", {class: "way", d: arc(a, b, lift * (raft ? 0.45 : 1))}, air);
      return f.spark ? spark(path) : wing(path, {size: f.size, failed: f.failed, cls: raft ? `raft ${f.kind}` : ""});
    };
    if (raft) {
      raftInAir++;
      const flight = go();
      flight.finally(() => raftInAir--);
      return flight;
    }
    const start = lastStart.then(() => sleep(70));
    lastStart = start;
    const flight = start.then(go);
    flights.add(flight);
    flight.finally(() => flights.delete(flight));
    return flight;
  }
  // Every pigeon of yours or the node's in the air has landed.
  const landed = () => Promise.all([...flights]);

  // A spark along a way: a heartbeat.
  function spark(path) {
    return new Promise(done => {
      const dot = svg("circle", {class: "spark", r: 2.6}, air),
        len = path.getTotalLength(),
        t0 = performance.now();
      const step = now => {
        const k = Math.min(1, (now - t0) / 700);
        if (!path.isConnected || k >= 1) {
          path.remove();
          dot.remove();
          done();
          return;
        }
        const pt = path.getPointAtLength(len * k);
        dot.setAttribute("cx", pt.x.toFixed(1));
        dot.setAttribute("cy", pt.y.toFixed(1));
        requestAnimationFrame(step);
      };
      requestAnimationFrame(step);
    });
  }

  // One pigeon along `path` (its own: gone with it).
  function wing(path, {size = 4096, failed = false, cls = ""}) {
    return new Promise(done => {
      const end = () => {
        path.remove();
        bird.g.remove();
        done();
      };
      const len = path.getTotalLength();
      const scale = (0.5 + Math.min(0.35, Math.log2(1 + size / 4096) / 12)) * (el.classList.contains("tall") ? 0.8 : 1);
      const bird = flying(air, {class: `flight${failed ? " failed" : ""}${cls ? ` ${cls}` : ""}`});
      const t0 = performance.now(),
        dur = Math.min(1400, 450 + len * 1.1);
      // Which way it faces, once for the flight; a failed one turns round at the far end.
      const a = path.getPointAtLength(0),
        b = path.getPointAtLength(len);
      const facing = b.x - a.x < 0 ? -1 : 1;
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
        const along = len * pos,
          returning = failed && e > 0.5,
          ahead = returning ? -3 : 3;
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

  return {
    draw,
    markOpen,
    focus,
    setTotal,
    setChannels,
    setObjects,
    setQueue,
    fly,
    landed,
    bump,
    hideAll,
    reveal,
    showAll,
    drawn: () => lofts.size > 0,
  };
}
