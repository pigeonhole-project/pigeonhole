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
import {birdDefs, block, boarded, dovecote, flag, perched, roofLetters, tower} from "./art.js";
import {appOf, placeNote} from "./channels.js";
import {ownerOf} from "./flights.js";
import {contents, floorsFor, layout} from "./layout.js";
import {arc as arcPoints} from "./pigeons/model.js";
import {makePigeons} from "./pigeons/render.js";

const QUEUE_SHOWN = 12; // repair jobs drawn over a block; the rest as "+n"
// A phone's screen: the town top to bottom, scrolled.
const NARROW = "(max-width: 640px)";

// A pigeon's way from one place to another: up over the roofs and down.
const arc = (a, b, lift) => {
  const [p0, p1, p2, p3] = arcPoints(a, b, lift);
  return `M${p0} C${p1} ${p2} ${p3}`;
};
// The same windows lit on every drawing.
const seeded = seed => () => {
  seed = (seed * 9301 + 49297) % 233280;
  return seed / 233280;
};

// channel(store): the channel as the page has it now (its windows are
// drawn from it; none: nothing posted yet, as in the wizard's sketch).
// onLoft(g, windows): the node on screen was drawn, its dovecote's
// entrances are free to dress (the other nodes' are shuttered).
// onRelayout(): the screen turned narrow or wide: draw again.
export function makeGraph(el, {channel, onOpen, onLoft, onRelayout}) {
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
    lift = 80;
  let lofts = new Map(); // node id (0: the one node) → its block and dovecote
  // A cluster as the page knows it: who leads, who is cut off, which node is on screen.
  let scene = {leader: null, cut: new Set(), shown: null, links: []};
  // A node's block: node `id`'s, else the one on screen, else the only one.
  // The one node's dovecote is every flight's; a cluster's, by its id.
  const loftOf = id => (lofts.has(0) ? lofts.get(0) : (lofts.get(id ?? scene.shown) ?? lofts.get(scene.leader) ?? lofts.values().next().value));
  // The dovecote a channel's way leads from (flights.js: whose traffic it is).
  const homeOf = c => loftOf(ownerOf(c, scene));
  let cards = new Map(); // store id → its block
  // What a block's windows hold: its channel's messages now.
  const holds = n => channel(n.store) ?? {};
  const levels = new Map(); // store id → its bots' send budget (setBudget), kept across redraws
  let edges = new Map(); // store id → the way to its block; "" → the client's

  const nodeOf = target =>
    target?.kind === "client" ? client : target?.kind === "loft" ? loftOf(target.node) : target?.kind === "store" ? cards.get(target.store) : null;

  // A clickable node: a group, named for screen readers.
  function node(target, parent, label) {
    const g = svg("g", {class: "node", tabindex: 0, role: "button", "aria-label": label}, parent);
    activate(g, () => onOpen(target));
    return {g};
  }
  // A frame round a block's walls (not the letters on its roof), shown when it is hovered, focused or open.
  const ring = (n, x, top, w, ground) => svg("rect", {class: "ring", x: x - 5, y: top - 5, width: w + 10, height: ground - top + 10, rx: 4}, n.g);

  // A channel's block: its windows, the letters on its roof.
  function card(c, at, parent) {
    const role = placeNote(c) + (c.bots > 1 ? `, ${t("demo.bots", {n: c.bots})}` : "") + (c.down ? `, ${t("demo.down")}` : "");
    const n = node({kind: "store", store: c.store}, parent, `${c.store}: ${role}`);
    n.g.classList.toggle("down", !!c.down);
    n.store = c.store;
    n.cols = at.cols;
    n.most = at.most;
    // A node's own two blocks come as tall as each other (its bridge lies on both).
    n.floors = at.floors ?? floorsFor(at.most);
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
    // Its bots' send budget left (budgets.js sendLevel): a bar on the roof.
    n.budget = svg("g", {class: "budget-bar", transform: `translate(${cx - 18},${b.roof + 2})`}, n.g);
    svg("rect", {class: "budget-bg", width: 36, height: 3, rx: 1.5}, n.budget);
    n.budgetFill = svg("rect", {class: "budget-fg", width: 0, height: 3, rx: 1.5}, n.budget);
    setBudgetOf(n, levels.get(c.store) ?? null);
    ring(n, at.x, b.roof + 8, at.w, at.ground);
    n.shown = 0;
    fill(n, false);
    return n;
  }

  // A channel's windows, from what it holds now; `fresh`: light the new ones up.
  function fill(n, fresh) {
    const c = holds(n);
    const {older, list, count} = contents(c);
    const room = n.slots.length - (count > n.slots.length ? n.cols : 0);
    const shown = Math.min(count, room),
      skip = count - shown;
    const pins = new Set((c.pinned || []).map(p => String(p.id)));
    n.win.replaceChildren();
    for (const [i, s] of n.slots.entries()) {
      const m = i < shown && skip + i >= older ? list[skip + i - older] : null;
      let cls = i >= shown ? "pane" : m?.kind === "text" ? "pane lit text" : m?.kind === "other" ? "pane lit svc" : "pane lit";
      if (fresh && i >= n.shown && i < shown) cls += " new";
      svg("rect", {class: cls, x: s.x, y: s.y, width: s.w, height: s.h}, n.win);
      if (m && pins.has(String(m.id)))
        svg("path", {class: "pin-flag", d: `M${s.x + s.w},${s.y + 1} L${s.x + s.w},${s.y - 9} L${s.x + s.w + 8},${s.y - 6} L${s.x + s.w},${s.y - 3}`}, n.win);
    }
    n.shown = shown;
    n.older.textContent = skip ? `+${skip}` : "";
    n.count.textContent = String(c.total ?? count);
  }
  // Where a window is, by its place among the shown.
  const windowAt = (n, i) => {
    const s = n.slots[Math.max(0, Math.min(n.slots.length - 1, i))];
    return [s.x + s.w / 2, s.y + s.h / 2];
  };

  // You: a block whose windows are your objects, a pigeon on its roof.
  function you(at, parent) {
    const n = node({kind: "client"}, parent, "");
    n.floors = floorsFor(at.most);
    n.at = at;
    const b = tower(n.g, at.x, at.ground, at.w, n.floors, at.cols, {shade: 2, deep: at.deep});
    n.slots = b.slots;
    n.win = svg("g", {}, n.g);
    n.sub = roofLetters(n.g, at.x + at.w / 2, b.roof, t("demo.you"), {size: 13});
    n.perch = [at.x + at.w - 14, b.roof - 6];
    perched(n.g, {transform: `translate(${at.x + at.w + 2},${b.roof - 12}) scale(1.3)`});
    ring(n, at.x, b.roof + 8, at.w, at.ground);
    return n;
  }
  function setObjects(count) {
    if (!client) return;
    client.objects = count;
    client.sub.textContent = String(count);
    client.g.setAttribute("aria-label", `${t("demo.you")}: ${t("demo.objects", {n: count})}`);
    client.win.replaceChildren();
    for (const [i, s] of client.slots.entries()) svg("rect", {class: i < count ? "pane lit" : "pane", x: s.x, y: s.y, width: s.w, height: s.h}, client.win);
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
    const at = layout(channels, {tall: tallNow, height: lastHeight});
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
    pigeons.attach(svg("g", {}, el));

    client = you(at.client, town);
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
    // Drawn anew mid-boot (a poll changed the town): what was up stays up.
    for (const t of revealed) up(t);
    if (waysUp) for (const e of edges.values()) e.classList.add("up");
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
      n.role = svg("tspan", {class: `node-role${id === scene.leader ? " leading" : ""}`}, sign);
      n.role.textContent = ` · ${role}`;
      n.term = svg("tspan", {class: "node-term"}, sign);
    }
    const cote = svg("g", {transform: `translate(${cx},${deck - 4}) scale(${L.scale})`}, n.g);
    const entrances = dovecote(cote);
    if (onScreen) onLoft(cote, entrances);
    else for (const w of entrances) boarded(cote, w);
    if (cluster && id === scene.leader && !scene.cut.has(id)) flag(cote);
    n.door = [cx, deck - 4 - 150 * L.scale];
    // Its election clock: an arc round the dovecote, wound up by each word
    // from a leader, running down to the election's timeout.
    if (cluster) {
      const r = 105 * L.scale;
      n.clockLen = 2 * Math.PI * r;
      n.clock = svg("circle", {class: "election-clock", cx, cy: deck - 4 - 80 * L.scale, r, transform: `rotate(-90 ${cx} ${deck - 4 - 80 * L.scale})`}, n.g);
      n.clock.style.display = "none";
    }
    ring(n, L.x - 4, deck - 4 - 175 * L.scale, L.w + 8, deck + 20);
    return n;
  }

  // Each node of a cluster as it says now (raft.js): {role, term, clock:
  // 1…0 or null}; its sign, its colours, its election clock.
  function setNodes(states) {
    for (const [id, n] of lofts) {
      const s = states.get(id);
      if (!s || !n.role) continue;
      if (s.role && !scene.cut.has(id)) n.role.textContent = ` · ${t(`nodes.role_${s.role}`)}`;
      n.term.textContent = s.term ? ` · ${t("nodes.term", {n: s.term})}` : "";
      n.g.classList.toggle("candidate", s.role === "candidate");
      n.clock.style.display = s.clock == null || scene.cut.has(id) ? "none" : "";
      if (s.clock != null) n.clock.setAttribute("stroke-dasharray", `${(s.clock * n.clockLen).toFixed(1)} ${n.clockLen.toFixed(1)}`);
      n.clock.classList.toggle("low", s.clock != null && s.clock < 0.25);
    }
  }

  // A thing pointed at elsewhere (a link: entities.js), marked; null: none.
  const highlight = target => {
    const marked = target && nodeOf(target);
    for (const n of [client, ...lofts.values(), ...cards.values()]) n?.g.classList.toggle("hl", n === marked);
  };

  const markOpen = target => {
    const open = nodeOf(target);
    for (const n of [client, ...lofts.values(), ...cards.values()]) n?.g.classList.toggle("open", n === open);
  };
  const focus = target => nodeOf(target)?.g.focus();

  // A channel's bots' send budget, as budgets.js sendLevel gives it ({frac,
  // state: ok | low | busy}; null: none known, the bar hidden).
  function setBudgetOf(n, level) {
    n.budget.classList.toggle("none", !level);
    if (!level) return;
    n.budgetFill.setAttribute("width", String(Math.round(36 * Math.max(0, Math.min(1, level.frac)))));
    n.budget.setAttribute("data-state", level.state);
  }
  function setBudget(store, level) {
    levels.set(store, level);
    const n = cards.get(store);
    if (n) setBudgetOf(n, level);
  }

  // A channel's count changed (a post landed, one was deleted): its windows anew.
  function setTotal(store) {
    const n = cards.get(store);
    if (n) fill(n, true);
  }
  // Every block's windows anew: the channels read again.
  function refill() {
    for (const n of cards.values()) fill(n, false);
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

  // ---- pigeons (pigeons/): the town tells them where its places are
  // A place's spot, chosen once: a window by what the flight is after
  // (flights.js); null when it is not drawn (or not yet shown).
  function spot(p) {
    // While the town comes up (the first boot), only to what is up already.
    const up = n => (n && (!el.classList.contains("booting") || n.g.classList.contains("up")) ? n : null);
    if (p.client) return up(client) ? {client: true} : null;
    if ("node" in p) return up(loftOf(p.node)) ? {node: p.node} : null;
    const n = up(cards.get(p.store));
    if (!n) return null;
    if (p.at === "sign") return {store: p.store, sign: true};
    const {count} = contents(holds(n));
    const shown = Math.min(count, n.slots.length - (count > n.slots.length ? n.cols : 0));
    return {store: p.store, i: p.at === "free" ? shown : p.at === "any" ? Math.floor(Math.random() * Math.max(1, shown)) : shown - 1};
  }
  // Where a spot is on the town as drawn now.
  function point(s) {
    if (s.client) return client?.perch ?? null;
    if ("node" in s) return loftOf(s.node)?.door ?? null;
    const n = cards.get(s.store);
    return !n ? null : s.sign ? n.sign : windowAt(n, s.i);
  }
  const pigeons = makePigeons({spot, point, lift: () => lift, small: () => el.classList.contains("tall")});

  // ---- the first boot: the places come up one by one, the ways last
  // What is up so far (kept across a redraw), and whether the ways are.
  let revealed = [],
    waysUp = false;
  function hideAll() {
    revealed = [];
    waysUp = false;
    el.classList.add("booting");
  }
  function up(target) {
    // The nodes: every one of a cluster at once.
    const shown = target.kind === "loft" && target.node == null ? [...lofts.values()] : [nodeOf(target)];
    for (const n of shown) n?.g.classList.add("up");
  }
  function reveal(target) {
    revealed.push(target);
    up(target);
  }
  // Every place is up: the ways fade in, as they are drawn (dotted), just
  // before the first pigeons take them.
  function showAll() {
    waysUp = true;
    for (const e of edges.values()) e.classList.add("up");
    setTimeout(
      () => {
        el.classList.remove("booting");
        revealed = [];
        waysUp = false;
      },
      reduced ? 0 : 450,
    );
  }

  return {
    setBudget,
    draw,
    setNodes,
    markOpen,
    highlight,
    focus,
    setTotal,
    refill,
    setObjects,
    setQueue,
    fly: pigeons.fly,
    landed: pigeons.landed,
    hideAll,
    reveal,
    showAll,
    drawn: () => lofts.size > 0,
  };
}
