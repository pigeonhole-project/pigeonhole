// pigeonhole's demo (demo.html): the node and its channels as a graph.
// Every flight on it is an event the node logged; clicking a node opens its
// panel. This module keeps the page's state and wires the views (pure: data
// and callbacks in, elements out) to the node's ops.
import {button, h, reducedMotion} from "../lib/dom.js";
import {bytes, t, tHtml} from "../lib/i18n.js";
import {s3Sentence} from "../lib/say.js";
import {appOf} from "./channels.js";
import {chatView} from "./chat/index.js";
import {clientView} from "./client-view.js";
import {readEvents} from "./events.js";
import {faultsView} from "./faults-view.js";
import {flightsOf, refused} from "./flights.js";
import {makeGraph} from "./graph.js";
import {loftView} from "./loft-view.js";
import {logView} from "./log-view.js";
import {connect} from "./node.js";
import {TASK_KEY, TASK_ON, taskWindows} from "./tasks.js";
import {makeWizard} from "./wizard.js";

const $ = id => document.getElementById(id);
const ok = reply => reply && !reply.error;
const pause = ms => new Promise(r => setTimeout(r, reducedMotion() ? 0 : ms));

const state = {
  demo: "", // the demo's own config (the loft's "reset")
  config: "", // the pigeonhole.toml the node runs (a cluster's: every node's)
  nodes: [1], // the nodes' ids: one, or a cluster's
  node: null, // the node the page looks at and sends to; null: whichever leads
  cluster: null, // the `cluster` op's: {leader, term, nodes}
  term: null, // the newest term seen: a new leader is told once
  cut: new Set(), // the nodes cut off from the others
  persistent: false, // the node's files outlive the page (OPFS)
  admin: false, // a real node's admin, not the demo
  buckets: [],
  bucket: "demo",
  channels: [], // the `channels` op's, live posts added as they fly
  objects: [], // [[key, size]] in the bucket
  log: null, // the `log` op's: {head, base, entries}
  bg: null, // the `background` op's (tasks.js)
  open: null, // the panel: {kind: "client" | "loft" | "log"} or {kind: "store", store}
  logView: "readable",
  busy: false, // an op runs: the panels' buttons wait
  wizard: false, // the first visit's setup is on screen
};
const channel = store => state.channels.find(c => c.store === store);
// The node on screen: the one picked, else the leader, else the first.
const shown = () => state.node ?? state.cluster?.leader ?? state.nodes[0];
// A request for the node the page talks to (the server picks the leader without one).
const onNode = request => (state.node == null || request.node != null ? request : {...request, node: state.node});
// The log the `log` op reads: the node on screen's.
const logChannel = () => state.channels.find(c => c.role === "log" && (c.node == null || c.node === shown()));
// The node on screen follows a leader it knows: the leader's tasks are not its.
const following = () => state.nodes.length > 1 && state.cluster?.leader != null && shown() !== state.cluster.leader;
const isOpen = kind => state.open?.kind === kind;
// The graph's node for a panel: the log's is its channel's card.
const nodeOf = open => (open?.kind === "log" ? {kind: "store", store: logChannel()?.store} : open);

const node = await connect(new URL(`../web-worker.js${new URL(import.meta.url).search}`, import.meta.url));

let updateWindows = () => {};
let relayout = () => redraw(); // the screen turned narrow or wide: what is on the graph, again
const graph = makeGraph($("graph"), {
  onOpen: openPanel,
  onRelayout: () => relayout(),
  onLoft: (g, entrances) => {
    updateWindows = taskWindows(
      g,
      entrances,
      task => {
        openPanel({kind: "loft"});
        $(`task-${task}`)?.scrollIntoView({block: "nearest"});
      },
      {follower: following()},
    );
  },
});
// The cluster as the graph draws it: who leads, who is cut off, which node is on screen.
let drawnLeader = null,
  drawnLinks = 0, // the cluster drawn last
  drawnEvery = 0; // the snapshots' entries the log's block was drawn for
const scene = () => ({
  leader: state.cluster?.leader ?? null,
  cut: state.cut,
  shown: shown(),
  links: state.cluster?.links || [],
  every: state.bg?.snapshot?.every ?? 0,
});
function redraw() {
  drawnLeader = state.cluster?.leader ?? null;
  drawnLinks = state.cluster?.links?.length ?? 0;
  drawnEvery = state.bg?.snapshot?.every ?? 0;
  graph.draw(state.channels, state.objects.length, scene());
  graph.markOpen(nodeOf(state.open));
  renderBackground();
}

// ------------------------------------------------------------ captions
// The sentence under the graph: a say.* string ([key, vars]; the values are escaped).
function say(sentence) {
  $("caption").innerHTML = sentence ? tHtml(...sentence) : "";
}
const sayError = error => $("caption").replaceChildren(h("b", "", t("try.error", {error})));
const status = text => {
  $("status").textContent = text;
};

// ------------------------------------------------------------ events
// What the node logged: pigeons to fly, posts and deletions in the channels.
// Returns what it said, as sentences, newest last.
// Whose traffic is whose (flights.js): the leader as last seen, the node on screen.
const flightCtx = () => ({leader: state.cluster?.leader ?? null, shown: shown(), cut: state.cut});
function apply(events) {
  const said = [];
  for (const e of events) {
    const c = channel(e.store);
    if (e.type === "posted" && c) {
      const m = {id: e.id, kind: e.kind, name: e.name, size: e.bytes, preview: e.preview, fresh: true, at: new Date()};
      c.messages.push(m);
      c.total++;
      const snap = c.role === "snapshots" && /-(\d+)\.snap/.exec(m.name || "");
      if (snap) said.push(["say.snapshot", {at: +snap[1]}]);
      for (const f of flightsOf(e, channel, flightCtx())) graph.fly({...f, onLand: () => landed(c.store)});
    } else if (e.type === "deleted" && c) {
      // Gone from the channel, as from a messenger's: no trace left.
      c.messages = c.messages.filter(m => !e.ids.includes(String(m.id)));
      c.total = Math.max(0, c.total - e.ids.length);
      graph.setTotal(c.store, c.total);
      if (panelShows(c.store)) renderPanel();
    } else if (e.type === "call" || e.type === "call_failed") {
      for (const f of flightsOf(e, channel, flightCtx())) graph.fly(f);
    } else if (e.type === "raft") {
      for (const f of flightsOf(e, channel, flightCtx())) graph.fly(f);
      // Over the chat, a frame is a post in the sender's outbox.
      const box = e.via === "chat" && e.at === "sent" && channel(`outbox-${e.from}`);
      if (box) {
        box.total = (box.total ?? 0) + 1;
        graph.setTotal(box.store);
      }
    } else if (e.type === "repair_pass" && e.repaired > 0) {
      said.push(["say.repair_pass", {n: e.repaired}]);
    } else if (e.type === "s3") {
      // What one channel got for it: the first data channel's new posts.
      const first = state.channels.find(x => x.role === "data");
      const posts = (first?.messages || []).filter(m => m.fresh).map(m => ({kind: m.kind, bytes: m.size}));
      said.push(s3Sentence(e, posts));
    }
  }
  return said;
}

// A pigeon landed on a channel.
function landed(store) {
  graph.setTotal(store, channel(store)?.total);
  if (panelShows(store)) renderPanel();
}
const panelShows = store => (isOpen("store") && state.open.store === store) || (isOpen("log") && logChannel()?.store === store);

// What the node did on its own between calls (a snapshot, a repair pass).
async function background(events) {
  const said = apply(readEvents(events));
  if (said.length) say(said.at(-1));
  if (said.some(([key]) => key === "say.repair_pass")) await refreshChannels();
  await graph.landed();
  if (said.some(([key]) => key === "say.snapshot") && (isOpen("loft") || isOpen("log"))) await loadLog();
}

// A call that is not an op of yours (a list, the log): what the node
// logged meanwhile is its own doing (its timers ran), drawn as such.
async function call(request, body) {
  const r = await node.call(onNode(request), body);
  if (r.events.length) background(r.events);
  return r;
}

// ------------------------------------------------------------ ops
// An op from a panel: its flights, its sentence, then the state after it.
async function op(request, body) {
  setBusy(true);
  for (const c of state.channels) for (const m of c.messages) m.fresh = false;
  const {reply, events} = await node.call(onNode(request), body);
  const said = apply(readEvents(events));
  if (reply?.error?.startsWith("not the leader")) {
    // You knocked on a follower: your pigeon turns back at its door.
    graph.fly(refused(shown()));
    say(["say.not_leader", {n: shown(), error: reply.error}]);
  } else if (reply?.error && state.channels.some(c => c.down)) say([request.op === "get" ? "say.read_failed_down" : "say.failed_down", {error: reply.error}]);
  else if (reply?.error) sayError(reply.error);
  else if (said.length) say(said.at(-1));
  await graph.landed();
  await refreshObjects();
  if (isOpen("log") || isOpen("loft")) await loadLog();
  setBusy(false);
  return reply;
}

function setBusy(on) {
  state.busy = on;
  for (const b of document.querySelectorAll(".panel button, .panel input")) if (!b.classList.contains("back")) b.disabled = on;
}

async function refreshObjects() {
  const {reply} = await call({op: "list", bucket: state.bucket});
  state.objects = reply?.objects || [];
  graph.setObjects(state.objects.length);
  view?.setObjects?.(state.objects);
  view?.setCounters?.(counters());
}

// The channels as the node has them now; the posts seen arriving stay
// marked. A local disk has files, not messages.
// In a cluster, each other node's own log and snapshots too: the node on
// screen lists only its own.
async function loadChannels() {
  const {reply} = await call({op: "channels"});
  const fresh = new Set(state.channels.flatMap(c => c.messages.filter(m => m.fresh).map(m => `${c.store}/${m.id}`)));
  const listed = [...(reply?.channels || [])];
  for (const id of state.nodes.length > 1 ? state.nodes.filter(id => id !== shown()) : []) {
    const other = await call({op: "channels", node: id});
    listed.push(...(other.reply?.channels || []).filter(c => c.node === id && c.role !== "outbox"));
  }
  state.channels = listed.map(c => ({...c, messages: (c.messages || []).map(m => ({...m, fresh: fresh.has(`${c.store}/${m.id}`)}))}));
}

// The channels anew, drawn (a fault changes them behind the events).
async function refreshChannels() {
  await graph.landed();
  await loadChannels();
  redraw();
  renderPanel();
}

// The log and its channel's posts, decoded.
async function loadLog() {
  const {reply} = await call({op: "log", limit: 200});
  await loadChannels();
  graph.setChannels(state.channels);
  state.log = reply;
  if (isOpen("log")) renderPanel();
  view?.setCounters?.(counters());
}

// ------------------------------------------------------------ background tasks
async function pollBackground() {
  const {reply} = await call({op: "background"});
  if (!ok(reply)) return;
  state.bg = reply;
  // The log's block is as tall as the entries between two snapshots need.
  if (graph.drawn() && !state.wizard && (reply.snapshot?.every ?? 0) !== drawnEvery) redraw();
  renderBackground();
  if (state.nodes.length > 1) await pollCluster();
}

// ------------------------------------------------------------ the cluster
// Who leads, as the node on screen sees it; a new leader is told.
async function pollCluster() {
  const {reply} = await call({op: "cluster"});
  if (!ok(reply)) return;
  // A newer term only: a node cut off still thinks it leads, in its old term.
  const seen = state.term;
  // A node back from a cut still names itself in its old term: the newer word stands.
  state.cluster = seen != null && reply.term < seen ? {...reply, leader: state.cluster?.leader ?? null} : reply;
  state.term = Math.max(seen ?? 0, reply.term);
  if (seen != null && reply.term > seen && reply.leader != null) say(["nodes.new_leader", {n: reply.leader, term: reply.term}]);
  renderNodes();
  // A new leader: its flag, its ways to the data channels.
  if (graph.drawn() && !state.wizard && (reply.leader !== drawnLeader || reply.links.length !== drawnLinks)) redraw();
}

// The nodes in the bar: which leads (★), which is cut off; the one on
// screen pressed. Picking a node shows it and sends to it; picking it
// again follows the leader.
function renderNodes() {
  const bar = $("nodes");
  bar.hidden = state.nodes.length < 2;
  bar.replaceChildren(
    ...state.nodes.map(id => {
      const marks = `${id === state.cluster?.leader ? " ★" : ""}${state.cut.has(id) ? " ✂" : ""}`;
      const b = button("node-pick", `${t("nodes.node", {n: id})}${marks}`, () => pickNode(state.node === id ? null : id));
      b.setAttribute("aria-pressed", String(state.node === id));
      b.classList.toggle("following", state.node == null && id === shown());
      return b;
    }),
  );
}

async function pickNode(id) {
  state.node = id;
  say(id == null ? ["nodes.following", {}] : ["nodes.looking", {n: id}]);
  renderNodes();
  await refreshChannels();
  await refreshObjects();
  await loadLog();
  await pollBackground();
}

// Node `id` cut off from the others, or back: its links and its outbox.
async function isolate(id, cut) {
  const {reply} = await node.call({op: "isolate", node: id, down: cut});
  if (!ok(reply)) return sayError(reply?.error);
  if (cut) state.cut.add(id);
  else state.cut.delete(id);
  say([cut ? "nodes.cut" : "nodes.back", {n: id}]);
  renderNodes();
  await refreshChannels(); // its outbox up or down
}
// While idle: the worker lets the node's loops run between calls.
setInterval(() => {
  if (!state.busy && !state.wizard && !document.hidden && graph.drawn()) pollBackground();
}, 1000);

function renderBackground() {
  const bg = state.bg;
  if (!bg) return;
  // Repair jobs wait under their channel's card.
  const jobs = bg.tasks.find(x => x.task === "repair")?.work || [];
  for (const c of state.channels)
    graph.setQueue(
      c.store,
      jobs.filter(j => j.store === c.store),
      j => t("demo.job", {chunk: j.chunk, store: c.store}),
    );
  updateWindows(bg);
  view?.setBackground?.(bg);
}

// On: back to the file's period (or a default when the file has it off);
// off: an override of "off". Both are entries in the log, as on a server.
async function toggleTask(task, on) {
  // In a cluster a node's snapshots are its own key, set (as every change of
  // the dynamic config: an entry in the log) through the leader.
  if (task === "snapshot" && state.nodes.length > 1) {
    const key = `node.${shown()}.snapshot_every`;
    const r = await call({op: "dynamic_set", key, value: on ? TASK_ON.snapshot : "off", node: state.cluster?.leader ?? undefined});
    if (r.reply?.error) sayError(r.reply.error);
    else say([on ? "bg.turned_on" : "bg.turned_off", {task: t("bg.snapshot"), key}]);
    return pollBackground();
  }
  const key = TASK_KEY[task];
  let r;
  if (!on) r = await call({op: "dynamic_set", key, value: "off"});
  else {
    const {reply} = await call({op: "dynamic"});
    const k = reply?.keys?.find(x => x.key === key);
    const fromFile = k?.overridden && k.file && k.file !== "off";
    r = await call(fromFile ? {op: "dynamic_unset", key} : {op: "dynamic_set", key, value: TASK_ON[task]});
  }
  if (r.reply?.error) sayError(r.reply.error);
  else say([on ? "bg.turned_on" : "bg.turned_off", {task: t(`bg.${task}`), key}]);
  await pollBackground();
}

async function runTask(task) {
  const {reply} = await call({op: "run_task", task});
  if (reply?.error) sayError(reply.error);
  else say(["bg.asked", {task: t(`bg.${task}`)}]);
  await pollBackground();
}

// ------------------------------------------------------------ panels
let view = null; // the open panel's view, for updates in place

async function openPanel(target) {
  if (state.wizard) return;
  // Another node's dovecote: that node on screen, then its panel.
  if (target.kind === "loft" && target.node != null && target.node !== shown()) await pickNode(target.node);
  // A log: that node's, read on it.
  const log = target.kind === "store" && channel(target.store)?.role === "log" ? channel(target.store) : null;
  if (log?.node != null && log.node !== shown()) await pickNode(log.node);
  if (log) target = {kind: "log"};
  state.open = target;
  $("loft").classList.add("zoomed");
  $("panel").hidden = false;
  graph.markOpen(nodeOf(target));
  if (target.kind === "log") loadLog();
  renderPanel();
  $("panel-close").focus();
}

function closePanel() {
  const was = state.open;
  state.open = view = null;
  $("loft").classList.remove("zoomed");
  $("panel").hidden = true;
  graph.markOpen(null);
  graph.focus(nodeOf(was));
}
$("panel-close").addEventListener("click", closePanel);
document.addEventListener("keydown", e => {
  if (e.key === "Escape" && state.open) closePanel();
});

const counters = () => ({objects: state.objects.length, dataChannels: state.channels.filter(c => c.role === "data").length, log: state.log});

function renderPanel() {
  const open = state.open;
  if (!open) return;
  const show = (title, sub, nodes, v = null) => {
    $("panel-title").textContent = title;
    $("panel-sub").textContent = sub;
    $("panel-body").replaceChildren(...nodes);
    view = v;
  };
  if (open.kind === "client") {
    const v = clientView(state, clientOps);
    show(t("demo.you"), t("demo.you_sub"), v.nodes, v);
  } else if (open.kind === "loft") {
    const id = shown();
    const cluster = state.nodes.length > 1 ? {node: id, cut: state.cut.has(id), leader: state.cluster?.leader ?? null} : null;
    // A cluster's "reset" is its own config back: the demo's is one node's.
    const demo = cluster ? state.config : state.demo;
    const v = loftView({counters: counters(), ...state, demo, cluster}, loftOps);
    show(cluster ? t("nodes.loft_title", {n: id}) : "pigeonhole", t(cluster ? "nodes.loft_sub" : "demo.loft_sub"), v.nodes, v);
    if (!state.log) loadLog();
  } else if (open.kind === "log") {
    const c = logChannel();
    const setView = v => {
      state.logView = v;
      renderPanel();
    };
    const sub = c?.kind === "local_disk" ? "demo.log_sub_disk" : "demo.log_sub";
    show(t("demo.log_title"), t(sub, {store: c?.store, app: appOf(c?.kind)}), logView(state.log, c, state.logView, setView));
  } else {
    const c = channel(open.store);
    if (!c) return;
    const nodes = [chatView(c, m => deleteMessage(c, m))];
    if (c.role === "data") nodes.unshift(faultsView(c, state.admin, faultOps(c)));
    show(c.store, t(`demo.channel_sub_${c.role}`, {app: appOf(c.kind), scope: c.scope}), nodes);
  }
}

// You: uploads and the bucket's objects.
const clientOps = {
  bucket(name) {
    state.bucket = name;
    refreshObjects();
  },
  upload: (name, body) => op({op: "put", bucket: state.bucket, key: name}, body),
  tooBig: () => say(["try.too_big", {}]),
  // 100 KB from somewhere in the object: only the blocks that hold it are read.
  async read(key, size) {
    const len = Math.min(size, 100_000),
      first = Math.floor(Math.random() * Math.max(1, size - len));
    const r = await op({op: "get", bucket: state.bucket, key, range: [first, first + len - 1]});
    if (ok(r)) say(["demo.read_done", {len: bytes(r.len), key}]);
  },
  copy: key => op({op: "copy", bucket: state.bucket, key, to: key.replace(/(\.[^.]*)?$/, ext => `-copy${ext}`)}),
  remove: key => op({op: "delete", bucket: state.bucket, key}),
};

// The node: its config, its background tasks.
const loftOps = {restart, erase, toggle: toggleTask, run: runTask, isolate};

// A data channel's faults, and what finds and mends them.
const faultOps = c => {
  const store = c.store;
  return {
    async outage() {
      const r = await op({op: "outage", store, down: !c.down});
      if (ok(r)) say([r.down ? "say.down" : "say.up", {store}]);
      await refreshChannels();
    },
    async wipe() {
      const r = await op({op: "wipe", store});
      if (ok(r)) say(["say.wiped", {store, n: r.deleted}]);
      await refreshChannels();
    },
    async verify() {
      const r = await op({op: "verify", store});
      if (ok(r)) say([r.gone ? "say.verified" : "say.verified_ok", {store, checked: r.checked, gone: r.gone}]);
    },
    async repair() {
      const r = await op({op: "repair"});
      if (ok(r)) say(["say.repair", {n: r.repaired}]);
      await refreshChannels();
    },
  };
};

// A reader deleted a message of a data channel, behind the node's back.
async function deleteMessage(c, m) {
  if (!confirm(t("demo.delete_confirm"))) return;
  const r = await op({op: "delete_message", store: c.store, message: String(m.id)});
  if (ok(r)) say(["say.deleted_one", {store: c.store, name: m.name || ""}]);
  await refreshChannels();
}

// ------------------------------------------------------------ boots
// The channels, the objects, the log: what the node has after a boot. Its
// posts are in the channels already: its events have nothing to add.
async function boot() {
  await loadChannels();
  await refreshObjects();
  redraw();
  loadLog();
}

// A new config: every node restarts on it.
async function restart(text) {
  view?.setError("");
  setBusy(true);
  const {reply} = await node.call({op: "restart", config: text});
  if (reply?.error) {
    view?.setError(reply.error);
    setBusy(false);
    return;
  }
  Object.assign(state, {config: text, nodes: reply.nodes});
  state.cut.clear();
  await boot();
  say(["say.restart", {n: reply.replayed}]);
  setBusy(false);
}

// Everything this browser keeps for the demo goes; the page starts over.
async function erase() {
  if (!confirm(t("try.erase_confirm"))) return;
  await node.call({op: "reset"});
  location.reload();
}

const ready = (ms, why) => status(`${t("try.ready", {ms: Math.round(ms)})} ${state.persistent ? t("try.kept") : t("try.not_kept")}${why ? ` (${why})` : ""}`);

function openWizard() {
  state.wizard = true;
  $("loft").classList.add("zoomed", "wizard");
  $("panel").hidden = false;
  $("panel-close").hidden = true;
  const panel = {title: $("panel-title"), sub: $("panel-sub"), body: $("panel-body")};
  const wizard = makeWizard(panel, {sketch: channels => graph.draw(channels, 0), start: startFromWizard});
  relayout = wizard.render;
  wizard.render();
  say(["wiz.caption", {}]);
}

// The wizard's config: its node (or every node) boots on it. Returns the
// error, if one does not.
async function startFromWizard(text) {
  const t0 = performance.now();
  const {reply, events} = await node.call({op: "open", config: text});
  const ms = performance.now() - t0;
  if (reply?.error) return reply.error;
  Object.assign(state, {config: text, nodes: reply.nodes});
  if (state.nodes.length > 1) await pollCluster();
  state.wizard = false;
  relayout = redraw;
  $("loft").classList.remove("zoomed", "wizard");
  $("panel").hidden = true;
  $("panel-close").hidden = false;
  await firstBoot(events);
  ready(ms);
  return null;
}

// The first boot, as it happens: the client, the node, each channel it
// opens, then the posts of its start (the log's claim).
async function firstBoot(events) {
  await loadChannels();
  await refreshObjects();
  // Empty channels: the boot's own posts fill them in, as they fly.
  for (const c of state.channels) {
    c.total = 0;
    c.messages = [];
  }
  redraw();
  graph.hideAll();
  graph.reveal({kind: "client"});
  say(["boot.client", {}]);
  await pause(500);
  graph.reveal({kind: "loft"});
  say(["boot.node", {}]);
  await pause(600);
  for (const c of state.channels) {
    graph.reveal({kind: "store", store: c.store});
    say([`boot.${c.role}`, {store: c.store, app: appOf(c.kind), bots: t("demo.bots", {n: c.bots || 1})}]);
    await pause(450);
  }
  graph.showAll();
  const evs = readEvents(events);
  apply(evs);
  const claim = evs.find(e => e.type === "claimed");
  if (claim) say(["boot.claimed", {gen: claim.generation, store: logChannel()?.store || ""}]);
  await graph.landed();
  await pause(1200);
  say(["demo.first_caption", {}]);
  loadLog();
}

// The node would not start on what this browser keeps: say why, and offer
// the way out (the loft's panel, where "erase" is, needs a running node).
function bootFailed(error) {
  status(t("try.error", {error}));
  say(["demo.boot_failed", {}]);
  $("caption").append(" ", button("btn danger", t("try.erase"), erase));
}

// The demo's node: the wizard on a first visit, else a boot on what the browser keeps.
async function startDemo() {
  const {reply} = await node.started;
  if (reply.error) {
    status(reply.error === "other-tab" ? t("try.other_tab") : t("try.error", {error: reply.error}));
    return;
  }
  Object.assign(state, {demo: reply.demo, config: reply.config, persistent: reply.persistent});
  if (!reply.saved) {
    status(t("wiz.status"));
    openWizard();
    return;
  }
  const t0 = performance.now();
  const booted = await node.call({op: "open"});
  if (booted.reply?.error) {
    bootFailed(booted.reply.error);
    return;
  }
  state.nodes = booted.reply.nodes;
  if (state.nodes.length > 1) await pollCluster();
  await boot();
  ready(performance.now() - t0, reply.why);
  say(["say.welcome_back", {n: booted.reply.replayed}]);
}

// A real node: no setup, no restart; its config read-only, its buckets.
async function startAdmin() {
  state.admin = true;
  document.body.classList.add("admin");
  const t0 = performance.now();
  const {reply} = await node.call({op: "info"});
  if (!ok(reply)) {
    status(t("try.error", {error: reply?.error || "no answer"}));
    return;
  }
  state.config = reply.config || "";
  state.buckets = reply.buckets || [];
  state.bucket = state.buckets[0] || "";
  await boot();
  status(t("admin.ready", {host: location.host, ms: Math.round(performance.now() - t0)}));
  say(["admin.caption", {n: state.buckets.length}]);
}

// Back from the browser's page cache, the worker is gone: start again.
addEventListener("pageshow", e => {
  if (e.persisted) location.reload();
});
node.onTick(background);
if (node.admin) startAdmin();
else startDemo();
