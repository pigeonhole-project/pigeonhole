// The wizard's setup → the pigeonhole.toml it makes (one for every node of
// a cluster), and the channels it will have (the graph's sketch). A setup:
//   {channels: ["telegram" | "discord", …], copies, quorum,
//    log: kind, snapshots: kind, every: entries, bots, pace: "demo" | "real",
//    nodes: 1 | 3, links: "direct" | "chat" | "mixed",
//    others: [{log, snapshots}, …]: nodes 2 and 3's own (node 1's: log, snapshots)}
// Its data channels form groups of `copies` (replicas); the groups are shards.
// Every node of a cluster shares them; each has its own log and snapshots.
import {t} from "../lib/i18n.js";

export const divisors = n => Array.from({length: n}, (_, i) => i + 1).filter(d => n % d === 0);

// Sends a minute per bot and channel: the services' defaults, or the demo's.
export const perMinute = (kind, pace) => (pace === "demo" ? 1200 : kind === "telegram" ? 18 : 60);

const short = kind => (kind === "telegram" ? "tg" : "dc");

// Each data channel: its store id, chat id, which copy group it is in.
function dataChannels(w) {
  const n = {telegram: 0, discord: 0};
  return w.channels.map((kind, i) => {
    const k = ++n[kind];
    return {
      id: `${short(kind)}-${k}`,
      kind,
      scope: kind === "telegram" ? `-100100000000${k}` : `100000000000000000${k}`,
      group: Math.floor(i / w.copies),
      copy: i % w.copies,
    };
  });
}

// Where a node keeps its own log and snapshots on a local disk (the
// browser's storage, in the demo): one store, a directory each.
const diskId = node => (node ? `node-${node}-disk` : "node-disk");
const diskPath = node => (node ? `/data/node-${node}` : "/data/node");

// The log's and the snapshots' store of node `node` (0: no cluster): a
// channel of its own, or its disk (as the node lists it: meta-log, meta-snap).
function metaStore(kind, what, node = 0) {
  const name = what === "snapshots" ? "snap" : what;
  if (kind === "local_disk") return {id: `meta-${name}`, kind, scope: `${diskPath(node)}/${what}`, disk: true};
  const tail = what === "log" ? "01" : "02";
  const n = node || 1;
  return {
    id: `${short(kind)}-${name}${node ? `-${node}` : ""}`,
    kind,
    scope: kind === "telegram" ? `-1001000000${n}${tail}` : `1000000000000000${n}${tail}`,
  };
}

// Node `node`'s own log and snapshots: node 1 has the metadata step's, the others theirs.
const placeOf = (w, node) => (node <= 1 ? {log: w.log, snapshots: w.snapshots} : w.others[node - 2]);

// Each node's log and snapshots stores (one node: node 0, no cluster).
function metas(w) {
  const nodes = w.nodes > 1 ? Array.from({length: w.nodes}, (_, i) => i + 1) : [0];
  return nodes.map(node => {
    const place = placeOf(w, node);
    return {node, kind: place.log, log: metaStore(place.log, "log", node), snaps: metaStore(place.snapshots, "snapshots", node)};
  });
}
// A meta store as [meta] or [[node]] names it: its channel, or its node's disk.
const storeId = (m, node) => (m.disk ? diskId(node) : m.id);

// The cluster: every node runs this one file, knowing itself from
// PIGEONHOLE_NODE (the demo runs them all). Its links are direct (gRPC's
// place; in a browser, in memory) or over the chat (each node's outbox, a
// fake Telegram channel); each [[node]] names its own log and snapshots.
function clusterToml(w, nodes) {
  const direct = n => w.links === "direct" || (w.links === "mixed" && n <= 2);
  const chat = () => w.links !== "direct";
  const out = [`# ${t("wiz.toml_cluster", {n: w.nodes})}`, "[cluster]", "id = 1", 'key_env = "PH_DEMO_CLUSTER_KEY"'];
  out.push('fake = "channel-data"', 'heartbeat = "1s"', 'election_timeout_min = "4s"', 'election_timeout_max = "8s"', 'tick = "100ms"', 'poll = "200ms"', "");
  for (const m of nodes) {
    out.push("[[node]]", `id = ${m.node}`);
    if (direct(m.node)) out.push(`grpc = "node-${m.node}:7001"`);
    if (chat()) out.push(`outbox = -100100000900${m.node}`);
    out.push(`log = "${storeId(m.log, m.node)}"`, `snapshot = "${storeId(m.snaps, m.node)}"`, "");
  }
  return out;
}

// The pigeonhole.toml: one node's, or every node's of a cluster.
export function tomlOf(w) {
  const data = dataChannels(w),
    shards = w.channels.length / w.copies;
  const quote = ids => `[${ids.map(id => `"${id}"`).join(", ")}]`;
  const store = (c, comment) => [
    ...(comment ? [`# ${comment}`] : []),
    "[[store]]",
    `id = "${c.id}"`,
    `kind = "${c.kind}"`,
    c.kind === "telegram" ? `chat_id = ${c.scope}` : `channel_id = ${c.scope}`,
    'fake = "channel-data"',
    `bots = ${w.bots}`,
    "",
  ];
  const out = [
    ...t("wiz.toml_head")
      .split("\n")
      .map(l => `# ${l}`),
    "",
  ];
  for (const [i, c] of data.entries()) out.push(...store(c, i === 0 ? t("wiz.toml_data") : null));

  // Where chunks go: one channel, copies of each, shards of each, or shards of copies.
  let chunkStore;
  if (w.channels.length === 1) {
    chunkStore = data[0].id;
  } else if (w.copies === w.channels.length) {
    out.push(
      `# ${t("wiz.toml_copies", {n: w.copies, q: w.quorum})}`,
      "[[replicas]]",
      'id = "copies"',
      `members = ${quote(data.map(c => c.id))}`,
      `write_quorum = ${w.quorum}`,
      "",
    );
    chunkStore = "copies";
  } else {
    let members = data.map(c => c.id);
    if (w.copies > 1) {
      members = [];
      for (let g = 0; g < shards; g++) {
        const id = `copies-${g + 1}`;
        out.push("[[replicas]]", `id = "${id}"`, `members = ${quote(data.filter(c => c.group === g).map(c => c.id))}`, `write_quorum = ${w.quorum}`, "");
        members.push(id);
      }
    }
    out.push(`# ${t("wiz.toml_shards", {n: shards})}`, "[[shards]]", 'id = "shards"', `members = ${quote(members)}`, "");
    chunkStore = "shards";
  }

  const nodes = metas(w);
  const chats = nodes.flatMap(m => [m.log, m.snaps]).filter(m => !m.disk);
  for (const [i, m] of chats.entries()) out.push(...store(m, i === 0 ? t("wiz.toml_meta") : null));
  for (const m of nodes.filter(m => m.log.disk || m.snaps.disk)) {
    out.push(`# ${t("wiz.toml_node_disk")}`, "[[store]]", `id = "${diskId(m.node)}"`, 'kind = "local_disk"', `path = "${diskPath(m.node)}"`, "");
  }
  out.push(`# ${t("wiz.toml_disk")}`, "[[store]]", 'id = "channel-data"', 'kind = "local_disk"', 'path = "/data/channels"', "");
  // One node: its log and snapshots in [meta]; a cluster's: in each [[node]].
  out.push("[meta]");
  if (!(w.nodes > 1)) out.push(`log = "${storeId(nodes[0].log, 0)}"`, `snapshot = "${storeId(nodes[0].snaps, 0)}"`);
  out.push(`snapshot_every_entries = ${w.every}`, 'snapshot_every = "off"');
  if (nodes.some(m => m.kind === "discord")) out.push(`# ${t("wiz.toml_lease")}`, 'lease = "5s"');
  out.push("");
  out.push("[chunk]", `store = "${chunkStore}"`, 'size = "4 MiB"', 'block_size = "1 MiB"', 'codec = "zstd"', "");
  out.push("[inline]", 'max_object_size = "1 KiB"', "", "[cache]", "enabled = false", "");
  out.push(`# ${t("wiz.toml_repair")}`, "[repair]", 'interval = "10s"', "");
  if (w.nodes > 1) out.push(...clusterToml(w, nodes));
  if (w.pace === "demo") {
    out.push(
      `# ${t("wiz.toml_fast")}`,
      "[telegram.send]",
      "rate_per_sec = 20.0",
      "burst = 20.0",
      "",
      "[discord.send]",
      "rate_per_sec = 20.0",
      "burst = 20.0",
      "",
    );
  } else {
    out.push(`# ${t("wiz.toml_real")}`, "");
  }
  return out.join("\n");
}

// The wizard's steps, in order: what the setup has decided by each.
export const STEPS = ["channels", "copies", "meta", "nodes", "bots", "review"];

// The channels of the setup as they will be, nothing posted (no `total`),
// as far as the wizard has come: the data channels first, their places once
// the copies are picked, the log and snapshots once their channels are,
// the bots once they are counted.
export function sketch(w, upTo = "review") {
  const reached = step => STEPS.indexOf(upTo) >= STEPS.indexOf(step);
  const shards = w.channels.length / w.copies;
  const bots = reached("bots") ? w.bots : 1;
  const chans = dataChannels(w).map(c => ({
    store: c.id,
    role: "data",
    kind: c.kind,
    scope: c.scope,
    bots,
    messages: [],
    place: reached("copies") ? {shard: c.group, shards, copy: c.copy, copies: w.copies} : null,
  }));
  if (!reached("meta")) return chans;
  for (const role of ["log", "snapshots"]) {
    const m = metaStore(w[role], role);
    chans.push({store: m.id, role, kind: m.kind, scope: m.scope, bots: m.disk ? 1 : bots, messages: []});
  }
  return chans;
}
