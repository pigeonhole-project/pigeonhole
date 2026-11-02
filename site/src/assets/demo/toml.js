// The wizard's setup → the pigeonhole.toml it makes, and the channels it
// will have (the graph's sketch). A setup:
//   {channels: ["telegram" | "discord", …], copies, quorum,
//    log: kind, snapshots: kind, every: entries, bots, pace: "demo" | "real"}
// Its data channels form groups of `copies` (replicas); the groups are shards.
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

// Where the node keeps its own log and snapshots on a local disk (the
// browser's storage, in the demo): one store, a directory each.
const NODE_DISK = {id: "node-disk", path: "/data/node"};

// The log's and the snapshots' store: a channel, or the node's disk (as
// the node lists it: meta-log, meta-snap).
function metaStore(kind, what) {
  const name = what === "snapshots" ? "snap" : what;
  if (kind === "local_disk") return {id: `meta-${name}`, kind, scope: `${NODE_DISK.path}/${what}`, disk: true};
  const tail = what === "log" ? "01" : "02";
  return {id: `${short(kind)}-${name}`, kind, scope: kind === "telegram" ? `-10010000001${tail}` : `10000000000000001${tail}`};
}

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

  const log = metaStore(w.log, "log"),
    snaps = metaStore(w.snapshots, "snapshots");
  const chats = [log, snaps].filter(m => !m.disk);
  for (const [i, m] of chats.entries()) out.push(...store(m, i === 0 ? t("wiz.toml_meta") : null));
  if (log.disk || snaps.disk) {
    out.push(`# ${t("wiz.toml_node_disk")}`, "[[store]]", `id = "${NODE_DISK.id}"`, 'kind = "local_disk"', `path = "${NODE_DISK.path}"`, "");
  }
  out.push(`# ${t("wiz.toml_disk")}`, "[[store]]", 'id = "channel-data"', 'kind = "local_disk"', 'path = "/data/channels"', "");
  const id = m => (m.disk ? NODE_DISK.id : m.id);
  out.push("[meta]", `log = "${id(log)}"`, `snapshot = "${id(snaps)}"`, `snapshot_every_entries = ${w.every}`);
  if (w.log === "discord") out.push(`# ${t("wiz.toml_lease")}`, 'lease = "5s"');
  out.push("");
  out.push("[chunk]", `store = "${chunkStore}"`, 'size = "4 MiB"', 'block_size = "1 MiB"', 'codec = "zstd"', "");
  out.push("[inline]", 'max_object_size = "1 KiB"', "", "[cache]", "enabled = false", "");
  out.push(`# ${t("wiz.toml_repair")}`, "[repair]", 'interval = "10s"', "");
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
export const STEPS = ["channels", "copies", "meta", "bots", "review"];

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
