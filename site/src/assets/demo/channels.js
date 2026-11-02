// A channel as the demo names it. A channel (from the node's `channels`
// op): {store, role: "data" | "log" | "snapshots", kind, scope, bots,
// down, place: {shard, shards, copy, copies}, total, messages, pinned}.
import {h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {channelLink, nodeLink} from "./entities.js";

// A store's kind as a name: the app, or what a server's own store is.
export const appOf = kind => ({telegram: "Telegram", discord: "Discord"})[kind] || t(`kind.${kind}`);

// What a channel is for; a data channel, its place among the others.
export function placeNote(c) {
  const app = appOf(c.kind),
    p = c.place;
  if (c.role !== "data" || !p || (p.shards === 1 && p.copies === 1)) return t(`demo.role_${c.role}`, {app});
  const shard = String.fromCharCode(65 + p.shard);
  if (p.shards > 1 && p.copies > 1) return t("wiz.note_shard_copy", {app, shard, copy: p.copy + 1});
  if (p.copies > 1) return t("wiz.note_copy", {app, copy: p.copy + 1, n: p.copies});
  return t("wiz.note_shard", {app, shard});
}

// A channel's kin, as links: the node it belongs to (a log, snapshots, an
// outbox); a data channel's other copies of the same shard, and the shard's
// neighbours. `channels`: every channel. Null when it has none.
export function kinOf(c, channels) {
  const row = h("p", "channel-kin");
  const list = (label, stores) => {
    if (!stores.length) return;
    if (row.childNodes.length) row.append(" · ");
    row.append(label, " ");
    for (const [i, s] of stores.entries()) row.append(...(i ? [", "] : []), channelLink(s));
  };
  if (c.node != null) row.append(t("demo.kin_node"), " ", nodeLink(c.node));
  if (c.role === "data" && c.place) {
    const data = channels.filter(x => x.role === "data" && x.place && x.store !== c.store);
    list(
      t("demo.kin_copies"),
      data.filter(x => x.place.shard === c.place.shard).map(x => x.store),
    );
    list(
      t("demo.kin_shards"),
      data.filter(x => x.place.shard !== c.place.shard).map(x => x.store),
    );
  }
  return row.childNodes.length ? row : null;
}
