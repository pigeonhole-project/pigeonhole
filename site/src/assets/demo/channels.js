// A channel as the demo names it. A channel (from the node's `channels`
// op): {store, role: "data" | "log" | "snapshots", kind, scope, bots,
// down, place: {shard, shards, copy, copies}, total, messages, pinned}.
import {t} from "../lib/i18n.js";

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
