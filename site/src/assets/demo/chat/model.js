// A channel's messages as a chat app shows them, the same for every app:
// the views (telegram.js, discord.js) only draw this.
import {t} from "../../lib/i18n.js";

export const SHOWN = 200; // the `channels` op returns the last 200
const DISCORD_EPOCH = 1420070400000n;

// When a message was posted: the channel says (`at`, ms); a post seen
// live has its Date; a Discord id carries its time anyway.
function postedAt(c, m) {
  if (m.at instanceof Date) return m.at;
  if (m.at != null) return new Date(+m.at);
  if (c.kind === "discord") {
    try {
      return new Date(Number((BigInt(m.id) >> 22n) + DISCORD_EPOCH));
    } catch {
      return null;
    }
  }
  return null;
}

// What a post of the log's channel is to the log (m.log, decoded by the node).
export function logNote(l) {
  if (!l) return null;
  if (l.what === "batch" && l.first != null) {
    return l.first === l.last ? t("demo.post_entry", {gen: l.gen, n: l.first}) : t("demo.post_batch", {gen: l.gen, first: l.first, last: l.last});
  }
  return t(`demo.post_${l.what}`, {gen: l.gen ?? "", label: l.label ?? ""});
}

// The owner marker (docs/store-owner.md): a pinned file named for its owner.
function ownerNote(m) {
  const owner = /^owner-([0-9a-f]{8})\.bin$/.exec(m.name || "")?.[1];
  return owner ? t("demo.owner_marker", {owner}) : null;
}

// `canDelete`: the page lets a reader delete a data channel's messages.
// {earlier: posts before the shown ones, pinned: the first pin's name or
// null, messages: [{id, kind: "text" | "file" | "service", text, name,
// size, at, pinned, notes, gone, fresh, quiet, deletable}]}
export function chatModel(c, canDelete = false) {
  const live = c.messages.slice(-SHOWN);
  const pins = new Set((c.pinned || []).map(p => String(p.id)));
  const messages = live.map(m => ({
    id: m.id,
    kind: m.kind === "text" ? "text" : m.kind === "other" ? "service" : "file",
    // A text post holds the payload: its name and size, then the text it became.
    text: m.kind === "text" ? `${m.name} ${m.size}\n${m.preview || ""}…` : null,
    name: m.name,
    size: m.size,
    at: postedAt(c, m),
    pinned: pins.has(String(m.id)),
    notes: [logNote(m.log), ownerNote(m)].filter(Boolean),
    gone: !!m.gone,
    fresh: !!m.fresh,
    // The log's own posts (its claim, probes): not its entries.
    quiet: !!m.log && m.log.what !== "batch",
    // Anyone in a data channel could delete a message, behind the node's back.
    deletable: canDelete && c.role === "data" && !m.gone,
  }));
  return {
    earlier: c.total - live.filter(m => !m.gone).length,
    pinned: c.pinned?.length ? (c.pinned[0].name ?? null) : null,
    pins: c.pinned?.length || 0,
    messages,
  };
}
