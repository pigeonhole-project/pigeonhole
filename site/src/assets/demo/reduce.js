// The page's channels (the `channels` op's, channels.js) as the node's
// events change them (events.js) and as the node lists them again. Pure:
// no DOM; the channels are changed in place, a new listing makes new ones.
import {s3Sentence} from "../lib/say.js";

// Event `e` on the channels: a post adds a message (fresh: seen arriving),
// a deletion takes them out. Returns {channel: the one it is about, or null;
// message: the one posted; said: what it says, as say.* sentences}.
export function apply(channels, e, at = new Date()) {
  const c = e.store == null ? null : (channels.find(x => x.store === e.store) ?? null);
  if (e.type === "posted" && c) {
    const m = {id: e.id, kind: e.kind, name: e.name, size: e.bytes, preview: e.preview, fresh: true, at};
    c.messages.push(m);
    c.total = (c.total ?? 0) + 1;
    const snap = c.role === "snapshots" && /-(\d+)\.snap/.exec(m.name || "");
    return {channel: c, message: m, said: snap ? [["say.snapshot", {at: +snap[1]}]] : []};
  }
  if (e.type === "deleted" && c) {
    // Gone from the channel, as from a messenger's: no trace left.
    c.total = Math.max(0, (c.total ?? 0) - gone(c, e.ids));
    c.messages = c.messages.filter(m => !e.ids.includes(String(m.id)));
    return {channel: c, said: []};
  }
  if (e.type === "repair_pass" && e.repaired > 0) return {channel: null, said: [["say.repair_pass", {n: e.repaired}]]};
  if (e.type === "s3") {
    // What one channel got for it: the first data channel's new posts.
    const first = channels.find(x => x.role === "data");
    const posts = (first?.messages || []).filter(m => m.fresh).map(m => ({kind: m.kind, bytes: m.size}));
    const said = s3Sentence(e, posts);
    return {channel: null, said: said ? [said] : []};
  }
  return {channel: c, said: []};
}

// Channel `c` as drawn: a post is `flying` until its pigeon lands (main.js),
// and only then shows, in the town and the panels. The messages landed, the
// total without the others; `flying`: the others' ids, oldest first.
export function drawn(c) {
  if (!c?.messages?.some(m => m.flying)) return c;
  const flying = c.messages.filter(m => m.flying);
  return {...c, messages: c.messages.filter(m => !m.flying), total: (c.total ?? c.messages.length) - flying.length, flying: flying.map(m => String(m.id))};
}

// How many of message ids `ids` channel `c` held: those it lists, and
// those below its first listed one, as many as it has unlisted. An outbox's
// prune deletes a run of ids, some of them empty already.
export function gone(c, ids) {
  const listed = new Set(c.messages.map(m => String(m.id)));
  const first = Math.min(...c.messages.map(m => Number(m.id)));
  const older = Math.max(0, (c.total ?? 0) - c.messages.length);
  const below = ids.filter(id => !listed.has(id) && Number(id) < first).length;
  return ids.filter(id => listed.has(id)).length + Math.min(older, below);
}

// Message id `a` came after `b` (ids are numbers, as strings too long for
// one: a Discord snowflake); `b` none: anything did. Ids that are no
// numbers (a disk's files) have no order.
const DIGITS = /^\d+$/;
function newer(a, b) {
  if (b == null) return true;
  const [x, y] = [String(a), String(b)];
  if (!DIGITS.test(x) || !DIGITS.test(y)) return false;
  return x.length !== y.length ? x.length > y.length : x > y;
}

// The channels as the node lists them now (`listed`), what the page saw
// arriving kept: a listed post seen arriving stays fresh (and flying, its
// pigeon not landed yet), and a fresh one newer than the listing (posted
// while it was read) stays, counted.
export function merge(channels, listed) {
  const was = new Map(channels.map(c => [c.store, c]));
  return listed.map(c => {
    const seen = (was.get(c.store)?.messages || []).filter(m => m.fresh);
    const fresh = new Set(seen.map(m => String(m.id)));
    const flying = new Set(seen.filter(m => m.flying).map(m => String(m.id)));
    const messages = (c.messages || []).map(m => ({...m, fresh: fresh.has(String(m.id)), flying: flying.has(String(m.id))}));
    const ids = new Set(messages.map(m => String(m.id)));
    const newest = messages.reduce((a, m) => (newer(m.id, a) ? m.id : a), null);
    const later = seen.filter(m => !ids.has(String(m.id)) && newer(m.id, newest));
    return {...c, messages: [...messages, ...later], total: (c.total ?? messages.length) + later.length};
  });
}
