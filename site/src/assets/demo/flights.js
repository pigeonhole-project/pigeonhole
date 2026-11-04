// What flies on the graph for each event the node logged (events.js): a
// pigeon from one place to another, from the moment a thing starts to the
// moment it ends, both in the node's log. The graph (graph.js) only turns
// places into points on the town; who flies where is decided here, and
// only here.
//
// A place: {client: true}           your block's roof
//          {node: id}               node `id`'s dovecote (0: the one node)
//          {store, at, ahead?}      a channel's block: `at` "new" its newest
//                                   window, "any" a lit one, "free" the next
//                                   empty one (`ahead`: past as many others
//                                   flying to it), "sign" its roof
// A flight: {key, from, to, size: bytes (a bigger pigeon), ms: how long it
//            takes on the node's clock (0: a moment, flown in the wall's
//            time), lane: "raft" (the cluster's own: lower, capped;
//            pigeons/model.js) or none (yours and the node's work), spark:
//            a heartbeat, not a pigeon, kind?, failed?: it turns back}
// A flight starts with its event and lands with the event of `key`
// (landingOf): a fake bot's call ends, a direct frame arrives.
//
// Who flies: a fake bot's call is its node's, when the log names it (an
// outbox's reader); a channel's traffic else comes from its own node (a
// node's log, snapshots and outbox) or the leader (the data channels: the
// leader is the one the page last saw leading).

// The node a channel's traffic comes from: its own node, else the leader.
// ctx: {leader: id | null, shown: the node on screen, cut: Set of the nodes cut off}.
export const ownerOf = (c, ctx) => c.node ?? ctx.leader ?? ctx.shown;

// The fake bots' calls that bring something from the channel (the rest take
// something to it); "push": an outbox's post reaching a reader.
const READS = new Set(["get_messages", "get_file", "get_message", "list", "pinned", "thread_name", "download", "push"]);

// The flight of a fake bot's call starting (event "api", at "start"); null
// for a channel not on the graph. `ahead`: the sends to the same channel
// already flying (they take the windows before this one's).
export function apiFlight(e, channel, ctx, ahead = 0) {
  const c = channel(e.store);
  if (!c) return null;
  const node = {node: e.node ?? ownerOf(c, ctx)};
  const reads = READS.has(e.call);
  const window = e.call === "push" ? {store: c.store, at: "new"} : {store: c.store, at: reads ? "any" : "free", ahead};
  return {
    key: `api:${e.id}`,
    from: reads ? window : node,
    to: reads ? node : window,
    size: e.bytes ?? (reads ? 4096 : 1024),
    ms: e.ms,
    ...(c.role === "outbox" ? {lane: "raft"} : {}),
  };
}

// The flight of a Raft frame leaving over a direct link (a request, or a
// vote's answer); null for a frame over the chat (its pigeons are the
// outbox's fake calls), or to or from a node cut off.
export function frameFlight(e, ctx) {
  if (e.type !== "raft" || e.at !== "sent" || e.frame == null) return null;
  if (ctx.cut?.has(e.from) || ctx.cut?.has(e.to)) return null;
  const voted = e.kind === "voted";
  return {
    key: `${voted ? "vote" : "frame"}:${e.frame}`,
    from: {node: e.from},
    to: {node: e.to},
    size: 1024,
    ms: e.ms ?? 0,
    lane: "raft",
    spark: e.kind === "heartbeat",
    kind: voted ? "vote" : e.kind,
  };
}

// The flight event `e` lands, and how: {key, failed?, kind?}; null for none.
export function landingOf(e) {
  if (e.type === "api" && e.at === "end") return {key: `api:${e.id}`, ...(e.ok ? {} : {failed: true})};
  if (e.type !== "raft" || e.frame == null) return null;
  if (e.at === "read") return {key: `frame:${e.frame}`};
  if (e.at === "answered") return {key: `vote:${e.frame}`, kind: e.granted ? "vote-for" : "vote-against"};
  return null;
}

// The node a Raft frame (events.js "raft") reaches from a leader, winding
// its election clock up: a heartbeat's or entries' as it arrives (read out
// of the outbox, or off a direct link); null for any other.
export function reaches(e) {
  if (e.type !== "raft" || (e.kind !== "heartbeat" && e.kind !== "append")) return null;
  return e.at === "read" ? e.to : null;
}

// You knocked on a node that does not lead: your pigeon turns back at its door.
export const refused = node => ({from: {client: true}, to: {node}, failed: true, size: 2048, ms: 0});
