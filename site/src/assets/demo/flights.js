// What flies on the graph for each event the node logged (events.js): a
// pigeon from one place to another. The graph (graph.js) only turns places
// into points on the town; who flies where is decided here, and only here.
//
// A place: {client: true}           your block's roof
//          {node: id}               node `id`'s dovecote (0: the one node)
//          {store, at}              a channel's block: `at` "new" its newest
//                                   window, "any" a lit one, "free" the next
//                                   empty one, "sign" its roof
// A flight: {from, to, size: bytes (a bigger pigeon), failed: it turns back
//            short of `to`, traffic: "raft" (the cluster's own: not queued
//            behind yours) | undefined, spark: a heartbeat, not a pigeon}
//
// Who sends: a node's log, snapshots and outbox are its own, so their
// posts are its; the data channels are written and read by the leader
// only. A log line names no node but in Raft's frames, so the leader is
// the one the page last saw leading.

// The node a channel's traffic comes from: its own node, else the leader.
// ctx: {leader: id | null, shown: the node on screen}.
export const ownerOf = (c, ctx) => c.node ?? ctx.leader ?? ctx.shown;

// The flights for event `e`; `channel(store)` finds a channel. None for an
// event that is not traffic, or a channel not on the graph.
export function flightsOf(e, channel, ctx) {
  if (e.type === "raft") return raft(e);
  const c = e.store == null ? null : channel(e.store);
  if (!c) return [];
  const node = {node: ownerOf(c, ctx)};
  if (e.type === "posted") return [{from: node, to: {store: c.store, at: "new"}, size: e.bytes}];
  if (e.type === "call" && (e.op === "get" || e.op === "exists"))
    return [{from: {store: c.store, at: "any"}, to: node, size: e.op === "get" ? e.bytes || 4096 : 512}];
  if (e.type === "call_failed") return [{from: node, to: {store: c.store, at: "free"}, size: 2048, failed: true}];
  return [];
}

// A Raft request: over a direct link, dovecote to dovecote; over the chat,
// into the sender's outbox ("sent"), or out of it to the reader ("read").
function raft(e) {
  const base = {traffic: "raft", size: 1024, spark: e.kind === "heartbeat", kind: e.kind};
  if (e.via !== "chat") return [{...base, from: {node: e.from}, to: {node: e.to}}];
  const outbox = `outbox-${e.from}`;
  if (e.at === "sent") return [{...base, from: {node: e.from}, to: {store: outbox, at: "new"}}];
  return [{...base, from: {store: outbox, at: "new"}, to: {node: e.to}}];
}

// You knocked on a node that does not lead: your pigeon turns back at its door.
export const refused = node => ({from: {client: true}, to: {node}, failed: true, size: 2048});
