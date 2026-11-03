// The node's log (tracing JSON lines) as the events the demo draws.
//
// The lines are read by their text: these messages (`fields.message`) are
// a contract with the Rust tracing sites that log them, at the level the
// env filter in crates/web (web.rs ph_open) lets through. `make web-smoke`
// checks it (crates/web/run-shim.mjs events: a cluster's run must log every
// message here and give every event type): a reworded or quieter line fails
// there, not silently on the page.
export const MESSAGES = {
  text: "fake channel: text", // the fake Telegram/Discord: the text a post holds (its preview)
  posted: "posted", // the Telegram/Discord stores, the Raft outbox: a message in a channel
  deleted: "deleted", // the Raft outbox (pigeonhole-raft-telegram): its prune
  call: "store call", // pigeonhole-blob metrics, in span `store` {instance, op, bytes}
  callFailed: "store call failed", // the same, failed
  failover: "replica stream failed; failing over", // pigeonhole-blob replicated: a read's next copy
  repaired: "repair pass complete", // pigeonhole-chunk-store repair
  leaseWait: "waiting out the lease", // pigeonhole-meta log/chain: a lease log another held
  claimed: "claimed the metadata log", // pigeonhole-meta: the tablet took its log
  frameSent: "raft frame sent", // openraft-chat, openraft-grpc
  voteAnswered: "raft vote answered", // openraft-chat, openraft-grpc
  frameReceived: "raft frame received", // openraft-chat (the demo reads the chat's only)
};
// An S3 call that succeeded: "<Op> ok" from the gateway (PutObject ok, …).
export const S3_TARGET = "pigeonhole_gateway_s3";
export const S3_OK = " ok";
// The spans read: `store` (a store call's), `fake_channel` (a fake channel's
// own tablet: not the node's, left out).
//
// The events:
//
//   {type: "posted", store, id, kind, name, bytes, preview?}   a message in a channel
//   {type: "deleted", store, ids}                              messages deleted from one
//   {type: "call", store, op, bytes}                           a store call (get, put, exists…)
//   {type: "call_failed", store, op}
//   {type: "repair_pass", repaired}
//   {type: "claimed", generation}                              the node took the metadata log
//   {type: "lease_wait", ms}                                   it waits out the lease another held on it
//   {type: "s3", op, ...fields}                                an S3 call that succeeded
//   {type: "raft", from, to, via, kind: "voted", granted, at: "answered"}
//                                                              a vote's answer, from the voter
//   {type: "raft", from, to, via, kind, at}                    a Raft request between nodes:
//       via "grpc" (direct), or "chat" through the sender's outbox; `at`
//       "sent" (into the outbox, over the chat) or "read" (out of it)
export const EVENT_TYPES = ["posted", "deleted", "call", "call_failed", "repair_pass", "claimed", "lease_wait", "s3", "raft"];

export function readEvents(lines) {
  const out = [];
  let preview = null; // the text a fake channel got, for the post that follows
  for (const line of lines) {
    const spans = Object.fromEntries((line.spans || []).map(s => [s.name, s]));
    if (spans.fake_channel) continue;
    const f = line.fields || {},
      msg = f.message || "";
    const store = spans.store;
    if (msg === MESSAGES.text) {
      preview = f.preview;
    } else if (msg === MESSAGES.posted) {
      const e = {type: "posted", store: f.store, id: f.message_id, kind: f.kind, name: f.name, bytes: +f.bytes};
      if (f.kind === "text") e.preview = preview;
      preview = null;
      out.push(e);
    } else if (msg === MESSAGES.deleted && f.store) {
      out.push({type: "deleted", store: f.store, ids: String(f.messages || "").match(/\d+/g) || []});
    } else if (msg === MESSAGES.call && store) {
      out.push({type: "call", store: store.instance, op: store.op, bytes: store.bytes == null ? null : +store.bytes});
    } else if (msg === MESSAGES.callFailed && store) {
      out.push({type: "call_failed", store: store.instance, op: store.op});
    } else if (msg === MESSAGES.failover) {
      // A read whose channel failed under it: the next copy is read instead.
      out.push({type: "call_failed", store: f.instance, op: "get"});
    } else if (msg === MESSAGES.repaired) {
      out.push({type: "repair_pass", repaired: +f.repaired});
    } else if (msg === MESSAGES.leaseWait) {
      out.push({type: "lease_wait", ms: +f.wait_ms});
    } else if (msg === MESSAGES.claimed) {
      out.push({type: "claimed", generation: f.generation});
    } else if (msg === MESSAGES.frameSent && f.reply === false) {
      out.push({type: "raft", from: +f.node, to: +f.to, via: f.via, kind: f.kind, at: "sent"});
    } else if (msg === MESSAGES.voteAnswered) {
      out.push({type: "raft", from: +f.node, to: +f.to, via: f.via, kind: "voted", granted: f.granted === true, at: "answered"});
    } else if (msg === MESSAGES.frameReceived && f.reply === false && f.via === "chat") {
      out.push({type: "raft", from: +f.from, to: +f.node, via: f.via, kind: f.kind, at: "read"});
    } else if ((line.target || "").startsWith(S3_TARGET) && msg.endsWith(S3_OK)) {
      const {message: _, ...fields} = f;
      out.push({type: "s3", ...fields, op: msg.slice(0, -S3_OK.length)});
    }
  }
  return out;
}
