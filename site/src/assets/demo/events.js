// The node's log (tracing JSON lines) as the events the demo draws:
//
//   {type: "posted", store, id, kind, name, bytes, preview?}   a message in a channel
//   {type: "deleted", store, ids}                              messages deleted from one
//   {type: "call", store, op, bytes}                           a store call (get, put, exists…)
//   {type: "call_failed", store, op}
//   {type: "repair_pass", repaired}
//   {type: "claimed", generation}                              the node took the metadata log
//   {type: "s3", op, ...fields}                                an S3 call that succeeded
//
// The fake channels' own tablets (span fake_channel) are not the node's: left out.
export function readEvents(lines) {
  const out = [];
  let preview = null; // the text a fake channel got, for the post that follows
  for (const line of lines) {
    const spans = Object.fromEntries((line.spans || []).map(s => [s.name, s]));
    if (spans.fake_channel) continue;
    const f = line.fields || {},
      msg = f.message || "";
    const store = spans.store;
    if (msg === "fake channel: text") {
      preview = f.preview;
    } else if (msg === "posted") {
      const e = {type: "posted", store: f.store, id: f.message_id, kind: f.kind, name: f.name, bytes: +f.bytes};
      if (f.kind === "text") e.preview = preview;
      preview = null;
      out.push(e);
    } else if (msg === "deleted" && f.store) {
      out.push({type: "deleted", store: f.store, ids: String(f.messages || "").match(/\d+/g) || []});
    } else if (msg === "store call" && store) {
      out.push({type: "call", store: store.instance, op: store.op, bytes: store.bytes == null ? null : +store.bytes});
    } else if (msg === "store call failed" && store) {
      out.push({type: "call_failed", store: store.instance, op: store.op});
    } else if (msg === "replica stream failed; failing over") {
      // A read whose channel failed under it: the next copy is read instead.
      out.push({type: "call_failed", store: f.instance, op: "get"});
    } else if (msg === "repair pass complete") {
      out.push({type: "repair_pass", repaired: +f.repaired});
    } else if (msg === "claimed the metadata log") {
      out.push({type: "claimed", generation: f.generation});
    } else if ((line.target || "").startsWith("pigeonhole_gateway_s3") && msg.endsWith(" ok")) {
      const {message: _, ...fields} = f;
      out.push({type: "s3", ...fields, op: msg.slice(0, -3)});
    }
  }
  return out;
}
