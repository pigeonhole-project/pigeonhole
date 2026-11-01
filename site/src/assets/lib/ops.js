// The node's op protocol, the same over HTTP (/admin/ops on a real node)
// and into the wasm (stdin): a JSON line with the body's length, then the
// body; out come JSON lines, the node's log and one {"reply": …}.

export function frame(request, body) {
  const len = body ? body.byteLength : 0;
  const head = new TextEncoder().encode(`${JSON.stringify({...request, len})}\n`);
  const out = new Uint8Array(head.length + len);
  out.set(head);
  if (body) out.set(body instanceof Uint8Array ? body : new Uint8Array(body), head.length);
  return out;
}

// Output lines → {reply, events}; a line that is not JSON is dropped.
export function unframe(lines) {
  const parsed = [];
  for (const l of lines) {
    try {
      parsed.push(JSON.parse(l));
    } catch {
      // a stray print: not the protocol's
    }
  }
  const reply = parsed.find(l => "reply" in l)?.reply;
  return {reply, events: parsed.filter(l => !("reply" in l))};
}
