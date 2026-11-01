// The stage: a terminal, two channels, the metadata log and a sentence.
// Used by replay.js (a recorded trace).

// Server log lines (tracing JSON) → stage steps; a recording (site/record.py)
// and a live run look alike. Posts come from the Telegram / Discord stores'
// `posted` (the fakes add the text they got), or, in recordings on memory
// stores, from `stored` joined to the store call.
function toSteps(lines) {
  const out = [], pending = {};
  let preview = null;
  for (const line of lines) {
    const f = line.fields || {}, msg = f.message || "";
    const spans = Object.fromEntries((line.spans || []).map(s => [s.name, s]));
    // A fake channel's own tablet (its log and commits): not the node's.
    if (spans.fake_channel) continue;
    let e = null;
    if (msg === "fake channel: text") {
      preview = f.preview;
    } else if (msg === "posted" && String(f.name || "").startsWith("log-batch")) {
      pending.log = +f.bytes;           // a log batch in a channel: drawn in the log row
      preview = null;
    } else if (msg === "posted") {
      e = {type: "blob", op: "put", store: f.store, msg: f.message_id, name: f.name, bytes: +f.bytes, kind: f.kind};
      if (f.kind === "text") e.preview = preview;
      preview = null;
    } else if (msg === "deleted" && f.store) {
      e = {type: "blob", op: "delete", store: f.store, msgs: String(f.messages || "").match(/\d+/g) || []};
    } else if (msg === "store call" && spans.store) {
      const s = spans.store;
      if (s.op === "put" && pending[s.instance]) {
        e = {type: "blob", op: "put", store: s.instance, bytes: +s.bytes, ...pending[s.instance]};
        delete pending[s.instance];
      } else if (s.op === "get") {
        e = {type: "blob", op: "get", store: s.instance, bytes: s.bytes != null ? +s.bytes : null};
      }
    } else if (msg === "stored" && f.store && f.store !== "memory") {
      pending[f.store] = {msg: f.message_id, name: f.name};
    } else if (msg === "stored" && String(f.name || "").startsWith("log-batch")) {
      pending.log = f.bytes;
    } else if (msg === "log batch linked") {
      e = {type: "log", first: f.first, last: f.last, bytes: pending.log};
      delete pending.log;
    } else if (msg === "commit" && spans.commit) {
      e = {type: "commit", position: +spans.commit.position, entries: +spans.commit.entries};
    } else if ((line.target || "").startsWith("pigeonhole_gateway_s3") && msg.endsWith(" ok")) {
      e = {type: "s3", op: msg.slice(0, -3), ...Object.fromEntries(Object.entries(f).filter(([k]) => k !== "message"))};
    }
    if (e) out.push(e);
  }
  return out;
}

function makeStage(stage) {
  const $ = role => stage.querySelector(`[data-role="${role}"]`);
  const term = $("term"), log = $("log"), say = $("say");
  const lanes = {telegram: $("telegram"), discord: $("discord")};
  for (const lane of Object.values(lanes)) lane.dataset.empty = lane.querySelector(".empty")?.textContent || "";
  const posts = new Map();          // "store/message id" → node
  let paneOf = {};                  // store id → "telegram" | "discord"; by name when unset
  let entry = null;
  const pane = store => paneOf[store] || (lanes[store] ? store : null);

  const secs = ms => `+${(ms / 1000).toFixed(2)} s`;
  function termLine(cls, text, ts) {
    const row = h("div", `line ${cls}`);
    const body = h("span", "", text);
    row.append(body);
    if (ts != null) row.append(h("span", "ts", typeof ts === "number" ? secs(ts) : ts));
    term.append(row);
    term.scrollTop = term.scrollHeight;
    return body;
  }

  function fly(p, label) {
    const from = term.getBoundingClientRect(), to = lanes[p].getBoundingClientRect(), box = stage.getBoundingClientRect();
    const chip = h("div", "flyer", label);
    chip.style.background = p === "telegram" ? "var(--tg)" : "var(--dc)";
    chip.style.left = `${from.right - box.left - 120}px`;
    chip.style.top = `${from.top - box.top + from.height * 0.55}px`;
    stage.append(chip);
    requestAnimationFrame(() => requestAnimationFrame(() => {
      chip.style.transform = `translate(${to.left - from.right + 140}px, ${to.bottom - from.top - from.height * 0.55 - 40}px)`;
      chip.style.opacity = "0.15";
    }));
    setTimeout(() => chip.remove(), 600);
  }

  function post(e) {
    const p = pane(e.store);
    if (!p) return;
    const lane = lanes[p];
    lane.querySelector(".empty")?.remove();
    const short = String(e.name || "").replace(/^ph-(\w{6})\w+(\w{4})\.bin$/, "ph-$1…$2.bin");
    const tag = e.store !== p ? ` · ${e.store}` : "";
    // Discord ids are snowflakes (time in the high bits): the tail tells them apart.
    const id = String(e.msg ?? "");
    const meta = `${bytes(e.bytes)} · ${t("msg", {n: id.length > 8 ? `…${id.slice(-6)}` : id})}${tag}`;
    const body = () => {
      const col = h("div");
      col.style.minWidth = "0";
      if (e.kind === "text") {
        col.append(h("div", "ttext", e.preview ? `${e.preview}…` : short), h("div", "fmeta", `${t("as_text")} · ${meta}`));
      } else {
        col.append(h("div", "fname", short), h("div", "fmeta", meta));
      }
      return col;
    };
    let node;
    if (p === "telegram") {
      node = h("div", "bubble pop" + (e.kind === "text" ? " text" : ""));
      if (e.kind !== "text") node.append(h("div", "file", "⬇"));
      node.append(body());
    } else {
      node = h("div", "post pop");
      const col = h("div");
      const name = h("div", "name", "pigeonhole");
      name.append(h("small", "", "BOT"));
      if (e.kind === "text") {
        col.append(name, body());
      } else {
        const att = h("div", "att");
        att.append(h("span", "ic", "🗎"), body());
        col.append(name, att);
      }
      node.append(h("div", "who", "P"), col);
    }
    if (e.msg != null) posts.set(`${e.store}/${e.msg}`, node);
    lane.append(node);
    lane.scrollTop = lane.scrollHeight;
  }

  // Posts deleted from a channel; `msgs` null: the whole store (it went down).
  function strike(store, msgs) {
    for (const [k, n] of posts) {
      const [s, m] = k.split("/");
      if (s === String(store) && (msgs == null || msgs.includes(m))) n.classList.add("gone");
    }
  }

  function logEntry(e) {
    entry = h("div", "entry new");
    entry.append(h("b", "", t("position", {n: e.first})), h("span", "", bytes(e.bytes || 0)));
    log.append(entry);
    log.scrollLeft = log.scrollWidth;
  }
  function committed(e) {
    if (entry) entry.querySelector("span").textContent += ` · ${t("commands", {n: e.entries})}`;
  }

  function narrate(html) {
    if (!html) return;
    const s = document.createElement("span");
    s.className = "fade";
    s.innerHTML = html;                          // our own strings; data in them is escaped
    say.replaceChildren(s);
  }

  function reset() {
    term.replaceChildren();
    log.replaceChildren();
    say.replaceChildren();
    for (const lane of Object.values(lanes)) lane.replaceChildren(h("p", "empty", lane.dataset.empty));
    posts.clear();
    stage.querySelectorAll(".flyer").forEach(f => f.remove());
    entry = null;
  }

  // Which pane each store of the config is drawn in, by its kind.
  function setStores(list) {
    paneOf = {};
    for (const s of list || []) if (s.kind === "telegram" || s.kind === "discord") paneOf[s.id] = s.kind;
  }

  // Draws one step; resolves after its animation, `wait(ms)` paces it.
  async function draw(e, wait) {
    if (e.type === "blob" && e.op === "put") {
      const p = pane(e.store);
      if (!p) return;
      fly(p, bytes(e.bytes));
      await wait(480);
      post(e);
      await wait(120);
    } else if (e.type === "blob" && e.op === "delete") {
      strike(e.store, e.msgs);
    } else if (e.type === "log") {
      logEntry(e);
      await wait(350);
    } else if (e.type === "commit") {
      committed(e);
      await wait(150);
    }
  }

  return {termLine, post, strike, logEntry, committed, narrate, reset, draw, setStores, pane};
}

const esc = v => String(v).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);

// What a group of steps did, as one sentence (the say.* strings).
function sentence(events, cmdText = "") {
  const op = events.find(e => e.type === "s3");
  const puts = events.filter(e => e.type === "blob" && e.op === "put");
  if (!op) return /\bls\b/.test(cmdText) ? t("say.ls") : "";
  const vars = {bucket: esc(op.bucket), key: esc(op.key || ""), n: +op.extents};
  if (op.op === "PutObject") {
    if (String(op.inline) === "true") return t("say.PutObjectInline", {key: vars.key, size: bytes(+op.size)});
    // One store's posts: the chunks of the object as that channel got them.
    const first = puts[0]?.store;
    const mine = puts.filter(e => e.store === first);
    const stored = mine.reduce((a, e) => a + e.bytes, 0);
    vars.size = bytes(+op.size);
    vars.stored = bytes(stored);
    vars.chunks = t("chunks", {n: mine.length});
    if (mine.length && mine.every(e => e.kind === "text")) return t("say.PutObjectText", vars);
    return t(stored < op.size * 0.8 ? "say.PutObjectZ" : "say.PutObject", vars);
  }
  if (op.op === "CopyObject") return t("say.CopyObject", {...vars, chunks: t("chunks", {n: +op.extents})});
  if (op.op === "DeleteObject") return t("say.DeleteObject", {...vars, n: +op.released});
  return t(`say.${op.op}`, vars);
}
