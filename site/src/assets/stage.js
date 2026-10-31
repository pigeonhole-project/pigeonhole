// The stage: a terminal, two channels, the metadata log and a sentence.
// Used by replay.js (a recorded trace) and try.js (pigeonhole live in wasm).

// Server log lines (tracing JSON) → stage steps; the same rules as
// site/record.py, so a recording and a live run look alike.
function toSteps(lines) {
  const out = [], pending = {};
  for (const line of lines) {
    const f = line.fields || {}, msg = f.message || "";
    const spans = Object.fromEntries((line.spans || []).map(s => [s.name, s]));
    let e = null;
    if (msg === "store call" && spans.store) {
      const s = spans.store;
      e = {type: "blob", store: s.instance, op: s.op, bytes: s.bytes != null ? +s.bytes : null};
      if (e.op === "put" && pending[e.store]) Object.assign(e, pending[e.store]), delete pending[e.store];
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
  const posts = {telegram: new Map(), discord: new Map()};   // message id → node
  let entry = null;

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

  function fly(store, label) {
    const from = term.getBoundingClientRect(), to = lanes[store].getBoundingClientRect(), box = stage.getBoundingClientRect();
    const chip = h("div", "flyer", label);
    chip.style.background = store === "telegram" ? "var(--tg)" : "var(--dc)";
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
    const lane = lanes[e.store];
    if (!lane) return;
    lane.querySelector(".empty")?.remove();
    const short = String(e.name || "").replace(/^ph-(\w{6})\w+(\w{4})\.bin$/, "ph-$1…$2.bin");
    const meta = `${bytes(e.bytes)} · ${t("msg", {n: e.msg})}`;
    const file = () => {
      const txt = h("div");
      txt.style.minWidth = "0";
      txt.append(h("div", "fname", short), h("div", "fmeta", meta));
      return txt;
    };
    let node;
    if (e.store === "telegram") {
      node = h("div", "bubble pop");
      node.append(h("div", "file", "⬇"), file());
    } else {
      node = h("div", "post pop");
      const col = h("div");
      const name = h("div", "name", "pigeonhole");
      name.append(h("small", "", "BOT"));
      const att = h("div", "att");
      att.append(h("span", "ic", "🗎"), file());
      col.append(name, att);
      node.append(h("div", "who", "P"), col);
    }
    if (e.msg != null) posts[e.store].set(String(e.msg), node);
    lane.append(node);
    lane.scrollTop = lane.scrollHeight;
  }

  // Marks every post of `store` as gone (the channel was lost) or one post deleted.
  function strike(store, msg) {
    const nodes = msg == null ? [...posts[store].values()] : [posts[store].get(String(msg))].filter(Boolean);
    for (const n of nodes) n.classList.add("gone");
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
    for (const m of Object.values(posts)) m.clear();
    stage.querySelectorAll(".flyer").forEach(f => f.remove());
    entry = null;
  }

  // Draws one step; resolves after its animation, `wait(ms)` paces it.
  async function draw(e, wait) {
    if (e.type === "blob" && e.op === "put") {
      fly(e.store, bytes(e.bytes));
      await wait(480);
      post(e);
      await wait(120);
    } else if (e.type === "log") {
      logEntry(e);
      await wait(350);
    } else if (e.type === "commit") {
      committed(e);
      await wait(150);
    }
  }

  return {termLine, post, strike, logEntry, committed, narrate, reset, draw, lanes};
}

const esc = v => String(v).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);

// What a group of steps did, as one sentence (the say.* strings).
function sentence(events, cmdText = "") {
  const op = events.find(e => e.type === "s3");
  const puts = events.filter(e => e.type === "blob" && e.op === "put" && e.store === "telegram");
  if (!op) return /\bls\b/.test(cmdText) ? t("say.ls") : "";
  const vars = {bucket: esc(op.bucket), key: esc(op.key || ""), n: +op.extents};
  if (op.op === "PutObject") {
    const stored = puts.reduce((a, e) => a + e.bytes, 0);
    vars.size = bytes(+op.size);
    vars.stored = bytes(stored);
    vars.chunks = t("chunks", {n: puts.length});
    return t(stored < op.size * 0.8 ? "say.PutObjectZ" : "say.PutObject", vars);
  }
  if (op.op === "CopyObject") return t("say.CopyObject", {...vars, chunks: t("chunks", {n: +op.extents})});
  if (op.op === "DeleteObject") return t("say.DeleteObject", {...vars, n: +op.released});
  return t(`say.${op.op}`, vars);
}
