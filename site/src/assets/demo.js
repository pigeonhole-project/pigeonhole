// pigeonhole's demo: the node (wasm, web-worker.js) and its channels as a
// graph. Every flight on it is an event the node logged; clicking a node
// opens its panel. Uses t(), bytes(), h() from site.js.
(function () {
  "use strict";
  const $ = id => document.getElementById(id);
  const svgNS = "http://www.w3.org/2000/svg";
  const s = (tag, attrs = {}, parent) => {
    const e = document.createElementNS(svgNS, tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    if (parent) parent.appendChild(e);
    return e;
  };
  const esc = v => String(v).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);
  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;

  // ------------------------------------------------------------ the worker
  const worker = new Worker(window.ASSETS + "web-worker.js" + (document.currentScript?.src.match(/\?v=\w+/)?.[0] || ""));
  // Let go of the files at once, for the next page (a reload, the other language).
  addEventListener("pagehide", () => worker.terminate());
  addEventListener("pageshow", e => { if (e.persisted) location.reload(); });
  const waiting = new Map();
  let nextId = 1, queue = Promise.resolve();
  worker.onmessage = ({data}) => {
    if (data.id === -1) { background(data.events); return; }
    const done = waiting.get(data.id);
    waiting.delete(data.id);
    if (done) done(data);
  };
  function call(request, body) {
    const run = queue.then(() => new Promise(res => {
      const id = nextId++;
      waiting.set(id, res);
      worker.postMessage({id, ...request, body}, body ? [body] : []);
    }));
    queue = run.catch(() => {});
    return run;
  }
  const started = new Promise(res => waiting.set(0, res));

  // ------------------------------------------------------------ state
  const state = {
    demo: "", config: "", persistent: false,
    channels: [],          // {store, role, kind, scope, total, messages: [the last 200]}
    objects: [],
    log: null,             // {head, base, entries}
    open: null,            // the node whose panel is open
    logView: "readable",   // the log panel: "readable" or "channel"
    busy: false,
  };
  const byStore = () => Object.fromEntries(state.channels.map(c => [c.store, c]));
  // The open panel is the log's (it opens by the log channel's id).
  const logOpen = () => byStore()[state.open]?.role === "log";

  // ------------------------------------------------------------ the graph
  const graph = $("graph");
  let nodes = {}, edges = {};
  const W = 1000;
  // A pigeon in flight, facing right: wings up from a small body, a tail.
  const BIRD = "M-9,1 Q-6,-1 -3,0 Q-1,-9 3,-11 Q2,-4 2,-1 Q6,-2 9,0 Q6,2 2,2 Q0,4 -3,3 Q-6,3 -9,1 Z";

  function layout() {
    const right = state.channels;
    const H = Math.max(480, 120 + right.length * 108);
    graph.setAttribute("viewBox", `0 0 ${W} ${H}`);
    graph.setAttribute("preserveAspectRatio", "xMidYMid meet");
    const pos = {client: [120, H / 2], loft: [440, H / 2]};
    right.forEach((c, i) => { pos[c.store] = [790, (H / (right.length + 1)) * (i + 1)]; });
    return {H, pos};
  }

  function draw() {
    graph.replaceChildren();
    nodes = {}; edges = {};
    const {pos} = layout();
    const defs = s("defs", {}, graph);
    const g = s("linearGradient", {id: "iris", x1: "0", y1: "0", x2: "1", y2: "1"}, defs);
    s("stop", {offset: "0", "stop-color": "var(--teal)"}, g);
    s("stop", {offset: "1", "stop-color": "var(--amethyst)"}, g);

    const edgeLayer = s("g", {}, graph);
    const curve = (a, b) => {
      const [x1, y1] = a, [x2, y2] = b, mx = (x1 + x2) / 2;
      return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
    };
    edges.client = s("path", {class: "edge live", d: curve([166, pos.client[1]], [345, pos.loft[1]])}, edgeLayer);
    for (const c of state.channels) {
      edges[c.store] = s("path", {class: "edge", d: curve([535, pos.loft[1]], [660, pos[c.store][1]])}, edgeLayer);
    }

    // You: the S3 client.
    const you = node("client", pos.client, t("demo.you"), t("demo.objects", {n: state.objects.length}));
    s("circle", {class: "shape", cx: 0, cy: 0, r: 46}, you.g);
    you.label(0, 72);

    // The loft: a dovecote, the node itself.
    const loft = node("loft", pos.loft, "pigeonhole", t("demo.loft_role"));
    dovecote(loft.g);
    loft.label(0, 100);

    for (const c of state.channels) {
      const app = c.kind === "telegram" ? "Telegram" : "Discord";
      const role = t(`demo.role_${c.role}`, {app});
      const n = node(c.store, pos[c.store], c.store, role);
      s("rect", {class: "shape", x: -130, y: -34, width: 260, height: 68, rx: 14}, n.g);
      s("circle", {class: "badge", cx: -106, cy: 0, r: 9, fill: c.kind === "telegram" ? "var(--tg)" : "var(--dc)"}, n.g);
      n.label(-88, 0, "start");
      // The channel's message count, top right.
      n.count = s("text", {class: "count", x: 116, y: -10, "text-anchor": "end"}, n.g);
      n.count.textContent = String(c.total);
      const tip = s("title", {}, n.g);
      tip.textContent = t("demo.messages", {n: c.total});
    }
    markOpen();
  }

  // A pigeon tower: a round body narrowing upward, a cone roof with a lantern
  // and a finial, two rows of arched entrances with perches, a plinth.
  function dovecote(g) {
    const line = {fill: "none", stroke: "var(--slate)", "stroke-width": 1.5, "stroke-linecap": "round"};
    s("rect", {class: "shape", x: -78, y: 64, width: 156, height: 12, rx: 3}, g);
    s("path", {class: "shape", d: "M-66,64 L-58,-18 L58,-18 L66,64 Z"}, g);
    s("rect", {class: "shape", x: -70, y: -26, width: 140, height: 9, rx: 3}, g);
    s("path", {class: "shape", d: "M-80,-26 Q-28,-64 -9,-98 L9,-98 Q28,-64 80,-26 Z"}, g);
    s("path", {...line, d: "M-60,-38 Q0,-52 60,-38"}, g);
    s("path", {...line, d: "M-38,-60 Q0,-72 38,-60"}, g);
    s("rect", {class: "shape", x: -10, y: -112, width: 20, height: 15, rx: 2}, g);
    s("path", {class: "shape", d: "M-14,-112 L0,-124 L14,-112 Z"}, g);
    s("path", {...line, d: "M0,-124 L0,-136"}, g);
    s("circle", {cx: 0, cy: -105, r: 3.5, fill: "var(--wing-2)"}, g);
    // Entrances: an arch over a dark hole, a perch under it.
    const arch = (x, y) => {
      s("path", {d: `M${x - 10},${y + 14} L${x - 10},${y} A10,10 0 0 1 ${x + 10},${y} L${x + 10},${y + 14} Z`, fill: "var(--wing-2)", "fill-opacity": .78}, g);
      s("path", {...line, d: `M${x - 15},${y + 18} L${x + 15},${y + 18}`}, g);
    };
    for (const x of [-34, 0, 34]) arch(x, -2);
    for (const x of [-36, 0, 36]) arch(x, 34);
  }

  // A clickable node: a group at `p`, its name, role and a count.
  function node(id, p, name, role, count) {
    const g = s("g", {class: "node", transform: `translate(${p[0]},${p[1]})`, tabindex: 0, role: "button", "aria-label": `${name}: ${role}`}, graph);
    g.addEventListener("click", () => openPanel(id));
    g.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openPanel(id); } });
    const n = {g, p, label(x, y, anchor = "middle") {
      const tx = s("text", {class: "name", x, y: anchor === "start" ? y - 6 : y, "text-anchor": anchor}, g);
      tx.textContent = name;
      const r = s("text", {class: "role", x, y: (anchor === "start" ? y - 6 : y) + 18, "text-anchor": anchor}, g);
      r.textContent = count ? `${role}, ${count}` : role;
      n.countText = r;
    }};
    nodes[id] = n;
    return n;
  }

  function markOpen() {
    for (const [id, n] of Object.entries(nodes)) n.g.classList.toggle("open", id === state.open);
  }

  // A pigeon: a dot along an edge, from the loft to a channel or back.
  const flights = [];
  let flying = Promise.resolve();
  function fly(store, back, size) {
    const path = edges[store];
    if (!path) return Promise.resolve();
    if (reduced) return Promise.resolve();
    const start = flying.then(() => new Promise(r => setTimeout(r, 70)));
    flying = start;
    return start.then(() => new Promise(done => {
      // A small pigeon, wings beating; bigger for bigger payloads.
      const scale = 0.9 + Math.min(1, Math.log2(1 + size / 4096) / 10);
      const bird = s("g", {class: "flight"}, graph);
      const body = s("path", {fill: "url(#iris)", d: BIRD}, bird);
      const len = path.getTotalLength(), t0 = performance.now(), dur = 680;
      flights.push(bird);
      const step = now => {
        const k = Math.min(1, (now - t0) / dur), e = k < .5 ? 2 * k * k : 1 - (-2 * k + 2) ** 2 / 2;
        const at = len * (back ? 1 - e : e);
        const pt = path.getPointAtLength(at), ahead = path.getPointAtLength(Math.min(len, Math.max(0, at + (back ? -1 : 1))));
        const flip = ahead.x < pt.x ? -1 : 1;
        const flap = 0.55 + 0.45 * Math.abs(Math.sin((now - t0) / 55));
        bird.setAttribute("transform", `translate(${pt.x},${pt.y}) scale(${flip * scale},${scale})`);
        body.setAttribute("transform", `scale(1,${flap})`);
        if (k < 1) requestAnimationFrame(step);
        else { bird.remove(); done(); }
      };
      requestAnimationFrame(step);
    }));
  }
  function bump(id) {
    const n = nodes[id];
    if (!n || reduced) return;
    // A flash of the outline: animating transform would undo the node's position.
    n.g.classList.remove("landed"); void n.g.getBBox(); n.g.classList.add("landed");
  }

  // ------------------------------------------------------------ events
  // The node's log lines: what to fly, what changed in which channel.
  function apply(events) {
    const stores = byStore();
    let preview = null;
    const said = [];
    for (const line of events) {
      if ((line.spans || []).some(sp => sp.name === "fake_channel")) continue;
      const f = line.fields || {}, msg = f.message || "";
      const span = (line.spans || []).find(sp => sp.name === "store");
      if (msg === "fake channel: text") preview = f.preview;
      else if (msg === "posted" && stores[f.store]) {
        const c = stores[f.store];
        const m = {id: f.message_id, kind: f.kind, name: f.name, size: +f.bytes, preview: f.kind === "text" ? preview : undefined, fresh: true, at: new Date()};
        preview = null;
        c.messages.push(m);
        c.total++;
        const at = c.role === "snapshots" && /-(\d+)\.snap/.exec(m.name || "");
        if (at) said.push({op: "Snapshot", at: +at[1]});
        fly(f.store, false, m.size).then(() => { bump(f.store); refreshCount(c); if (state.open === f.store) renderPanel(); });
      } else if (msg === "deleted" && stores[f.store]) {
        const ids = String(f.messages || "").match(/\d+/g) || [];
        for (const m of stores[f.store].messages) if (ids.includes(String(m.id))) m.gone = true;
        stores[f.store].total = Math.max(0, stores[f.store].total - ids.length);
        refreshCount(stores[f.store]);
        if (state.open === f.store) renderPanel();
      } else if (msg === "store call" && span?.op === "get" && stores[span.instance]) {
        fly(span.instance, true, +span.bytes || 4096);
      } else if ((line.target || "").startsWith("pigeonhole_gateway_s3") && msg.endsWith(" ok")) {
        said.push({op: msg.slice(0, -3), ...f});
      }
    }
    return said;
  }

  function refreshCount(c) {
    const n = nodes[c.store];
    if (!n) return;
    if (n.count) n.count.textContent = String(c.total);
  }

  // What an S3 call did, in a sentence.
  function sentence(op) {
    const firstData = state.channels.find(c => c.role === "data");
    const key = esc(op.key || op.bucket || "");
    if (op.op === "PutObject") {
      if (String(op.inline) === "true") return t("say.PutObjectInline", {key, size: bytes(+op.size)});
      const mine = (firstData?.messages || []).filter(m => m.fresh);
      const stored = mine.reduce((a, m) => a + m.size, 0);
      const vars = {key, size: bytes(+op.size), stored: bytes(stored), chunks: t("chunks", {n: mine.length || +op.extents})};
      if (mine.length && mine.every(m => m.kind === "text")) return t("say.PutObjectText", vars);
      return t(stored && stored < op.size * 0.8 ? "say.PutObjectZ" : "say.PutObject", vars);
    }
    if (op.op === "CopyObject") return t("say.CopyObject", {key, chunks: t("chunks", {n: +op.extents})});
    if (op.op === "DeleteObject") return t("say.DeleteObject", {key});
    if (op.op === "Snapshot") return t("say.snapshot", {at: op.at});
    return "";
  }
  function caption(html) {
    $("caption").innerHTML = html || "";        // our own strings; data in them is escaped
  }

  // What the node did on its own between calls (a snapshot, as a server does).
  async function background(events) {
    const said = apply(events);
    const snap = said.find(x => x.op === "Snapshot");
    if (snap) caption(t("say.snapshot", {at: snap.at}));
    await flying;
    if (snap && (state.open === "loft" || logOpen())) {
      await loadLog();
      renderPanel();
    }
  }

  // ------------------------------------------------------------ calls
  async function op(request, body) {
    setBusy(true);
    for (const c of state.channels) for (const m of c.messages) m.fresh = false;
    const data = await call(request, body);
    const said = apply(data.events);
    if (data.reply?.error) caption(`<b>${esc(t("try.error", {error: data.reply.error}))}</b>`);
    else if (said.length) caption(sentence(said.at(-1)));
    await flying;
    await refreshObjects();
    if (logOpen() || state.open === "loft") await loadLog();
    setBusy(false);
    return data.reply;
  }

  async function refreshObjects() {
    const {reply} = await call({op: "list"});
    state.objects = reply?.objects || [];
    const n = nodes.client;
    if (n) n.countText.textContent = `${t("demo.you")}, ${t("demo.objects", {n: state.objects.length})}`;
    if (state.open === "client") renderObjects();
  }

  async function loadChannels() {
    const {reply} = await call({op: "channels"});
    state.channels = reply?.channels || [];
  }

  async function loadLog() {
    const [{reply}, ch] = [await call({op: "log", limit: 200}), await call({op: "channels"})];
    if (ch.reply?.channels) {
      // Keep the live messages' flags; take the decoded log posts.
      const fresh = byStore();
      state.channels = ch.reply.channels.map(c => ({...c, messages: c.messages.map(m => ({...m, gone: false}))}));
      for (const c of state.channels) if (fresh[c.store] && nodes[c.store]) refreshCount(c);
    }
    state.log = reply;
    if (logOpen()) renderPanel();
  }

  function setBusy(on) {
    state.busy = on;
    document.querySelectorAll(".panel button, .panel input").forEach(b => {
      if (!b.classList.contains("back")) b.disabled = on;
    });
  }

  // ------------------------------------------------------------ panels
  function openPanel(id) {
    state.open = id;
    $("loft").classList.add("zoomed");
    $("panel").hidden = false;
    markOpen();
    if (id === "log" || state.channels.find(c => c.store === id)?.role === "log") {
      state.open = state.channels.find(c => c.role === "log")?.store || id;
      loadLog();
    }
    renderPanel();
    $("panel-close").focus();
  }
  function closePanel() {
    const was = state.open;
    state.open = null;
    $("loft").classList.remove("zoomed");
    $("panel").hidden = true;
    markOpen();
    nodes[was]?.g.focus();
  }
  $("panel-close").addEventListener("click", closePanel);
  document.addEventListener("keydown", e => { if (e.key === "Escape" && state.open) closePanel(); });

  function renderPanel() {
    const id = state.open, body = $("panel-body");
    if (!id) return;
    const c = byStore()[id];
    if (id === "client") {
      $("panel-title").textContent = t("demo.you");
      $("panel-sub").textContent = t("demo.you_sub");
      body.replaceChildren($("tpl-client").content.cloneNode(true));
      wireClient();
      renderObjects();
    } else if (id === "loft") {
      $("panel-title").textContent = "pigeonhole";
      $("panel-sub").textContent = t("demo.loft_sub");
      body.replaceChildren($("tpl-node").content.cloneNode(true));
      wireNode();
    } else if (c?.role === "log") {
      $("panel-title").textContent = t("demo.log_title");
      $("panel-sub").textContent = t("demo.log_sub", {store: c.store, app: c.kind === "telegram" ? "Telegram" : "Discord"});
      renderLog(body);
    } else if (c) {
      const app = c.kind === "telegram" ? "Telegram" : "Discord";
      $("panel-title").textContent = c.store;
      $("panel-sub").textContent = t(`demo.channel_sub_${c.role}`, {app, scope: c.scope});
      body.replaceChildren(feed(c));
    }
  }

  // A channel as its app shows it: Telegram's channel view, Discord's text channel.
  const DISCORD_EPOCH = 1420070400000n;
  const hhmm = d => d.toLocaleTimeString(window.LANG, {hour: "2-digit", minute: "2-digit"});
  function when(c, m) {
    if (c.kind === "discord") {
      try { return new Date(Number((BigInt(m.id) >> 22n) + DISCORD_EPOCH)); } catch { return null; }
    }
    return m.at || null;
  }
  const svgIcon = (d, cls) => {
    const sv = document.createElementNS(svgNS, "svg");
    sv.setAttribute("viewBox", "0 0 24 24");
    sv.setAttribute("class", cls);
    sv.setAttribute("aria-hidden", "true");
    const p = document.createElementNS(svgNS, "path");
    p.setAttribute("d", d);
    sv.append(p);
    return sv;
  };
  const ICON_DOWN = "M12 4v11m0 0l-4.5-4.5M12 15l4.5-4.5M6 19h12";
  const ICON_DOC = "M7 3h7l5 5v13H7z M14 3v5h5";
  const ICON_EYE = "M2 12s3.6-6 10-6 10 6 10 6-3.6 6-10 6S2 12 2 12zm10 3a3 3 0 100-6 3 3 0 000 6z";
  const payload = m => (m.kind === "text" ? `${m.name} ${m.size}\n${m.preview || ""}…` : "");

  // What a post of the log's channel is to the log.
  function logLabel(l) {
    if (!l) return null;
    if (l.what === "batch" && l.first != null) {
      return l.first === l.last ? t("demo.post_entry", {gen: l.gen, n: l.first}) : t("demo.post_batch", {gen: l.gen, first: l.first, last: l.last});
    }
    return t(`demo.post_${l.what}`, {gen: l.gen ?? "", label: l.label ?? ""});
  }
  const service = m => m.log && m.log.what !== "batch";

  function feed(c) {
    return c.kind === "telegram" ? telegramView(c) : discordView(c);
  }

  function telegramView(c) {
    const app = h("div", "app tg");
    const head = h("div", "tg-head");
    const ava = h("div", "tg-ava", "PB");
    const who = h("div");
    who.append(h("b", "", t("demo.tg_title", {store: c.store})), h("small", "", t("demo.tg_subs")));
    head.append(ava, who);
    const chat = h("div", "tg-chat");
    const live = c.messages.slice(-200);
    const earlier = c.total - live.filter(m => !m.gone).length;
    if (earlier > 0) chat.append(h("div", "tg-day", t("demo.earlier", {n: earlier})));
    if (!live.length) chat.append(h("p", "tg-empty", t("demo.no_messages")));
    else chat.append(h("div", "tg-day", t("demo.today")));
    for (const m of live) {
      const b = h("div", "tg-post" + (m.gone ? " gone" : "") + (m.fresh ? " fresh" : "") + (service(m) ? " service" : ""));
      if (m.kind === "text") b.append(h("div", "tg-text cjk", payload(m)));
      else if (m.kind === "other") b.append(h("div", "tg-text", t("demo.tg_service")));
      else {
        const doc = h("div", "tg-doc");
        const ic = h("div", "tg-doc-ic");
        ic.append(svgIcon(ICON_DOWN, "ic"));
        const col = h("div");
        col.append(h("div", "tg-doc-name", m.name), h("div", "tg-doc-size", bytes(m.size)));
        doc.append(ic, col);
        b.append(doc);
      }
      if (m.log) b.append(h("div", "post-log", logLabel(m.log)));
      const foot = h("div", "tg-meta");
      foot.append(svgIcon(ICON_EYE, "eye"), document.createTextNode("1"));
      const at = when(c, m);
      if (at) foot.append(document.createTextNode(` ${hhmm(at)}`));
      b.append(foot);
      chat.append(b);
    }
    const input = h("div", "tg-input", t("demo.tg_broadcast"));
    app.append(head, chat, input);
    requestAnimationFrame(() => { chat.scrollTop = chat.scrollHeight; });
    return app;
  }

  function discordView(c) {
    const app = h("div", "app dc");
    const head = h("div", "dc-head");
    head.append(h("span", "dc-hash", "#"), h("b", "", c.store), h("span", "dc-topic", t(`demo.dc_topic_${c.role}`)));
    const chat = h("div", "dc-chat");
    const live = c.messages.slice(-200);
    const earlier = c.total - live.filter(m => !m.gone).length;
    if (earlier > 0) chat.append(h("div", "dc-day", t("demo.earlier", {n: earlier})));
    if (!live.length) chat.append(h("p", "dc-empty", t("demo.no_messages")));
    else chat.append(h("div", "dc-day", t("demo.today")));
    let last = null;
    for (const m of live) {
      const at = when(c, m);
      // Discord groups a run of messages by one author within minutes.
      const grouped = last && at && last - at > -7 * 60 * 1000;
      const row = h("div", "dc-msg" + (grouped ? " grouped" : "") + (m.gone ? " gone" : "") + (m.fresh ? " fresh" : "") + (service(m) ? " service" : ""));
      if (!grouped) {
        row.append(h("div", "dc-ava", "🐦"));
        const top = h("div", "dc-top");
        top.append(h("span", "dc-name", "pigeonhole"), h("span", "dc-tag", "APP"));
        if (at) top.append(h("span", "dc-time", t("demo.today_at", {time: hhmm(at)})));
        row.append(top);
      }
      const body = h("div", "dc-body");
      if (m.kind === "text") body.append(h("div", "dc-text cjk", payload(m)));
      else if (m.kind === "other") body.append(h("div", "dc-text", t("demo.tg_service")));
      else {
        const card = h("div", "dc-file");
        card.append(svgIcon(ICON_DOC, "doc"));
        const col = h("div");
        col.append(h("div", "dc-file-name", m.name), h("div", "dc-file-size", bytes(m.size)));
        card.append(col);
        body.append(card);
      }
      if (m.log) body.append(h("div", "post-log", logLabel(m.log)));
      row.append(body);
      chat.append(row);
      last = at;
    }
    const input = h("div", "dc-input", t("demo.dc_message", {store: c.store}));
    app.append(head, chat, input);
    requestAnimationFrame(() => { chat.scrollTop = chat.scrollHeight; });
    return app;
  }

  // The log: entries, each opening into its commands.
  function summary(cmd) {
    const [verb, f] = Object.entries(cmd.command || {})[0] || ["?", {}];
    const parts = [];
    if (f && typeof f === "object") {
      if (f.key) parts.push(`${f.bucket ? f.bucket + "/" : ""}${f.key}`);
      else if (f.name && cmd.namespace === "s3") parts.push(f.name);
      if (f.dst_key || f.to) parts.push(`→ ${f.dst_key || f.to}`);
      if (f.size != null) parts.push(bytes(+f.size));
      if (f.chunk_id != null) parts.push(t("demo.chunk_n", {n: f.chunk_id}));
      if (f.logical_size != null) parts.push(bytes(+f.logical_size));
      if (Array.isArray(f.chunk_ids)) parts.push(t("demo.chunks_list", {ids: f.chunk_ids.join(", ")}));
      if (Array.isArray(f.replicas)) parts.push(t("demo.replicas", {stores: f.replicas.map(r => r.instance).join(", ")}));
      if (Array.isArray(f.instances)) parts.push(f.instances.map(i => i[0]).join(", "));
    }
    return {verb, fields: parts.join(", ")};
  }

  function renderLog(body) {
    const v = state.log;
    body.replaceChildren();
    // Two views of one log: its entries decoded, or the channel's posts.
    const tabs = h("div", "tabs");
    tabs.setAttribute("role", "tablist");
    for (const view of ["readable", "channel"]) {
      const b = h("button", "tab", t(`demo.log_view_${view}`));
      b.type = "button";
      b.setAttribute("role", "tab");
      b.setAttribute("aria-selected", String(state.logView === view));
      b.addEventListener("click", () => { state.logView = view; renderPanel(); });
      tabs.append(b);
    }
    body.append(tabs);
    body.append(h("p", "log-note", t("demo.log_note")));
    const logChannel = state.channels.find(x => x.role === "log");
    if (state.logView === "channel") {
      if (logChannel) body.append(feed(logChannel));
      return;
    }
    // Position → the post holding it.
    const postOf = new Map();
    for (const m of logChannel?.messages || []) {
      const l = m.log;
      if (l?.what === "batch" && l.first != null) for (let p = l.first; p <= l.last; p++) postOf.set(p, m.id);
    }
    if (!v) { body.append(h("p", "empty", t("try.loading"))); return; }
    const head = h("div", "log-head");
    head.append(h("span", "", t("demo.log_head", {n: v.head})));
    head.append(h("span", "", v.base ? t("demo.log_base", {n: v.base.at}) : t("demo.log_nobase")));
    body.append(head);
    const list = h("ol", "entries");
    for (const e of [...v.entries].reverse()) {
      const li = h("li", "entry" + (v.base && e.position === v.base.at + 1 ? " base" : ""));
      const btn = h("button");
      btn.type = "button";
      btn.setAttribute("aria-expanded", "false");
      const what = e.kind === "commands"
        ? e.commands.map(c => `${c.namespace} ${summary(c).verb}`).join("; ")
        : t(`demo.entry_${e.kind}`);
      const post = postOf.get(e.position);
      const where = post != null ? t("demo.in_post", {id: String(post).length > 8 ? `…${String(post).slice(-6)}` : post}) : t("demo.term", {n: e.term});
      btn.append(h("span", "pos", String(e.position)), h("span", "what", what), h("span", "term", where));
      li.append(btn);
      const cmds = h("ul", "cmds");
      cmds.hidden = true;
      for (const c of e.commands) {
        const row = h("li", "cmd");
        const sm = summary(c);
        row.append(h("span", "ns", c.namespace), h("span", "verb", sm.verb));
        if (sm.fields) row.append(h("span", "fields", sm.fields));
        const raw = h("details");
        raw.append(h("summary", "", t("demo.raw")), h("pre", "", JSON.stringify(c.command, null, 2)));
        row.append(raw);
        cmds.append(row);
      }
      if (!e.commands.length) cmds.append(h("li", "cmd", t(`demo.entry_${e.kind}_long`)));
      btn.addEventListener("click", () => {
        cmds.hidden = !cmds.hidden;
        btn.setAttribute("aria-expanded", String(!cmds.hidden));
      });
      li.append(cmds);
      list.append(li);
    }
    body.append(list);
  }

  // You: uploads and the bucket.
  function wireClient() {
    const drop = $("drop");
    const put = (name, buf) => op({op: "put", key: name}, buf);
    $("file").addEventListener("change", async ev => {
      for (const f of ev.target.files) {
        if (f.size > 64 << 20) { caption(esc(t("try.too_big"))); continue; }
        await put(f.name, await f.arrayBuffer());
      }
      ev.target.value = "";
    });
    drop.addEventListener("dragover", e => { e.preventDefault(); drop.classList.add("over"); });
    drop.addEventListener("dragleave", () => drop.classList.remove("over"));
    drop.addEventListener("drop", async e => {
      e.preventDefault();
      drop.classList.remove("over");
      for (const f of e.dataTransfer.files) if (f.size <= 64 << 20) await put(f.name, await f.arrayBuffer());
    });
    for (const b of document.querySelectorAll("[data-sample]")) b.addEventListener("click", () => {
      // A new object each time: photo-1.jpg, photo-2.jpg… (an upload under
      // a name already there overwrites it, as S3 does).
      const fresh = (base, ext) => {
        let i = 1;
        while (state.objects.some(([k]) => k === `${base}-${i}${ext}`)) i++;
        return `${base}-${i}${ext}`;
      };
      if (b.dataset.sample === "photo") {
        const buf = new Uint8Array(7 << 20);
        for (let i = 0; i < buf.length; i += 65536) crypto.getRandomValues(buf.subarray(i, i + 65536));
        put(fresh("photo", ".jpg"), buf.buffer);
      } else {
        const text = t("demo.sample_text") + "\n";
        put(fresh("notes", ".md"), new TextEncoder().encode(text.repeat(Math.ceil((3 << 20) / text.length))).buffer);
      }
    });
  }

  function renderObjects() {
    const rows = $("objects");
    if (!rows) return;
    rows.replaceChildren();
    if (!state.objects.length) {
      const tr = h("tr");
      tr.append(h("td", "empty", t("try.no_objects")));
      rows.append(tr);
    }
    for (const [key, size] of state.objects) {
      const tr = h("tr");
      tr.append(h("td", "key", key), h("td", "n", bytes(size)));
      const acts = h("td", "acts");
      const btn = (label, fn) => {
        const b = h("button", "mini", label);
        b.type = "button";
        b.addEventListener("click", fn);
        acts.append(b);
      };
      btn(t("try.read"), () => {
        const len = Math.min(size, 100_000), first = Math.floor(Math.random() * Math.max(1, size - len));
        op({op: "get", key, range: [first, first + len - 1]}).then(r => {
          if (r && !r.error) caption(t("demo.read_done", {len: bytes(r.len), key: esc(key)}));
        });
      });
      btn(t("try.copy"), () => op({op: "copy", key, to: key.replace(/(\.[^.]*)?$/, m => `-copy${m}`)}));
      btn(t("try.delete"), () => op({op: "delete", key}));
      tr.append(acts);
      rows.append(tr);
    }
  }

  // The loft: counters, the config, repair and the sweeper.
  function wireNode() {
    const counters = $("counters");
    const counter = (n, label) => {
      const d = h("div", "counter");
      d.append(h("b", "", String(n)), h("span", "", label));
      counters.append(d);
    };
    counter(state.objects.length, t("demo.c_objects"));
    counter(state.channels.filter(c => c.role === "data").length, t("demo.c_channels"));
    counter(state.log?.head ?? "…", t("demo.c_head"));
    counter(state.log?.base?.at ?? 0, t("demo.c_base"));
    const editor = $("config-text");
    editor.value = state.config;
    $("config-reset").addEventListener("click", () => { editor.value = state.demo; $("config-error").textContent = ""; });
    $("config-apply").addEventListener("click", () => restart(editor.value));
    editor.addEventListener("keydown", e => { if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); restart(editor.value); } });
    $("erase").addEventListener("click", async () => {
      if (!confirm(t("try.erase_confirm"))) return;
      await call({op: "reset"});
      location.reload();
    });
    document.querySelector('[data-act="repair"]').addEventListener("click", async () => {
      const r = await op({op: "repair"});
      if (r && !r.error) caption(t("say.repair", {n: r.repaired}));
    });
    document.querySelector('[data-act="sweep"]').addEventListener("click", async () => {
      const r = await op({op: "sweep"});
      if (r && !r.error) caption(t("say.sweep", {n: r.deleted}));
    });
    if (!state.log) loadLog().then(() => { if (state.open === "loft") renderPanel(); });
  }

  async function restart(text) {
    $("config-error").textContent = "";
    setBusy(true);
    const data = await call({op: "restart", config: text});
    if (data.reply?.error) {
      $("config-error").textContent = data.reply.error;
      setBusy(false);
      return;
    }
    state.config = text;
    await boot(data.reply, data.events);
    caption(t("say.restart", {n: data.reply.replayed}));
    setBusy(false);
  }

  // ------------------------------------------------------------ start
  async function boot(reply, events) {
    await loadChannels();
    await refreshObjects();
    draw();
    apply(events || []);
    loadLog();
  }

  started.then(async data => {
    if (data.reply?.error) {
      $("status").textContent = data.reply.error === "other-tab" ? t("try.other_tab") : t("try.error", {error: data.reply.error});
      return;
    }
    state.demo = data.reply.demo;
    state.config = data.reply.config;
    state.persistent = data.reply.persistent;
    const t0 = performance.now();
    const booted = await call({op: "open"});
    if (booted.reply?.error) { $("status").textContent = t("try.error", {error: booted.reply.error}); return; }
    await boot(booted.reply);
    $("status").textContent = `${t("try.ready", {ms: Math.round(performance.now() - t0)})} ${state.persistent ? t("try.kept") : t("try.not_kept")}${data.reply.why ? ` (${data.reply.why})` : ""}`;
    caption(data.reply.saved ? t("say.welcome_back", {n: booted.reply.replayed}) : t("demo.first_caption"));
  });
})();
