// Plays a trace recorded by site/record.py into the #replay stage:
// commands in the terminal, chunks as posts in the two channels, the
// metadata log as a row of entries, and a sentence on what just happened.
(function () {
  const stage = document.getElementById("replay");
  const trace = window.TRACE;
  if (!stage || !trace) return;
  const $ = role => stage.querySelector(`[data-role="${role}"]`);
  const term = $("term"), log = $("log"), say = $("say");
  const lanes = {telegram: $("telegram"), discord: $("discord")};
  const playBtn = stage.querySelector('[data-act="play"]');
  const speedBtn = stage.querySelector('[data-act="speed"]');

  const date = new Date(trace.recorded).toLocaleDateString(window.LANG, {day: "numeric", month: "long", year: "numeric"});
  $("meta").textContent = t("recorded", {commit: trace.commit, date});

  // ---------------------------------------------------------------- the script
  // One group per command: the command, what the server did, its output.
  const groups = [];
  for (const s of trace.steps) {
    if (s.type === "cmd") groups.push({cmd: s, events: [], out: null});
    else if (s.type === "out") groups.at(-1).out = s;
    else groups.at(-1).events.push(s);
  }

  const esc = v => String(v).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);
  function sentence(g) {
    const op = g.events.find(e => e.type === "s3");
    const puts = g.events.filter(e => e.type === "blob" && e.op === "put" && e.store === "telegram");
    if (!op) return /\bls\b/.test(g.cmd.text) ? t("say.ls") : "";
    const vars = {bucket: esc(op.bucket), key: esc(op.key || ""), n: op.extents};
    if (op.op === "PutObject") {
      const stored = puts.reduce((a, e) => a + e.bytes, 0);
      vars.size = bytes(op.size);
      vars.stored = bytes(stored);
      vars.chunks = t("chunks", {n: puts.length});
      return t(stored < op.size * 0.8 ? "say.PutObjectZ" : "say.PutObject", vars);
    }
    if (op.op === "CopyObject") return t("say.CopyObject", {...vars, chunks: t("chunks", {n: op.extents})});
    return t(`say.${op.op}`, vars);
  }

  // ---------------------------------------------------------------- timing
  let speed = 1, paused = false, run = 0, resume = null;
  function sleep(ms, id) {
    return new Promise(res => {
      const step = () => {
        if (id !== run) return;                  // restarted: drop this run
        if (paused) { resume = step; return; }
        res();
      };
      setTimeout(step, ms / speed);
    });
  }
  const alive = id => id === run;

  // ---------------------------------------------------------------- drawing
  const secs = ms => `+${(ms / 1000).toFixed(2)} s`;
  function termLine(cls, text, ts) {
    const row = h("div", `line ${cls}`);
    const body = h("span", "", text);
    row.append(body);
    if (ts != null) row.append(h("span", "ts", secs(ts)));
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
    lane.querySelector(".empty")?.remove();
    const short = e.name.replace(/^ph-(\w{6})\w+(\w{4})\.bin$/, "ph-$1…$2.bin");
    const meta = `${bytes(e.bytes)} · ${t("msg", {n: e.msg})}`;
    let node;
    if (e.store === "telegram") {
      node = h("div", "bubble pop");
      const f = h("div", "file", "⬇");
      const txt = h("div");
      txt.style.minWidth = "0";
      txt.append(h("div", "fname", short), h("div", "fmeta", meta));
      node.append(f, txt);
    } else {
      node = h("div", "post pop");
      const who = h("div", "who", "P");
      const col = h("div");
      const name = h("div", "name", "pigeonhole");
      name.append(h("small", "", "BOT"));
      const att = h("div", "att");
      const txt = h("div");
      txt.style.minWidth = "0";
      txt.append(h("div", "fname", short), h("div", "fmeta", meta));
      att.append(h("span", "ic", "🗎"), txt);
      col.append(name, att);
      node.append(who, col);
    }
    lane.append(node);
    lane.scrollTop = lane.scrollHeight;
  }

  let entry = null;
  function logEntry(e) {
    entry = h("div", "entry new");
    entry.append(h("b", "", t("position", {n: e.first})), h("span", "", bytes(e.bytes || 0)));
    log.append(entry);
    log.scrollLeft = log.scrollWidth;
  }
  function committed(e) {
    if (!entry) return;
    entry.querySelector("span").textContent += ` · ${t("commands", {n: e.entries})}`;
  }

  function narrate(html) {
    if (!html) return;
    const s = document.createElement("span");
    s.className = "fade";
    s.innerHTML = html;                          // our own strings; data in them is escaped
    say.replaceChildren(s);
  }

  // ---------------------------------------------------------------- the run
  function reset() {
    term.replaceChildren();
    log.replaceChildren();
    say.replaceChildren();
    for (const lane of Object.values(lanes)) lane.replaceChildren(h("p", "empty", lane.dataset.empty));
    stage.querySelectorAll(".flyer").forEach(f => f.remove());
    entry = null;
  }

  async function play() {
    const id = ++run;
    reset();
    for (const g of groups) {
      const line = termLine("cmd", "", g.cmd.t);
      const caret = h("span", "caret");
      line.append(caret);
      for (const ch of g.cmd.text) {
        caret.before(ch);
        await sleep(22, id);
      }
      caret.remove();
      await sleep(300, id);
      for (const e of g.events) {
        if (!alive(id)) return;
        if (e.type === "blob" && e.op === "put") {
          fly(e.store, bytes(e.bytes));
          await sleep(480, id);
          post(e);
          await sleep(120, id);
        } else if (e.type === "log") {
          logEntry(e);
          await sleep(350, id);
        } else if (e.type === "commit") {
          committed(e);
          await sleep(150, id);
        }
      }
      if (g.out && g.out.text) for (const l of g.out.text.split("\n")) termLine("out", l, null);
      narrate(sentence(g));
      await sleep(g.events.some(e => e.type === "s3") || /\bls\b/.test(g.cmd.text) ? 3200 : 900, id);
    }
    narrate(t("say.end"));
    await sleep(7000, id);
    if (alive(id)) play();
  }

  // ---------------------------------------------------------------- controls
  for (const lane of Object.values(lanes)) lane.dataset.empty = lane.querySelector(".empty")?.textContent || "";
  playBtn.addEventListener("click", () => {
    paused = !paused;
    playBtn.textContent = paused ? t("play") : t("pause");
    if (!paused && resume) { const r = resume; resume = null; r(); }
  });
  stage.querySelector('[data-act="restart"]').addEventListener("click", () => {
    paused = false; resume = null;
    playBtn.textContent = t("pause");
    play();
  });
  speedBtn.addEventListener("click", () => {
    speed = speed === 1 ? 2 : speed === 2 ? 0.5 : 1;
    speedBtn.textContent = `${speed}×`;
  });

  // Start when the stage scrolls into view; reduced motion starts paused.
  let started = false;
  new IntersectionObserver(es => {
    if (started || !es.some(e => e.isIntersecting)) return;
    started = true;
    if (matchMedia("(prefers-reduced-motion: reduce)").matches) {
      paused = true;
      playBtn.textContent = t("play");
    }
    play();
  }, {threshold: 0.25}).observe(stage);
})();
