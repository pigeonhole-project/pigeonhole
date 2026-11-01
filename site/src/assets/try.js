// pigeonhole live: the wasm build runs in a worker (web-worker.js); every
// call's log events are drawn on the stage as they would be from a server.
(function () {
  const root = document.getElementById("live");
  if (!root) return;
  const stage = makeStage(root);
  const status = document.getElementById("status");
  const rows = document.getElementById("obj-rows");
  const buttons = [...document.querySelectorAll(".toolbar button, .toolbar input, .config-actions button")];
  const worker = new Worker(window.ASSETS + "web-worker.js" + (document.currentScript?.src.match(/\?v=\w+/)?.[0] || ""));
  const waiting = new Map();
  let nextId = 1, busy = Promise.resolve(), t0 = performance.now();

  const setBusy = on => buttons.forEach(b => (b.disabled = on));
  setBusy(true);

  worker.onmessage = ({data}) => {
    const done = waiting.get(data.id);
    waiting.delete(data.id);
    if (done) done(data);
  };
  // The module is up (ph_open): show its config, then boot the node on it.
  const editor = document.getElementById("config-text");
  const configError = document.getElementById("config-error");
  let demoConfig = "";
  waiting.set(0, async data => {
    if (data.reply?.error) { status.textContent = t("try.error", {error: data.reply.error}); return; }
    demoConfig = data.reply.demo;
    editor.value = data.reply.config;
    const booted = await call({op: "open"});
    if (booted.reply?.error) { status.textContent = t("try.error", {error: booted.reply.error}); return; }
    stage.setStores(booted.reply.stores);
    const where = t(data.reply.persistent ? "try.kept" : "try.not_kept");
    status.textContent = `${t("try.ready", {ms: Math.round(performance.now() - t0)})} ${where}`;
    stage.termLine("out", t("try.booted"), `${Math.round(booted.ms)} ms`);
    if (data.reply.saved) {
      stage.termLine("out", t("try.welcome_back", {n: booted.reply.replayed}), null);
      stage.narrate(t("say.welcome_back", {n: booted.reply.replayed}));
    }
    await showSteps(toSteps(booted.events), true);
    await refresh();
    setBusy(false);
  });

  // A new pigeonhole.toml is a restart: the tablet replays its log.
  async function restart() {
    configError.textContent = "";
    setBusy(true);
    stage.termLine("cmd", t("try.restart_cmd"), null);
    const data = await call({op: "restart", config: editor.value});
    if (data.reply?.error) {
      configError.textContent = data.reply.error;
      stage.termLine("out err", t("try.config_refused"), `${Math.round(data.ms)} ms`);
    } else {
      stage.setStores(data.reply.stores);
      await showSteps(toSteps(data.events), true);
      stage.termLine("out", t("try.out.restart", {n: data.reply.replayed}), `${Math.round(data.ms)} ms`);
      stage.narrate(t("say.restart", {n: data.reply.replayed}));
      await refresh();
    }
    setBusy(false);
  }
  document.getElementById("config-apply").addEventListener("click", restart);
  document.getElementById("config-reset").addEventListener("click", () => { editor.value = demoConfig; configError.textContent = ""; });
  document.getElementById("erase").addEventListener("click", async () => {
    if (!confirm(t("try.erase_confirm"))) return;
    setBusy(true);
    await call({op: "reset"});
    location.reload();
  });
  editor.addEventListener("keydown", ev => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === "Enter") { ev.preventDefault(); restart(); }
  });

  // One call at a time: the module is single-threaded.
  function call(request, body) {
    const run = busy.then(() => new Promise(res => {
      const id = nextId++;
      waiting.set(id, res);
      worker.postMessage({id, ...request, body}, body ? [body] : []);
    }));
    busy = run.catch(() => {});
    return run;
  }

  const fast = ms => new Promise(r => setTimeout(r, ms / 2));
  async function showSteps(steps, quiet) {
    for (const e of steps) await stage.draw(e, quiet ? () => Promise.resolve() : fast);
  }

  async function op(label, request, body) {
    setBusy(true);
    stage.termLine("cmd", label, null);
    const data = await call(request, body);
    const steps = toSteps(data.events);
    const reads = steps.filter(e => e.type === "blob" && e.op === "get");
    await showSteps(steps);
    if (data.reply?.error) {
      stage.termLine("out err", data.reply.error, `${Math.round(data.ms)} ms`);
    } else {
      stage.termLine("out", summary(request, data.reply, reads), `${Math.round(data.ms)} ms`);
      stage.narrate(say(request, data.reply, steps, reads));
    }
    await refresh();
    setBusy(false);
  }

  function summary(r, reply, reads) {
    if (r.op === "get") return t("try.out.get", {len: bytes(reply.len), reads: reads.length});
    if (r.op === "repair") return t("try.out.repair", {n: reply.repaired, queued: reply.queued});
    if (r.op === "sweep") return t("try.out.sweep", {n: reply.deleted});
    return "ok";
  }

  function say(r, reply, steps, reads) {
    if (r.op === "get") {
      const got = reads.reduce((a, e) => a + (e.bytes || 0), 0);
      return t("say.get", {key: esc(r.key), len: bytes(reply.len), reads: reads.length, fetched: bytes(got)});
    }
    if (r.op === "repair") return t("say.repair", {n: reply.repaired});
    if (r.op === "sweep") return t("say.sweep", {n: reply.deleted});
    return sentence(steps);
  }

  async function refresh() {
    const {reply} = await call({op: "list"});
    rows.replaceChildren();
    const objects = reply?.objects || [];
    if (!objects.length) {
      const tr = h("tr");
      tr.append(h("td", "muted", t("try.no_objects")));
      rows.append(tr);
    }
    for (const [key, size] of objects) {
      const tr = h("tr");
      tr.append(h("td", "key", key), h("td", "n", bytes(size)));
      const acts = h("td", "acts");
      const btn = (label, fn) => {
        const b = h("button", "mini", label);
        b.type = "button";
        b.addEventListener("click", fn);
        buttons.push(b);
        acts.append(b);
      };
      btn(t("try.read"), () => {
        const len = Math.min(size, 100_000), first = Math.floor(Math.random() * Math.max(1, size - len));
        op(`GET demo/${key}  Range: bytes=${first}-${first + len - 1}`, {op: "get", key, range: [first, first + len - 1]});
      });
      btn(t("try.copy"), () => {
        const to = key.replace(/(\.[^.]*)?$/, m => `-copy${m}`);
        op(`COPY demo/${key} → demo/${to}`, {op: "copy", key, to});
      });
      btn(t("try.delete"), () => op(`DELETE demo/${key}`, {op: "delete", key}));
      tr.append(acts);
      rows.append(tr);
    }
  }

  function put(name, buf) {
    return op(`PUT demo/${name}  (${bytes(buf.byteLength)})`, {op: "put", key: name}, buf);
  }

  document.getElementById("file").addEventListener("change", async ev => {
    const f = ev.target.files[0];
    ev.target.value = "";
    if (!f) return;
    if (f.size > 64 << 20) { status.textContent = t("try.too_big"); return; }
    put(f.name, await f.arrayBuffer());
  });
  for (const b of document.querySelectorAll("[data-sample]")) b.addEventListener("click", () => {
    if (b.dataset.sample === "photo") {
      const buf = new Uint8Array(7 << 20);
      for (let i = 0; i < buf.length; i += 65536) crypto.getRandomValues(buf.subarray(i, i + 65536));
      put("photo.jpg", buf.buffer);
    } else {
      const text = (document.querySelector(".lede")?.textContent || "pigeonhole ") + "\n";
      put("notes.md", new TextEncoder().encode(text.repeat(Math.ceil((3 << 20) / text.length))).buffer);
    }
  });
  document.querySelector('[data-act="repair"]').addEventListener("click", () => op("repair --once", {op: "repair"}));
  document.querySelector('[data-act="sweep"]').addEventListener("click", () => op("sweep --once", {op: "sweep"}));
})();
