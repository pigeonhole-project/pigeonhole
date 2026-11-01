// The home page's stage: a terminal, two channels, the metadata log and a
// sentence, drawn from a recorded trace's steps (site/record.py), in
// panes named by the recording's stores: "telegram" and "discord".
import {h} from "./lib/dom.js";
import {bytes, t, tHtml} from "./lib/i18n.js";

export function makeStage(stage) {
  const $ = role => stage.querySelector(`[data-role="${role}"]`);
  const term = $("term"),
    log = $("log"),
    say = $("say");
  const lanes = {telegram: $("telegram"), discord: $("discord")};
  for (const lane of Object.values(lanes)) lane.dataset.empty = lane.querySelector(".empty")?.textContent || "";
  let entry = null; // the log entry being committed

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

  // A chip with the payload's size, from the terminal to the channel.
  function fly(lane, label) {
    const from = term.getBoundingClientRect(),
      to = lanes[lane].getBoundingClientRect(),
      box = stage.getBoundingClientRect();
    const chip = h("div", "flyer", label);
    chip.style.background = lane === "telegram" ? "var(--tg)" : "var(--dc)";
    chip.style.left = `${from.right - box.left - 120}px`;
    chip.style.top = `${from.top - box.top + from.height * 0.55}px`;
    stage.append(chip);
    requestAnimationFrame(() =>
      requestAnimationFrame(() => {
        chip.style.transform = `translate(${to.left - from.right + 140}px, ${to.bottom - from.top - from.height * 0.55 - 40}px)`;
        chip.style.opacity = "0.15";
      }),
    );
    setTimeout(() => chip.remove(), 600);
  }

  // A post's text: the payload as text, or the file's name; its size and id.
  function postBody(e) {
    const short = String(e.name || "").replace(/^ph-(\w{6})\w+(\w{4})\.bin$/, "ph-$1…$2.bin");
    // Discord ids are snowflakes (time in the high bits): the tail tells them apart.
    const id = String(e.msg ?? "");
    const meta = `${bytes(e.bytes)} · ${t("msg", {n: id.length > 8 ? `…${id.slice(-6)}` : id})}`;
    const col = h("div");
    col.style.minWidth = "0";
    if (e.kind === "text") col.append(h("div", "ttext", e.preview ? `${e.preview}…` : short), h("div", "fmeta", `${t("as_text")} · ${meta}`));
    else col.append(h("div", "fname", short), h("div", "fmeta", meta));
    return col;
  }

  function telegramPost(e) {
    const node = h("div", `bubble pop${e.kind === "text" ? " text" : ""}`);
    if (e.kind !== "text") node.append(h("div", "file", "⬇"));
    node.append(postBody(e));
    return node;
  }

  function discordPost(e) {
    const name = h("div", "name", "pigeonhole");
    name.append(h("small", "", "BOT"));
    const col = h("div");
    if (e.kind === "text") col.append(name, postBody(e));
    else {
      const att = h("div", "att");
      att.append(h("span", "ic", "🗎"), postBody(e));
      col.append(name, att);
    }
    const node = h("div", "post pop");
    node.append(h("div", "who", "P"), col);
    return node;
  }

  function post(e) {
    const lane = lanes[e.store];
    lane.querySelector(".empty")?.remove();
    lane.append(e.store === "telegram" ? telegramPost(e) : discordPost(e));
    lane.scrollTop = lane.scrollHeight;
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

  // A say.* string ([key, vars]; the values are escaped), fading in.
  function narrate(sentence) {
    if (!sentence) return;
    const s = h("span", "fade");
    s.innerHTML = tHtml(...sentence);
    say.replaceChildren(s);
  }

  function reset() {
    term.replaceChildren();
    log.replaceChildren();
    say.replaceChildren();
    for (const lane of Object.values(lanes)) lane.replaceChildren(h("p", "empty", lane.dataset.empty));
    for (const f of stage.querySelectorAll(".flyer")) f.remove();
    entry = null;
  }

  // Draws one step; resolves after its animation, `wait(ms)` paces it.
  async function draw(e, wait) {
    if (e.type === "blob" && e.op === "put" && lanes[e.store]) {
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

  return {termLine, narrate, reset, draw};
}
