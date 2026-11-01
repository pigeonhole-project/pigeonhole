// The home page's stage: a terminal, two channels, the metadata log and a
// sentence, drawn from a recorded trace (site/record.py). The channels are
// the demo's chat views (demo/chat), named by the recording's stores:
// "telegram" and "discord"; the log's entries open into their commands as
// on the demo's log panel (demo/log-view.js).
//
// trace: {steps, log: the entries with their commands}; `at(t)`: the Date
// of a step's time t (ms into the recording).
import {chatView} from "./demo/chat/index.js";
import {entryView} from "./demo/log-view.js";
import {button, h} from "./lib/dom.js";
import {bytes, t, tHtml} from "./lib/i18n.js";

// Both recorded channels hold the chunks under one name, as a deployment's would.
const CHANNEL = "blobs";

export function makeStage(stage, {entries, at}) {
  const $ = role => stage.querySelector(`[data-role="${role}"]`);
  const term = $("term"),
    log = $("log"),
    opened = $("entry"),
    say = $("say");
  const lanes = Object.fromEntries(["telegram", "discord"].map(kind => [kind, {el: $(kind), channel: null}]));
  let chip = null; // the log entry being committed

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

  // A tag with the payload's size, from the terminal to the channel.
  function fly(kind, label) {
    const from = term.getBoundingClientRect(),
      to = lanes[kind].el.getBoundingClientRect(),
      box = stage.getBoundingClientRect();
    const tag = h("div", "flyer", label);
    tag.style.background = kind === "telegram" ? "var(--tg)" : "var(--dc)";
    tag.style.left = `${from.right - box.left - 120}px`;
    tag.style.top = `${from.top - box.top + from.height * 0.55}px`;
    stage.append(tag);
    requestAnimationFrame(() =>
      requestAnimationFrame(() => {
        tag.style.transform = `translate(${to.left - from.right + 140}px, ${to.bottom - from.top - from.height * 0.55 - 40}px)`;
        tag.style.opacity = "0.15";
      }),
    );
    setTimeout(() => tag.remove(), 600);
  }

  const drawLane = lane => lane.el.replaceChildren(chatView(lane.channel));

  // A post in a channel, the newest one marked.
  function post(e) {
    const {channel} = lanes[e.store];
    for (const m of channel.messages) m.fresh = false;
    channel.messages.push({id: e.msg, kind: e.kind, name: e.name, size: e.bytes, preview: e.preview, fresh: true, at: at(e.t)});
    channel.total++;
    drawLane(lanes[e.store]);
  }

  // A log batch: a chip for its entries; clicked, they open under the row.
  function logEntry(e) {
    const mine = entries.filter(x => x.position >= e.first && x.position <= e.last);
    const b = button("log-chip pop", null, () => {
      const open = b.getAttribute("aria-expanded") !== "true";
      for (const c of log.querySelectorAll(".log-chip")) c.setAttribute("aria-expanded", "false");
      b.setAttribute("aria-expanded", String(open));
      opened.replaceChildren(...(open ? mine.map(x => entryView(x, {open: true})) : []));
      opened.hidden = !open;
    });
    b.setAttribute("aria-expanded", "false");
    b.append(h("b", "", t("position", {n: e.first})), h("span", "", bytes(e.bytes || 0)));
    log.append(b);
    chip = b;
    log.scrollLeft = log.scrollWidth;
  }
  function committed(e) {
    if (chip) chip.querySelector("span").textContent += ` · ${t("commands", {n: e.entries})}`;
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
    opened.replaceChildren();
    opened.hidden = true;
    say.replaceChildren();
    for (const [kind, lane] of Object.entries(lanes)) {
      lane.channel = {store: CHANNEL, kind, role: "data", total: 0, messages: []};
      drawLane(lane);
    }
    for (const f of stage.querySelectorAll(".flyer")) f.remove();
    chip = null;
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
