// The home page's stage: a terminal, two channels, the metadata log and a
// sentence, drawn from a recorded trace (site/record.py). The channels are
// the demo's chat views (demo/chat), named by the recording's stores:
// "telegram" and "discord"; the log, as on the demo's log panel
// (demo/log-view.js), in two views: its batches, opening into their
// entries and commands, or its channel's posts.
//
// entries: the log's entries with their commands; logChannel: its channel
// as the recording ended (the `channels` op's); logPosts: the ids of the
// posts the recording puts there; `at(t)`: the Date of a step's time t (ms
// into the recording).
import {chatView} from "./demo/chat/index.js";
import {entryView, logTabs} from "./demo/log-view.js";
import {button, h} from "./lib/dom.js";
import {bytes, t, tHtml} from "./lib/i18n.js";

// Both recorded channels hold the chunks under one name, as a deployment's would.
const CHANNEL = "blobs";

export function makeStage(stage, {entries, logChannel, logPosts, at}) {
  const $ = role => stage.querySelector(`[data-role="${role}"]`);
  const term = $("term"),
    log = $("log"),
    opened = $("entry"),
    say = $("say");
  const lanes = Object.fromEntries(["telegram", "discord"].map(kind => [kind, {el: $(kind), channel: null}]));
  // The log's channel: the posts before the recording at first, its own as they come.
  const logLane = {el: $("log-channel"), channel: null};
  let view = "readable";

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

  // The log in one view or the other.
  function showLog(v) {
    view = v;
    $("log-tabs").replaceChildren(logTabs(view, showLog, logChannel?.kind));
    log.hidden = view !== "readable";
    opened.hidden = view !== "readable" || !log.querySelector('[aria-expanded="true"]');
    logLane.el.hidden = view !== "channel";
    // Drawn while hidden, the chat could not scroll to its newest post.
    if (view === "channel" && logLane.channel) drawLane(logLane);
  }

  // A log batch: a chip for its entries (clicked, they open under the row),
  // and its post in the log's channel.
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
    b.append(h("b", "", t("position", {n: e.first})), h("span", "", `${bytes(e.bytes || 0)} · ${t("commands", {n: e.commands})}`));
    log.append(b);
    log.scrollLeft = log.scrollWidth;
    const m = logChannel?.messages.find(x => String(x.id) === e.msg);
    if (m) {
      for (const x of logLane.channel.messages) x.fresh = false;
      logLane.channel.messages.push({...m, fresh: true});
      logLane.channel.total++;
      drawLane(logLane);
    }
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
    if (logChannel) {
      const before = logChannel.messages.filter(m => !logPosts.has(String(m.id)));
      logLane.channel = {...logChannel, total: before.length, messages: before};
      drawLane(logLane);
    }
    showLog(view);
    for (const f of stage.querySelectorAll(".flyer")) f.remove();
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
    }
  }

  return {termLine, narrate, reset, draw};
}
