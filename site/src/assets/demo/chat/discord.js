// A channel as Discord shows it: a text channel, the bot's messages grouped.

import {h, icon} from "../../lib/dom.js";
import {bytes, t} from "../../lib/i18n.js";
import {chatStart, deletable, flags, hhmm, ICON_DOC, ICON_PIN, notes} from "./parts.js";
import {scrolled} from "./scroll.js";

// Discord runs one author's messages together within a few minutes.
const GROUP_MS = 7 * 60 * 1000;

export function discordView(c, model, onDelete, follow) {
  const head = h("div", "dc-head");
  head.append(h("span", "dc-hash", "#"), h("b", "", c.store), h("span", "dc-topic", t(`demo.dc_topic_${c.role}`)));
  if (model.pins) {
    const pins = h("span", "dc-pins");
    pins.title = t("demo.pinned_n", {n: model.pins});
    pins.append(icon(ICON_PIN, "ic"), String(model.pins));
    head.append(pins);
  }

  const chat = h("div", "dc-chat");
  chatStart(chat, model, "dc");
  let last = null;
  for (const m of model.messages) {
    const grouped = !!(last && m.at && m.at - last < GROUP_MS);
    chat.append(message(m, grouped, onDelete));
    last = m.at;
  }

  const app = h("div", "app dc");
  const box = h("div", "chat-box");
  box.append(chat);
  app.append(head, box, h("div", "dc-input", t("demo.dc_message", {store: c.store})));
  scrolled(chat, box, follow ? null : c.store, "dc");
  return app;
}

function message(m, grouped, onDelete) {
  const row = h("div", `dc-msg${grouped ? " grouped" : ""}${flags(m)}`);
  row.dataset.id = m.id;
  if (grouped) {
    if (m.at) row.append(h("span", "dc-gutter-time", hhmm(m.at)));
  } else {
    // The first of a run: the avatar, the bot's name and tag, the time.
    const top = h("div", "dc-top");
    top.append(h("span", "dc-name", "pigeonhole"), h("span", "dc-tag", "APP"));
    if (m.at) top.append(h("span", "dc-time", t("demo.today_at", {time: hhmm(m.at)})));
    row.append(h("div", "dc-ava", "🐦"), top);
  }
  const body = h("div", "dc-body");
  if (m.pinned) body.append(h("div", "dc-pinned", t("demo.pinned")));
  if (m.kind === "text") body.append(h("div", "dc-text cjk", m.text));
  else if (m.kind === "service") body.append(h("div", "dc-text", t("demo.channel_created")));
  else {
    const card = h("div", "dc-file"),
      col = h("div");
    col.append(h("div", "dc-file-name", m.name), h("div", "dc-file-size", bytes(m.size)));
    card.append(icon(ICON_DOC, "doc"), col);
    body.append(card);
  }
  body.append(...notes(m));
  row.append(body);
  if (m.deletable) deletable(row, () => onDelete(m));
  return row;
}
