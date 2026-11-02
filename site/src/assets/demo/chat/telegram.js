// A channel as Telegram shows it: a broadcast channel, posts as bubbles.

import {h, icon} from "../../lib/dom.js";
import {bytes, t} from "../../lib/i18n.js";
import {chatStart, deletable, flags, frameNotes, hhmm, ICON_DOWN, ICON_EYE, ICON_PIN, notes, scrolled} from "./parts.js";

export function telegramView(c, model, onDelete, follow) {
  const head = h("div", "tg-head");
  const who = h("div");
  who.append(h("b", "", t("demo.tg_title", {store: c.store})), h("small", "", t("demo.tg_subs")));
  head.append(h("div", "tg-ava", "PB"), who);

  const chat = h("div", "tg-chat");
  chatStart(chat, model, "tg");
  for (const m of model.messages) chat.append(m.kind === "service" ? service(m) : post(c, m, onDelete));

  const app = h("div", "app tg");
  app.append(head);
  if (model.pins) app.append(pinBar(model.pinned));
  app.append(chat, h("div", "tg-input", t("demo.tg_broadcast")));
  scrolled(chat, follow ? null : c.store);
  return app;
}

// The pinned message, over the chat.
function pinBar(name) {
  const bar = h("div", "tg-pinbar"),
    col = h("div");
  col.append(h("b", "", t("demo.pinned_message")), h("div", "tg-pinbar-text", name || t("demo.channel_created")));
  bar.append(icon(ICON_PIN, "ic"), col);
  return bar;
}

// A service message (a pin): a pill in the middle, as Telegram draws it.
function service(m) {
  const line = h("div", "tg-line tg-service-line");
  line.dataset.id = m.id;
  line.append(h("div", "tg-day", t("demo.tg_pinned_service")));
  return line;
}

function post(c, m, onDelete) {
  const b = h("div", `tg-post${flags(m)}`);
  if (m.kind === "text") b.append(h("div", "tg-text cjk", m.text));
  else {
    const doc = h("div", "tg-doc"),
      ic = h("div", "tg-doc-ic"),
      col = h("div");
    ic.append(icon(ICON_DOWN, "ic"));
    col.append(h("div", "tg-doc-name", m.name), h("div", "tg-doc-size", bytes(m.size)));
    doc.append(ic, col);
    b.append(doc);
  }
  b.append(...notes(m), ...frameNotes(b, m.frame, `${c.store}/${m.id}`));
  // Pinned, one view (the bot's), the time.
  const foot = h("div", "tg-meta");
  if (m.pinned) foot.append(icon(ICON_PIN, "pin"));
  foot.append(icon(ICON_EYE, "eye"), "1");
  if (m.at) foot.append(` ${hhmm(m.at)}`);
  b.append(foot);
  const line = h("div", "tg-line");
  line.dataset.id = m.id;
  line.append(b);
  if (m.deletable) deletable(line, () => onDelete(m));
  return line;
}
