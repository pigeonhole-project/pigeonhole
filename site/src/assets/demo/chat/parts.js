// What both chat views draw alike.
import {button, h, icon} from "../../lib/dom.js";
import {t} from "../../lib/i18n.js";

export const ICON_DOWN = "M12 4v11m0 0l-4.5-4.5M12 15l4.5-4.5M6 19h12";
export const ICON_DOC = "M7 3h7l5 5v13H7z M14 3v5h5";
export const ICON_EYE = "M2 12s3.6-6 10-6 10 6 10 6-3.6 6-10 6S2 12 2 12zm10 3a3 3 0 100-6 3 3 0 000 6z";
export const ICON_PIN = "M14 4l6 6-3 1-4 4 1 5-2 1-4-5-5 5M10 9l5 5";
const ICON_TRASH = "M5 7h14M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3";

export const hhmm = d => d.toLocaleTimeString(globalThis.LANG, {hour: "2-digit", minute: "2-digit"});

// The top of the chat: how many posts are not shown, then today's divider
// (or that there is nothing). `cls`: the app's prefix.
export function chatStart(chat, model, cls) {
  if (model.earlier > 0) chat.append(h("div", `${cls}-day`, t("demo.earlier", {n: model.earlier})));
  if (!model.messages.length) chat.append(h("p", `${cls}-empty`, t("demo.no_messages")));
  else chat.append(h("div", `${cls}-day`, t("demo.today")));
}

// The node's notes on a post (what it is to the log, an owner marker).
export const notes = m => m.notes.map(n => h("div", "post-log", n));

// A message anyone could delete, as in the app: a trash button on hover or
// focus, and the context menu (right click).
export function deletable(el, onDelete) {
  const b = button("msg-del", null, onDelete);
  b.setAttribute("aria-label", t("demo.delete"));
  b.title = t("demo.delete");
  b.append(icon(ICON_TRASH, "ic"));
  el.classList.add("deletable");
  el.append(b);
  el.addEventListener("contextmenu", e => {
    e.preventDefault();
    onDelete();
  });
}

// The chat scrolled to its newest post once it is on the page.
export function scrolledDown(chat) {
  requestAnimationFrame(() => {
    chat.scrollTop = chat.scrollHeight;
  });
}

export const flags = m => (m.gone ? " gone" : "") + (m.fresh ? " fresh" : "") + (m.quiet ? " service" : "");
