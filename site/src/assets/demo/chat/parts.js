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

// The posts opened (`store/id`): open again when the chat is drawn anew.
const opened = new Set();

// An outbox's post: the frame it holds, a line per envelope, and every
// field behind a click on the post. `key`: the post's, `store/id`.
export function frameNotes(post, frame, key) {
  if (!frame) return [];
  const out = [frame.head && h("div", "post-log", frame.head), ...frame.lines.map(l => h("div", "post-log frame-line", l))].filter(Boolean);
  if (!frame.fields.length) return out;
  const more = h("details", "frame-fields");
  more.open = opened.has(key);
  more.addEventListener("toggle", () => (more.open ? opened.add(key) : opened.delete(key)));
  more.append(h("summary", "", t("frame.fields")));
  for (const fields of frame.fields) {
    const table = h("dl");
    for (const [k, v] of fields) table.append(h("dt", "", k), h("dd", "", Array.isArray(v) ? v.join("–") : String(v)));
    more.append(table);
  }
  // The whole post opens it, not its summary alone.
  post.classList.add("openable");
  post.addEventListener("click", e => {
    if (e.target.closest("summary, dl")) return;
    more.open = !more.open;
  });
  out.push(more);
  return out;
}

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

// Where the reader is in each channel's chat they scrolled or clicked in:
// the post at the top of the view (`data-id`), how far below its top.
const places = new Map();

// A channel's chat once it is on the page: at its newest post until the
// reader scrolls it or clicks in it, then where they left it, whatever came
// in or fell off since. `store` null: at its newest post always.
export function scrolled(chat, store) {
  requestAnimationFrame(() => {
    const place = store != null && places.get(store);
    const anchor = place && [...chat.querySelectorAll("[data-id]")].find(el => el.dataset.id === place.id);
    if (anchor) chat.scrollTop += anchor.getBoundingClientRect().top - chat.getBoundingClientRect().top - place.offset;
    else chat.scrollTop = chat.scrollHeight;
    if (store == null) return;
    // The scroll just made is not the reader's.
    const ours = chat.scrollTop;
    chat.addEventListener("scroll", () => Math.abs(chat.scrollTop - ours) >= 1 && remember(chat, store), {passive: true});
    chat.addEventListener("click", () => remember(chat, store));
  });
}

function remember(chat, store) {
  const top = chat.getBoundingClientRect().top;
  const first = [...chat.querySelectorAll("[data-id]")].find(el => el.getBoundingClientRect().bottom > top);
  if (first) places.set(store, {id: first.dataset.id, offset: first.getBoundingClientRect().top - top});
}

// A channel opened anew: at its newest post again.
export const forgetPlaces = () => places.clear();

export const flags = m => (m.fresh ? " fresh" : "") + (m.quiet ? " service" : "");
