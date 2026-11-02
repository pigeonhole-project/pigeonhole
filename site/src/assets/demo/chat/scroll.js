// Where a channel's chat is scrolled to, as a messenger keeps it: at the
// newest post until the reader scrolls or clicks in it, then where they
// left it, whatever came in or fell off since, with how many posts came in
// below (Telegram's round button, Discord's bar); back at the bottom, or
// taken there, it follows the newest again.
import {button, h, icon} from "../../lib/dom.js";
import {t} from "../../lib/i18n.js";

const ICON_CHEVRON = "M6 9l6 6 6-6";

// Per channel the reader holds in place: the post at the top of the view
// (`data-id`), how far below the view's top, and the newest post they saw.
const places = new Map();

// A channel opened anew: at its newest post again.
export const forgetPlaces = () => places.clear();

const posts = chat => [...chat.querySelectorAll("[data-id]")];
const byId = (chat, id) => posts(chat).find(el => el.dataset.id === id);
const atBottom = chat => chat.scrollHeight - chat.scrollTop - chat.clientHeight < 2;

// The chat `chat` of store `store` (null: a recording, always at its
// newest), drawn into `box` (its frame, which holds the new posts' count)
// in app `app` ("tg", "dc").
export function scrolled(chat, box, store, app) {
  const below =
    store != null &&
    newBelow(box, app, () => {
      places.delete(store);
      // At once: a smooth scroll's steps would be the reader's.
      chat.scrollTop = chat.scrollHeight;
    });
  requestAnimationFrame(() => {
    const place = store != null && places.get(store);
    const anchor = place && byId(chat, place.id);
    if (anchor) chat.scrollTop += anchor.getBoundingClientRect().top - chat.getBoundingClientRect().top - place.offset;
    else chat.scrollTop = chat.scrollHeight;
    if (store == null) return;
    const show = () => below(places.has(store) ? unseen(chat, places.get(store)) : 0);
    show();
    // The scroll just made is not the reader's.
    const ours = chat.scrollTop;
    chat.addEventListener(
      "scroll",
      () => {
        if (Math.abs(chat.scrollTop - ours) < 1) return;
        if (atBottom(chat)) places.delete(store);
        else hold(chat, store);
        show();
      },
      {passive: true},
    );
    chat.addEventListener("click", () => {
      hold(chat, store);
      show();
    });
  });
}

// The reader holds the chat where it is now. Seen: every post there was
// when they took hold (they were at the newest), and any they scroll to.
function hold(chat, store) {
  const top = chat.getBoundingClientRect().top,
    bottom = chat.getBoundingClientRect().bottom;
  const all = posts(chat);
  const first = all.find(el => el.getBoundingClientRect().bottom > top);
  if (!first) return;
  const last = all.findLast(el => el.getBoundingClientRect().top < bottom);
  const was = places.get(store)?.seen;
  const seen = !places.has(store) ? all.at(-1).dataset.id : all.indexOf(byId(chat, was)) > all.indexOf(last) ? was : last?.dataset.id;
  places.set(store, {id: first.dataset.id, offset: first.getBoundingClientRect().top - top, seen});
}

// The posts below the newest one the reader saw.
function unseen(chat, place) {
  const all = posts(chat);
  const i = all.indexOf(byId(chat, place.seen));
  return i < 0 ? 0 : all.length - 1 - i;
}

// How many posts came in below, as the app shows it; a click takes the
// reader there. Returns show(n): hidden at 0.
function newBelow(box, app, onJump) {
  const el = app === "dc" ? discordBar(onJump) : telegramButton(onJump);
  box.append(el);
  const show = n => {
    el.hidden = n === 0;
    el.querySelector(".chat-new-n").textContent = app === "dc" ? t("demo.dc_new", {n}) : String(n);
    el.setAttribute("aria-label", t("demo.new_below", {n}));
  };
  show(0);
  return show;
}

// Telegram: a round button with a chevron, the count on it.
function telegramButton(onJump) {
  const b = button("chat-new tg-new", null, onJump);
  b.append(icon(ICON_CHEVRON, "ic"), h("span", "chat-new-n"));
  return b;
}

// Discord: a bar over the input, the count and "Jump to present".
function discordBar(onJump) {
  const bar = button("chat-new dc-new", null, onJump);
  bar.append(h("span", "chat-new-n"), h("b", "", t("demo.dc_jump")));
  return bar;
}
