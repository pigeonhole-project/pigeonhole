// The bots' budgets (op `budgets`): what each bot may still spend in a
// channel now, by class of call (send, read, delete), and its own across
// every channel. Pure: data in, a level or elements out.
//
// An entry: {service, bot, chat, classes: {send: B, read: B, delete: B},
// bot_total: B}; B: {tokens, capacity, per_sec, cooling_ms, waiting}.
import {h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";

const serviceOf = kind => (kind === "discord" ? "discord" : "telegram");

// Channel `c`'s entries: one per bot that posts or reads there.
export const budgetsOf = (c, budgets) => (budgets || []).filter(b => b.service === serviceOf(c.kind) && b.chat === String(c.scope));

// A bucket's state: "busy" while calls wait for it or it cools down after a
// rate limit, "low" under a third, else "ok".
const stateOf = b => (b.waiting > 0 || b.cooling_ms > 0 ? "busy" : b.tokens < b.capacity / 3 ? "low" : "ok");
const RANK = {ok: 0, low: 1, busy: 2};

// What the graph shows under a channel's sign: its sends, the worst of its
// bots, each bounded by the bot's own budget too. null: none known.
export function sendLevel(entries) {
  let worst = null;
  for (const e of entries) {
    for (const b of [e.classes?.send, e.bot_total].filter(Boolean)) {
      const level = {frac: b.capacity > 0 ? b.tokens / b.capacity : 0, state: stateOf(b)};
      if (!worst || RANK[level.state] > RANK[worst.state] || (level.state === worst.state && level.frac < worst.frac)) worst = level;
    }
  }
  return worst;
}

// A fake bot's name is its id; a real one's is its id already.
const botName = bot => String(bot).replace(/^fake-bot-/, "");

const fmt = n => (Number.isInteger(n) ? String(n) : n.toFixed(1));

// The panel's table: a row per bot and class, then the bot as a whole.
export function budgetTable(entries) {
  const box = h("div", "budgets");
  box.append(h("h3", "", t("budget.title")));
  if (!entries.length) {
    box.append(h("p", "note", t("budget.none")));
    return box;
  }
  const table = h("table", "budget-table");
  const head = h("tr");
  for (const k of ["bot", "class", "left", "rate", "waiting"]) head.append(h("th", "", t(`budget.${k}`)));
  table.append(head);
  for (const e of entries) {
    const rows = [...Object.entries(e.classes || {}), ["bot", e.bot_total]];
    rows.forEach(([cls, b], i) => {
      const tr = h("tr", `budget-${stateOf(b)}${cls === "bot" ? " budget-whole" : ""}`);
      tr.append(h("td", "", i === 0 ? botName(e.bot) : ""));
      tr.append(h("td", "", t(`budget.class_${cls}`)));
      const left = h("td", "budget-left");
      const bar = h("span", "budget-meter");
      const fill = h("span", "budget-fill");
      fill.style.width = `${Math.round(100 * Math.min(1, b.tokens / (b.capacity || 1)))}%`;
      bar.append(fill);
      left.append(bar, h("span", "", `${fmt(b.tokens)} / ${fmt(b.capacity)}`));
      tr.append(left);
      tr.append(h("td", "", t("budget.per_sec", {n: fmt(b.per_sec)})));
      const wait = b.cooling_ms > 0 ? t("budget.cooling", {s: (b.cooling_ms / 1000).toFixed(1)}) : b.waiting > 0 ? String(b.waiting) : "";
      tr.append(h("td", "", wait));
      table.append(tr);
    });
  }
  box.append(table);
  return box;
}
