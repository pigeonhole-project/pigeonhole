// The node's own log as the page keeps it: every line the wasm wrote
// (tracing's JSON: {timestamp, level, target, fields: {message, …},
// spans: [{name, …}]}), the last `max` of them, and each as a line of text
// for the log panel (log-tail-view.js): the time, the level, where it comes
// from (the target, its spans), the message, its fields. Pure: no DOM.

export const LEVELS = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];

// The last `max()` lines (max: a function, a setting that may change).
export function makeTail(max) {
  let lines = [];
  return {
    push(more) {
      lines.push(...more);
      if (lines.length > max()) lines = lines.slice(lines.length - max());
    },
    lines: () => lines,
    clear() {
      lines = [];
    },
  };
}

// The component a line comes from: its target's crate ("pigeonhole_meta").
export const componentOf = l => (l.target || "").split("::")[0] || "?";
// A component as the panel names it: without the project's prefix.
export const short = c => c.replace(/^pigeonhole_/, "");

// What the panel hides: {levels: Set, components: Set}; a line shows
// unless its level or its component is hidden.
export const shows = (l, hidden) => !hidden.levels.has(l.level) && !hidden.components.has(componentOf(l));

// How many lines of each value `key(line)` gives: Map(value → n), the
// values in `order` first, then the rest by name.
export function counts(lines, key, order = []) {
  const n = new Map(order.map(k => [k, 0]));
  for (const l of lines) n.set(key(l), (n.get(key(l)) ?? 0) + 1);
  const rest = [...n.keys()].filter(k => !order.includes(k)).sort();
  return new Map([...order, ...rest].map(k => [k, n.get(k)]));
}

// ANSI's SGR: what the terminal (xterm.js) colours a piece with.
const SGR = {dim: "2", red: "31", yellow: "33", green: "32", blue: "34", magenta: "35", cyan: "36"};
const paint = (on, colour, text) => (on && colour ? `\x1b[${SGR[colour]}m${text}\x1b[0m` : text);
const LEVEL_COLOUR = {ERROR: "red", WARN: "yellow", INFO: "green", DEBUG: "blue", TRACE: "dim"};

// A field's value as text: a string as it is, unless it has a space (then
// quoted); past FIELD_MAX characters cut (the whole is in the download).
const FIELD_MAX = 120;
const value = v => {
  const s = typeof v === "string" && !/[\s"]/.test(v) ? v : JSON.stringify(v);
  return s.length > FIELD_MAX ? `${s.slice(0, FIELD_MAX - 1)}…` : s;
};
const fieldsOf = (o, skip = []) =>
  Object.entries(o || {})
    .filter(([k]) => !skip.includes(k))
    .map(([k, v]) => `${k}=${value(v)}`)
    .join(" ");

// The time of day of a line's timestamp, to the millisecond ("12:03:41.207").
function clock(ts) {
  const m = /T(\d\d:\d\d:\d\d)(\.\d{1,3})?/.exec(ts || "");
  return m ? `${m[1]}${(m[2] || ".000").padEnd(4, "0")}` : "--:--:--.---";
}

// Line `l` as one line of text; `colour`: with the terminal's colours.
//   12:03:41.207 DEBUG pigeonhole_blob store{instance=tg-1 op=put}: store call ms=12
export function format(l, colour = true) {
  const f = l.fields || {};
  const spans = (l.spans || []).map(s => `${s.name}${fieldsOf(s, ["name"]) ? `{${fieldsOf(s, ["name"])}}` : ""}`).join(":");
  const where = `${l.target || ""}${spans ? ` ${spans}` : ""}`;
  const rest = fieldsOf(f, ["message"]);
  return [
    paint(colour, "dim", clock(l.timestamp)),
    paint(colour, LEVEL_COLOUR[l.level], (l.level || "").padEnd(5)),
    `${paint(colour, "cyan", where)}:`,
    f.message ?? "",
    rest ? paint(colour, "dim", rest) : "",
  ]
    .filter(Boolean)
    .join(" ");
}

// The lines kept, one JSON object per line: what "download" saves.
export const jsonl = lines => lines.map(l => JSON.stringify(l)).join("\n") + (lines.length ? "\n" : "");
