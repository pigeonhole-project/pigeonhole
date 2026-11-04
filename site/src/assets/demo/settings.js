// The demo's knobs: how the pigeons fly, how much the graph shows, how
// often the page asks the node. All of them here, turned in the settings
// panel (settings-view.js) and kept in this browser; the modules read
// `settings` as they go, so a change shows at once (or, for `redraw`
// knobs, once the town is drawn again: the panel does that).
//
// A knob: {key, group, def: its default, min, max, step, unit?: "ms" |
// "ms/px" | "x", redraw?: the town drawn again, timer?: a page timer
// restarted, clock?: the node's clock's speed, fake?: the fakes' delays
// (both told to the node)}.
// Its label and note are i18n's "set.<key>" and "set.<key>_note".
export const KNOBS = [
  {key: "speed", group: "time", def: 0.25, min: 0.05, max: 2, step: 0.05, unit: "x", clock: true},
  {key: "latencyMin", group: "network", def: 80, min: 0, max: 3000, step: 10, unit: "ms", fake: true},
  {key: "latencyMax", group: "network", def: 250, min: 0, max: 3000, step: 10, unit: "ms", fake: true},
  {key: "linkMin", group: "network", def: 30, min: 0, max: 1000, step: 5, unit: "ms", fake: true},
  {key: "linkMax", group: "network", def: 90, min: 0, max: 1000, step: 5, unit: "ms", fake: true},
  {key: "flightMin", group: "pigeons", def: 450, min: 50, max: 5000, step: 50, unit: "ms"},
  {key: "flightMax", group: "pigeons", def: 1400, min: 50, max: 10000, step: 50, unit: "ms"},
  {key: "flightPerPx", group: "pigeons", def: 1.1, min: 0, max: 10, step: 0.1, unit: "ms/px"},
  {key: "sparkMs", group: "pigeons", def: 700, min: 50, max: 5000, step: 50, unit: "ms"},
  {key: "raftCap", group: "pigeons", def: 6, min: 0, max: 100, step: 1},
  {key: "raftLift", group: "pigeons", def: 0.45, min: 0, max: 2, step: 0.05},
  {key: "floors", group: "town", def: 8, min: 2, max: 30, step: 1, redraw: true},
  {key: "queueShown", group: "town", def: 12, min: 0, max: 60, step: 1, redraw: true},
  {key: "jobsShown", group: "town", def: 40, min: 0, max: 500, step: 1},
  {key: "chronicle", group: "town", def: 100, min: 1, max: 1000, step: 1},
  {key: "pollMs", group: "page", def: 1000, min: 200, max: 10000, step: 100, unit: "ms", timer: true},
  {key: "clockMs", group: "page", def: 100, min: 20, max: 2000, step: 10, unit: "ms", timer: true},
];
export const GROUPS = ["time", "network", "pigeons", "town", "page"];
export const DEFAULTS = Object.freeze(Object.fromEntries(KNOBS.map(k => [k.key, k.def])));

const STORE = "ph-demo-settings";
const byKey = new Map(KNOBS.map(k => [k.key, k]));
const watchers = new Set();

// The knobs as they are now: the defaults, with what this browser kept.
export const settings = {...DEFAULTS};

// localStorage, on a page (not under node --test, whose own warns when touched).
const storage = () => (typeof document === "undefined" ? null : globalThis.localStorage);
try {
  const kept = JSON.parse(storage()?.getItem(STORE) || "{}");
  for (const [key, v] of Object.entries(kept)) if (byKey.has(key)) settings[key] = clamp(byKey.get(key), v);
} catch {
  // Kept settings that do not read: the defaults.
}

// Value `v` for knob `k`: a number within its range, else its default.
export function clamp(k, v) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.min(k.max, Math.max(k.min, n)) : k.def;
}

function save() {
  const changed = Object.fromEntries(Object.entries(settings).filter(([key, v]) => v !== DEFAULTS[key]));
  try {
    storage()?.setItem(STORE, JSON.stringify(changed));
  } catch {
    // A browser that keeps nothing: the settings last as long as the page.
  }
}
function tell(keys) {
  const knobs = keys.map(key => byKey.get(key));
  for (const fn of watchers) fn(knobs);
}

// Knob `key` set to `v` (clamped); the watchers hear of it.
export function set(key, v) {
  const k = byKey.get(key);
  if (!k) return;
  settings[key] = clamp(k, v);
  save();
  tell([key]);
}
// Every knob back to its default.
export function reset() {
  const changed = KNOBS.filter(k => settings[k.key] !== k.def).map(k => k.key);
  Object.assign(settings, DEFAULTS);
  save();
  if (changed.length) tell(changed);
}
// fn(knobs) after every change: the knobs that changed. Returns the unwatch.
export function watch(fn) {
  watchers.add(fn);
  return () => watchers.delete(fn);
}
