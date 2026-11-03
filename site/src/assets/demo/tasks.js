// The node's background loops (dynamic config, the `background` op): each a
// queue of work and a clock to its next pass; each can be turned off, or
// run now. They live in the dovecote's windows and in the loft's panel.
//
// The op's reply: {tasks: [{task, every: "10s" | "off", next_in_ms, work: [...]}],
//                  snapshot: {head, base, at: the newest snapshot's position, every}}.
import {activate, button, canHover, h, svg} from "../lib/dom.js";
import {bytes, t} from "../lib/i18n.js";
import {boarded, perched} from "./art.js";
import {sentence} from "./entities.js";
import {settings} from "./settings.js";

const TASKS = ["repair", "sweep", "cool", "snapshot"];
// The tasks only the leader runs (the chunk stores are its); every node
// takes snapshots of its own state.
const LEADER_TASKS = new Set(["repair", "sweep", "cool"]);
// The dynamic key that sets a task's period; the period "on" means when the file has it off.
export const TASK_KEY = {repair: "repair.interval", sweep: "sweep.interval", cool: "inline.cool_every", snapshot: "meta.snapshot_every"};
export const TASK_ON = {repair: "10s", sweep: "30s", cool: "30s", snapshot: "1m"};
// The dovecote's entrance each task lives in (dovecote(): the top row is 0–2).
const TASK_WINDOW = {repair: 0, sweep: 1, cool: 2, snapshot: 4};

// "1m 30s", "250ms" → ms; "off" (or nothing) → null.
export function ms(text) {
  if (!text || text === "off") return null;
  const unit = {ms: 1, s: 1000, m: 60000, min: 60000, h: 3600000, d: 86400000};
  let total = 0;
  for (const [, n, u] of String(text).matchAll(/([\d.]+)\s*(ms|min|s|m|h|d)/g)) total += +n * unit[u];
  return total || null;
}

// "45 s", "9 min": how long until a pass.
function soon(msLeft) {
  const sec = Math.ceil((msLeft || 0) / 1000);
  return sec < 120 ? t("time.s", {n: sec}) : t("time.min", {n: Math.round(sec / 60)});
}

// A task as the page shows it: on or off, how far to its next pass (0…1),
// how much work waits (`of`: the snapshot's every N entries), in words.
export function describe(bg, name) {
  const task = bg?.tasks.find(x => x.task === name);
  if (!task) return null;
  const period = ms(task.every);
  const toNext = period && task.next_in_ms != null ? 1 - Math.min(1, task.next_in_ms / period) : 0;
  let n, of, work, base;
  if (name === "snapshot") {
    // Counted from the newest snapshot (a hand-run one too), not the log's start.
    base = bg.snapshot.at ?? bg.snapshot.base;
    n = Math.max(0, bg.snapshot.head - base);
    of = bg.snapshot.every || undefined;
    work = t(of ? "bg.entries_short" : "bg.entries_since", {n, of});
  } else {
    n = task.work.length;
    work = n ? t("bg.waiting", {n}) : t("bg.idle");
  }
  return {
    name,
    task,
    period,
    toNext,
    n,
    of,
    base,
    title: t(`bg.${name}`),
    // A snapshot without a timer still comes every so many entries.
    next: period ? t("bg.next", {when: soon(task.next_in_ms)}) : of ? t("bg.at_entries", {of}) : t("bg.parked"),
    work,
  };
}

// A repair job ({chunk, store}): a sentence (its store a link, entities.js).
const jobSentence = item => ["demo.job", {chunk: item.chunk, store: item.store}];

// A piece of work as a tile; a repair job's channel, a link to it.
function jobTile(name, item) {
  const li = h("li", "job-tile");
  if (name === "repair") li.append(...sentence(jobSentence(item)));
  else li.textContent = workLabel(name, item);
  return li;
}

// One piece of a task's work, in words.
export function workLabel(name, item) {
  if (name === "repair") return t(...jobSentence(item));
  if (name === "sweep") return t("bg.sweep_item", {chunk: item.chunk});
  return t("bg.cool_item", {key: item.key, size: bytes(item.size), reason: t(`bg.reason_${item.reason}`)});
}

// ---- the dovecote's windows
// The light rises as the next pass nears (a snapshot: as entries pile up,
// too); a pigeon sits in while work waits, a badge counts it; a task that
// is off has its shutters closed. The windows no task lives in are boarded
// up, and on a `follower` the leader's tasks' too. onOpen(task): a window
// was clicked. Returns update(bg).
export function taskWindows(g, entrances, onOpen, {follower = false} = {}) {
  const windows = {};
  for (const name of TASKS) {
    const w = entrances[TASK_WINDOW[name]];
    if (follower && LEADER_TASKS.has(name)) {
      boarded(g, w);
      continue;
    }
    const win = svg("g", {class: "win", tabindex: 0, role: "button", "aria-label": t(`bg.${name}`)}, g);
    const clip = svg("clipPath", {id: `win-${name}`}, win);
    svg("path", {d: w.d}, clip);
    // Lived in: a faint glow always, the light rising over it.
    svg("path", {class: "win-glow", d: w.d}, win);
    const light = svg("rect", {class: "win-light", x: w.x - 10, y: w.y + 14, width: 20, height: 0, "clip-path": `url(#win-${name})`}, win);
    svg("path", {class: "win-frame", d: w.d}, win);
    perched(win, {class: "win-bird", transform: `translate(${w.x},${w.y + 4}) scale(.82)`});
    const badge = svg("g", {class: "win-badge"}, win);
    svg("circle", {cx: w.x + 11, cy: w.y - 9, r: 7}, badge);
    const count = svg("text", {x: w.x + 11, y: w.y - 6, "text-anchor": "middle"}, badge);
    const title = svg("title", {}, win);
    win.addEventListener("mouseenter", () => canHover() && tip.show(name, win));
    win.addEventListener("focus", () => tip.show(name, win));
    win.addEventListener("mouseleave", tip.hide);
    win.addEventListener("blur", tip.hide);
    // Without hover (a phone) a tap shows the tooltip first, the next one opens.
    activate(win, e => {
      e.stopPropagation();
      if (!canHover() && !tip.showing(name)) {
        tip.show(name, win);
        return;
      }
      tip.hide();
      onOpen(name);
    });
    windows[name] = {win, w, light, count, title};
  }
  const used = new Set(Object.values(TASK_WINDOW));
  for (const [i, w] of entrances.entries()) if (!used.has(i)) boarded(g, w);

  return bg => {
    for (const name of TASKS) {
      const d = describe(bg, name),
        win = windows[name];
      if (!d || !win) continue;
      const lit = d.of ? Math.max(d.toNext, Math.min(1, d.n / d.of)) : d.toNext;
      const height = Math.round(24 * lit);
      win.light.setAttribute("y", String(win.w.y + 14 - height));
      win.light.setAttribute("height", String(height));
      win.count.textContent = d.n > 99 ? "99+" : String(d.n);
      win.win.classList.toggle("off", !d.period && !d.of);
      win.win.classList.toggle("busy", d.n > 0);
      const when = d.period ? t("bg.tip", {task: d.title, when: soon(d.task.next_in_ms)}) : d.of ? `${d.title}: ${d.next}` : t("bg.tip_off", {task: d.title});
      win.title.textContent = name === "snapshot" || d.n > 0 ? `${when}; ${d.work}` : when;
    }
    tip.refresh(bg);
  };
}

// A window's tooltip: what the task is, when it passes next, what waits;
// it follows the background while it is up. Beside the window, kept on the
// screen; a tap elsewhere puts it away.
const tip = (() => {
  let box = null,
    shown = null,
    last = null;
  function fill() {
    box.replaceChildren(h("b", "", t(`bg.${shown}`)), h("p", "", t(`bg.${shown}_note`)));
    const d = describe(last, shown);
    if (!d) return;
    box.append(h("div", "tip-when", d.next), h("div", `tip-work${d.n ? " busy" : ""}`, d.work));
  }
  return {
    show(name, el) {
      if (!box) {
        box = h("div", "tip");
        box.setAttribute("role", "tooltip");
        document.body.append(box);
        document.addEventListener("pointerdown", e => {
          if (shown && !e.target.closest?.(".win")) this.hide();
        });
      }
      shown = name;
      fill();
      box.hidden = false;
      const r = el.getBoundingClientRect(),
        room = document.documentElement.clientWidth - box.offsetWidth - 8;
      box.style.left = `${Math.round(Math.max(8, Math.min(r.right + 12, room)))}px`;
      const below = r.top - 8 + box.offsetHeight > innerHeight;
      box.style.top = `${Math.round(below ? r.top - box.offsetHeight - 8 : r.top - 8)}px`;
    },
    showing: name => shown === name,
    hide() {
      shown = null;
      if (box) box.hidden = true;
    },
    refresh(bg) {
      last = bg;
      if (shown) fill();
    },
  };
})();

// ---- the loft's panel: a card per task in `box`, updated in place (a
// poll keeps the focus where it was). onToggle(name, on), onRun(name);
// `follower`: why the leader's tasks are not here (a follower runs none of
// them; nodes, its links among them), shown above; its snapshots are its
// own. Returns {update(bg), focus(task): its card scrolled into view}.
export function taskCards(box, {onToggle, onRun, follower = null}) {
  const cards = TASKS.map(name => taskCard(name, onToggle, onRun, !!follower));
  const note = h("p", "field-note tasks-ro");
  if (follower) note.append(...follower);
  box.replaceChildren(...(follower ? [note] : []), ...cards.map(c => c.el));
  return {
    update(bg) {
      for (const c of cards) c.update(describe(bg, c.name));
    },
    focus(task) {
      cards.find(c => c.name === task)?.el.scrollIntoView({block: "nearest"});
    },
  };
}

function taskCard(name, onToggle, onRun, follower) {
  // A follower's view of a task only the leader runs: no clock, no queue.
  const away = follower && LEADER_TASKS.has(name);
  const el = h("div", "task");
  let on = false;
  const sw = button("switch", null, () => onToggle(name, !on));
  sw.setAttribute("role", "switch");
  sw.setAttribute("aria-label", t("bg.on_off", {task: t(`bg.${name}`)}));
  const run = button("mini", t("bg.run_now"), () => onRun(name));
  // Not this node's task at all: nothing to switch or run here.
  sw.hidden = run.hidden = away;
  const every = h("span", "task-every");
  const head = h("div", "task-head");
  head.append(h("b", "", t(`bg.${name}`)), sw, every, run);
  const meter = cls => {
    const m = h("div", "meter"),
      fill = h("div", cls);
    m.append(fill);
    return {m, fill};
  };
  const clock = meter("fill"),
    clockText = h("div", "task-when");
  el.append(head, h("p", "task-note", t(`bg.${name}_note`)), clock.m, clockText);
  // The snapshot's entries since the last one; the other tasks' queues.
  const entries = meter("fill amethyst"),
    entriesText = h("div", "task-when");
  const jobs = h("ol", "jobs");
  if (name === "snapshot") el.append(entries.m, entriesText);
  else el.append(jobs);

  function update(d) {
    el.hidden = !d;
    if (!d) return;
    on = !!d.period;
    el.classList.toggle("off", away || (!on && !d.of));
    sw.setAttribute("aria-checked", String(on));
    if (away) {
      every.textContent = t("bg.on_leader");
      clock.m.hidden = jobs.hidden = true;
      clockText.textContent = t("bg.not_here");
      return;
    }
    every.textContent = on ? t("bg.every", {every: d.task.every}) : d.of ? t("bg.every_entries", {of: d.of}) : t("bg.off");
    // No timer, no clock: a snapshot by entries has its own meter.
    clock.m.hidden = !on;
    clock.fill.style.width = `${100 * d.toNext}%`;
    clockText.textContent = d.next;
    if (name === "snapshot") {
      entries.m.hidden = entriesText.hidden = !d.of;
      if (d.of) {
        entries.fill.style.width = `${Math.min(100, (100 * d.n) / d.of)}%`;
        entriesText.textContent = t("bg.entries", {n: d.n, of: d.of, at: d.base});
      }
      return;
    }
    const work = d.task.work;
    const most = settings.jobsShown;
    jobs.replaceChildren(...work.slice(0, most).map(item => jobTile(name, item)));
    if (work.length > most) jobs.append(h("li", "job-tile more", `+${work.length - most}`));
    if (!work.length) jobs.append(h("li", "empty", t("bg.idle")));
  }
  return {name, el, update};
}
