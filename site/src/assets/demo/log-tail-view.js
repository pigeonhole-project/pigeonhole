// The node's log, live, in a panel under the town: every line the wasm
// writes (log-tail.js keeps them), in a terminal (xterm.js, vendor/xterm,
// MIT: loaded the first time the panel opens): any of the levels and any
// of the components (the crates) hidden or shown, a search, and the lines
// kept to clear or download. What is hidden is kept in this browser.
//
// makeLogPanel(el, node, onShown): `el` the panel (empty), `node` the
// demo's (node.js onLog); onShown(open) as it opens or closes. Returns
// {toggle(), close(), isOpen()}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {componentOf, counts, format, jsonl, LEVELS, makeTail, short, shows} from "./log-tail.js";
import {settings} from "./settings.js";

// The assets' version (?v=…), for the vendored modules as for this one.
const V = new URL(import.meta.url).search;
const vendor = name => new URL(`../vendor/xterm/${name}${V}`, import.meta.url).href;
const ORDER = [...LEVELS].reverse(); // the levels' toggles: ERROR first
const KEPT = "ph-log-hidden"; // localStorage: {levels: [...], components: [...]}

// xterm.js and its add-ons, and its stylesheet, once.
let loaded = null;
function load() {
  loaded ??= (async () => {
    const css = h("link");
    Object.assign(css, {rel: "stylesheet", href: vendor("xterm.css")});
    document.head.append(css);
    const [{Terminal}, {FitAddon}, {SearchAddon}] = await Promise.all(["xterm.mjs", "addon-fit.mjs", "addon-search.mjs"].map(m => import(vendor(m))));
    return {Terminal, FitAddon, SearchAddon};
  })();
  return loaded;
}

// The terminal's colours: the page's, as its theme has them now.
function theme() {
  const css = getComputedStyle(document.documentElement);
  const v = name => css.getPropertyValue(name).trim();
  return {background: v("--plume"), foreground: v("--wing"), selectionBackground: v("--slate-2"), cursor: v("--plume")};
}

// What was hidden last time, else nothing.
function hiddenKept() {
  try {
    const k = JSON.parse(localStorage.getItem(KEPT) || "{}");
    return {levels: new Set(k.levels || []), components: new Set(k.components || [])};
  } catch {
    return {levels: new Set(), components: new Set()};
  }
}
function keep(hidden) {
  try {
    localStorage.setItem(KEPT, JSON.stringify({levels: [...hidden.levels], components: [...hidden.components]}));
  } catch {
    // A browser that keeps nothing: hidden for this page only.
  }
}

export function makeLogPanel(el, node, onShown = () => {}) {
  const tail = makeTail(() => settings.logLines);
  const hidden = hiddenKept();
  let term = null,
    fit = null,
    search = null,
    queue = [], // lines kept, not written to the terminal yet
    frame = 0,
    known = ""; // the components and levels the bar shows, as a key

  // The bar: the levels' and the components' menus, the search, the rest.
  const levels = checklist(
    hidden.levels,
    (shown, all) => t("log.levels", {shown, all}),
    l => l,
    l => `log-lvl ${l.toLowerCase()}`,
  );
  const components = checklist(hidden.components, (shown, all) => t("log.components", {shown, all}), short);
  const find = Object.assign(h("input", "log-find"), {type: "search", placeholder: t("log.search")});
  find.setAttribute("aria-label", t("log.search"));
  const count = h("span", "log-count");
  const screen = h("div", "log-screen");
  const bar = h("div", "log-bar");
  bar.append(
    h("strong", "", t("log.title")),
    levels.el,
    components.el,
    find,
    button("btn mini", "↑", () => search?.findPrevious(find.value)),
    button("btn mini", "↓", () => search?.findNext(find.value)),
    count,
    button("btn mini", t("log.clear"), clear),
    button("btn mini", t("log.download"), download),
    button("btn mini", "×", () => close()),
  );
  el.append(bar, screen);
  // A click elsewhere closes an open menu.
  document.addEventListener("click", e => {
    for (const m of [levels, components]) if (m.isOpen() && !e.composedPath().includes(m.el)) m.shut();
  });

  // A drop-down list with a check box per value (a level, a component) and
  // how many lines have it; unchecked: in `off`, its lines hidden. `title(shown,
  // all)` names it; `name(value)` and `cls(value)` a row.
  function checklist(off, title, name, cls = () => "") {
    const el = h("details", "log-menu");
    const summary = h("summary", "btn mini");
    const list = h("div", "log-menu-list");
    el.append(summary, list);
    const changed = () => {
      keep(hidden);
      drawBar();
      rewrite();
    };
    let values = [];
    return {
      el,
      // The values and their counts (a Map), as the lines kept have them.
      update(byValue, rows) {
        values = [...byValue.keys()];
        summary.textContent = title(values.filter(v => !off.has(v)).length, values.length);
        if (!rows) {
          for (const row of list.querySelectorAll("label[data-value]")) {
            row.querySelector("input").checked = !off.has(row.dataset.value);
            row.querySelector("small").textContent = String(byValue.get(row.dataset.value) ?? 0);
          }
          return;
        }
        const all = h("div", "row");
        all.append(
          button("btn mini", t("log.all"), () => {
            off.clear();
            changed();
          }),
          button("btn mini", t("log.none"), () => {
            for (const v of values) off.add(v);
            changed();
          }),
        );
        list.replaceChildren(
          all,
          ...[...byValue].map(([v, n]) => {
            const box = Object.assign(h("input"), {type: "checkbox", checked: !off.has(v)});
            box.addEventListener("change", () => {
              if (box.checked) off.delete(v);
              else off.add(v);
              changed();
            });
            const label = h("label", `log-menu-row ${cls(v)}`);
            label.title = v;
            label.dataset.value = v;
            label.append(box, ` ${name(v)} `, h("small", "", String(n)));
            return label;
          }),
        );
      },
      isOpen: () => el.open,
      shut() {
        el.open = false;
      },
    };
  }

  // The menus as the lines kept count them: their rows drawn anew when
  // `force`, or a level or a component first seen; else their counts.
  function drawBar(force = false) {
    const lines = tail.lines();
    const byLevel = counts(
      lines,
      l => l.level,
      ORDER.filter(l => l !== "TRACE"),
    );
    const byComponent = counts(lines, componentOf);
    const key = [...byLevel.keys(), "|", ...byComponent.keys()].join(" ");
    count.textContent = t("log.count", {n: lines.length});
    const rows = force || key !== known;
    known = key;
    levels.update(byLevel, rows);
    components.update(byComponent, rows);
  }

  // What came since the last frame, written at once.
  function flush() {
    frame = 0;
    drawBar();
    if (!term || el.hidden || !queue.length) return;
    const lines = queue.filter(l => shows(l, hidden));
    queue = [];
    if (lines.length) term.write(`${lines.map(l => format(l)).join("\r\n")}\r\n`);
  }
  const later = () => {
    if (!frame) frame = requestAnimationFrame(flush);
  };
  // Every line kept again, as the toggles now hide them.
  function rewrite() {
    if (!term) return;
    term.reset();
    term.write("\x1b[?25l"); // no cursor: nothing is typed here
    queue = [...tail.lines()];
    flush();
  }

  node.onLog(lines => {
    tail.push(lines);
    queue.push(...lines);
    if (queue.length > settings.logLines) queue = queue.slice(queue.length - settings.logLines);
    later();
  });

  find.addEventListener("keydown", e => {
    if (e.key === "Enter") (e.shiftKey ? search?.findPrevious : search?.findNext)?.call(search, find.value);
  });
  el.addEventListener("keydown", e => {
    if (e.key === "Escape") close();
  });

  function clear() {
    tail.clear();
    queue = [];
    term?.reset();
    term?.write("\x1b[?25l");
    drawBar(true);
  }
  function download() {
    const a = h("a");
    a.href = URL.createObjectURL(new Blob([jsonl(tail.lines())], {type: "application/x-ndjson"}));
    a.download = "pigeonhole-node.jsonl";
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }

  async function open() {
    el.hidden = false;
    onShown(true);
    drawBar(true);
    const {Terminal, FitAddon, SearchAddon} = await load();
    if (!term) {
      const font = getComputedStyle(document.documentElement).getPropertyValue("--mono").trim();
      term = new Terminal({
        convertEol: true,
        disableStdin: true,
        scrollback: settings.logLines,
        fontFamily: font,
        fontSize: 12,
        theme: theme(),
      });
      fit = new FitAddon();
      search = new SearchAddon();
      term.loadAddon(fit);
      term.loadAddon(search);
      term.open(screen);
      new ResizeObserver(() => {
        if (!el.hidden) fit.fit();
      }).observe(screen);
      rewrite();
    } else {
      term.options.theme = theme();
      flush();
    }
    fit.fit();
    find.focus();
  }
  function close() {
    el.hidden = true;
    levels.shut();
    components.shut();
    onShown(false);
  }

  return {
    toggle: () => (el.hidden ? open() : close()),
    close,
    isOpen: () => !el.hidden,
  };
}
