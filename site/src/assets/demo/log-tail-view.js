// The node's log, live, in a panel under the town: every line the wasm
// writes (log-tail.js keeps them), in a terminal (xterm.js, vendor/xterm,
// MIT: loaded the first time the panel opens), with the lowest level shown,
// a search, and the lines kept to clear or download.
//
// makeLogPanel(el, node, onShown): `el` the panel (empty), `node` the
// demo's (node.js onLog); onShown(open) as it opens or closes. Returns
// {toggle(), close(), isOpen()}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {format, jsonl, makeTail, passes} from "./log-tail.js";
import {settings} from "./settings.js";

// The assets' version (?v=…), for the vendored modules as for this one.
const V = new URL(import.meta.url).search;
const vendor = name => new URL(`../vendor/xterm/${name}${V}`, import.meta.url).href;
const SHOWN = ["DEBUG", "INFO", "WARN"]; // the lowest level a reader picks

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

export function makeLogPanel(el, node, onShown = () => {}) {
  const tail = makeTail(() => settings.logLines);
  let level = "DEBUG",
    term = null,
    fit = null,
    search = null,
    queue = [], // lines kept, not written to the terminal yet
    frame = 0;

  // The panel: its bar, then the terminal.
  const levels = h("select", "log-level");
  for (const l of SHOWN) levels.append(Object.assign(h("option", "", t(`log.level_${l.toLowerCase()}`)), {value: l}));
  levels.setAttribute("aria-label", t("log.level"));
  const find = Object.assign(h("input", "log-find"), {type: "search", placeholder: t("log.search")});
  find.setAttribute("aria-label", t("log.search"));
  const count = h("span", "log-count");
  const screen = h("div", "log-screen");
  const bar = h("div", "log-bar");
  bar.append(
    h("strong", "", t("log.title")),
    levels,
    find,
    button("btn mini", "↑", () => search?.findPrevious(find.value)),
    button("btn mini", "↓", () => search?.findNext(find.value)),
    count,
    button("btn mini", t("log.clear"), clear),
    button("btn mini", t("log.download"), download),
    button("btn mini", "×", () => close()),
  );
  el.append(bar, screen);

  const shownCount = () => {
    count.textContent = t("log.count", {n: tail.lines().length});
  };
  // What came since the last frame, written at once.
  function flush() {
    frame = 0;
    shownCount();
    if (!term || el.hidden || !queue.length) return;
    const lines = queue.filter(l => passes(l, level));
    queue = [];
    if (lines.length) term.write(`${lines.map(l => format(l)).join("\r\n")}\r\n`);
  }
  const later = () => {
    if (!frame) frame = requestAnimationFrame(flush);
  };
  // Every line kept again, at the level now picked.
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

  levels.addEventListener("change", () => {
    level = levels.value;
    rewrite();
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
    shownCount();
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
    onShown(false);
  }

  return {
    toggle: () => (el.hidden ? open() : close()),
    close,
    isOpen: () => !el.hidden,
  };
}
