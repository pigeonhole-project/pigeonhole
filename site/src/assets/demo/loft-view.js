// The node's panel: counters, the background tasks, the config (on a real
// node, read-only).
//
// on: {restart(text), erase(), toggle(task, on), run(task)}.
// Returns {nodes, setCounters({objects, dataChannels, log}), setBackground(bg), setError(text)}.
import {h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {taskCards} from "./tasks.js";

export function loftView({counters: counts, config, demo, admin, bg}, on) {
  const view = document.getElementById("tpl-node").content.cloneNode(true);
  const $ = id => view.querySelector(`#${id}`);

  const counters = $("counters");
  const setCounters = ({objects, dataChannels, log}) => {
    const counter = (n, label) => {
      const d = h("div", "counter");
      d.append(h("b", "", String(n)), h("span", "", label));
      return d;
    };
    counters.replaceChildren(
      counter(objects, t("demo.c_objects")),
      counter(dataChannels, t("demo.c_channels")),
      counter(log?.head ?? "…", t("demo.c_head")),
      counter(log?.base?.at ?? 0, t("demo.c_base")),
    );
  };
  setCounters(counts);

  const setBackground = taskCards($("bg"), {onToggle: on.toggle, onRun: on.run});
  setBackground(bg);

  const editor = $("config-text"),
    error = $("config-error");
  editor.value = config;
  editor.readOnly = admin;
  const restart = () => on.restart(editor.value);
  $("config-reset").addEventListener("click", () => {
    editor.value = demo;
    error.textContent = "";
  });
  $("config-apply").addEventListener("click", restart);
  editor.addEventListener("keydown", e => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      restart();
    }
  });
  $("erase").addEventListener("click", on.erase);

  return {
    nodes: [...view.childNodes],
    setCounters,
    setBackground,
    setError: text => {
      error.textContent = text;
    },
  };
}
