// The node's panel: counters, the background tasks, the config (on a real
// node, read-only).
//
// on: {restart(text), erase(), toggle(task, on), run(task), isolate(node, cut)}.
// cluster: {node, cut, leader}, the node of a cluster this panel is: it can
// be cut off; a follower runs none of the leader's tasks, only its snapshots.
// Returns {nodes, setCounters({objects, dataChannels, log}), setBackground(bg), setError(text)}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {taskCards} from "./tasks.js";

export function loftView({counters: counts, config, demo, admin, bg, cluster}, on) {
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

  if (cluster) {
    const {node, cut} = cluster;
    const row = h("div", "row node-cut");
    row.append(
      button(`btn${cut ? " primary" : " danger"}`, t(cut ? "nodes.bring_back" : "nodes.cut_off", {n: node}), () => on.isolate(node, !cut)),
      h("p", "field-note", t("nodes.cut_note")),
    );
    counters.after(row);
  }

  const follower = cluster && cluster.leader !== cluster.node;
  const note = follower ? t("nodes.tasks_leader", {n: cluster.leader ?? "?"}) : null;
  const setBackground = taskCards($("bg"), {onToggle: on.toggle, onRun: on.run, follower: note});
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
