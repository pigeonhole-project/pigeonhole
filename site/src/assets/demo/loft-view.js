// The node's panel: counters, the background tasks, the config (on a real
// node, read-only; a cluster's is in the cluster's panel).
//
// on: {restart(text), erase(), toggle(task, on), run(task), isolate(node, cut)}.
// cluster: {node, cut, leader}, the node of a cluster this panel is: it can
// be cut off; a follower runs none of the leader's tasks, only its snapshots.
// Returns {nodes, setCounters({objects, dataChannels, log}), setBackground(bg), setError(text)}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {configView} from "./config-view.js";
import {taskCards} from "./tasks.js";

export function loftView(props, on) {
  const {counters: counts, bg, cluster} = props;
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

  // One node's config is its own; a cluster's is every node's: in the cluster's panel.
  const config = cluster ? null : configView(props, on);
  if (config) $("config-slot").replaceWith(...config.nodes);
  else $("config-slot").replaceWith(h("p", "field-note", t("nodes.config_in_cluster")));

  return {
    nodes: [...view.childNodes],
    setCounters,
    setBackground,
    setError: text => config?.setError(text),
  };
}
