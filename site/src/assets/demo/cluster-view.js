// The cluster's panel: its nodes as each says, whether the last change of
// the dynamic config reached each, the one pigeonhole.toml they all run, and
// the chronicle of their Raft (raft.js).
//
// data: {nodes: [id], own: Map(id → that node's `cluster` op), leader: the
//        leader's `cluster` op (lag, links, quorum) or null, cut: Set,
//        dynamic: the leader's `dynamic` op or null, chronicle: [{at, sentence}],
//        config, demo, admin}
// on: {restart(text), erase()}.
// Returns {nodes, update(data), setError(text)}.
import {h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {configView} from "./config-view.js";
import {nodeLink, sentence} from "./entities.js";

export function clusterView(data, on) {
  const table = h("div", "cluster-nodes"),
    sync = h("p", "field-note cluster-sync"),
    chronicle = h("ol", "chronicle");
  const config = configView(data, on);

  function update(d) {
    table.replaceChildren(nodesTable(d));
    sync.textContent = syncNote(d.dynamic);
    chronicle.replaceChildren(
      ...d.chronicle
        .slice()
        .reverse()
        .map(({at, sentence: said}) => {
          const li = h("li", "");
          li.append(h("span", "chronicle-at", t("time.ago", {s: Math.round((performance.now() - at) / 1000)})), ...sentence(said));
          return li;
        }),
    );
    if (!d.chronicle.length) chronicle.append(h("li", "empty", t("cluster.quiet")));
  }
  update(data);

  return {
    nodes: [h("h3", "", t("cluster.nodes")), table, h("h3", "", t("cluster.dynamic")), sync, ...config.nodes, h("h3", "", t("cluster.chronicle")), chronicle],
    update,
    setError: config.setError,
  };
}

// A row per node: what it says it is, its term, how far it has applied,
// how far behind the leader it is, its links, and whether it is cut off.
function nodesTable({nodes, own, leader, cut}) {
  const tbl = h("table", "cluster-table");
  const head = h("tr", "");
  for (const key of ["node", "role", "term", "applied", "lag", "links"]) head.append(h("th", "", t(`cluster.col_${key}`)));
  tbl.append(head);
  for (const id of nodes) {
    const v = own.get(id),
      me = v?.nodes.find(n => n.id === id),
      fromLeader = leader?.nodes.find(n => n.id === id);
    const role = cut.has(id) ? t("nodes.role_cut") : me?.role ? t(`nodes.role_${me.role}`) : "…";
    const quorum = me?.role === "leader" && v.read_only ? ` (${t("cluster.no_quorum")})` : "";
    const peers = (leader?.links || v?.links || []).filter(l => l.a === id || l.b === id);
    const row = h("tr", me?.role === "leader" && !cut.has(id) ? "leading" : "");
    const links = h("td", "");
    for (const [i, l] of peers.entries()) {
      if (i) links.append(", ");
      links.append(nodeLink(l.a === id ? l.b : l.a), ` ${l.via === "chat" ? t("cluster.via_chat") : "gRPC"}`);
    }
    if (!peers.length) links.textContent = "—";
    const cell = text => h("td", "", String(text));
    const name = h("td", "");
    name.append(nodeLink(id));
    row.append(name, cell(role + quorum), cell(v?.term ?? "…"), cell(me?.applied ?? "…"), cell(fromLeader?.lag ?? "—"), links);
    tbl.append(row);
  }
  return tbl;
}

// Whether the last change of the dynamic config reached every node.
function syncNote(dynamic) {
  if (!dynamic?.changed_at) return t("cluster.no_changes");
  const nodes = dynamic.nodes || [];
  const behind = nodes.filter(n => n.synced === false).map(n => n.id);
  return behind.length ? t("cluster.not_synced", {at: dynamic.changed_at, nodes: behind.join(", ")}) : t("cluster.synced", {at: dynamic.changed_at});
}
