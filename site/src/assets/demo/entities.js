// The things the demo shows, and a name for one wherever it is mentioned.
// Each has a panel; all but the cluster have a place on the graph. A
// reference is the graph's target for it (graph.js), and the panel opens on
// it (main.js openPanel):
//   {kind: "client"}            you
//   {kind: "loft", node}        a node (node: a cluster's id; none: the one node)
//   {kind: "store", store}      a channel (data, log, snapshots, outbox)
//   {kind: "cluster"}           the cluster
//
// A mention is a link: on a screen with hover, pointing at it marks the
// thing on the graph and a click opens it; by touch, a tap marks it and a
// second tap opens it.
import {canHover, h} from "../lib/dom.js";
import {t, tHtmlWith} from "../lib/i18n.js";

export const ref = {
  client: () => ({kind: "client"}),
  node: id => ({kind: "loft", node: id}),
  channel: store => ({kind: "store", store}),
  cluster: () => ({kind: "cluster"}),
};

// What a link does: main.js sets them once ({mark(ref | null), open(ref)}).
let on = {mark: () => {}, open: () => {}};
export const setLinks = handlers => {
  on = handlers;
};

let armed = null; // the link a first tap marked

// A link to `r`, reading `text`.
export function link(r, text) {
  const a = h("button", "ref", text);
  a.type = "button";
  const mark = () => on.mark(r),
    unmark = () => on.mark(null);
  a.addEventListener("mouseenter", () => canHover() && mark());
  a.addEventListener("mouseleave", () => canHover() && unmark());
  a.addEventListener("focus", mark);
  a.addEventListener("blur", unmark);
  a.addEventListener("click", e => {
    e.stopPropagation();
    if (!canHover() && armed !== a) {
      armed = a;
      mark();
      return;
    }
    armed = null;
    unmark();
    on.open(r);
  });
  return a;
}

// A node's name, a link to it.
export const nodeLink = id => link(ref.node(id), t("nodes.node", {n: id}));
// A channel's name, a link to it.
export const channelLink = store => link(ref.channel(store), store);

// The mentions a say.* sentence's values are: a key's {store} is a channel;
// in what the cluster says (raft.*, nodes.*), {n}, {from}, {to} are nodes.
function mentionOf(key, name, value) {
  if (name === "store" && typeof value === "string" && value) return channelLink(value);
  // The sentence says "node {n}" itself: the number is the link.
  if (/^(raft\.|nodes\.|say\.not_leader$)/.test(key) && ["n", "from", "to"].includes(name) && Number.isInteger(value))
    return link(ref.node(value), String(value));
  return null;
}

// A sentence ([key, vars]) as nodes: its HTML, each mention a link.
export function sentence([key, vars = {}]) {
  const links = [];
  const html = tHtmlWith(key, vars, (name, value) => {
    const el = mentionOf(key, name, value);
    if (!el) return null;
    links.push(el);
    return `<span data-mention="${links.length - 1}"></span>`;
  });
  const box = h("span", "");
  box.innerHTML = html;
  for (const slot of box.querySelectorAll("[data-mention]")) slot.replaceWith(links[+slot.dataset.mention]);
  return [...box.childNodes];
}
