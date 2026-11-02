// Where everything of the town goes (graph.js draws it): the view's size,
// its streets, and on them you, the node's block (or each node's, in a
// cluster), each channel's block. A layout:
//   {width, height, lift: how high pigeons climb,
//    client: a block, lofts: Map(node → a node's block; 0: the one node),
//    stores: Map(store → a block)}
// a channel's block {x, w, cols, ground, most: floors at most, deep: its
// wall under the ground, size?: its letters'}; a node's {x, w, floors, ground, scale, deep,
// cote: its dovecote's middle from x, letters: the name's middle from x, size}.
import {FLOOR} from "./art.js";

export const COL = 26; // a window's column
const MIN_FLOORS = 6,
  HEADROOM = 2; // a block's floors at least; empty floors over its last message

// What a channel's windows hold, oldest first: as many unnamed messages as
// its total has beyond the ones it lists, then those (the deleted too).
export function contents(c) {
  const list = c.messages || [];
  const older = Math.max(0, (c.total ?? 0) - list.filter(m => !m.gone).length);
  return {older, list, count: older + list.length};
}
// A block's floors for so many windows, `cols` to a floor.
export const floorsFor = (count, cols, most) => Math.max(MIN_FLOORS, Math.min(most, Math.ceil(count / cols) + HEADROOM));

// The channels by what they are: the chunk stores, a node's own (its log
// and snapshots: the one node's, or node `id`'s), the Raft outboxes.
const dataOf = channels => channels.filter(c => c.role === "data");
const ownOf = (channels, id = null) => channels.filter(c => (c.role === "log" || c.role === "snapshots") && (id == null || c.node === id));
const outboxesOf = channels => channels.filter(c => c.role === "outbox");
// A cluster's nodes, as their channels name them; none: one node.
export const nodesOf = channels => [...new Set(channels.map(c => c.node).filter(n => n != null))].sort((a, b) => a - b);

// Blocks side by side from `x`, `each` wide with `gap` between, their
// columns fitting [lo, hi]; returns where the street ends.
function row(at, channels, x, each, gap, lo, hi, place) {
  for (const c of channels) {
    const cols = Math.max(lo, Math.min(hi, Math.floor(each / COL)));
    at.set(c.store, {x: x + (each - cols * COL) / 2, w: cols * COL, cols, ...place});
    x += each + gap;
  }
  return x - gap;
}
// The floors the tallest of these channels needs, `cols` to a floor.
const floorsOf = (channels, cols, most) => Math.max(MIN_FLOORS, ...channels.map(c => floorsFor(contents(c).count, cols, most)));

// Wide: one street, the view as tall as the screen's shape asks; you, the
// node's block, its own channels, then the data channels…
function wide(channels, height) {
  const own = ownOf(channels),
    data = dataOf(channels);
  const ground = height - 6,
    most = Math.floor((ground - 12 - 70 - height * 0.12) / FLOOR);
  const at = new Map();
  let x = 390;
  for (const c of own) {
    const cols = c.role === "log" ? 3 : 4;
    at.set(c.store, {x, w: cols * COL, cols, ground, most, deep: 2000});
    x += cols * COL + 18;
  }
  x += 24;
  const gap = 14;
  row(at, data, x, (1180 - x - (data.length - 1) * gap) / Math.max(1, data.length), gap, 2, 6, {ground, most, deep: 2000});
  return {
    width: 1200,
    height,
    client: {x: 30, w: 3 * COL, cols: 3, ground, most, deep: 2000},
    lofts: new Map([[0, {x: 150, w: 230, floors: 9, ground, scale: 0.8, deep: 2000, cote: 60, letters: 176, size: 13}]]),
    stores: at,
    lift: 80,
  };
}

// …or two streets top to bottom, for a phone: you, the node and its own
// channels on the first, the data channels on the one in front of it.
function tall(channels) {
  const own = ownOf(channels),
    data = dataOf(channels);
  const first = 400,
    most = 12;
  const at = new Map();
  let x = 226;
  for (const c of own) {
    const cols = c.role === "log" ? 2 : 3;
    at.set(c.store, {x, w: cols * COL, cols, ground: first, most, deep: 0});
    x += cols * COL + 10;
  }
  const gap = 10,
    each = (388 - (data.length - 1) * gap) / Math.max(1, data.length);
  const cols = Math.max(2, Math.min(8, Math.floor(each / COL)));
  const second = first + 70 + floorsOf(data, cols, most) * FLOOR + 12;
  row(at, data, 6, each, gap, 2, 8, {ground: second, most, deep: 2000});
  return {
    width: 400,
    height: second + 8,
    client: {x: 6, w: 2 * COL, cols: 2, ground: first, most, deep: 0},
    lofts: new Map([[0, {x: 66, w: 150, floors: 7, ground: first, scale: 0.5, deep: 0, cote: 38, letters: 112, size: 10}]]),
    stores: at,
    lift: 40,
  };
}

// Thin blocks of two columns side by side from `x`, each as wide as its
// name on the roof needs (at letters of `size`); returns where they end.
function thin(at, channels, x, gap, place) {
  for (const c of channels) {
    const each = Math.max(2 * COL + 6, c.store.length * place.size * 0.62 + place.size * 2.6 + 6);
    at.set(c.store, {x: x + (each - 2 * COL) / 2, w: 2 * COL, cols: 2, ...place});
    x += each + gap;
  }
  return x - gap;
}

// A cluster, wide: you; each node's block, its log and snapshots beside
// it; the outboxes; the data channels. Wider than one node's town.
const STAGGER = [10, 8, 9]; // the nodes' blocks, not all of a height
function wideCluster(channels, nodes, height) {
  const data = dataOf(channels);
  const ground = height - 6,
    most = Math.floor((ground - 12 - 70 - height * 0.12) / FLOOR);
  const at = new Map(),
    lofts = new Map(),
    place = {ground, most, deep: 2000};
  let x = 130;
  for (const [i, id] of nodes.entries()) {
    lofts.set(id, {x, w: 150, floors: STAGGER[i % STAGGER.length], ground, scale: 0.6, deep: 2000, cote: 44, letters: 116, size: 10});
    x = thin(at, ownOf(channels, id), x + 156, 2, {...place, size: 10}) + 18;
  }
  x = thin(at, outboxesOf(channels), x, 2, {...place, size: 10}) + 24;
  const gap = 14,
    width = Math.max(1200, x + data.length * (4 * COL + gap) + 6);
  row(at, data, x, (width - 20 - x - (data.length - 1) * gap) / Math.max(1, data.length), gap, 2, 6, place);
  return {width, height, client: {x: 30, w: 3 * COL, cols: 3, ground, most, deep: 2000}, lofts, stores: at, lift: 80};
}

// A cluster on a phone: three streets, back to front: you and the nodes;
// each node's log and snapshots; the outboxes and the data channels.
function tallCluster(channels, nodes) {
  const own = nodes.flatMap(id => ownOf(channels, id)),
    front = [...outboxesOf(channels), ...dataOf(channels)];
  const most = 12,
    first = 300;
  const at = new Map(),
    lofts = new Map();
  const step = (394 - 62) / Math.max(1, nodes.length);
  for (const [i, id] of nodes.entries())
    lofts.set(id, {
      x: 62 + i * step,
      w: step - 10,
      floors: STAGGER[i % STAGGER.length] - 4,
      ground: first,
      scale: 0.34,
      deep: 0,
      cote: 26,
      letters: step - 37,
      size: 8,
    });
  const second = first + 50 + floorsOf(own, 2, most) * FLOOR;
  thin(at, own, 4, 0, {ground: second, most, deep: 0, size: 6.5});
  const each = (388 - (front.length - 1) * 8) / Math.max(1, front.length),
    cols = Math.max(1, Math.min(6, Math.floor(each / COL)));
  const third = second + 60 + floorsOf(front, cols, most) * FLOOR;
  row(at, front, 6, each, 8, 1, 6, {ground: third, most, deep: 2000, size: 8});
  return {width: 400, height: third + 8, client: {x: 6, w: 2 * COL, cols: 2, ground: first, most, deep: 0}, lofts, stores: at, lift: 40};
}

// The town for these channels: one node's or a cluster's, wide or tall.
export function layout(channels, {tall: tallNow, height}) {
  const nodes = nodesOf(channels);
  if (nodes.length) return tallNow ? tallCluster(channels, nodes) : wideCluster(channels, nodes, height);
  return tallNow ? tall(channels) : wide(channels, height);
}
