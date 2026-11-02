// Where everything of the town goes (graph.js draws it): the view's size,
// its streets, and on them you and each channel's block. A node is its
// dovecote, up on a bridge over its own log's and snapshots' blocks: it
// stands on them. A layout:
//   {width, height, lift: how high pigeons climb,
//    client: a block, lofts: Map(node → a node's dovecote; 0: the one node),
//    stores: Map(store → a block)}
// a channel's block {x, w, cols, ground, most: floors at most, deep: its
// wall under the ground, floors?: at least, size?: its letters'}; a
// node's {x, w: the bridge, from one block's side to the other's, roof:
// the blocks' roofs, scale: the dovecote's, size: its name's letters}.
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
// A block's floors for so many windows, `cols` to a floor (`least`: at least).
export const floorsFor = (count, cols, most, least = 0) => Math.max(MIN_FLOORS, least, Math.min(most, Math.ceil(count / cols) + HEADROOM));

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

// A node's own two blocks side by side from `x`, as tall as each other
// (the bridge lies on both): the log's room for the entries between two
// snapshots (`every`: one post each, at most), or for what they hold now.
// `cols(c)`: a block's columns; `fit`: its room as wide as its name needs.
// Returns its loft.
function pair(at, own, x, {cols = () => 2, fit = false, gap, size, every, ground, most, deep, scale}) {
  const width = c => cols(c) * COL;
  const slot = c => (fit ? Math.max(width(c) + 6, c.store.length * size * 0.62 + size * 2.6 + 6) : width(c));
  const need = c => floorsFor(contents(c).count, width(c) / COL, most, c.role === "log" && every ? Math.min(most, Math.ceil(every / (width(c) / COL)) + 1) : 0);
  // As tall as they need, and low enough for the dovecote over them to show
  // (a trial on ground 0 has no such bound).
  const room = ground > 0 ? Math.floor((ground - 12 - size - 44 - 175 * scale - 16) / FLOOR) : Infinity;
  const floors = Math.max(MIN_FLOORS, Math.min(room, Math.max(...own.map(need), 0)));
  const x0 = x;
  for (const c of own) {
    const each = slot(c);
    // `most`: what the dovecote leaves room for (a block full to it is not drawn again).
    at.set(c.store, {
      x: x + (each - width(c)) / 2,
      w: width(c),
      cols: width(c) / COL,
      ground,
      most: Math.max(floors, Math.min(most, room)),
      deep,
      floors,
      size,
    });
    x += each + gap;
  }
  const end = own.length ? x - gap : x0 + 150;
  return {x: x0, w: end - x0, roof: ground - floors * FLOOR - 12, ground, scale, size, alone: !own.length};
}

// The ground a node's blocks need under the view's top for its dovecote
// to show: `trial(own)` lays them out on ground 0; at least `least`.
const groundFor = (trial, own, least) => {
  const loft = trial(own);
  return Math.max(least, Math.ceil(-loft.roof + loft.size + 44 + 175 * loft.scale + 12));
};

// Wide: one street, the view as tall as the screen's shape asks; you, the
// node on its log and snapshots, then the data channels…
function wide(channels, height, every) {
  const data = dataOf(channels);
  const ground = height - 6,
    most = Math.floor((ground - 12 - 70 - height * 0.12) / FLOOR);
  const at = new Map();
  const loft = pair(at, ownOf(channels), 150, {cols: () => 3, fit: true, gap: 10, size: 15, every, ground, most, deep: 2000, scale: 0.8});
  const x = loft.x + loft.w + 50,
    gap = 14;
  row(at, data, x, (1180 - x - (data.length - 1) * gap) / Math.max(1, data.length), gap, 2, 6, {ground, most, deep: 2000});
  return {width: 1200, height, client: {x: 30, w: 3 * COL, cols: 3, ground, most, deep: 2000}, lofts: new Map([[0, loft]]), stores: at, lift: 80};
}

// …or two streets top to bottom, for a phone: you and the node on the
// first, the data channels on the one in front of it.
function tall(channels, every) {
  const data = dataOf(channels);
  const most = 12;
  const at = new Map();
  const first = groundFor(
    own => pair(at, own, 90, {cols: () => 2, fit: true, gap: 8, size: 11, every, ground: 0, most, deep: 0, scale: 0.55}),
    ownOf(channels),
    400,
  );
  const loft = pair(at, ownOf(channels), 90, {cols: () => 2, fit: true, gap: 8, size: 11, every, ground: first, most, deep: 0, scale: 0.55});
  const gap = 10,
    each = (388 - (data.length - 1) * gap) / Math.max(1, data.length);
  const cols = Math.max(2, Math.min(8, Math.floor(each / COL)));
  const second = first + 70 + floorsOf(data, cols, most) * FLOOR + 12;
  row(at, data, 6, each, gap, 2, 8, {ground: second, most, deep: 2000});
  return {width: 400, height: second + 8, client: {x: 6, w: 2 * COL, cols: 2, ground: first, most, deep: 0}, lofts: new Map([[0, loft]]), stores: at, lift: 40};
}

// Blocks as wide as their names need, side by side from `x` (thin: two
// columns); returns where they end.
function thin(at, channels, x, gap, place) {
  for (const c of channels) {
    const each = Math.max(2 * COL + 6, c.store.length * place.size * 0.62 + place.size * 2.6 + 6);
    at.set(c.store, {x: x + (each - 2 * COL) / 2, w: 2 * COL, cols: 2, ...place});
    x += each + gap;
  }
  return x - gap;
}

// A cluster, wide: you; each node on its log and snapshots; the outboxes;
// the data channels. Wider than one node's town.
function wideCluster(channels, nodes, height, every) {
  const data = dataOf(channels);
  const ground = height - 6,
    most = Math.floor((ground - 12 - 70 - height * 0.12) / FLOOR);
  const at = new Map(),
    lofts = new Map(),
    place = {ground, most, deep: 2000};
  let x = 140;
  for (const id of nodes) {
    const loft = pair(at, ownOf(channels, id), x, {fit: true, gap: 4, size: 10, every, ...place, scale: 0.55});
    lofts.set(id, loft);
    x = loft.x + loft.w + 30;
  }
  x = thin(at, outboxesOf(channels), x, 2, {...place, size: 10}) + 24;
  const gap = 14,
    width = Math.max(1200, x + data.length * (4 * COL + gap) + 6);
  row(at, data, x, (width - 20 - x - (data.length - 1) * gap) / Math.max(1, data.length), gap, 2, 6, place);
  return {width, height, client: {x: 30, w: 3 * COL, cols: 3, ground, most, deep: 2000}, lofts, stores: at, lift: 80};
}

// A cluster on a phone: two streets, back to front: the nodes on their
// logs and snapshots; you, the outboxes and the data channels.
function tallCluster(channels, nodes, every) {
  const front = [...outboxesOf(channels), ...dataOf(channels)];
  const most = 12;
  const at = new Map(),
    lofts = new Map();
  const place = ground => ({fit: true, gap: 2, size: 6, every, ground, most, deep: 0, scale: 0.32});
  const first = Math.max(...nodes.map(id => groundFor(own => pair(at, own, 0, place(0)), ownOf(channels, id), 330)));
  let x = 6;
  for (const id of nodes) {
    const loft = pair(at, ownOf(channels, id), x, place(first));
    lofts.set(id, loft);
    x = loft.x + loft.w + 10;
  }
  const each = (388 - 60 - (front.length - 1) * 8) / Math.max(1, front.length),
    cols = Math.max(1, Math.min(6, Math.floor(each / COL)));
  const second = first + 60 + floorsOf(front, cols, most) * FLOOR;
  row(at, front, 66, each, 8, 1, 6, {ground: second, most, deep: 2000, size: 8});
  return {width: 400, height: second + 8, client: {x: 6, w: 2 * COL, cols: 2, ground: second, most, deep: 2000}, lofts, stores: at, lift: 40};
}

// The town for these channels: one node's or a cluster's, wide or tall;
// `every`: the entries between two snapshots.
export function layout(channels, {tall: tallNow, height, every = 0}) {
  const nodes = nodesOf(channels);
  if (nodes.length) return tallNow ? tallCluster(channels, nodes, every) : wideCluster(channels, nodes, height, every);
  return tallNow ? tall(channels, every) : wide(channels, height, every);
}
