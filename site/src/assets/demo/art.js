// The demo's drawings, in SVG: the pigeons, the dovecote, the channels'
// hutches, the blocks of flats they stand on, the apps' logos.
import {svg} from "../lib/dom.js";
import {FLIGHT, SITTING} from "./pigeon-art.js";

// ---- pigeons: Itay Alon's (pigeon-art.js), drawn once into the picture as
// symbols (birdDefs()), each bird a <use> of the drawing it is at.
const LEGS = [
  ["#554645", 3.2, "M76,79 L75,90 M84,79 L84,90"],
  ["#C24059", 1.8, "M76,79 L75,90 M71,91 L79,91 M84,79 L84,90 M80,91 L88,91"],
];
export function birdDefs(root) {
  const defs = svg("defs", {}, root);
  const shapes = (parent, list) => {
    for (const [fill, d] of list) svg("path", {fill, d}, parent);
  };
  for (const [i, frame] of FLIGHT.entries()) shapes(svg("symbol", {id: `pg-${i}`, viewBox: "0 0 141 110", width: 141, height: 110}, defs), frame);
  const sit = svg("symbol", {id: "pg-sit", viewBox: "0 0 141 110", width: 141, height: 110}, defs);
  shapes(svg("g", {transform: "rotate(-10 80 60)"}, sit), SITTING);
  for (const [stroke, width, d] of LEGS) svg("path", {d, fill: "none", stroke, "stroke-width": width, "stroke-linecap": "round"}, sit);
}

// A pigeon sitting, side on, facing right, about 14 tall, its feet at y 8.
export function perched(parent, attrs = {}) {
  const g = svg("g", attrs, parent);
  svg("use", {href: "#pg-sit", transform: "translate(0,8) scale(0.2) translate(-80,-91)"}, g);
  return g;
}

// A pigeon in flight, side on, facing right, about 38 long, centred on its
// body; move it with beat(bird, phase) or glide(bird).
export function flying(parent, attrs = {}) {
  const g = svg("g", attrs, parent);
  const use = svg("use", {href: "#pg-0", transform: "scale(0.34) translate(-74,-62)"}, g);
  return {g, use, at: 0};
}
const show = (bird, i) => {
  if (bird.at === i) return;
  bird.at = i;
  bird.use.setAttribute("href", `#pg-${i}`);
};
// A wing beat at phase k (0..1), through the nine drawings.
export const beat = (bird, k) => show(bird, Math.floor((((k % 1) + 1) % 1) * FLIGHT.length));
// Gliding: the drawing with the wings swept back along the body.
export const glide = bird => show(bird, 3);

const n2 = v => v.toFixed(2);

// ---- the dovecote: the node, on its block's roof. Its foot is at y 0;
// two rows of three arched entrances, the background tasks' windows.
// Returns the entrances ({x, y, d: the arch's outline}), the top row first.
export function dovecote(g) {
  svg("rect", {class: "cote-board", x: -70, y: -6, width: 140, height: 6, rx: 1.5}, g);
  svg("rect", {class: "cote", x: -60, y: -86, width: 120, height: 80}, g);
  svg("path", {class: "cote-roof", d: "M-72,-84 L0,-140 L72,-84 Z"}, g);
  svg("rect", {class: "cote", x: -11, y: -160, width: 22, height: 24}, g);
  svg("path", {class: "cote-roof", d: "M-16,-158 L0,-172 L16,-158 Z"}, g);
  svg("rect", {class: "cote-hole", x: -5, y: -153, width: 10, height: 13, rx: 5}, g);
  const windows = [];
  const arch = (x, y) => {
    const d = `M${x - 10},${y + 14} L${x - 10},${y} A10,10 0 0 1 ${x + 10},${y} L${x + 10},${y + 14} Z`;
    svg("path", {class: "cote-hole", d}, g);
    svg("rect", {class: "cote-board", x: x - 15, y: y + 14, width: 30, height: 3, rx: 1}, g);
    windows.push({x, y, d});
  };
  for (const x of [-36, 0, 36]) arch(x, -64);
  for (const x of [-36, 0, 36]) arch(x, -32);
  return windows;
}

// An entrance no task lives in: shuttered.
export function boarded(parent, w) {
  const g = svg("g", {class: "win-boarded"}, parent);
  svg("rect", {class: "shutter", x: w.x - 11, y: w.y - 8, width: 11, height: 22, rx: 1}, g);
  svg("rect", {class: "shutter", x: w.x, y: w.y - 8, width: 11, height: 22, rx: 1}, g);
}

// ---- a channel: a whole block of flats, a window to a message. Its floors
// from `ground` up, `cols` windows to a floor; returns its roof's y and
// each window's box ({x, y, w, h}), the ground floor's first, row by row.
// (The windows' panes are the caller's to draw: what each holds.)
export const FLOOR = 22;
export function tower(parent, x, ground, w, floors, cols, {shade = 1, deep = 2000} = {}) {
  const g = svg("g", {class: "block"}, parent);
  const roof = ground - floors * FLOOR - 12,
    pw = w / cols;
  svg("rect", {class: `wall-${shade}`, x, y: roof, width: w, height: ground - roof + deep}, g);
  svg("rect", {class: "parapet", x: x - 2, y: roof - 4, width: w + 4, height: 5}, g);
  for (let c = 0; c < cols; c++) svg("rect", {class: "vent", x: x + c * pw + pw / 2 - 4, y: roof + 4, width: 8, height: 4, rx: 1}, g);
  let seams = "";
  for (let c = 1; c < cols; c++) seams += `M${n2(x + c * pw)},${roof + 12} L${n2(x + c * pw)},${ground + deep} `;
  const slots = [];
  for (let r = 0; r < floors; r++) {
    const fy = ground - (r + 1) * FLOOR;
    seams += `M${x},${fy + FLOOR} L${x + w},${fy + FLOOR} `;
    for (let c = 0; c < cols; c++) slots.push({x: x + c * pw + pw / 2 - 6, y: fy + 5, w: 12, h: 13});
  }
  svg("path", {class: "seam", d: seams}, g);
  return {roof, slots, g};
}

// Letters on a roof, as on a Soviet block's (a slogan on a lattice), red by
// day, neon by night: a name, the app's emblem before it if `kind`, a
// count after it. `x` the letters' middle; returns the count's text.
export function roofLetters(parent, x, roof, name, {kind = null, app = "", size = 15} = {}) {
  const g = svg("g", {class: "roof-letters"}, parent);
  const w = name.length * size * 0.62 + (kind ? size + 4 : 0) + size * 1.6,
    left = x - w / 2,
    base = roof - 9;
  // the lattice the letters stand on
  let d = `M${left},${base + 2} L${left + w},${base + 2} M${left},${roof - 4} L${left + w},${roof - 4}`;
  for (let i = 0; i <= Math.ceil(w / 12); i++) {
    const a = Math.min(left + i * 12, left + w);
    d += ` M${a},${roof - 4} L${Math.min(a + 6, left + w)},${base + 2}`;
  }
  svg("path", {class: "lattice", d}, g);
  let at = left;
  if (kind) {
    logo(kind, g, at + size / 2, base - size * 0.36, app, size / 2);
    at += size + 4;
  }
  svg("text", {class: "letters", x: at, y: base, "font-size": size}, g).textContent = name;
  return svg("text", {class: "letters count", x: left + w, y: base, "font-size": size * 0.8, "text-anchor": "end"}, g);
}

// ---- a Soviet block of flats: concrete panels and their seams, a window to
// a panel, stacks of loggias with coloured screens, the attic's vents, the
// lift's machine room and aerials on the roof. Its roof at y, `h` down from
// it; `rnd` a seeded random (which windows are lit). Returns its group.
export function block(parent, x, y, w, h, rnd, {shade = 1, loggias = [], room = null, aerials = 1, floor = 26} = {}) {
  const cols = Math.max(2, Math.round(w / 30)),
    pw = w / cols;
  const g = svg("g", {class: "block"}, parent);
  if (room != null) {
    svg("rect", {class: "wall-3", x: x + room, y: y - 20, width: 36, height: 20}, g);
    svg("rect", {class: "parapet", x: x + room - 2, y: y - 22, width: 40, height: 3}, g);
  }
  for (let a = 0; a < aerials; a++) {
    const ax = x + w * (0.25 + (0.5 * a) / Math.max(1, aerials - 1)) + (rnd() - 0.5) * 30;
    svg(
      "path",
      {
        class: "aerial",
        d: `M${ax},${y} L${ax},${y - 30} M${ax - 9},${y - 26} L${ax + 9},${y - 26} M${ax - 7},${y - 20} L${ax + 7},${y - 20} M${ax - 5},${y - 14} L${ax + 5},${y - 14}`,
      },
      g,
    );
  }
  svg("rect", {class: `wall-${shade}`, x, y, width: w, height: h}, g);
  svg("rect", {class: "parapet", x: x - 2, y: y - 4, width: w + 4, height: 5}, g);
  for (let c = 0; c < cols; c++) svg("rect", {class: "vent", x: x + c * pw + pw / 2 - 4, y: y + 5, width: 8, height: 4, rx: 1}, g);
  const top = y + 14;
  let seams = "";
  for (let c = 1; c < cols; c++) seams += `M${n2(x + c * pw)},${top} L${n2(x + c * pw)},${y + h} `;
  for (let r = 0; top + r * floor < y + h; r++) {
    const fy = top + r * floor;
    seams += `M${x},${fy} L${x + w},${fy} `;
    for (let c = 0; c < cols; c++) {
      const px = x + c * pw,
        lit = rnd() < 0.08 ? " lit" : "";
      if (loggias.includes(c)) {
        svg("rect", {class: `pane${lit}`, x: px + pw / 2 - 6, y: fy + 4, width: 12, height: 13}, g);
        svg("rect", {class: `screen-${c % 3}`, x: px + 2, y: fy + 14, width: pw - 4, height: 9}, g);
        svg("rect", {class: "parapet", x: px + 1, y: fy + 23, width: pw - 2, height: 2}, g);
      } else {
        svg("rect", {class: `pane${lit}`, x: px + pw / 2 - 7, y: fy + 6, width: 14, height: 14}, g);
      }
    }
  }
  svg("path", {class: "seam", d: seams}, g);
  return g;
}

// A store's mark: the app's logo (simplified), a drive for a local disk, else a dot.
export function logo(kind, parent, x, y, title, r = 11) {
  const mark = svg("g", {class: "badge", transform: `translate(${x},${y}) scale(${r / 11})`}, parent);
  if (kind === "telegram") {
    svg("circle", {r: 11, fill: "#2AABEE"}, mark);
    // The paper plane.
    svg(
      "path",
      {
        fill: "#fff",
        d: "M-6.2,-0.4 L5.8,-5.2 Q6.6,-5.4 6.4,-4.4 L4.4,5 Q4.2,5.8 3.4,5.3 L0.4,3.1 L-1.1,4.6 Q-1.6,5 -1.7,4.3 L-1.9,1.7 L3.9,-3.6 L-2.8,0.9 L-6,-0.1 Q-6.8,-0.3 -6.2,-0.4 Z",
      },
      mark,
    );
  } else if (kind === "discord") {
    svg("circle", {r: 11, fill: "#5865F2"}, mark);
    // The controller-shaped face, its two eyes.
    svg(
      "path",
      {
        fill: "#fff",
        d: "M-5.6,-3.9 Q-3.4,-5 -1.6,-5.2 L-1.1,-4.3 Q0,-4.5 1.1,-4.3 L1.6,-5.2 Q3.4,-5 5.6,-3.9 Q7.4,-0.6 7.1,3.7 Q5.3,5.1 3.2,5.6 L2.4,4.4 Q0,5.1 -2.4,4.4 L-3.2,5.6 Q-5.3,5.1 -7.1,3.7 Q-7.4,-0.6 -5.6,-3.9 Z",
      },
      mark,
    );
    svg("ellipse", {cx: -2.4, cy: 0.9, rx: 1.3, ry: 1.5, fill: "#5865F2"}, mark);
    svg("ellipse", {cx: 2.4, cy: 0.9, rx: 1.3, ry: 1.5, fill: "#5865F2"}, mark);
  } else if (kind === "local_disk") {
    // A drive: its case, a slot, a light.
    svg("rect", {x: -10, y: -7, width: 20, height: 14, rx: 3, fill: "var(--slate)"}, mark);
    svg("path", {d: "M-6,3 L3,3", stroke: "var(--plume)", "stroke-width": 1.6, "stroke-linecap": "round"}, mark);
    svg("circle", {cx: 6, cy: 3, r: 1.4, fill: "var(--teal)"}, mark);
  } else {
    svg("circle", {r: 9, fill: "var(--slate)"}, mark);
  }
  svg("title", {}, mark).textContent = title;
  return mark;
}
