// The demo's drawings, in SVG: the pigeons, the dovecote, the channels'
// hutches, the blocks of flats they stand on, the apps' logos.
import {svg} from "../lib/dom.js";

// ---- pigeons: rock doves, flat. Their colours are classes (demo.css: .dv-*);
// the neck's sheen is a gradient, birdDefs() puts it in the picture.
export function birdDefs(root) {
  const g = svg("linearGradient", {id: "dv-iris", x1: 0, y1: 0, x2: 0.6, y2: 1}, svg("defs", {}, root));
  svg("stop", {offset: 0, "stop-color": "#3fae8f"}, g);
  svg("stop", {offset: 0.55, "stop-color": "#4d8f9a"}, g);
  svg("stop", {offset: 1, "stop-color": "#8e5bb5"}, g);
}

// A pigeon sitting, side on, facing right, about 14 tall, its feet at y 8:
// puffed breast, the folded wing with its two bars, the tail down behind.
export function perched(parent, attrs = {}) {
  const outer = svg("g", attrs, parent);
  const g = svg("g", {transform: "translate(0,8) scale(0.55)"}, outer);
  svg("path", {class: "dv-body", d: "M-6,-9 L-19,-4.6 Q-19.6,-3 -18.6,-2.2 L-5,-4.4 Z"}, g);
  svg("path", {class: "dv-band", d: "M-16.4,-5.5 L-19,-4.6 Q-19.6,-3 -18.6,-2.2 L-16,-2.7 Z"}, g);
  svg(
    "path",
    {
      class: "dv-body",
      d: "M-9,-6.4 Q-6,-14.6 2.6,-15.6 Q5.6,-16.4 6.4,-20.6 Q7.2,-24.4 10.6,-24.4 Q13.6,-24.2 13.8,-21.6 L16.6,-20.6 L13.4,-20 Q12.8,-17.6 12.4,-16 Q16.8,-12.4 15.8,-7.6 Q14.4,-2.4 7.6,-2 Q-1,-1.8 -9,-6.4 Z",
    },
    g,
  );
  svg("path", {class: "dv-head", d: "M6.4,-20.6 Q7.2,-24.4 10.6,-24.4 Q13.6,-24.2 13.8,-21.6 L13.4,-20 Q12.8,-17.6 12.6,-16.4 Q9,-17.6 6.4,-20.6 Z"}, g);
  svg("path", {class: "dv-neck", d: "M5.8,-17.4 Q6.6,-18.8 6.8,-20 Q9.4,-17.8 12.4,-16.6 Q14.4,-14.6 14.6,-12.4 Q11,-12.4 8,-14 Q6.2,-15.4 5.8,-17.4 Z"}, g);
  svg("path", {class: "dv-beak", d: "M13.6,-22 L16.8,-20.6 L13.4,-19.8 Z"}, g);
  svg("circle", {class: "dv-cere", cx: 14, cy: -21.6, r: 0.6}, g);
  svg("circle", {class: "dv-eye", cx: 11.2, cy: -21.6, r: 1.05}, g);
  svg("circle", {class: "dv-pupil", cx: 11.4, cy: -21.6, r: 0.48}, g);
  svg("path", {class: "dv-prim", d: "M-4,-8.4 L-15.6,-5 Q-11,-4 -3,-4.8 Z"}, g);
  svg("path", {class: "dv-wing", d: "M-5.6,-8.2 Q-2,-14 5,-13.4 Q8.6,-10.4 7,-6.6 Q0,-4.2 -5.6,-8.2 Z"}, g);
  svg("path", {class: "dv-bar", "stroke-width": 1.2, d: "M-1.6,-12 Q1.4,-9.8 2.2,-6"}, g);
  svg("path", {class: "dv-bar", "stroke-width": 1.2, d: "M-4.4,-10.4 Q-1.8,-8.4 -1,-5.4"}, g);
  svg("path", {class: "dv-feet", "stroke-width": 1.2, d: "M4.6,-2.4 L4.4,-0.2 M3,0 L6.2,0 M8.4,-2.4 L8.2,-0.2 M6.8,0 L10,0"}, g);
  return outer;
}

// A pigeon in flight: its near wing traced from Muybridge's (Animal
// Locomotion, 1887, plate 755, side on, frames 1-5, mirrored to fly right)
// in the body's coordinates: shoulder front, two points along the leading
// edge to the wrist, one to the tip, the tip, three along the primaries'
// trailing edge, one along the secondaries', shoulder back. `pitch`: the
// body's tilt in that frame (deg, - = nose up). Muybridge's 2-4 reach
// forward past the head, a take-off stroke; cruising, the stroke goes down
// and back, so those are swung back about the shoulder.
const swingBack = (w, sx) => w.map((v, i) => (i % 2 === 0 && v > sx ? sx - (v - sx) * 0.7 : v));
const FRAMES = [
  {pitch: -11.6, w: [6.2, -3.1, 11.3, -15.7, 12.4, -27.3, 12, -34.2, 10.8, -39.9, 6.5, -36.2, 2, -31.1, -0.7, -23.7, -3.2, -11.8, -4, -4.6]},
  {pitch: 3.4, w: swingBack([6.2, -2.1, 13.2, 3.5, 19.3, 8.9, 24.5, 14.1, 29.6, 19.6, 24.2, 18.7, 17.4, 15.9, 10.8, 12.3, 6.3, 8.3, 0.4, 2], 6)},
  {pitch: 3.4, w: swingBack([3.8, -3.7, 14.7, 1.5, 24.8, 5.7, 32.9, 10.7, 36.4, 17.3, 32.3, 19.9, 24.8, 19.8, 14, 17.9, 3.7, 14.2, -2.1, 4.2], 5)},
  {pitch: 8.4, w: swingBack([4.4, -8.8, 10.2, -14.5, 13.1, -14.5, 25.7, -9.8, 40.4, -4.4, 35.8, -2.3, 29.9, -1.4, 21.9, 1, 13.8, 3.4, 1.7, 1.3], 5)},
  {pitch: -3.6, w: [2.8, -4.5, 2.1, -12.7, 1.5, -21, 2, -28.7, 2.7, -36.4, 4.4, -32.5, 5.6, -24.6, 5.4, -16.7, 4.3, -9.1, 1.4, -3.4]},
].map(f => ({pitch: f.pitch, w: f.w.map(v => v * 0.95)})); // Muybridge's bird → ours
// Gliding: spread a little, held back along the body, the tip past the tail.
const GLIDE = {pitch: 0, w: [5.6, -3.6, 1, -7.6, -5, -9, -12, -9.4, -21, -8, -18.4, -5.6, -15, -4.6, -11, -3.6, -5, -2.4, 0.6, -1.6]};

const pairs = w => {
  const p = [];
  for (let i = 0; i < w.length; i += 2) p.push([w[i], w[i + 1]]);
  return p;
};
const mix = (a, b, t) => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
const n2 = v => v.toFixed(2);
const at = p => `${n2(p[0])},${n2(p[1])}`;
// A smooth run through points (Catmull-Rom as cubic Béziers), without its M.
function run(p) {
  let d = "";
  for (let i = 0; i < p.length - 1; i++) {
    const p0 = p[i - 1] || p[i],
      p1 = p[i],
      p2 = p[i + 1],
      p3 = p[i + 2] || p2;
    d += ` C${at([p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6])} ${at([p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6])} ${at(p2)}`;
  }
  return d;
}
// The primaries' ends from the tip back along the trailing edge, notched.
function fingers(span) {
  let d = "";
  for (let i = 0; i < span.length - 1; i++) {
    const p = span[i],
      q = span[i + 1],
      l = Math.hypot(q[0] - p[0], q[1] - p[1]) || 1;
    for (const j of [1, 2]) {
      const notch = mix(p, q, (j - 0.5) / 2);
      d += ` L${at([notch[0] - ((q[1] - p[1]) / l) * 0.8, notch[1] + ((q[0] - p[0]) / l) * 0.8])} L${at(mix(p, q, j / 2))}`;
    }
  }
  return d;
}
// The wing's paths: its outline (the primaries' grey), the arm (the
// secondaries), the pale coverts over the arm's front, the two black bars.
function wingPaths(w) {
  const p = pairs(w);
  const closed = pts => `M${at(pts[0])}${run(pts)} Z`;
  // A wing swung back has its wrist's chord the other way round: keep the bars from crossing.
  const flipped = (p[9][0] - p[0][0]) * (p[8][0] - p[2][0]) + (p[9][1] - p[0][1]) * (p[8][1] - p[2][1]) < 0;
  const bar = u => `M${at(mix(p[0], p[9], u))} L${at(mix(p[2], p[8], flipped ? 1 - u : u))}`;
  return {
    outline: `M${at(p[0])}${run(p.slice(0, 5))}${fingers(p.slice(4, 8))}${run(p.slice(7, 10))} Z`,
    arm: closed([p[0], p[1], p[2], mix(p[2], p[7], 0.15), p[7], p[8], p[9]]),
    coverts: closed([p[0], p[1], mix(p[1], p[2], 0.8), mix(p[2], p[8], 0.5), mix(p[9], p[8], 0.4), p[9]]),
    bars: `${bar(0.55)} ${bar(0.78)}`,
  };
}
const smooth = x => x * x * (3 - 2 * x);
function frameAt(k) {
  const x = (k % 1) * FRAMES.length,
    i = Math.floor(x),
    t = smooth(x - i);
  const a = FRAMES[i],
    b = FRAMES[(i + 1) % FRAMES.length];
  return {pitch: a.pitch + (b.pitch - a.pitch) * t, w: a.w.map((v, j) => v + (b.w[j] - v) * t)};
}

// A pigeon in flight, side on, facing right, about 38 long; move it with
// beat(bird, phase) or glide(bird).
export function flying(parent, attrs = {}) {
  const g = svg("g", attrs, parent);
  const body = svg("g", {}, g); // pitches with the beat
  const farG = svg("g", {transform: "translate(-1.6,-0.8)"}, body);
  const far = svg("path", {class: "dv-prim"}, farG);
  svg("path", {class: "dv-body", d: "M-8,-1.2 L-18,-3.8 Q-19.8,0 -18,3.8 L-8,2.6 Z"}, body);
  svg("path", {class: "dv-band", d: "M-15.8,-3.3 L-18,-3.8 Q-19.8,0 -18,3.8 L-15.8,3.3 Q-16.6,0 -15.8,-3.3 Z"}, body);
  svg(
    "path",
    {
      class: "dv-body",
      d: "M-9,-1.8 Q-3,-5.4 5,-4.4 Q8.6,-4.4 10.4,-6.2 Q12.4,-8 14.6,-7.2 Q16.2,-6.4 15.8,-4.6 L15.4,-3.2 Q14,-0.6 11.6,0.6 Q8,5.4 1,5.6 Q-6,5 -9,2.6 Z",
    },
    body,
  );
  svg("path", {class: "dv-head", d: "M10.4,-6.2 Q12.4,-8 14.6,-7.2 Q16.2,-6.4 15.8,-4.6 L15.4,-3.2 Q14,-0.8 11.8,0.2 Q11.6,-3.4 10.4,-6.2 Z"}, body);
  svg("path", {class: "dv-neck", d: "M8,-4.6 Q9.6,-5 10.6,-6 Q11.8,-3 11.8,0.4 Q10.2,1.6 8.4,2 Q9,-1.4 8,-4.6 Z"}, body);
  svg("path", {class: "dv-beak", d: "M15.8,-4.8 L18.8,-3.6 L15.6,-3.2 Z"}, body);
  svg("circle", {class: "dv-cere", cx: 16.1, cy: -4.7, r: 0.6}, body);
  svg("circle", {class: "dv-eye", cx: 13.9, cy: -5.1, r: 1}, body);
  svg("circle", {class: "dv-pupil", cx: 14.1, cy: -5.1, r: 0.45}, body);
  svg("path", {class: "dv-feet", "stroke-width": 1, d: "M-1,5.4 L-4,6"}, body);
  const near = svg("g", {}, body);
  const bird = {
    g,
    body,
    far,
    farG,
    outline: svg("path", {class: "dv-prim"}, near),
    arm: svg("path", {class: "dv-body"}, near),
    coverts: svg("path", {class: "dv-wing"}, near),
    bars: svg("path", {class: "dv-bar", "stroke-width": 1.1}, near),
  };
  pose(bird, FRAMES[0]);
  return bird;
}
function pose(bird, frame) {
  const p = wingPaths(frame.w);
  for (const k of ["outline", "arm", "coverts", "bars"]) bird[k].setAttribute("d", p[k]);
  bird.far.setAttribute("d", p.outline);
  // The far wing shows over the back only while the wings are up.
  const tip = frame.w[9];
  bird.farG.setAttribute("opacity", tip < -10 ? Math.min(0.6, (-tip - 10) / 10).toFixed(2) : "0");
  bird.body.setAttribute("transform", `rotate(${(frame.pitch * 0.8).toFixed(1)})`);
}
// A wing beat at phase k (0..1), through the frames.
export const beat = (bird, k) => pose(bird, frameAt(k));
// Gliding, the wings held back along the body.
export const glide = bird => pose(bird, GLIDE);

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

// ---- a channel: a hutch on a roof, its foot at y 0, the app's logo on
// its gable; `shut`: the channel is down. Returns its door's y and the
// gable's top (for a count over it).
export function hutch(g, kind, title, shut) {
  svg("rect", {class: "cote", x: -22, y: -32, width: 44, height: 32}, g);
  svg("path", {class: "cote-roof", d: "M-27,-30 L0,-50 L27,-30 Z"}, g);
  logo(kind, g, 0, -36, title, 6.5);
  svg("path", {class: "cote-hole", d: "M-8,-2 L-8,-14 A8,8 0 0 1 8,-14 L8,-2 Z"}, g);
  if (shut) {
    svg("rect", {class: "shutter", x: -9, y: -22, width: 9, height: 20}, g);
    svg("rect", {class: "shutter", x: 0, y: -22, width: 9, height: 20}, g);
  }
  svg("rect", {class: "cote-board", x: -26, y: -1, width: 52, height: 4, rx: 1}, g);
  return {door: -8, top: -50};
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

// A blue plaque on a block (as a Soviet one's: the street, the number):
// its title and, under it, a line that can change (returned).
export function plaque(parent, x, y, title, sub, {center = false} = {}) {
  const w = Math.max(title.length * 8.4, (sub || "").length * 6.4) + 24;
  const g = svg("g", {class: "plaque", transform: `translate(${center ? x - w / 2 : x},${y})`}, parent);
  svg("rect", {class: "plaque-board", width: w, height: sub == null ? 26 : 42, rx: 3}, g);
  svg("text", {class: "plaque-title", x: 12, y: 18}, g).textContent = title;
  const line = svg("text", {class: "plaque-sub", x: 12, y: 34}, g);
  if (sub != null) line.textContent = sub;
  return line;
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
