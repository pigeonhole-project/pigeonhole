// The demo's drawings, in SVG: the pigeons, the dovecote, the apps' logos.
import {svg} from "../lib/dom.js";

// A pigeon in flight, side on, facing right: the body, and the wing raised
// behind it (it beats about the shoulder at 2.8,-1.2).
export const FLY_BODY = "M-11,1.5 L-6.5,-0.2 Q-2,-1.6 3,-1.2 Q4.4,-4 7,-3.8 Q9,-3.6 9.2,-2.2 L11,-1.6 L9,-1 Q8.6,1.4 6.2,2.4 Q3.6,4.4 -2,4 Q-7,3.6 -11,1.5 Z";
export const FLY_WING = "M2.8,-1.2 Q1.6,-6.4 -2.4,-11 Q-2.2,-8.4 -4.4,-9.6 Q-3.8,-7.2 -6.4,-8.2 Q-5.2,-5.8 -7.6,-6.2 Q-5.4,-3.4 -3.8,-0.8 Z";

// A pigeon sitting, side on, facing right: the body, its legs and its wing's line.
export function perched(parent, attrs) {
  const bird = svg("g", attrs, parent);
  svg(
    "path",
    {
      class: "bird-body",
      d: "M-10,3.5 L-5,1 Q-3,-2.5 1.5,-2.5 Q2,-6.5 5,-6.5 Q7.4,-6.5 7.6,-4.6 L9.4,-4 L7.6,-3.4 Q7.4,-2 6.2,-1.4 Q9,1 7.4,4.2 Q5.4,6.6 1.4,6.6 L-3,6.2 Q-6.4,5.4 -10,3.5 Z",
    },
    bird,
  );
  svg("path", {class: "bird-lines", d: "M1.2,6.4 L1.2,8.8 M3.4,6.4 L3.4,8.8 M-5.6,1.6 Q-1,-0.6 3.4,2.6"}, bird);
  svg("circle", {class: "bird-eye", cx: 5.6, cy: -4.4, r: 0.8}, bird);
  return bird;
}

// A pigeon tower: a round body narrowing upward, a cone roof with a lantern
// and a finial, two rows of arched entrances with perches, a plinth. Returns
// the entrances ({x, y, d: the arch's outline}), the top row first.
export function dovecote(g) {
  const line = {fill: "none", stroke: "var(--slate)", "stroke-width": 1.5, "stroke-linecap": "round"};
  svg("rect", {class: "shape", x: -78, y: 64, width: 156, height: 12, rx: 3}, g);
  svg("path", {class: "shape", d: "M-66,64 L-58,-18 L58,-18 L66,64 Z"}, g);
  svg("rect", {class: "shape", x: -70, y: -26, width: 140, height: 9, rx: 3}, g);
  svg("path", {class: "shape", d: "M-80,-26 Q-28,-64 -9,-98 L9,-98 Q28,-64 80,-26 Z"}, g);
  svg("path", {...line, d: "M-60,-38 Q0,-52 60,-38"}, g);
  svg("path", {...line, d: "M-38,-60 Q0,-72 38,-60"}, g);
  svg("rect", {class: "shape", x: -10, y: -112, width: 20, height: 15, rx: 2}, g);
  svg("path", {class: "shape", d: "M-14,-112 L0,-124 L14,-112 Z"}, g);
  svg("path", {...line, d: "M0,-124 L0,-136"}, g);
  svg("circle", {cx: 0, cy: -105, r: 3.5, fill: "var(--wing-2)"}, g);
  // Entrances: an arch over a dark hole, a perch under it.
  const windows = [];
  const arch = (x, y) => {
    const d = `M${x - 10},${y + 14} L${x - 10},${y} A10,10 0 0 1 ${x + 10},${y} L${x + 10},${y + 14} Z`;
    svg("path", {d, fill: "var(--wing-2)", "fill-opacity": 0.78}, g);
    svg("path", {...line, d: `M${x - 15},${y + 18} L${x + 15},${y + 18}`}, g);
    windows.push({x, y, d});
  };
  for (const x of [-34, 0, 34]) arch(x, -2);
  for (const x of [-36, 0, 36]) arch(x, 34);
  return windows;
}

// An entrance no one lives in: boarded up.
export function boarded(parent, w) {
  const g = svg("g", {class: "win-boarded"}, parent);
  svg("path", {class: "win-board-back", d: w.d}, g);
  svg("path", {class: "win-plank", d: `M${w.x - 9},${w.y - 4} L${w.x + 9},${w.y + 12}`}, g);
  svg("path", {class: "win-plank", d: `M${w.x + 9},${w.y - 4} L${w.x - 9},${w.y + 12}`}, g);
}

// A store's mark on its card: the app's logo (simplified), else a dot.
export function logo(kind, parent, x, y, title) {
  const mark = svg("g", {class: "badge", transform: `translate(${x},${y})`}, parent);
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
