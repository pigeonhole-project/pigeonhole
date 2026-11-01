// Building the page's elements. Values that come from data go in as text
// (textContent, attributes), never through innerHTML.
const svgNS = "http://www.w3.org/2000/svg";

// h("div", "row", "text") → <div class="row">text</div>.
export function h(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

// A button that is not a form's submit.
export function button(cls, text, onClick) {
  const b = h("button", cls, text);
  b.type = "button";
  if (onClick) b.addEventListener("click", onClick);
  return b;
}

// svg("rect", {x: 1}, parent) → an SVG element with those attributes.
export function svg(tag, attrs = {}, parent = null) {
  const e = document.createElementNS(svgNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

// A 24×24 line icon from one path.
export function icon(d, cls) {
  const sv = svg("svg", {viewBox: "0 0 24 24", class: cls, "aria-hidden": "true"});
  svg("path", {d}, sv);
  return sv;
}

// Enter and Space on a focusable non-button do what a click does.
export function activate(el, fn) {
  el.addEventListener("click", fn);
  el.addEventListener("keydown", e => {
    if (e.key !== "Enter" && e.key !== " ") return;
    e.preventDefault();
    fn(e);
  });
}

export const reducedMotion = () => matchMedia("(prefers-reduced-motion: reduce)").matches;
