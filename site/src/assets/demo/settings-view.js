// The settings panel: every knob of settings.js, by group, a slider and a
// number each, and the way to the node's own settings (its config and
// background work, the cluster's dynamic config, the wizard again).
//
// data: {cluster: a cluster's (its panel is there), admin: a real node's}
// on: {reset(): every knob back (the panel drawn again), openNode(),
//      openCluster(), erase()}. Returns {nodes}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";
import {GROUPS, KNOBS, set, settings} from "./settings.js";

const UNITS = {ms: () => t("set.unit_ms"), "ms/px": () => t("set.unit_ms_px")};

export function settingsView({cluster, admin}, on) {
  const nodes = [h("p", "field-note", t("set.lede"))];
  for (const group of GROUPS) {
    nodes.push(h("h3", "", t(`set.group_${group}`)));
    for (const k of KNOBS.filter(x => x.group === group)) nodes.push(knob(k));
  }
  nodes.push(h("div", "row", null));
  nodes.at(-1).append(button("btn", t("set.reset"), on.reset));

  nodes.push(h("h3", "", t("set.group_node")));
  const links = h("div", "row", null);
  links.append(button("btn", t("set.open_node"), on.openNode));
  if (cluster) links.append(button("btn", t("set.open_cluster"), on.openCluster));
  if (!admin) links.append(button("btn danger", t("set.wizard"), on.erase));
  nodes.push(links, h("p", "field-note", t(admin ? "set.node_note_admin" : "set.node_note")));
  return {nodes};
}

// One knob: its label, a slider and a number (either sets it, the other
// shows it), its unit, its note; an on/off one, a checkbox.
function knob(k) {
  if (k.bool) return toggle(k);
  const field = h("div", "field knob", null);
  const id = `knob-${k.key}`;
  const label = h("label", "field-label", t(`set.${k.key}`));
  label.htmlFor = id;
  label.id = `${id}-label`;
  const attrs = {min: k.min, max: k.max, step: k.step, value: settings[k.key]};
  const slider = Object.assign(h("input", "knob-range", null), {type: "range", ...attrs});
  slider.setAttribute("aria-labelledby", label.id);
  const number = Object.assign(h("input", "num", null), {type: "number", id, ...attrs});
  const to = v => {
    set(k.key, v);
    slider.value = number.value = String(settings[k.key]);
  };
  slider.addEventListener("input", () => to(slider.value));
  number.addEventListener("change", () => to(number.value));
  const row = h("div", "knob-row", null);
  row.append(slider, number, h("span", "knob-unit", k.unit ? UNITS[k.unit]() : ""));
  field.append(label, row, h("p", "field-note", t(`set.${k.key}_note`)));
  return field;
}

function toggle(k) {
  const field = h("div", "field knob", null);
  const box = Object.assign(h("input", "", null), {type: "checkbox", checked: !!settings[k.key]});
  box.addEventListener("change", () => set(k.key, box.checked ? 1 : 0));
  const label = h("label", "field-label knob-check", null);
  label.append(box, ` ${t(`set.${k.key}`)}`);
  field.append(label, h("p", "field-note", t(`set.${k.key}_note`)));
  return field;
}
