// The metadata log's panel, two views of one log: its entries decoded
// (`readable`), or the posts of its channel (`channel`).
//
// log: the `log` op's reply {head, base: {at} | null, entries: [{position,
// term, kind, commands: [{namespace, command}]}]}; channel: the log's channel.
import {button, h} from "../lib/dom.js";
import {bytes, t} from "../lib/i18n.js";
import {chatView} from "./chat/index.js";

export const VIEWS = ["readable", "channel"];

// The two views' tabs, `view` on; onView(v): another picked. `kind`: the
// log's store (on a local disk, its records are files, not posts).
export function logTabs(view, onView, kind) {
  const tabs = h("div", "tabs");
  tabs.setAttribute("role", "tablist");
  for (const v of VIEWS) {
    const label = v === "channel" && kind === "local_disk" ? "disk" : v;
    const b = button("tab", t(`demo.log_view_${label}`), () => onView(v));
    b.setAttribute("role", "tab");
    b.setAttribute("aria-selected", String(view === v));
    tabs.append(b);
  }
  return tabs;
}

export function logView(log, channel, view, onView) {
  const out = [logTabs(view, onView, channel?.kind), h("p", "log-note", t("demo.log_note"))];
  if (view === "channel") {
    if (channel) out.push(chatView(channel));
  } else if (!log) {
    out.push(h("p", "empty", t("try.loading")));
  } else {
    out.push(...entries(log, channel));
  }
  return out;
}

function entries(log, channel) {
  // Position → the post holding it.
  const postOf = new Map();
  for (const m of channel?.messages || []) {
    const l = m.log;
    if (l?.what === "batch" && l.first != null) for (let p = l.first; p <= l.last; p++) postOf.set(p, m.id);
  }
  const head = h("div", "log-head");
  head.append(h("span", "", t("demo.log_head", {n: log.head})), h("span", "", log.base ? t("demo.log_base", {n: log.base.at}) : t("demo.log_nobase")));
  const list = h("ol", "entries");
  for (const e of [...log.entries].reverse()) list.append(entryView(e, {base: log.base, post: postOf.get(e.position)}));
  return [head, list];
}

// An entry: its position, what it holds, where it is (the post that holds
// it, or its term); it opens into its commands. `base`: the snapshot's.
export function entryView(e, {base = null, post = null, open = false} = {}) {
  const li = h("li", `entry${base && e.position === base.at + 1 ? " base" : ""}`);
  const what = e.kind === "commands" ? e.commands.map(c => `${c.namespace} ${summary(c).verb}`).join("; ") : t(`demo.entry_${e.kind}`);
  const where = post != null ? t("demo.in_post", {id: String(post).length > 8 ? `…${String(post).slice(-6)}` : post}) : t("demo.term", {n: e.term});
  const cmds = h("ul", "cmds");
  cmds.hidden = !open;
  for (const c of e.commands) cmds.append(command(c));
  if (!e.commands.length) cmds.append(h("li", "cmd", t(`demo.entry_${e.kind}_long`)));
  const btn = button("", null, () => {
    cmds.hidden = !cmds.hidden;
    btn.setAttribute("aria-expanded", String(!cmds.hidden));
  });
  btn.setAttribute("aria-expanded", String(open));
  btn.append(h("span", "pos", String(e.position)), h("span", "what", what), h("span", "where", where));
  li.append(btn, cmds);
  return li;
}

function command(c) {
  const row = h("li", "cmd");
  const sm = summary(c);
  row.append(h("span", "ns", c.namespace), h("span", "verb", sm.verb));
  if (sm.fields) row.append(h("span", "fields", sm.fields));
  const raw = h("details");
  raw.append(h("summary", "", t("demo.raw")), h("pre", "", JSON.stringify(c.command, null, 2)));
  row.append(raw);
  return row;
}

// A command in a line: its verb and the fields that say what it touched.
export function summary(cmd) {
  const [verb, f] = Object.entries(cmd.command || {})[0] || ["?", {}];
  const parts = [];
  if (f && typeof f === "object") {
    if (f.key) parts.push(`${f.bucket ? `${f.bucket}/` : ""}${f.key}`);
    else if (f.name && cmd.namespace === "s3") parts.push(f.name);
    if (f.dst_key || f.to) parts.push(`→ ${f.dst_key || f.to}`);
    if (f.size != null) parts.push(bytes(+f.size));
    if (f.chunk_id != null) parts.push(t("demo.chunk_n", {n: f.chunk_id}));
    if (f.logical_size != null) parts.push(bytes(+f.logical_size));
    if (Array.isArray(f.chunk_ids)) parts.push(t("demo.chunks_list", {ids: f.chunk_ids.join(", ")}));
    if (Array.isArray(f.replicas)) parts.push(t("demo.replicas", {stores: f.replicas.map(r => r.instance).join(", ")}));
    if (Array.isArray(f.instances)) parts.push(f.instances.map(i => i[0]).join(", "));
  }
  return {verb, fields: parts.join(", ")};
}
