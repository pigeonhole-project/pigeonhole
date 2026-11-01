// A local disk as a file manager would show it: the store's directory, its
// segment files (the newest open), and the records in each, in the order
// they were written: a blob's name and size, a log batch's positions, the
// deleted ones struck through.
//
// The `channels` op's entry for a disk: {store, scope: the directory,
// files: [{name, size, records: [{key, name, size, dead, chained, log?}]}],
// total: the live records, error?}.
import {h} from "../../lib/dom.js";
import {bytes, t} from "../../lib/i18n.js";
import {logNote} from "./model.js";

export function diskView(c) {
  const head = h("div", "disk-head");
  head.append(h("b", "", c.store), h("span", "disk-path", c.scope));
  const tree = h("div", "disk-tree");
  const files = c.files || [];
  if (c.error) tree.append(h("p", "disk-empty", c.error));
  else if (!files.length) tree.append(h("p", "disk-empty", t("demo.no_files")));
  for (const [i, f] of files.entries()) tree.append(segment(f, i === files.length - 1));
  const foot = h("div", "disk-foot", t("demo.disk_records", {n: c.total ?? 0}));
  const app = h("div", "app disk");
  app.append(head, tree, foot);
  return app;
}

function segment(f, open) {
  const box = h("details", "disk-file");
  box.open = open;
  const name = h("summary", "");
  name.append(h("span", "disk-name", f.name), h("span", "disk-size", bytes(f.size)));
  const list = h("ol", "disk-records");
  for (const r of f.records) list.append(record(r));
  box.append(name, list);
  return box;
}

function record(r) {
  const row = h("li", `disk-record${r.dead ? " gone" : ""}${r.chained ? " chained" : ""}`);
  row.append(h("span", "disk-name", r.name), h("span", "disk-size", bytes(r.size)));
  const note = logNote(r.log);
  if (note) row.append(h("div", "post-log", note));
  return row;
}
