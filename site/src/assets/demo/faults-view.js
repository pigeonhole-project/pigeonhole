// Breaking a data channel: down (every call fails), wiped (its messages
// gone behind the node's back); a check finds what is gone, repair puts it
// back. A real node's channels only get the check and repair.
//
// on: {outage(), wipe(), verify(), repair()}.
import {button, h} from "../lib/dom.js";
import {t} from "../lib/i18n.js";

export function faultsView(c, admin, on) {
  const box = h("div", "faults");
  box.append(h("p", "faults-note", t(admin ? "admin.channel_note" : c.down ? "demo.fault_down_note" : "demo.fault_note")));
  const row = h("div", "row");
  if (!admin) {
    row.append(
      button(`btn ${c.down ? "primary" : ""}`, t(c.down ? "demo.bring_back" : "demo.take_down"), on.outage),
      button("btn danger", t("demo.wipe"), on.wipe),
    );
  }
  row.append(button("btn", t("demo.verify"), on.verify), button("btn", t("demo.repair"), on.repair));
  box.append(row);
  return box;
}
