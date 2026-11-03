// A channel's panel: its kin (the node it belongs to, a data channel's
// other copies and shards), a data channel's faults, its bots' budgets, and
// the channel as its app shows it (chat/).
//
// props: {channel, channels: every channel, budgets: the `budgets` op's, admin}.
// on: {faults: faults-view.js's, deleteMessage(message)}.
// Returns {nodes, setBudgets(budgets)}: the budgets polled again.
import {budgetsOf, budgetTable} from "./budgets.js";
import {kinOf} from "./channels.js";
import {chatView} from "./chat/index.js";
import {faultsView} from "./faults-view.js";

export function storeView({channel: c, channels, budgets, admin}, on) {
  let table = budgetTable(budgetsOf(c, budgets));
  const nodes = [table, chatView(c, on.deleteMessage)];
  if (c.role === "data") nodes.unshift(faultsView(c, admin, on.faults));
  const kin = kinOf(c, channels);
  if (kin) nodes.unshift(kin);
  return {
    nodes,
    setBudgets(next) {
      const fresh = budgetTable(budgetsOf(c, next));
      table.replaceWith(fresh);
      table = fresh;
    },
  };
}
