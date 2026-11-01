// A channel as its app shows it: Telegram's channel view, Discord's text
// channel; other kinds (a server's disk, memory) have no such view.
// onDelete(message): a reader deleted a message of the channel.
import {h} from "../../lib/dom.js";
import {t} from "../../lib/i18n.js";
import {appOf} from "../channels.js";
import {discordView} from "./discord.js";
import {chatModel} from "./model.js";
import {telegramView} from "./telegram.js";

const VIEWS = {telegram: telegramView, discord: discordView};

export function chatView(c, onDelete = () => {}) {
  const view = VIEWS[c.kind];
  if (!view) return h("p", "empty", t("admin.no_view", {kind: appOf(c.kind)}));
  return view(c, chatModel(c), onDelete);
}
