// A store as its app shows it: Telegram's channel view, Discord's text
// channel, a local disk's files; memory has no such view.
// onDelete(message): a reader deleted a message of the channel; without
// it, nothing can be deleted.
import {h} from "../../lib/dom.js";
import {t} from "../../lib/i18n.js";
import {appOf} from "../channels.js";
import {discordView} from "./discord.js";
import {diskView} from "./disk.js";
import {chatModel} from "./model.js";
import {telegramView} from "./telegram.js";

const VIEWS = {
  telegram: (c, onDelete) => telegramView(c, chatModel(c, !!onDelete), onDelete),
  discord: (c, onDelete) => discordView(c, chatModel(c, !!onDelete), onDelete),
  local_disk: c => diskView(c),
};

export function chatView(c, onDelete = null) {
  const view = VIEWS[c.kind];
  return view ? view(c, onDelete) : h("p", "empty", t("admin.no_view", {kind: appOf(c.kind)}));
}
