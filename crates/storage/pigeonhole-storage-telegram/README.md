# pigeonhole-storage-telegram

Telegram Bot API blob backend (`LegacyBlobStoreTg` / `LegacyBlobStore` and typed
`BlobBackend` + `Sweepable` + `TypedBootstrapPointer`).

## `deleteMessage` / `deleteMessages` age limits

Bot API ([`deleteMessage`](https://core.telegram.org/bots/api#deletemessage),
[`deleteMessages`](https://core.telegram.org/bots/api#deletemessages)):

- **Default:** a message can only be deleted if it was sent **less than 48 hours
  ago**.
- **Groups:** if the bot is a chat **administrator**, it can delete **any**
  message there (including older than 48h).
- **Supergroups / channels:** if the bot has the **`can_delete_messages`**
  administrator right, it can delete **any** message there (no 48h age cap for
  GC). Without that right, bots are limited to their own outgoing messages and
  the 48h window (channels also need `can_post_messages` to delete their own).

Pigeonhole expects the bot to be admin with pin rights for bootstrap; for
unbounded sweeper GC in channels/supergroups, also grant **`can_delete_messages`**.
If that right is missing, the sweeper grace interval must stay **≥ 48 hours**
so pending deletes remain within the Bot API window.

Batch deletes use `deleteMessages` (up to **100** `message_ids` per call);
missing messages are skipped (treated as success).
