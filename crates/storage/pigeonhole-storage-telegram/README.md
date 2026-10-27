# pigeonhole-storage-telegram

Telegram Bot API blob backend (`BlobBackend` + `Sweepable` + `TypedBootstrapPointer`).

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

For sweeper GC in channels / supergroups grant the bot **`can_delete_messages`**:
without it, garbage older than 48 hours (and every message the bot did not
send) cannot be deleted and stays in the chat. The sweeper treats every
message id up to its horizon as a candidate, so keep nothing else in a blob
chat (see docs/sweep-invariants.md).

Batch deletes use `deleteMessages` (up to **100** `message_ids` per call);
missing messages are skipped (treated as success).
