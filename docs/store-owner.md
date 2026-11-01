# Whose store: the owner of a deployment

Two deployments pointed at one blob store by mistake (two configs naming
one chat or channel, each with a log of its own) know nothing of each
other: no fence spans two logs. Each one's orphan sweep would take the
other's blobs for its own garbage and delete them.

## The owner in every blob id

A deployment chooses an `Owner` (32 random bits) when its metadata first
starts, and keeps it in the tablet (`ChunkCommand::SetOwner`: it survives
snapshots and a lossy restore). Every `BlobId` it mints carries the owner
in its top 32 bits, so a blob's name tells whose it is
(`ph-<owner 8 hex><24 hex>.bin`). `BlobBackend::candidates` takes the owner
and returns only its blobs: a sweep never sees another deployment's.
Blobs minted outside a deployment (tests, the fake channels' own files)
are `Owner(0)`'s.

## A marker on the channel

That keeps blobs safe, not budgets: two deployments on one channel still
share its rate limits, quietly. So a store is also marked, and a
deployment refuses a store marked for another (`ChunkStore::open_replicated`,
read-write members only):

- **Telegram, Discord**: a message `owner-<8 hex>.bin`, pinned. A start
  reads the pin: a marker → whose it is; none (or something else pinned)
  → post ours and pin it (`random_id` / nonce from the name: a repeat
  re-pins the same message), then read again.
- **Disk, memory**: not marked (a directory or a process is one
  deployment's by setup).

A bot sees only the latest pin on Telegram (`channels.getFullChannel`;
bots may not search), so of two deployments racing on a new channel the
last to pin keeps it and the other learns so on its read back or its next
start; meanwhile their blobs are safe from each other by the owner bits.
Discord lists every pin: the oldest marker wins.

Rights: Telegram needs pin messages (supergroup) or edit messages
(channel); Discord needs Manage Messages, which deletes already take.

Not caught: one deployment started twice (a copied metadata directory):
same owner. Only a shared log's fence (Telegram, Discord) stops that.
