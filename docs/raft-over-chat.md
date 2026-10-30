# Raft over chat: openraft's network through Telegram and Discord channels

Status: design (2026-10-30). Not implemented.

A transport for [openraft](https://docs.rs/openraft) whose wire is chat
messages: Raft nodes talk by posting into channels and reading them. With it,
and with each node's log and snapshots already in a chat
(docs/meta-backends.md § Raft between nodes), a pigeonhole cluster needs no
open port and no address: nodes can run behind NAT, on laptops, anywhere
that reaches Telegram or Discord.

The transport does not depend on pigeonhole. It is a crate of its own
(`openraft-chat`) that any openraft application can use; pigeonhole is its
first user.

## The seam in openraft

openraft does no I/O. Its network is two traits the application implements,
plus calls the application makes into its own `Raft` when a message arrives.

| side | API | what it carries |
|---|---|---|
| send | `RaftNetworkFactory::new_client(target, node)` | a client for one peer |
| send | `RaftNetwork::append_entries(req, opt)` | log entries from the leader; an empty one is a heartbeat |
| send | `RaftNetwork::vote(req, opt)` | a candidate asking for a vote |
| send | `RaftNetwork::full_snapshot(vote, snapshot, cancel, opt)` | a whole snapshot for a follower too far behind |
| receive | `raft.append_entries(req)`, `raft.vote(req)`, `raft.install_full_snapshot(vote, snapshot)` | the local node handles the request and returns the response |

Every send is request → response with a deadline (`RPCOption::hard_ttl`).
Errors tell openraft what to do next:

- `Unreachable`: the peer is down, back off;
- `Network`: retry;
- a remote error: the peer answered with one.

pigeonhole today has `NoNetwork` (meta/pigeonhole-meta/src/raft.rs), which
answers `Unreachable` to everything: a cluster of one. Step 8.3 of the
roadmap plans a tonic gRPC network with the same three calls plus
`Forward`, a follower's `propose` sent to the leader (application-level,
not openraft's). A chat transport is another implementation of exactly that
set, chosen per cluster in the config.

Raft suits a lossy wire. Its RPCs may be lost, duplicated, delayed and
reordered without harm, since terms and log indices decide; a lost message is
a timeout and a retry. A chat is such a wire, only slow.

## Shape

```
openraft-chat
├── Mailbox            trait: post a frame, read frames after a position
│   ├── telegram       MTProto (grammers), a channel per node
│   ├── discord        HTTP, a channel per node
│   └── fake           in memory, with latency / loss / duplication (tests, madsim)
├── Transport          implements RaftNetworkFactory + RaftNetwork over Mailboxes
├── Inbox              polls the peers' mailboxes, dispatches requests to the
│                      local Raft, and responses to waiting senders
└── Frame              wire format, HMAC, encoding
```

`Mailbox` is the only part that knows a chat:

```rust
#[async_trait]
pub trait Mailbox: Send + Sync + 'static {
    /// Post one frame; returns its position (message id).
    async fn post(&self, frame: Bytes) -> Result<u64>;
    /// Frames after `after`, oldest first, at most `limit`.
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>>;
    /// Delete own frames up to `upto` (housekeeping).
    async fn delete_upto(&self, upto: u64) -> Result<()>;
}
```

The Telegram and Discord mailboxes reuse what pigeonhole's storage crates
already have: the MTProto client and Discord HTTP client, the rate limiters
(`pigeonhole-limits`), and the text encoding for small payloads
(`storage-api::text`). For a standalone crate those are dependencies to pull
out or copy; the Raft logic does not care.

## Topology: an outbox per node

Every node owns one channel, its **outbox**. It posts all it sends there:
requests to any peer and responses to requests it received. Every node reads
every other node's outbox.

- **Per-node rate.** A channel's posting limit is per channel, so each node
  has its own budget, and adding nodes does not share it.
- **Coalescing.** All a node has to send in one tick (heartbeats to every
  follower, responses, votes) goes out as **one post**: a frame holds several
  envelopes, each addressed to a node. A 3-node leader sends one post per
  tick, not one per follower.
- **Reading** is one call per peer per poll: "messages after the last id I
  saw". Telegram message ids in a channel are dense and increasing, Discord
  ids are increasing snowflakes; both read in order.
- **Authorship.** A node's bot is the only member with post rights in its
  outbox, so the channel itself says who wrote a frame. The HMAC below says
  so cryptographically.

The alternative, one channel per ordered pair (n·(n−1) channels), gives a
budget per link. That buys nothing once frames coalesce, and it costs
channels and reads that grow quadratically.

### Budgets

| | Telegram channel | Discord channel |
|---|---|---|
| posts | ~20 / min (`send_rate_per_sec = 0.3`, burst 3) | ~5 / 5 s |
| reads (ours) | `info` budget 2 / s per bot | ~5 / s |

A node posts at most once per **tick** and reads each peer once per
**poll**. For n nodes:

- posts per node: `1 / tick` (Telegram: tick ≥ 3.5 s to stay under 0.3/s
  with room for the burst; Discord: tick ≥ 1 s);
- reads per node: `(n − 1) / poll` against the bot's read budget (Telegram,
  3 nodes on one bot: 6 reads per poll across the processes; poll ≥ 3 s).

Each node uses its own bot, so read budgets do not stack on one token.

## Wire format

A frame is one chat message:

```
Frame {
    version: u8,
    cluster: u128,            // refuses frames of another cluster
    from: NodeId,
    seq: u64,                 // per sender, strictly increasing
    sent_ms: u64,             // sender's clock, for logs only
    envelopes: Vec<Envelope>,
    mac: [u8; 32],            // HMAC-SHA256(cluster key, everything above)
}

Envelope {
    to: NodeId,
    rpc: u64,                 // the sender's id for this call
    body: Body,
}

enum Body {
    AppendEntries(AppendEntriesRequest),   Appended(Result<AppendEntriesResponse, RaftError>),
    Vote(VoteRequest),                     Voted(Result<VoteResponse, RaftError>),
    Snapshot(SnapshotHeader, SnapshotRef), Installed(Result<SnapshotResponse, Fatal>),
    Forward(Bytes),                        Forwarded(Result<Bytes, String>),
}
```

- **Encoding**: postcard. A frame that fits a text message (Telegram 4096,
  Discord 2000 characters; with pigeonhole's 15-bits-per-character text
  codec that is ~7.5 KB and ~3.6 KB) goes as text. Larger ones go as a
  document or attachment. Heartbeats and votes are tens of bytes, so the
  common frame is text.
- **Responses** reuse the request's `rpc` id, addressed back to the
  requester, in the responder's own outbox.

## Sending and receiving

**Send** (`RaftNetwork::append_entries` etc.):

1. register `rpc` in a table of waiting calls;
2. queue the envelope for the next tick's frame;
3. wait for the response or `hard_ttl`.

On timeout, return `Unreachable` if nothing came from that peer for a while,
else `Network`. A response arriving after the deadline is dropped.

**Tick** (every `tick`, or at once when an urgent envelope is queued and the
budget allows): post one frame with every queued envelope. If the post fails
(rate limit, outage), the envelopes stay queued; the calls time out and
openraft retries as it would on a broken TCP connection.

**Poll** (every `poll`, per peer outbox): read frames after the last
position, verify the MAC, check `cluster`, drop frames with `seq` ≤ the last
seen from that sender (duplicates), and handle each envelope addressed to
this node:

- requests go to the local `Raft` (`append_entries`, `vote`,
  `install_full_snapshot`, or the application's `Forward` handler); the
  response is queued for this node's next tick;
- responses complete a waiting call by `rpc` id.

**Duplicates and replays** are harmless to Raft but cost work. `seq`
filters them, and `seq` survives restarts: it is restored from the last
frame in the node's own outbox.

## Timing

A round trip costs at most a tick on each side plus a poll on each side plus
two posts:

```
rtt ≲ 2·tick + 2·poll + 2·post_latency
```

On Telegram with tick 4 s, poll 3 s and post ~0.5 s: up to ~15 s, ~7 s on
average. On Discord with tick 1 s, poll 1 s: ~3–5 s.

openraft's defaults (heartbeat 50 ms, election 150–300 ms) assume a LAN.
Raft needs `heartbeat < election_timeout_min`, and the election timeout must
cover a few round trips, or followers depose healthy leaders:

| | heartbeat_interval | election_timeout | install_snapshot_timeout |
|---|---|---|---|
| Telegram | 10 s | 45–90 s | 10 min |
| Discord | 3 s | 15–30 s | 5 min |

A heartbeat is not a post by itself: it rides in the next tick's frame. The
heartbeat interval only has to be at least one tick.

Consequences:

- **Commit latency** is a round trip leader → follower → leader plus the
  node's own log append (a chat post when the log is a chat). That is
  seconds on Discord and tens of seconds on Telegram. Fine for
  pigeonhole's metadata at low write rates; not a database for chatty
  workloads.
- **Failover** takes an election timeout plus a round trip.

## Snapshots

`full_snapshot` sends a whole snapshot.

- **Generic:** the snapshot goes as a document or attachment frame (Telegram
  up to 2 GB, Discord 20 MiB per attachment as in pigeonhole's Discord store; larger ones in parts),
  with `SnapshotHeader` (meta, size, crc) in the envelope.
- **In pigeonhole:** the snapshot already sits in the leader's
  `SnapshotStore` (a chat, often). The envelope carries the `SnapshotRef` and
  the follower reads it from there, if it can reach that store. This is the
  one place where the transport may know pigeonhole, through a hook the
  application provides (`SnapshotRef` in, bytes out); the generic path
  stays as the fallback.

## Security

- **HMAC-SHA256** over every frame with a per-cluster key from the
  environment. Nodes drop frames that fail it, and log it.
- **`cluster`** id in every frame: two clusters sharing channels by mistake
  do not mix.
- **Authorship:** only the node's own bot can post in its outbox (channel
  admin rights). With one bot per node, the poster's bot id is checked
  against the node id too.
- **Replay:** `seq` per sender. Raft's terms make an old request harmless
  anyway.
- **Confidentiality:** none. Frames carry log entries in clear, like the
  chat log itself does today. Encryption is the separate "encryption per
  block" item on the roadmap.

## Housekeeping

Each node deletes its own outbox frames once they are old: older than
`2 × election_timeout_max`, since nothing reads them after that. It does
this in batches on spare budget (`wait_idle`), as the log collection does.
Telegram deletes up to 100 ids per call, Discord bulk-deletes messages
younger than 14 days.

## Configuration (pigeonhole)

```toml
[cluster]
transport = "telegram"          # "grpc" | "telegram" | "discord"
key_env = "PH_CLUSTER_KEY"      # HMAC key
tick = "4s"
poll = "3s"
heartbeat = "10s"
election_timeout = ["45s", "90s"]

[[node]]
id = "a"
outbox = { kind = "telegram", chat_id = -100111, bot_token_env = "TG_BOT_TOKEN_A" }
log = "a-disk"
snapshot = "a-disk"

[[node]]
id = "b"
outbox = { kind = "telegram", chat_id = -100222, bot_token_env = "TG_BOT_TOKEN_B" }
log = "b-chat"
snapshot = "b-snap"
```

`[[node]] addr` (gRPC) and `outbox` are alternatives: a cluster uses one
transport.

## What has to be true (checks before code)

1. **Reading another bot's posts in a channel.** By id (`channels.getMessages`)
   a bot reads every post of a channel it belongs to, whoever wrote it. That
   is what pigeonhole already does to read its log. Check it live with a second
   bot.
2. **Discord: other bots' content.** Without the Message Content intent,
   Discord returns empty `content` for messages that are not the bot's own,
   on REST too. The intent is a switch in the developer portal (no review
   under 100 servers). Check it, or carry frames as attachments, whose URLs
   and bytes are not gated.
3. **Polling cost** stays within the read budgets above for 3 nodes at the
   suggested poll intervals. Measure it with `pigeonhole_limiter_wait_seconds`.
4. **MTProto updates** (push) as a later optimization over polling: a bot gets
   `updateNewChannelMessage` for channels it is in. Check it with two bots.

## Tests

- **Unit:** frame encoding round trip; MAC rejection; `seq` dedup across
  restarts; coalescing (one post per tick whatever the number of envelopes).
- **Simulation** (madsim, `Mailbox::fake` with latency, loss, duplication and
  reordering):
  - 3 nodes elect a leader;
  - writes commit;
  - a partition of the leader elects a new one, and the old one steps down
    when it heals;
  - `kill -9` of the leader;
  - a lagging follower installs a snapshot.

  The same scenarios the gRPC network will need, so both transports share
  them.
- **Live** (not in CI): 3 nodes on Telegram (two or three bots), the same
  on Discord. Measure election time, commit latency and failover against the
  table above.

## Steps

1. The checks above (1, 2), with a throwaway program: two bots, two
   channels, post and read.
2. `openraft-chat` with `Mailbox::fake`, `Transport`, `Inbox`, `Frame`; the
   simulation scenarios.
3. Telegram and Discord mailboxes; live runs.
4. In pigeonhole: `[cluster] transport`, `[[node]] outbox`, `Forward`, with
   the step-8 work (node storage, leader-only side effects) that every
   transport needs.

## Risks

- **Latency.** Tens of seconds per commit on Telegram may be too slow even
  for metadata under real use; Discord is the realistic chat transport.
- **Platform terms.** Bots chatting with bots in a loop is the kind of
  traffic platforms rate-limit or ban. Ticks keep it to a post every few
  seconds per node, far below the limits, but it is automated traffic all
  the same.
- **Partial outages.** A chat that accepts posts but delivers reads late
  looks like a slow network, which Raft handles. One that silently drops
  posts looks like loss, which Raft also handles. Neither breaks safety;
  both cost availability.
