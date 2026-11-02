# Cluster: the tablet on several nodes

Status (2026-11-02):
- the binary runs as a node of a Raft cluster;
- two nodes talk over gRPC, or over chat channels (Telegram outboxes);
- the leader does all the work;
- tested: in process over every transport, under madsim over a lossy chat,
  and as three processes on localhost (`make cluster-smoke`);
- three nodes run in the browser demo over `Direct` and fake Telegram
  outboxes.

Left (§ Status): a live run on real Telegram channels, a Discord outbox,
membership changes.

Each node keeps its own copy of the metadata tablet: its own log and
snapshots, rebuilt from them on boot as on a single node
([meta-backends.md](meta-backends.md)). openraft keeps the copies equal.
The blob stores (`[chunk] store`) are shared: every node is configured
with the same ones.

## The leader does the work

Only the leader writes, and only the leader serves:

- **S3.** A follower answers every S3 request with 503
  `ServiceUnavailable`, naming the leader. Writes would fail there, and
  reads would see a state that may lag the leader's. There is no proxy
  between nodes and no follower read.
- **Finding the leader.** Two checks answer on every node with
  `{node, leader, leading, read_only}`:
  - `GET /_pigeonhole/leader`: 200 on the leader with its quorum, 503
    elsewhere. A load balancer's health check on it sends clients where
    writes work.
  - `GET /_pigeonhole/readable`: 200 on the leader, read-only or not (§
    Losing the quorum), 503 elsewhere: for a balancer that should keep
    reads going while writes cannot be.

  `/metrics` answers on every node.
- **Background work.** Sweeper, repair, cooler and the CAS GC skip their
  passes unless `MetaStore::has_quorum` (the leader, not read-only). A new leader takes the work over
  with its next pass. Each node's snapshot ticker runs on its own:
  snapshots are per node.
- **Taking over** (`ChunkStore::take_over`, on boot when leading and again
  on every gain of leadership). The leader:
  - records the configured stores;
  - chooses or reads the deployment's owner ([store-owner.md](store-owner.md))
    and marks the stores with it;
  - seeds the sweep's horizon from the committed parts.
- **Chunk ids** go on after the newest the tablet applied, so a node that
  took over never hands out an id its predecessor used.
- **The dynamic config** ([dynamic-config.md](dynamic-config.md)).
  Overrides reach every node by replication, and `DynConfig::follow`
  recomputes the knobs there. On a follower, `PUT`/`DELETE
  /admin/config/…` answer 503.

Pins (uploads in flight, reads in flight) live in the leader's process. Its
sweeper is the only one that deletes, so they need not travel. When the
leader changes, its uploads in flight can no longer commit. The new
leader's horizon starts below them, so they are left alone for now and
swept as orphans once its own uploads pass them.

## Losing the quorum

openraft 0.9 keeps a leader that no quorum answers any more a leader: it
learns of a new term only from a node that has one. Until then it would
take writes that cannot commit. So the tablet watches how long ago a
quorum last acknowledged the leader (openraft's `millis_since_quorum_ack`,
the gauge `pigeonhole_meta_raft_quorum_ack_age_seconds`). Past the lease,
`election_timeout.1`, the leader is **read-only**: the other nodes may have
elected another leader by then. Over gRPC that is 3 s, over a chat 90 s
(§ Timing). It is never read-only in a cluster of one.

What each node does (three nodes, two of them gone or cut off):

| | the leader left alone | a follower left alone |
|---|---|---|
| S3 reads (GET, HEAD) | served, from what it applied | 503 |
| S3 writes (PUT, POST, DELETE) | 503, before reading the body | 503 |
| `/_pigeonhole/leader` | 503, `read_only: true` | 503, `leader: null` |
| `/_pigeonhole/readable` | 200 | 503 |
| background passes | skipped | skipped |

- **Reads** on a read-only leader show its state as of its last commit.
  If the other nodes are alive, only cut off, they may have elected a
  leader and committed since: those reads may be stale.
- **Writes** are refused before anything is uploaded:
  - the S3 port's gate answers 503 `ServiceUnavailable` (`Retry-After: 1`);
  - inside, `MetaStore::propose_all` refuses with `NoQuorum` before a
    proposal reaches Raft;
  - the admin API and the demo's admin refuse too, with 503 and an error;
  - REAPI answers `UNAVAILABLE`.
- **A write in flight** when the quorum goes ends with `CommitUnknown`
  (503) once the node turns read-only, not when the client's timeout runs
  out. Proposals still queued are dropped, so they never commit. The
  entry Raft already holds is different. It commits if this node still
  leads when a quorum is back, and it is lost if another node took over.
  A PUT is safe to send again. A conditional one (`If-None-Match: *`) may
  get 412 for its own earlier attempt.
- **Back:** once a quorum acknowledges the leader again, the node is
  writable at the next check (a quarter of the lease). A leader that
  meanwhile lost to a newer term follows instead.
- Logs: `no quorum: read-only`, then `quorum back (or not leading):
  writable`. Gauge: `pigeonhole_meta_read_only`.

A follower with no leader stays as it was: 503 on everything. Serving
possibly stale reads there is a separate choice.

## Configuration

One pigeonhole.toml serves the whole cluster, as zoo.cfg does ZooKeeper's:
the same `[cluster]`, `[[node]]` list and blob stores on every host. A node
knows which one it is from `PIGEONHOLE_NODE` (or `[cluster] node`). Each
`[[node]]` names its own log and snapshots stores; a node opens only its
own and leaves the others' unresolved (their env vars need not be set).
A store serves one node: two nodes naming the same log is an error.

```toml
[cluster]
node = 1                        # or PIGEONHOLE_NODE
id = 7                          # frames of another cluster are dropped
key_env = "PH_CLUSTER_KEY"      # 64 hex digits: `openssl rand -hex 32`
chat = "telegram"               # the outboxes' chat (the default)
bot_token_envs = ["RAFT_BOT_TOKEN"]   # this node's bots; each host sets its own
# api_id_env / api_hash_env: TG_API_ID / TG_API_HASH by default
# heartbeat, election_timeout_min / _max, tick, poll: set for the slowest
# link unless given (§ Timing)

[[node]]
id = 1
grpc = "10.0.0.1:7001"          # serves Raft here; the others call it here
outbox = -1001111111111         # the channel it posts its frames into
log = "log-1"                   # its [meta] log and snapshots: [[store]] ids
snapshot = "snapshots-1"

[[node]]
id = 2
grpc = "10.0.0.2:7001"
outbox = -1002222222222
log = "log-2"
snapshot = "snapshots-2"

[[node]]
id = 3
outbox = -1003333333333         # only a channel: 1 and 2 reach it over the chat
log = "log-3"
snapshot = "snapshots-3"
```

The file is the same on every host. The bots are not: `bot_token_envs`
names env vars whose values differ from host to host. A node posts into its
own outbox and reads its chat peers' outboxes through its own bots (several:
a pool, as a store's). So every node's bots are admins of every outbox it
uses: its own, to post, and its chat peers', to read and be pushed their
posts. Only nodes with a chat link log in.

A fresh cluster is initialized by the member with the lowest id. The others
wait for its leader. Membership is fixed by the `[[node]]` list.

## Transports

The transport is chosen for each pair of nodes, not for the cluster.
Between two nodes:

- **gRPC**, when both have `grpc`;
- **the chat**, when both have an `outbox`;
- a config error otherwise.

`openraft_grpc::Mixed` is the node's network factory. openraft asks it for a
client per peer, and it hands out a gRPC client for some peers and a chat
client for the others. Every node serves both: a gRPC server on its address,
and the outboxes of its chat peers, which it reads. Either way requests
reach the same `Raft`.

- **gRPC** (`openraft-grpc`, feature `transport`): one unary RPC per Raft
  call. The request and the response are each one frame of the chat's wire
  format (§ Wire format) holding one envelope, so both transports check the
  same key.
- **`Direct`** (`openraft-grpc`, no feature): the same link in memory. A
  call goes straight to the peer's `Raft`, with an optional delay and
  cut-off nodes. It serves tests, the simulation, and the in-browser demo,
  where no socket opens: with `default-features = false` there is no tonic,
  and the crate builds for wasm.
- **The chat** (`openraft-chat`): § The chat transport.

**Timing.** A leader's heartbeat must stay under every follower's election
timeout, so one chat link sets the pace of the whole cluster (§ Timing): the
defaults follow the slowest link. Commits stay fast all the same: two nodes
on gRPC are a quorum of three, and the chat node catches up behind them.

## The seam in openraft

openraft does no I/O. Its network is two traits the application implements,
plus calls the application makes into its own `Raft` when a message arrives.

| side | API | what it carries |
|---|---|---|
| send | `RaftNetworkFactory::new_client(target, node)` | a client for one peer |
| send | `RaftNetwork::append_entries(req, opt)` | log entries from the leader; an empty one is a heartbeat |
| send | `RaftNetwork::vote(req, opt)` | a candidate asking for a vote |
| send | `RaftNetwork::install_snapshot(req, opt)` | a chunk of a snapshot, for a follower behind the leader's purged log |
| receive | `raft.append_entries(req)`, `raft.vote(req)`, `raft.install_snapshot(req)` | the local node handles the request and returns the response |

Every send is request → response with a deadline (`RPCOption::hard_ttl`).
Errors tell openraft what to do next:

- `Unreachable`: the peer is down, back off;
- `Network`: retry;
- a remote error: the peer answered with one.

A cluster of one runs with `NoNetwork` (meta/pigeonhole-meta/src/raft.rs),
which answers `Unreachable` to everything.

Raft suits a lossy wire. Its RPCs may be lost, duplicated, delayed and
reordered without harm, since terms and log indices decide; a lost message is
a timeout and a retry. A chat is such a wire, only slow.

## The chat transport

Raft nodes talk by posting into channels and reading them. A node reached
only this way needs no open port and no address: it can run behind NAT, on
a laptop, anywhere that reaches Telegram.

The crate `openraft-chat` does not depend on pigeonhole; any openraft
application can use it:

```
openraft-chat
├── Mailbox      trait: an outbox, as a node sees it
├── FakeChat     in memory, with delay / loss / duplication / isolation (tests, madsim)
├── ChatNetwork  RaftNetworkFactory + RaftNetwork over mailboxes: a send loop
│                (one frame per tick) and a read loop per peer outbox
├── Handler      what a request is handed to (the node's Raft)
└── frame        wire format, HMAC
```

The Telegram outbox lives in pigeonhole (§ The Telegram outbox): it reuses
the store's MTProto client, rate limiters and text encoding.

```rust
#[async_trait]
pub trait Mailbox: Send + Sync + 'static {
    /// Post one frame; its position (the message id).
    async fn post(&self, frame: Bytes) -> Result<u64>;
    /// Frames after `after`, oldest first, at most `limit`.
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>>;
    /// The position of the newest frame: where a reader that does not care
    /// for the past starts.
    async fn head(&self) -> Result<u64>;
    /// Return once something may have been posted after `after`, or after
    /// `timeout` (pushed mailboxes wake early).
    async fn wait(&self, after: u64, timeout: Duration);
}
```

### Topology: an outbox per node

Every node owns one channel, its **outbox**. It posts all it sends there:
requests to any peer and responses to requests it received. Every node
reads the outboxes of its chat peers.

- **Coalescing.** All a node has to send in one tick goes out as one post:
  heartbeats to every follower, responses, votes. A frame holds several
  envelopes, each addressed to a node. A 3-node leader sends one post per
  tick, not one per follower.
- **Reading: push first, polling as the safety net.**
  - Push: a bot that is a channel's member gets every new post as an update
    (MTProto `updateNewChannelMessage`). There is no poll interval, and no
    read budget is spent.
  - Polling: "messages after the last id I saw", one window per peer per
    poll. It fills the gaps updates leave after a reconnect.
- **Authorship.** Only the node's own bots have post rights in its outbox.
  The HMAC below says who wrote a frame cryptographically.

The alternative is one channel per pair, an honest duplex: each node reads
only what is meant for it, and each link can be cut or moved alone. It
costs N−1 posts per tick on the leader instead of one, and channels that
grow quadratically. The outbox stays (decided 2026-11-02). In a mixed
cluster the outboxes of the gRPC nodes carry frames for the chat node only,
which is close to a pair channel anyway.

### Budgets

| per bot | Telegram | Discord |
|---|---|---|
| posts | ~20 / min (measured 19.7) | ~57 / min (measured, no 429) |
| reads (ours) | `info` budget 2 / s | ~5 / s |

A node posts at most once per **tick** and reads each peer once per
**poll**. For n nodes:

- posts per node: `1 / tick` against its bot's budget (Telegram: tick
  ≥ 3.5 s to stay under ~0.33/s with room for the burst; Discord: tick
  ≥ 1.1 s);
- reads per node: `(n − 1) / poll` against the same bot's read budget, or
  none with push updates.

Bots shared between nodes share these budgets: one bot per node.

### Wire format

A frame is one chat message (or, over gRPC, one request or response):

```
Sealed { frame: Vec<u8>, mac: [u8; 32] }   // mac = HMAC-SHA256(cluster key, frame)

Frame {
    version: u8,
    cluster: u128,            // refuses frames of another cluster
    from: NodeId,
    seq: u64,                 // per sender, strictly increasing
    envelopes: Vec<Envelope>,
}

Envelope { to: NodeId, rpc: u64, body: Body }   // rpc: the sender's id for the call

enum Body {
    AppendEntries(AppendEntriesRequest),     Appended(Result<AppendEntriesResponse, RaftError>),
    Vote(VoteRequest),                       Voted(Result<VoteResponse, RaftError>),
    InstallSnapshot(InstallSnapshotRequest), Installed(Result<InstallSnapshotResponse, RaftError<…>>),
}
```

- **Encoding**: postcard. On Telegram a frame that fits a text message (~7.5
  KB with the 15-bits-per-character text codec) goes as text, a larger one
  as a document. Heartbeats and votes are tens of bytes, so the common frame
  is text.
- **Responses** reuse the request's `rpc` id, addressed back to the
  requester, in the responder's own outbox.

### Sending and receiving

**Send** (`RaftNetwork::append_entries` etc.):
1. register `rpc` in a table of waiting calls;
2. queue the envelope for the next tick's frame;
3. wait for the response, or `hard_ttl`; a timeout is `Timeout`, and
   openraft retries.

**Tick** (every `tick`): post one frame with every queued envelope. If the
post fails (rate limit, outage), the calls time out and openraft retries,
as it would on a broken TCP connection.

**Read** (per peer outbox, woken by `wait`):
1. read the frames after the last position;
2. verify the MAC and the cluster;
3. drop frames with `seq` ≤ the last seen from that sender (duplicates);
4. handle each envelope addressed to this node:
   - a request goes to the local `Raft`, and its response is queued for
     this node's next tick;
   - a response completes the waiting call with its `rpc` id, if that call
     went to this sender; any other response (late, to another peer's
     call, to an earlier run of this node) is dropped and counted
     (`openraft_chat_stray_responses_total`).

Reading starts at each outbox's head as of `serve`: frames posted before
the node came up are not replayed. `seq` and `rpc` ids start from the
clock, so they keep growing across restarts: a run never takes a frame or a
response meant for an earlier one for its own.

**Prune** (`Settings::keep`, `2 × election_timeout_max` in a node): past
that age no call waits for a frame. Once per `keep`, on a tick with
nothing queued, a node prunes its own outbox (`Mailbox::prune`): every
frame of this run older than `keep` goes, the newest one stays. The first
prune comes a `keep` after the run starts, and takes the frames of the
runs before with it.

### Timing

A round trip costs at most a tick on each side, plus a poll on each side
when reading by polling, plus two posts:

```
rtt ≲ 2·tick + 2·poll + 2·post_latency      (polling)
rtt ≲ 2·tick + 2·post_latency                (push updates)
```

On Telegram with tick 4 s, poll 3 s and post ~0.5 s: up to ~15 s polling,
~9 s with push (a push arrived 0.19 s after the post, § Checks).

openraft's defaults (heartbeat 50 ms, election 150–300 ms) assume a LAN.
Raft needs `heartbeat < election_timeout_min`, and the election timeout must
cover a few round trips, or followers depose healthy leaders. The defaults
`[cluster]` picks:

| links | heartbeat | election timeout | tick / poll |
|---|---|---|---|
| gRPC only | 250 ms | 1.5–3 s | — |
| any Telegram link | 10 s | 45–90 s | 4 s / 3 s |

`install_snapshot_timeout` is four maximal election timeouts (openraft's
200 ms timed out every send over a chat). A heartbeat is not a post by
itself: it rides in the next tick's frame.

- **Commit latency** is a round trip leader → quorum → leader, plus the
  leader's own log append (a chat post when the log is a chat). With a
  gRPC quorum it is the gRPC round trip.
- **Failover** takes an election timeout plus a round trip.

### The Telegram outbox

`pigeonhole_raft_telegram::TelegramMailbox` (crates/net/pigeonhole-raft-telegram:
a `Mailbox` on the store's `ChannelApi`, over real channels in the binary
and fake ones in the demo):
- a frame is a message named `raft.bin`: text when it fits, a document
  otherwise; its position is the message id;
- bots can read only by id, so a reader reads ids upward, 100 at a time,
  and skips holes and foreign messages (a window with nothing in it is
  skipped while the channel is known to go past it);
- a reader learns how far the channel goes from its own posts, from the
  posts pushed to any of the process's bots (`channel_log::on_post`; every
  bot logs in with updates), and, at the start, from a lookup: windows of
  100 ids with doubling strides, then halving, up from the pinned anchor
  (below);
- `wait` wakes on a push, else after `poll`;
- `prune` pins the frame it keeps from as the anchor, then deletes the ids
  below it, 100 a call, at most 10 calls a prune (the rest the next time).
  The lookup starts at the anchor, above which nothing was deleted, so it
  never meets a run of 100 deleted ids. The node's bot needs the right to
  pin messages in its outbox.

## Snapshots and the kept log

A follower catches up from the leader's log as far as the log reaches back,
and from the leader's snapshot behind that. A leader keeps
`[meta] keep_log_entries` entries behind its newest snapshot (default: as
many as `snapshot_every_entries`, so about one interval; `"off"`: none) —
openraft's `max_in_snapshot_log_to_keep`, which purges by count and does not
wait for a follower. A cluster of one keeps none. So a follower that was cut
off for less than an interval gets the entries it lacks; one that was gone
longer, or a new node, gets a snapshot.

The base then lags the snapshot: the log starts at the purge point, and the
snapshot it names holds a later position (`log::Base`). A boot loads the
snapshot and replays what follows it; the entries between the base and the
snapshot are there for followers only. The cost is chat messages: those
behind the snapshot are collected one interval later.

A follower behind the kept log gets the leader's snapshot through
`install_snapshot`: openraft sends it in chunks, each a request over the
link (a chat frame, a document when large). Three things this needed:
- the send timeout above;
- a follower's purge waits for the snapshot it installs, since Raft asks
  to purge while the state machine is still storing it;
- a snapshot of the follower's own, built before the install and stored
  after it, does not take the installed one's place.

Not built: a snapshot as a reference. The snapshot already sits in the
leader's `SnapshotStore`, often a chat, and the envelope could carry its
`SnapshotRef` for the follower to read there.

## Security

- **HMAC-SHA256** over every frame, chat or gRPC, with a per-cluster key
  from the environment. Nodes drop frames that fail it, and log it.
- **`cluster`** id in every frame: two clusters sharing channels by mistake
  do not mix.
- **Replay:** `seq` per sender. Raft's terms make an old request harmless
  anyway.
- **Not built:** checking a post's bot against the node it claims to come
  from, and TLS on gRPC. Keep gRPC on a private network.
- **Confidentiality:** none. Frames carry log entries in clear, like a chat
  log does. Encryption is the "encryption per block" item on the roadmap.

## Observability

The tablet records openraft's own metrics (`Raft::metrics()`) as gauges on
every `record_gauges`: the node's role, term, the leader it knows, its last
log and applied indexes, and on the leader each peer's replication lag and
the time since a quorum last acknowledged it. openraft-chat records its side
through the `metrics` facade: posts and their time, envelopes per frame,
every call's round trip by RPC and outcome (ok, timeout), frames read by
outcome (ok, duplicate, bad_frame, foreign) and read errors. gRPC links
record no metrics of their own yet. Names and labels:
[metrics.md](metrics.md); the Grafana dashboard has a Cluster (Raft) row of
them.

**For the admin UI's graph:**
- The admin op `cluster` (`MetaStore::cluster_view` plus the config) gives
  `{me, leader, term, nodes, links}`.
  - Each node is `{id, role, grpc, outbox, chat, log, snapshots, applied, lag}`.
  - `log` and `snapshots` are known for this node only, as is `applied`.
  - `lag` is known on the leader.
  - `links` are `{a, b, via: grpc|chat}`.
- `channels` lists a node's own log and snapshots with its `node`, and each
  outbox as `outbox-<node>` (role `outbox`).
- These log lines are stable; the graph animates them:
  - `raft frame sent` and `raft frame received` (debug), with fields
    `to`/`from`, `kind` (heartbeat, append, vote, snapshot), `reply`,
    `via` (grpc, chat) and `bytes`. `Direct` logs as `grpc`.
  - `leader changed` (info), with `term` and `leader` (absent during an
    election).
  - `not the leader` (info), with `op` (method and path) and `leader`:
    a follower turning an S3 request away.

## Tests

- **openraft-chat** (`tests/transport.rs`): MAC, call and answer,
  coalescing, partitions, duplicates, loss, a foreign key, metrics.
- **The tablet** (`pigeonhole-meta` `tests/cluster.rs`):
  - over `FakeChat`: election, commits everywhere, a write to a follower,
    a killed leader, a partitioned leader that heals, gauges and lag, the
    cluster view, a follower that catches up from a snapshot or from the
    kept log, a node that restarts from a base behind its snapshot, a
    leader left alone (read-only, `NoQuorum`, back with a quorum), a write
    in flight as the quorum goes (`CommitUnknown`), a cluster of one never
    read-only;
  - over gRPC alone;
  - two gRPC nodes and a chat node;
  - a `Direct` pair and a chat node (this one under madsim too).
- **The binary**:
  - `tests/raft_telegram.rs`: two gRPC nodes and one on `TelegramMailbox`
    over fake Telegram channels, through the leader's death;
  - `tests/leader_http.rs`: the S3 gate and both health checks on a
    leader left alone (writes 503 with their bodies unread, reads 200).
- **The demo's node** (`pigeonhole-web` `tests/cluster.rs`): three nodes in
  one process, over `Direct` and fake Telegram outboxes, one config for all.
- **Simulation** (`pigeonhole-sim/tests/cluster.rs`, madsim): three nodes
  over a `FakeChat` that delays, loses and doubles posts, while a node at a
  time is cut off and healed. Every acknowledged command survives, and the
  nodes converge.
- **Processes** (`make cluster-smoke`): three on localhost, a write through
  the leader, `kill -9` of the leader, a read and a write through the new
  one, the old one back as a follower. `CHAT=telegram` puts node 3 on
  Telegram outboxes (`TG_RAFT_OUTBOX_1..3` in `.env`).

## Checks (2026-10-30)

1. **Reading another bot's posts in a channel**: a second bot reads the
   first one's post by id (`channels.getMessages`) 0.19 s after it was sent
   (`tests/live.rs` `second_bot_sees_first_bots_posts`).
2. **Push of another bot's posts**: the second bot got
   `updateNewChannelMessage` 0.19 s after the post, once it had asked for
   updates (`updates.getState`; `TelegramMtproto::connect_with_updates`).
   Posts in a channel are the channel's, so the "bots do not see bots" rule
   of groups does not apply. Open: whether several processes logged in as
   one bot all get the updates. One bot per node avoids the question.
3. **Discord** (open, before a Discord outbox): without the Message Content
   intent, Discord returns empty `content` for other bots' messages, on REST
   too. Either switch the intent on in the developer portal, or carry frames
   as attachments, whose bytes are not gated.

A second Telegram bot takes a person: create it in BotFather, put its token
in `.env`, and add it as an admin of each channel. A bot cannot invite
another one (`CHAT_ADMIN_INVITE_REQUIRED`, tried 2026-10-30). Both bots log
in with the same `TG_API_ID` / `TG_API_HASH`: those identify the client
application, not the account.

## Status

Done:
- the chat transport (`openraft-chat`) and `MetaStore::open_in`;
- metrics and the Grafana row;
- snapshot catch-up, and catch-up from the kept log;
- leader-only work, `DynConfig::follow`, 503 on followers;
- read-only without a quorum (§ Losing the quorum);
- gRPC, `Direct` and per-pair transports;
- `[cluster]` / `[[node]]`;
- the Telegram outbox;
- outbox pruning (`Settings::keep`, a pinned anchor);
- the cluster view and the graph's log lines;
- one `pigeonhole.toml` for the whole cluster;
- a cluster of three in the demo.

Left:
1. A live run of three nodes, one of them on real Telegram channels
   (`CHAT=telegram make cluster-smoke`): election time, commit latency and
   failover against § Timing.
2. A Discord outbox (check 3 first).
3. `seq` restored from the node's own outbox rather than the clock; the
   poster's bot checked against the node.
4. A snapshot as a reference (§ Snapshots and the kept log).
5. Membership changes (add or remove a node, learners) through the admin
   API, instead of a fixed `[[node]]` list.
6. Maybe: a status message per node in its outbox (role, term, leader,
   lag), edited in place, so the channel shows the cluster's state without
   Grafana.

## Risks

- **Latency.** Tens of seconds per commit when the quorum needs a Telegram
  link. With a gRPC quorum the chat node only lags.
- **Platform terms.** Bots chatting with bots in a loop is the kind of
  traffic platforms rate-limit or ban. Ticks keep it to a post every few
  seconds per node, far below the limits, but it is automated traffic all
  the same.
- **Partial outages.** A chat that accepts posts but delivers reads late
  looks like a slow network, which Raft handles. One that silently drops
  posts looks like loss, which Raft also handles. Neither breaks safety;
  both cost availability.
