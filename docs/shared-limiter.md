# A shared limiter: budgets in levels, a pool of bots, sticky retries

Status: **design**. Nothing here is implemented yet. Read
[bench-discord-vs-telegram.md](bench-discord-vs-telegram.md) (what limits
us today) and [telegram-mtproto.md](telegram-mtproto.md) (the order fence,
which does not care which bot posts) first.

## Why

Today every chat service call is paced by `pigeonhole-limits`: a
`ChatLimiter` per (bot, chat), with a bucket per class of call (send, info,
get_file, delete), and a `BotLimiter` per bot that all its chats share. It
works for one bot per store. It does not take us further:

- **One bot per store.** A store has one `bot_token_env`, so a channel's send
  budget is one bot's (~20 messages a minute on Telegram, 5 per 5 s on
  Discord). The log's 3.3 s per entry on Telegram is exactly that budget
  (`send_rate_per_sec = 0.3`), not the network. Two stores on one chat are
  refused ("same place"), so a second bot cannot help.
- **Wired by hand.** Each method of `MtprotoChannel` and `DiscordClient`
  calls `acquire_*` and `penalize_*` itself; a new call gets no limit unless
  someone remembers it. Discord's thread `Pacer` is a separate mechanism.
- **Buckets are taken one after another.** `ChatLimiter::take` waits for the
  chat bucket, takes its token, then waits for the bot's. A call that then
  waits on the bot holds a chat token nobody else can use, and the order of
  waiting is not the order of calls.
- **Nothing above the bot.** Several bots in one process share one IP, and
  some limits are per IP: Cloudflare in front of Discord bans an address
  after 10 000 invalid requests in 10 minutes, whatever the token; Telegram
  limits connections and logins per address. Today there is no place to
  put such a budget.
- **Bound to this program.** The classes are ours (send / info / get_file /
  delete for a channel used as storage). A bot that talks to people needs
  others (private chats, edits, callback answers) and could not reuse it.

The goal: **one limiter for the process**. Each call takes its budget from
every level it belongs to in one step. The limiter picks the bot that can go
soonest, and a retry stays on the bot that made the first attempt.

## The model

### Levels and buckets

A **bucket** is one budget: a token bucket, a fixed window, or a cap on
calls in flight (see § Bucket kinds). Buckets sit on levels, from the
widest down:

| Level | Key | Examples |
|---|---|---|
| **Egress** | the outbound address (one by default; one per proxy) | Cloudflare's invalid-request ban, connections per IP |
| **Account** | a bot token | Telegram ~30 calls/s per bot; Discord 50 requests/s (`global`) |
| **Account × class** | bot, class | Telegram `upload.getFile` floods per bot; Discord threads 50 per 300 s |
| **Scope × account × class** | chat, bot, class | Telegram ~20 sends a minute per chat; Discord 5 per 5 s per channel |
| **Scope × class** (shared) | chat, class | a per-channel limit that holds for every bot together, if any (Discord `X-RateLimit-Scope: shared`) |

A level that a service does not have is simply not configured. The shared
scope level exists so that we can answer, with numbers, whether a channel's
limit is per bot or per channel (§ Open questions). If it turns out to be
per channel, a pool of bots in one channel gains nothing, and the limiter
shows that instead of hitting floods.

### A call and its plan

A call is described by what it spends, not by how it is wired:

```rust
struct Call {
    class: Class,           // Send, Read, File, Delete, Info, Thread, … (per service)
    scope: Option<Scope>,   // the chat / channel, if the call has one
    cost: u32,              // tokens, usually 1
}
```

The **plan** of a call on one account is the list of buckets it takes from:
for a Telegram send to chat `c` by bot `b`, that is egress, `b`,
`(c, b, Send)`, and `(c, Send)` if the shared level is on. Plans come from
the service's **rules** (`TelegramRules`, `DiscordRules`): which levels a
class touches, and the rates per level and class. The limiter itself knows
no service.

### Taking a plan at once: reservations

A plan is taken **atomically**: all of its buckets, or none.

Buckets keep a balance that may go below zero (a reservation, like Go's
`rate.Reserve`):

1. Under one lock (contention is low: tens of calls a second), compute
   `ready_at` = the latest of every bucket's earliest slot for `cost`
   (cool-downs included).
2. Debit every bucket of the plan as of `ready_at` and record the
   reservation.
3. Sleep until `ready_at`, then call.

Callers run in the order they asked, and no call holds one bucket while it
waits for another. A call needing four buckets cannot starve behind
one-bucket calls. If the future is dropped before `ready_at`, the
reservation is refunded. A penalty that lands while calls are reserved
pushes their `ready_at` (they wake, re-plan, re-reserve).

### Choosing the account: a pool

A store may have several accounts (bots) for one scope:

```toml
[[store]]
id = "tg-log"
kind = "telegram"
chat_id = -1001234567890
bot_token_envs = ["BOT_A", "BOT_B", "BOT_C"]   # all admins of the channel
```

`pool.begin(call)` computes `ready_at` of the call's plan for every account
of the pool and takes the earliest. Ties go to the account used least
recently, so load spreads even when every bucket is full. The egress level
is the same for every account behind one address, so it does not tell them
apart. It still bounds the sum: ten bots behind one IP never go past the
IP's budget.

`begin` returns a **ticket**: the chosen account and its reservation.

### Sticky retries

A ticket stands for **one logical operation**, from the first attempt to the
last. Every retry of it reserves the same account's plan again
(`ticket.retry()`), never another account's:

- **Deduplication is per author.** A Telegram send carries a `random_id`,
  a Discord send a `nonce`; the server merges a repeat into the first
  message only for the same sender. A retry that moved to another bot after
  a lost answer could post the batch twice.
- **Handles are per account.** An MTProto `access_hash` (of a channel, a
  document) is issued to one user or bot, and a file location read by one
  bot is what that bot may download. Reads that resolved a message on one
  bot finish on it.
- **One lane.** The retry also stays on the account's connection pool (the
  `Lane`), so a resend follows the same session.

A new operation (a new `random_id`, the next batch) gets a new ticket and
may land on another account: this is where the pool spreads load.

### Penalties

The service tells us to wait; its rules say **which level** the wait is
for, from the method and the answer:

| Answer | Penalized |
|---|---|
| Telegram `FLOOD_WAIT_X` on a send | `(chat, bot, Send)` |
| Telegram `FLOOD_WAIT_X` on `upload.getFile` | `(bot, File)` and `bot` |
| Telegram `FLOOD_WAIT_X` on auth / connect | egress |
| Discord 429, `X-RateLimit-Scope: user` | the route's bucket of that bot |
| Discord 429, `global: true` | the bot |
| Discord 429, `X-RateLimit-Scope: shared` | the shared scope bucket (every bot) |
| Discord 429 without a JSON body (Cloudflare), 1015 | egress |

A penalty freezes the bucket until the wait is over and pushes every
reservation on it, as `penalize` does today. The ticket's own retry waits
for it: retries are sticky (§ Sticky retries), even when another bot is free.

### Learning from answers

Discord sends the state of a route's bucket with every answer
(`X-RateLimit-Limit`, `-Remaining`, `-Reset-After`, `-Bucket`). The rules
turn that into `observe(bucket, remaining, reset_after)`: the limiter
adopts the server's count when it is stricter than ours. Telegram tells
nothing until a flood wait, so its rates stay configured.

### Bucket kinds

One interface, three kinds:

```rust
trait Bucket {
    fn ready_at(&self, now: Instant, cost: u32) -> Instant;
    fn reserve(&mut self, at: Instant, cost: u32);
    fn refund(&mut self, at: Instant, cost: u32);
    fn penalize(&mut self, until: Instant);
    fn observe(&mut self, remaining: u32, reset_after: Duration) {}
}
```

- **Token bucket** (rate, burst): most API limits.
- **Window / GCRA** (n per period, a burst): Discord threads, 50 per 300 s;
  today's `Pacer` becomes one.
- **In flight** (a count): uploads and downloads at once. Not time-based:
  the permit is held while the call runs and returned at the end. Per
  account or per egress: whether bandwidth is capped per bot or per address
  is open (§ Open questions).

### Background work

`wait_idle` stays: background work (log collection, the sweeper) reserves
nothing until every bucket of its plan has no reservation waiting and is at
least half full, then takes a ticket as usual. With a pool it asks for
**any** idle account.

## The API

```rust
let limiter = Limiter::new(rules);                 // one per process
let pool = limiter.pool(scope, accounts);          // a store's bots for its chat

let mut ticket = pool.begin(Call::send(scope)).await;   // picks the bot, waits
loop {
    match ticket.account().send(…, random_id).await {
        Ok(v) => break ticket.done(v),
        Err(e) => match rules.classify(&e) {
            Verdict::Wait { level, after } => { ticket.penalize(level, after); ticket.retry().await }
            Verdict::Transient => ticket.backoff().await,        // same account
            Verdict::Fatal => break Err(e),
        },
    }
}
```

`retry_call` (today in the Telegram client) becomes the generic driver of
this loop, with the deadline (`CALL_BUDGET`), so a service only writes
`classify` and the call itself:

```rust
limiter.run(&pool, Call::send(scope), |account| account.send(…, random_id)).await
```

Over MTProto the call is `invoke(&request)` on the account's sender pool,
with `impl Budgeted for` the TL functions we use (`messages.SendMedia` →
`Send`, `upload.GetFile` → `File`, …). Every call then goes through the
limiter, wired or not; a function without `Budgeted` does not compile.

## Crates

- **`pigeonhole-limits`**: buckets, plans, reservations, pools, tickets,
  the retry driver, metrics. It knows no service and depends on tokio
  only. It should build outside this workspace (no `madsim-tokio` alias
  required; the simulation keeps working through the alias, as today).
- **The service crates** bring their rules: classes, levels per class,
  default rates, `classify`, `observe`. They implement `Budgeted` for their
  requests.
- **The binary** builds one `Limiter` per service and the pools from
  `[[store]]`.

A bot that has nothing to do with pigeonhole uses the first crate and its
own rules, for example private chats at ~1 message per second per chat.

## Config

```toml
[telegram.egress]
rate_per_sec = 50.0              # every call of every bot from this address
[telegram.account]
rate_per_sec = 25.0              # was bot_rate_per_sec
burst = 30.0
[telegram.scope.send]            # per chat and bot; was send_rate_per_sec
rate_per_sec = 0.3
burst = 3.0
# [telegram.shared.send]         # per chat for all bots together; off by default
```

Each `[[store]]` takes `bot_token_envs` (a list; one entry is today's
`bot_token_env`) and with it the pool. Two stores on one chat stay refused:
one store, many bots.

## Metrics

- `pigeonhole_limiter_wait_seconds{level, class}`: the wait, by the level
  that set `ready_at` (which budget is the bottleneck).
- `pigeonhole_limiter_penalties_total{service, level}`.
- `pigeonhole_pool_calls_total{store, account}`: how the pool spreads load.
- `pigeonhole_limiter_reserved{level}`: calls waiting on a level.

## Plan

1. **Spike** (before any code): two and three bots posting into one test
   channel at their own pace for minutes, on Telegram and Discord. Count
   messages per minute and flood waits per bot. This decides whether the
   shared scope level is real.
2. Reservations and plans in `pigeonhole-limits`, behind today's
   `ChatLimiter` API (no behaviour change; one bot per store).
3. `Limiter::run` and `classify` for Telegram and Discord; `Budgeted` for
   the MTProto requests; the hand-wired `acquire_*` / `penalize_*` go.
4. Pools and `bot_token_envs`; sticky tickets.
5. Egress, the Discord `Pacer` as a window bucket, `observe` from headers.
6. Bench: log entries per second with 1, 2, 4 bots against
   [bench-discord-vs-telegram.md](bench-discord-vs-telegram.md).

## Open questions

- **Per bot or per channel?** Is Telegram's ~20 sends a minute in a channel
  per bot or for the channel as a whole? The same for Discord's 5 per 5 s.
  The spike answers both.
- **Handles across bots.** Is a document's `access_hash` / file reference
  per bot (assumed, hence sticky reads), or can bot B download what bot A
  located?
- **Rerouting a flood wait.** A Telegram `FLOOD_WAIT` guarantees the call
  was not applied, so moving that operation to another bot with a new
  `random_id` would be safe, unlike a retry after a lost answer. Worth it
  when the wait is long and another bot is free? Not in the first version:
  retries are sticky.
- **Bandwidth.** Is the ~2.7 MiB/s upload per stream capped per bot,
  per connection or per address? That decides whether the in-flight cap for
  uploads is per account or per egress.
- **Several egresses.** Bots behind different proxies get their own egress
  buckets. Is that worth supporting, given that the services may treat it
  as evasion?
