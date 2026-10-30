# A shared limiter: budgets in levels, a pool of bots, sticky retries

Status: **design**; the spike is done (§ Spike findings: send limits are
per bot on both services). Nothing else is implemented yet. Read
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

A level that a service does not have is simply not configured. The spike
found channel send limits per bot on both services (§ Spike findings), so
the shared scope level is off by default; it stays in the model for limits
that are per channel (a Discord answer with `X-RateLimit-Scope: shared`).

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

### Taking a plan at once

A plan is taken **atomically**: a token from every bucket of it, or none.

Under the buckets' locks, taken in one order (by address, so two plans never
deadlock), the limiter computes the wait of every bucket (cool-downs
included). If all have a token, it takes one from each. Otherwise it takes
nothing and sleeps until the latest bucket should have one, then tries
again. No call holds one bucket while it waits for another: today's
`ChatLimiter::take` took the chat's token and then waited for the bot's,
keeping that chat token from everyone meanwhile.

This is not a reservation, and waiters are not served in the order they
came. Reserving ahead (debiting every bucket as of the time the slowest is
ready, as Go's `rate.Reserve` does for one bucket) was the first design. It
does not fit buckets with a burst: in GCRA terms the debit moves a wide
bucket's theoretical arrival time to the slow bucket's slot. A send waiting
3 s for its chat would then hold every other chat of the bot ~2 s behind
it, with the bot's 30 tokens unused. Done exactly, a reservation needs a
list of future debits per bucket. That is worth it only if a call that needs
several buckets is seen to starve. The metric `limiter_wait_seconds` shows
it: today every call takes exactly two buckets (its chat's and its bot's).

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

`begin` waits for the chosen account's plan and returns a **ticket**: that
account, with the tokens of its first attempt taken.

### Sticky retries

A ticket stands for **one logical operation**, from the first attempt to the
last. Every retry of it takes the same account's plan again
(`ticket.retry()`), never another account's:

- **Deduplication is per author.** A Telegram send carries a `random_id`,
  a Discord send a `nonce`; the server merges a repeat into the first
  message only for the same sender. A retry that moved to another bot after
  a lost answer could post the batch twice.
- **Handles are per account.** An MTProto `access_hash` (of a channel, a
  document) is issued to one user or bot. Another bot can read the same
  message (§ Spike findings), but through its own `getMessages`, with its
  own handles. So a read that resolved a message on one bot finishes on it.
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

A penalty freezes the bucket until the wait is over, as `penalize` does
today; every call whose plan has that bucket waits it out. The ticket's own retry waits
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
    fn wait(&mut self, now: Instant, cost: u32) -> Duration;   // zero: can take now
    fn take(&mut self, now: Instant, cost: u32);
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

`wait_idle` stays: background work (log collection, the sweeper) takes
nothing until every bucket of its plan has no caller waiting and is at
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

- **`pigeonhole-limits`**: buckets, plans taken at once, pools, tickets,
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
- `pigeonhole_limiter_waiting{level}`: calls waiting on a level.

## Spike findings (2026-10-30)

Two bots per service, admins of the blobs test channel, the same host and
IP. Each bot posted text messages one after another, as fast as the server
let it: first one bot alone, then both at once. Our own limits were set far
above the server's. Everything was deleted afterwards
(`crates/storage/pigeonhole-storage-telegram/tests/spike_bots.rs`,
`scripts/spike-discord-bots.py`).

| | one bot | two bots at once | channel total |
|---|---|---|---|
| Telegram (MTProto, 3 min a phase) | 19.7 sends/min | 20.3 + 20.3 | **40.7/min** |
| Discord (REST v10, 1 min a phase) | 57/min | 59 + 58 | **117/min** |

- **Send limits are per bot, not per channel**, on both services: a second
  bot in the same channel doubled the channel's rate, and neither bot slowed
  down the other.
- **Telegram** paces a bot to ~20 messages a minute in a channel: a burst of
  about five, then `FLOOD_WAIT` of 3–5 s (50 of 59 waits), and every ~18
  messages one of 35–40 s. Each bot got its own waits with the same pattern;
  no wait looked shared.
- **Discord** reports one bucket hash (`X-RateLimit-Limit: 5`) for the route
  to both bots, but counts it per bot. Following the headers, neither bot
  got a 429.
- **Across bots, on both services**, bot B sees bot A's messages (text and a
  document / an attachment, with content), downloads A's document over
  MTProto (the bytes match) and deletes A's messages. On Discord B saw
  content with the Message Content intent as configured for the test bots.
  A bot without it may get empty `content` / `attachments` for other bots'
  messages, so a pool should check that at startup.

So a pool of N bots in one channel multiplies its send budget by N. On
Telegram the log's ~3.3 s per entry is one bot's budget: two bots halve it,
until the round trip (~0.3–0.5 s per send) is what is left.

## Plan

1. ✅ **Spike**: per bot on both services (§ Spike findings).
2. ✅ Plans taken at once in `pigeonhole-limits` (`take_all`), behind
   today's `ChatLimiter` API (one bot per store).
3. `Limiter::run` and `classify` for Telegram and Discord; `Budgeted` for
   the MTProto requests; the hand-wired `acquire_*` / `penalize_*` go.
4. Pools and `bot_token_envs`; sticky tickets.
5. Egress, the Discord `Pacer` as a window bucket, `observe` from headers.
6. Bench: log entries per second with 1, 2, 4 bots against
   [bench-discord-vs-telegram.md](bench-discord-vs-telegram.md).

## Open questions

- **Per bot or per channel?** Answered: per bot on both (§ Spike findings).
- **Handles across bots.** Answered in part: bot B can download what bot A
  posted, through its own `getMessages`. Whether A's `FileLocation` works
  for B directly was not tried: reads stay sticky.
- **More bots.** Two bots doubled the rate. Does it keep scaling at 4–8
  bots from one IP, or does an egress limit appear?
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
