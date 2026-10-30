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
  limits connections and logins per address. Neither is a rate of calls,
  and neither has been seen yet, so the model leaves them out until a live
  run shows one (§ Levels and buckets).
- **Bound to this program.** The classes are ours (send / info / get_file /
  delete for a channel used as storage). A bot that talks to people needs
  others (private chats, edits, callback answers) and could not reuse it.

The goal: **one limiter for the process**. Each call takes its budget from
every level it belongs to in one step. The limiter picks the bot that can go
soonest, and a retry stays on the bot that made the first attempt.

## The model

### Levels and buckets

A **bucket** is one budget, a token bucket (a rate and a burst). Buckets
sit on two levels:

| Level | Key | Examples |
|---|---|---|
| **Bot** | a bot token | Telegram ~30 calls/s per bot; Discord 50 requests/s (`global`) |
| **Class** | chat, bot, class | Telegram ~20 sends a minute per chat; Discord 5 per 5 s per channel |

A **class** is a kind of call with a bucket of its own in each chat: `send`,
`read`, `delete` on both services. The service names its classes and their
default rates (`pigeonhole_storage_telegram::limits()`,
`pigeonhole_storage_discord::limits()`); `pigeonhole-limits` knows no
service.

**Not in the model** until a live run shows them: a level above the bot (the
outbound address) and a class shared by every bot of a channel. The spike
found channel send limits per bot on both services (§ Spike findings), so
nothing measured needs the shared level. What is known about per-address
limits does not fit a rate: Cloudflare in front of Discord bans an address
for 10 000 *invalid* answers (401, 403, 429) in 10 minutes, which is an
error budget (a counter over a sliding window that stops calls near the
limit), and Telegram's are about connections and logins, not calls.

### A call and its plan

A call is described by what it spends, not by how it is wired:

```rust
pub struct Call {
    pub service: &'static str,   // a metrics label
    pub what: &'static str,      // the method
    pub spend: Spend,            // Nothing (a CDN read) | Bot | Class(class)
    pub cools: Option<Level>,    // what its rate limit cools by default
}
```

A call of a class takes a token of the class's bucket in its chat and one of
its bot's: that is its **plan**. `Spend::Bot` takes the bot's only (Telegram
`getFile`, upload parts); every API call takes at least that.
`Spend::Nothing` is for CDN downloads, which are not API calls.

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

`pool.begin(call)` computes when the call's plan would be ready for every
account of the pool and takes the earliest. Ties go to the account used
least recently, so load spreads even when every bucket is full. The choice
is made once: if another account frees up while the call waits for the
chosen one, the call does not switch.

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

**One exception: a rate limit moves.** A `FLOOD_WAIT` or a 429
(`Verdict::Wait`) guarantees the call was not applied. So when another
account of the pool would be ready sooner than the wait, the operation takes
a new ticket there, with a new `random_id` / nonce, and the first account's
bucket stays cooled. Telegram gives a bot a 35–40 s wait every ~18 sends,
and the log is written one entry after another: without moving, an entry
would wait that out while another bot is free, and a pool would not give
×N. A lost answer, a 5xx or a network error (`Verdict::Transient`) never
moves: the outcome is unknown, and only the same account with the same
`random_id` deduplicates. A read with per-account handles resolves the
message again on the new account. (Decided with the user, 2026-10-30.)

### Penalties

The service says what a failure means (`classify` → `Verdict`). A rate limit
(`Verdict::Wait { after, level }`) cools the call's default level
(`Call::cools`), or the level the service names:

| Answer | Cooled |
|---|---|
| Telegram `FLOOD_WAIT_X` on a send, a read, a delete | the class in that chat |
| Telegram `FLOOD_WAIT_X` on `upload.getFile`, an upload part | the bot |
| Discord 429 | the route's class in that channel |
| Discord 429, `global: true` | the bot |

A penalty freezes the bucket until the wait is over, and the bucket starts
empty then: a full burst right after a 35 s `FLOOD_WAIT` is what earns the
next one. Every call whose plan has that bucket waits it out, the ticket's
own retry too, unless the operation moves to another account (§ Sticky
retries).

### Fairness

Waiters are not served in the order they came, and a retry after a penalty
has no priority over a fresh call: after waiting out a 35 s flood it may
lose the bucket's first token to a call that just arrived, and wait again.
Whether that happens is measured first, by the wait of retries after a
penalty, apart from other waits (§ Metrics). Priority for those retries
comes only if that wait shows it.

### Learning from answers

Discord sends the state of a route's bucket with every answer
(`X-RateLimit-Limit`, `-Remaining`, `-Reset-After`, `-Bucket`). The rules
turn that into `observe(bucket, remaining, reset_after)`: the limiter
adopts the server's count when it is stricter than ours. Telegram tells
nothing until a flood wait, so its rates stay configured.

### Bucket kinds

Token buckets for rates, and semaphores for transfers at once (uploads,
CDN downloads per chat). Discord's thread budget (50 per 300 s in a fixed
window) has its own pacer (`Pacer`, GCRA) fed from the answers' headers. It
becomes a kind of bucket, and the headers feed the limiter
(`observe(remaining, reset_after)`), only if the bench shows a need (§ Plan).

### Background work

`wait_idle` stays: background work (log collection, the sweeper) takes
nothing until every bucket of its plan has no caller waiting and is at
least half full, then takes a ticket as usual. With a pool it asks for
**any** idle account.

## The API

What exists (step 3): a service describes each request once, and every
request goes through one loop.

```rust
// Telegram: a request without a budget does not compile.
budgeted! {
    functions::messages::SendMedia => "messages.sendMedia", Spend::Class(SEND), Level::Class;
    functions::upload::GetFile => "upload.getFile", Spend::Bot, Level::Bot;
    …
}
// Discord: one Call per endpoint.
const SEND_MESSAGE: Call = api("send message", SEND);

pigeonhole_limits::run(Some(&limiter), &call, &RETRY, classify, || attempt()).await
```

`run` takes the call's tokens before each attempt, asks `classify`, cools
on a rate limit and retries within `Retry` (a deadline, attempts, a
backoff). Step 4 puts a pool in front of it:

```rust
let mut ticket = pool.begin(&call).await;          // picks the bot, takes its plan
let result = ticket.run(&RETRY, classify, |bot| bot.send(…, random_id)).await;
```

## Crates

- **`pigeonhole-limits`**: buckets, plans taken at once, the retry loop,
  then pools and tickets. It knows no service and depends on tokio (the
  workspace's `madsim-tokio` alias, so the simulation sees its clock) and
  metrics.
- **The service crates** bring their classes and default rates
  (`limits()`), `classify`, and a `Call` per request (`Budgeted` on
  Telegram, constants on Discord).
- **The binary** builds one registry of limiters per service, and the pools
  from `[[store]]`.

## Config

```toml
[telegram]
upload_concurrency = 2
download_concurrency = 8
[telegram.bot]                   # every call of a bot
rate_per_sec = 25.0
burst = 30.0
[telegram.send]                  # a class: per chat and bot
rate_per_sec = 0.3
burst = 3.0
# [telegram.read], [telegram.delete]; [discord] takes the same tables
```

Unset values keep the service's defaults; a class the service does not have
is refused. Step 4 adds `bot_token_envs` to a `[[store]]` (a list; one
entry is today's `bot_token_env`) and with it the pool. Two stores on one
chat stay refused: one store, many bots.

## Metrics

- `pigeonhole_limiter_wait_seconds{chat, op}`: waits for our own buckets
  (exists).
- `pigeonhole_backend_throttle_wait_seconds{service, method}`: waits the
  service asked for (exists).
- `pigeonhole_limiter_retry_wait_seconds{service, method}`: the wait of a
  retry after a penalty, until its next attempt (§ Fairness).
- `pigeonhole_pool_calls_total{store, account}`: how the pool spreads load.

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
2. ✅ Plans taken at once in `pigeonhole-limits` (`take_all`).
3. ✅ `pigeonhole_limits::run` (a `Call`, a `Retry`, the service's
   `classify` → `Verdict`) for Telegram and Discord; `Budgeted` for the
   MTProto requests, so `TelegramMtproto::invoke` is the only way out
   besides the login; the hand-wired `acquire_*` / `penalize_*` are gone.
   The buckets run on the runtime's clock (`tokio::time::Instant`), so the
   simulation and paused tests see them too.
   After review (pigeonhole-ce): classes belong to the services; every API
   call takes its bot's token; a cooled bucket starts empty; a Discord
   429 cools its route's class, the bot only when `global`; one `Call` per
   Discord endpoint; config tables per rate.
4. Pools and `bot_token_envs`; tickets; a rate limit moves to a free
   account; the retry-wait metric.
5. Bench: log entries per second with 1, 2, 4 bots against
   [bench-discord-vs-telegram.md](bench-discord-vs-telegram.md).
6. Only as far as the bench shows a need: the Discord `Pacer` as a kind of
   bucket, `observe` from headers, priority for retries after a penalty.

## Open questions

- **Per bot or per channel?** Answered: per bot on both (§ Spike findings).
- **Handles across bots.** Answered in part: bot B can download what bot A
  posted, through its own `getMessages`. Whether A's `FileLocation` works
  for B directly was not tried: reads stay sticky.
- **More bots.** Two bots doubled the rate. Does it keep scaling at 4–8
  bots from one IP, or does a per-address limit appear (§ Levels and
  buckets: not in the model until seen)?
- **Bandwidth.** Is the ~2.7 MiB/s upload per stream capped per bot,
  per connection or per address?
