# etcd on Telegram

```
./demos/etcd-on-telegram/run.sh                         # local disk
DEMO_BACKEND=discord ./demos/etcd-on-telegram/run.sh    # metadata and objects in Discord
DEMO_BACKEND=chats ./demos/etcd-on-telegram/run.sh      # objects in Telegram: see below
```

[T4](https://github.com/t4db/t4) is a key-value store that speaks the etcd v3
API and keeps its WAL and checkpoints in S3; [kine](https://github.com/k3s-io/kine)
can use it as the datastore of k3s or any kube-apiserver. Its nodes elect a
leader through one object, `leader-lock`, written with `If-None-Match: *` /
`If-Match: <etag>`. Pointed at pigeonhole, that object is a message in a
Telegram channel and the decision who wrote it first is pigeonhole's log in
Discord.

The demo:

1. starts pigeonhole and two T4 nodes on it; one elects itself, the other
   follows;
2. writes Kubernetes-shaped keys with etcdctl, runs a transaction and a
   lease;
3. lists what T4 keeps in the bucket (`leader-lock`, WAL segments);
4. `kill -9`s the leader: the follower catches up from the bucket and takes
   the lock;
5. writes through the new leader.

## What to expect

| | disk | discord | chats (objects in Telegram) |
|---|---|---|---|
| election | < 1 s | ~1 s | ~1 s |
| etcd put (a lone leader flushes every batch to S3) | ms | ~0.5 s | 0.4–0.6 s, tails to several s |
| takeover after `kill -9` | ~10 s | ~11 s | ~12 s, then the new leader may step down |

T4's lock is valid for 6 s and each renewal gets 5 s (both fixed in T4).
Right after a takeover T4 uploads a checkpoint, a burst of objects. In a
Telegram channel (~20 posts a minute) the lock renewal queues behind them,
times out, may still land, and the next renewal, carrying the old ETag, is
refused: T4 reads that as "lock taken" and steps down. Safe (never two
leaders), but etcd answers `no leader` until a node elects itself again.
Discord takes posts fast enough that this does not happen.
(docs/conditional-writes.md § Live.)

## Toward Kubernetes

k3s only runs on Linux (not tried yet). T4 speaks the etcd protocol, so k3s
can take it as an external etcd: `t4 run …` as here, then
`k3s server --datastore-endpoint=http://127.0.0.1:23791`. kine can also embed
T4 as a driver (`kine --endpoint t4:///var/lib/t4 --s3-bucket … --s3-endpoint …`,
kine's `pkg/drivers/t4`). Either way the cluster's state lives in the channel.
