# etcd on Telegram

```
./demos/etcd-on-telegram/run.sh
DEMO_BACKEND=chats ./demos/etcd-on-telegram/run.sh
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

| | disk | chats |
|---|---|---|
| election | < 1 s | ~1 s |
| etcd put (a lone leader flushes every batch to S3) | ms | 0.4–0.6 s, tails to several s |
| takeover after `kill -9` | a few s | ~10 s |

T4's leader lock is valid for 6 s. With the metadata log on Discord, a write
now and then waits several seconds for a lease renewal, so a lock renewal
can time out right after a takeover; it has held in our runs, but the
margins are thin (docs/conditional-writes.md § Live).

## Toward Kubernetes

k3s only runs on Linux (not tried yet). T4 speaks the etcd protocol, so k3s
can take it as an external etcd: `t4 run …` as here, then
`k3s server --datastore-endpoint=http://127.0.0.1:23791`. kine can also embed
T4 as a driver (`kine --endpoint t4:///var/lib/t4 --s3-bucket … --s3-endpoint …`,
kine's `pkg/drivers/t4`). Either way the cluster's state lives in the channel.
