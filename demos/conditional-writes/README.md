# Conditional writes

```
./demos/conditional-writes/run.sh
DEMO_BACKEND=chats ./demos/conditional-writes/run.sh
```

S3's two preconditions on a write, by curl against pigeonhole:

- `If-None-Match: *`: create only if the key is free. The second writer
  gets `412 Precondition Failed`.
- `If-Match: <etag>`: compare-and-swap. A stale ETag is refused; the current
  one writes. A missing key is `404`.
- Ten writers race to create one key at once: exactly one gets `200`.

Every S3 write is one entry in pigeonhole's metadata log, and the condition
is checked when that entry applies, so the race has a single winner on any
backend: a Telegram channel as much as a disk. This is the primitive that
leader locks (T4, Litestream) and table commits (Delta, Iceberg, SlateDB)
are built on. Design: [docs/conditional-writes.md](../../docs/conditional-writes.md).
