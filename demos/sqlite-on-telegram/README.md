# SQLite on Telegram

```
./demos/sqlite-on-telegram/run.sh
DEMO_BACKEND=chats ./demos/sqlite-on-telegram/run.sh
```

[Litestream](https://litestream.io) streams a SQLite database's WAL to S3
as LTX files and restores it from there, to the latest state or to a moment
in the past. Pointed at pigeonhole, the database lives in a chat.

The demo:

1. creates a SQLite database and starts `litestream replicate` into
   pigeonhole;
2. inserts a few rows, notes the time, inserts more;
3. lists what Litestream keeps in the bucket;
4. deletes the local database;
5. restores it whole, then as of the noted time (only the first rows).

Litestream also has a SQLite VFS that queries the replica in the bucket
directly, and in v0.5 a lease on S3 conditional writes so that only one
instance writes a replica; both rest on what pigeonhole's gateway offers.
