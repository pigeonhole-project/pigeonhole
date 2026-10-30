//! Against the test guild's channels (`DISCORD_BLOBS_CHANNEL_ID`,
//! `DISCORD_LOG_CHANNEL_ID`, `DISCORD_SNAP_CHANNEL_ID`, `DISCORD_BOT_TOKEN` in
//! the workspace `.env`): `cargo test -p pigeonhole-storage-discord --test
//! live -- --ignored --nocapture --test-threads 1`. The channels must be
//! dedicated: each test empties its channel at the end (messages and links).

use bytes::Bytes;
use futures::TryStreamExt;
use pigeonhole_limits::ChatLimiter;
use pigeonhole_meta::{
    Base, BlobSnapshotStore, ChainLogStore, Entry, LogError, LogId, LogStore, SnapshotRef, SnapshotStore,
    StoredSnapshot,
};
use pigeonhole_storage_api::{collect_stream, BlobBackend, BlobId, InstanceInfo, InstanceRole};
use pigeonhole_storage_discord::{DiscordApi, DiscordBlobStore, DiscordChannel, DiscordClient, PAGE};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn env(k: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

fn channel(var: &str) -> (Arc<DiscordChannel>, Arc<ChatLimiter>, DiscordClient, u64) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .try_init();
    let id: u64 = env(var).parse().unwrap();
    let client = DiscordClient::new(env("DISCORD_BOT_TOKEN")).unwrap();
    let limiter = Arc::new(ChatLimiter::new(pigeonhole_storage_discord::limits()));
    (Arc::new(client.channel(id, limiter.clone())), limiter, client, id)
}

fn store_on(ch: &Arc<DiscordChannel>, limiter: &Arc<ChatLimiter>, id: u64) -> Arc<DiscordBlobStore<DiscordChannel>> {
    let info = InstanceInfo {
        id: format!("live-{id}"),
        kind: pigeonhole_storage_discord::KIND,
        fingerprint: format!("dc:live:{id}"),
        location: format!("dc:channel:{id}"),
        role: InstanceRole::ReadWrite,
    };
    Arc::new(DiscordBlobStore::new(ch.clone(), limiter.clone(), None, info))
}

/// Delete every message of a dedicated test channel, and the links (threads)
/// of those messages and of what they name as `prev`.
async fn empty(ch: &DiscordChannel) {
    let mut ids = Vec::new();
    let mut before = None;
    loop {
        let page = ch.list(before, None, PAGE).await.unwrap();
        ids.extend(page.iter().map(|m| m.id));
        for m in &page {
            let name = m
                .attachment
                .as_ref()
                .map(|a| a.filename.clone())
                .or_else(|| pigeonhole_storage_api::text::name_of(&m.content).map(str::to_string));
            if let Some(prev) = name.and_then(|n| n.strip_suffix(".bin")?.rsplit_once('~')?.1.parse::<u64>().ok()) {
                ids.push(prev);
            }
        }
        match page.first() {
            Some(m) if page.len() == PAGE => before = Some(m.id),
            _ => break,
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ch.delete_messages(&ids).await.unwrap();
    for id in &ids {
        ch.delete_thread(*id).await.unwrap();
    }
    println!("emptied: {} messages / links", ids.len());
}

fn entry(i: u64) -> Entry {
    Entry { term: 1, payload: i.to_le_bytes().to_vec() }
}

#[tokio::test]
#[ignore = "needs a Discord bot and a test channel"]
async fn blob_store_against_a_live_channel() {
    let (ch, limiter, client, id) = channel("DISCORD_BLOBS_CHANNEL_ID");
    client.ensure_permissions(id, false, &limiter).await.unwrap();
    let store = store_on(&ch, &limiter, id);

    let big: Bytes = (0..15 * 1024 * 1024 + 4321).map(|i: usize| (i * 7 % 253) as u8).collect::<Vec<_>>().into();
    let t = Instant::now();
    let name = BlobId::random().name();
    let big_id = store.put(big.clone(), &name).await.unwrap();
    println!("put {} B as an attachment in {:?}", big.len(), t.elapsed());
    assert_eq!(store.put(big.clone(), &name).await.unwrap(), big_id, "a repeat posts nothing new (nonce)");
    let fresh = store_on(&ch, &limiter, id);
    let t = Instant::now();
    assert!(collect_stream(fresh.get(&big_id, None).await.unwrap()).await.unwrap() == big);
    println!("get by a fresh store in {:?}", t.elapsed());
    let part = collect_stream(fresh.get(&big_id, Some(1_048_000..3_200_000)).await.unwrap()).await.unwrap();
    assert!(part == big.slice(1_048_000..3_200_000));

    let small: Bytes = (0..3_600).map(|i: usize| (i * 13) as u8).collect::<Vec<_>>().into();
    let small_id = store.put(small.clone(), &BlobId::random().name()).await.unwrap();
    assert!(collect_stream(fresh.get(&small_id, None).await.unwrap()).await.unwrap() == small);
    println!("{} B as a text message, read back exactly", small.len());

    pigeonhole_testkit::run_typed_conformance(store.as_ref()).await.unwrap();
    empty(&ch).await;
}

#[tokio::test]
#[ignore = "needs a Discord bot and a test channel"]
async fn chain_log_against_a_live_channel() {
    let (ch, limiter, client, id) = channel("DISCORD_LOG_CHANNEL_ID");
    client.ensure_permissions(id, true, &limiter).await.unwrap();
    let open = || Arc::new(ChainLogStore::new(store_on(&ch, &limiter, id)));

    let a = open();
    let t = Instant::now();
    a.claim(1).await.unwrap();
    println!("claim on an empty channel (anchor + link) in {:?}", t.elapsed());
    let t = Instant::now();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    println!("5 appends in {:?}", t.elapsed());

    // Two claimants at once: exactly one holds the log.
    let (b, c) = (open(), open());
    let (rb, rc) = tokio::join!(b.claim(2), c.claim(2));
    assert_eq!([&rb, &rc].iter().filter(|r| r.is_ok()).count(), 1, "{rb:?} {rc:?}");
    let winner = if rb.is_ok() { b } else { c };
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    winner.append(2, &[entry(6)]).await.unwrap();
    winner.rebase(2, Base { at: 4, term: 1, leader: 0, snapshot: SnapshotRef("s4".into()) }, 1).await.unwrap();
    winner.settle().await; // the collection runs in the background
    winner.append(2, &[entry(7)]).await.unwrap();

    let reader = open();
    let t = Instant::now();
    let d = reader.discover().await.unwrap().unwrap();
    let got: Vec<u64> = reader
        .read(4, d.head)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, e)| u64::from_le_bytes(e.payload.try_into().unwrap()))
        .collect();
    println!("discover + read in {:?}", t.elapsed());
    assert_eq!((d.gen, d.head.position, got), (2, 7, vec![5, 6, 7]));
    empty(&ch).await;
}

/// docs/discord-lease.md: batches are plain posts; a claimant waits out
/// the lease.
#[tokio::test]
#[ignore = "needs a Discord bot and a test channel"]
async fn lease_log_against_a_live_channel() {
    let (ch, limiter, client, id) = channel("DISCORD_LOG_CHANNEL_ID");
    client.ensure_permissions(id, true, &limiter).await.unwrap();
    let lease = pigeonhole_meta::Lease::new(std::time::Duration::from_secs(10));
    let open = || Arc::new(ChainLogStore::new(store_on(&ch, &limiter, id)).with_lease(lease));

    let a = open();
    let t = Instant::now();
    a.claim(1).await.unwrap();
    println!("claim on an empty channel in {:?}", t.elapsed());
    let t = Instant::now();
    for i in 1..=10 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    println!("10 appends in {:?} (a renewal included if a third of the lease went by)", t.elapsed());

    let b = open();
    let t = Instant::now();
    assert_eq!(b.claim(2).await.unwrap().position, 10);
    println!("a second claim waited out the lease: {:?}", t.elapsed());
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    b.append(2, &[entry(11)]).await.unwrap();

    let reader = open();
    let d = reader.discover().await.unwrap().unwrap();
    let got: Vec<u64> = reader
        .read(0, d.head)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, e)| u64::from_le_bytes(e.payload.try_into().unwrap()))
        .collect();
    assert_eq!((d.gen, got), (2, (1..=11).collect::<Vec<_>>()));

    // A clean shutdown: the next claim goes in at once.
    b.release(2).await.unwrap();
    let t = Instant::now();
    assert_eq!(open().claim(3).await.unwrap().position, 11);
    println!("a claim after a release in {:?}", t.elapsed());
    empty(&ch).await;
}

#[tokio::test]
#[ignore = "needs a Discord bot and a test channel"]
async fn snapshots_against_a_live_channel() {
    let (ch, limiter, client, id) = channel("DISCORD_SNAP_CHANNEL_ID");
    client.ensure_permissions(id, true, &limiter).await.unwrap();
    let store = BlobSnapshotStore::new(store_on(&ch, &limiter, id));
    let mut last: Option<SnapshotRef> = None;
    for at in 1..=3u64 {
        let snap = StoredSnapshot { log_id: LogId(7), at, data: vec![at as u8; 5000 * at as usize] };
        let r = store.put(&snap).await.unwrap();
        if let Some(old) = last.replace(r) {
            store.remove(&old).await.unwrap();
        }
    }
    let fresh = BlobSnapshotStore::new(store_on(&ch, &limiter, id));
    assert_eq!(fresh.newest().await.unwrap().map(|s| s.at), Some(3));
    empty(&ch).await;
}

/// Empty the three test channels (after a live run of anything, e.g.
/// `METRICS_DEMO_BACKEND=discord scripts/metrics-demo.sh`).
#[tokio::test]
#[ignore = "needs a Discord bot and the test channels"]
async fn empty_test_channels() {
    for var in ["DISCORD_BLOBS_CHANNEL_ID", "DISCORD_LOG_CHANNEL_ID", "DISCORD_SNAP_CHANNEL_ID"] {
        let (ch, ..) = channel(var);
        empty(&ch).await;
    }
}
