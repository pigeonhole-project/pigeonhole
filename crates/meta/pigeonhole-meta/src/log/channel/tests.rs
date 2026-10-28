use super::fake::FakeChannel;
use super::*;
use crate::log::conformance;
use futures::TryStreamExt;

fn opts() -> ChannelLogOptions {
    ChannelLogOptions { empty_retries: 2, empty_retry_delay: Duration::from_millis(1) }
}

fn store(chat: &Arc<FakeChannel>) -> Arc<ChannelLogStore<FakeChannel>> {
    Arc::new(ChannelLogStore::new(chat.clone(), opts()))
}

fn entry(i: u64) -> Entry {
    Entry { term: 1, payload: i.to_le_bytes().to_vec() }
}

async fn history(log: &dyn LogStore) -> Vec<u64> {
    let d = log.discover().await.unwrap().unwrap();
    log.read(0, d.head)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, e)| u64::from_le_bytes(e.payload.try_into().unwrap()))
        .collect()
}

#[tokio::test]
async fn passes_the_logstore_conformance_suite() {
    conformance::run_all(|| {
        let chat = Arc::new(FakeChannel::default());
        move || {
            let chat = chat.clone();
            async move { Arc::new(ChannelLogStore::new(chat, opts())) as Arc<dyn LogStore> }
        }
    })
    .await;
}

#[tokio::test]
async fn appends_in_a_dedicated_channel_read_nothing() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    let reads = chat.reads();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    assert_eq!(chat.reads(), reads, "each batch lands right above the last: one call per append");
}

#[tokio::test]
async fn a_gap_is_read_and_foreign_messages_are_skipped() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    chat.post_text();
    chat.post_document("photo.jpg", b"x");
    let reads = chat.reads();
    a.append(1, &[entry(2)]).await.unwrap();
    assert_eq!(chat.reads(), reads + 1, "one read of the two ids in between");
    assert_eq!(history(store(&chat).as_ref()).await, vec![1, 2]);
}

#[tokio::test]
async fn a_straggler_above_a_newer_claim_is_invalid_for_everyone() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let b = store(&chat);
    assert_eq!(b.claim(2).await.unwrap().position, 1);

    // A's next batch lands above B's claim (sent late, or after the claim):
    // A sees the claim in its range and acknowledges nothing.
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    assert!(matches!(a.append(1, &[entry(98)]).await, Err(LogError::Fenced { current: 2 })), "stays fenced");

    // The straggler is the top batch, yet no reader takes it.
    assert_eq!(history(store(&chat).as_ref()).await, vec![1]);
    b.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(history(store(&chat).as_ref()).await, vec![1, 2]);
    let c = store(&chat);
    assert_eq!(c.claim(3).await.unwrap().position, 2);
    assert_eq!(history(c.as_ref()).await, vec![1, 2]);
}

#[tokio::test]
async fn an_unconfirmed_batch_becomes_the_head() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // The batch lands, but A never learns it (and dies).
    chat.lose_next_answer();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Other(_))));
    assert!(a.append(1, &[entry(3)]).await.is_err(), "the handle cannot go on");

    // No newer claim precedes it: it is valid, and the next claimant takes
    // it as part of the history (never acknowledged, as Raft allows).
    let b = store(&chat);
    assert_eq!(b.claim(2).await.unwrap().position, 2);
    b.append(2, &[entry(3)]).await.unwrap();
    assert_eq!(history(b.as_ref()).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn racing_claims_leave_one_writer() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();

    // A higher claim below ours wins.
    let (b, c) = (store(&chat), store(&chat));
    c.claim(3).await.unwrap();
    assert!(matches!(b.claim(2).await, Err(LogError::Fenced { current: 3 })));
    // Ours below a higher one: we hold it until our next write sees it.
    let d = store(&chat);
    assert!(matches!(d.claim(4).await, Ok(_)));
    assert!(matches!(c.append(3, &[entry(2)]).await, Err(LogError::Fenced { current: 4 })));
    // Two claims of one generation: the lower id wins.
    chat.post_document(&claim_name(5), b"raced");
    assert!(matches!(store(&chat).claim(5).await, Err(LogError::Fenced { current: 5 })));
    // That claim fences D, and never wrote a batch: the log is still A's.
    assert!(matches!(d.append(4, &[entry(3)]).await, Err(LogError::Fenced { current: 5 })));
    let e = store(&chat);
    assert_eq!(e.claim(6).await.unwrap().position, 1);
    e.append(6, &[entry(4)]).await.unwrap();
    assert_eq!(history(e.as_ref()).await, vec![1, 4]);
}

#[tokio::test]
async fn a_claim_visible_late_still_fences() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // B's claim is invisible for the first read of A's range.
    chat.hide_next(1);
    store(&chat).claim(2).await.unwrap();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));

    // One that stays invisible: the outcome is unknown, never Ok.
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    chat.hide_next(100);
    store(&chat).claim(2).await.unwrap();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Other(_))));
}

#[tokio::test]
async fn a_changed_log_message_is_detected() {
    let chat = Arc::new(FakeChannel::default());
    let a = store(&chat);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    a.append(1, &[entry(2)]).await.unwrap();
    let first_batch = chat.names().iter().find(|(_, n)| n == "log-1-batch.bin").unwrap().0;
    chat.edit(first_batch + 1);
    let head = a.discover().await.unwrap().unwrap().head;
    let read: Result<Vec<_>, _> = a.read(0, head).try_collect().await;
    assert!(matches!(read, Err(LogError::Lost(_))), "{read:?}");
}

#[tokio::test]
async fn a_rebase_collects_our_messages_and_nothing_else() {
    let chat = Arc::new(FakeChannel::default());
    let foreign = chat.post_text();
    let a = store(&chat);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    a.rebase(1, Base { at: 3, term: 1, leader: 0, snapshot: crate::snapshot::SnapshotRef("s3".into()) }).await.unwrap();
    // Batches 4 and 5 and the rebase batch stay; the claim, the first
    // (empty) batch and batches 1–3 go; the foreign message stays.
    let names = chat.names();
    assert_eq!(names.iter().filter(|(_, n)| n.starts_with("log-")).count(), 3, "{names:?}");
    assert!(names.iter().any(|(id, _)| *id == foreign));

    a.append(1, &[entry(6)]).await.unwrap();
    let b = store(&chat);
    assert_eq!(b.claim(2).await.unwrap().position, 6);
    let head = b.discover().await.unwrap().unwrap().head;
    let got: Vec<_> = b.read(3, head).try_collect().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5, 6]);
}

#[tokio::test]
async fn a_claimed_log_without_a_batch_is_discovered_empty() {
    let chat = Arc::new(FakeChannel::default());
    chat.post_document(&claim_name(3), b"a claim whose writer died");
    let d = store(&chat).discover().await.unwrap().unwrap();
    assert_eq!((d.gen, d.head.position, d.base), (3, 0, None));
    let a = store(&chat);
    assert!(matches!(a.claim(3).await, Err(LogError::Fenced { current: 3 })));
    assert_eq!(a.claim(4).await.unwrap().position, 0);
}

#[tokio::test]
async fn a_fresh_walk_reads_each_page_once() {
    let chat = Arc::new(FakeChannel::default());
    for _ in 0..330 {
        chat.post_text();
    }
    let log = store(&chat);
    assert_eq!(log.discover().await.unwrap(), None);
    assert!(chat.reads() <= 4, "{} reads", chat.reads());
}
