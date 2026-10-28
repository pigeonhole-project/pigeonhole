use super::*;
use crate::log::conformance;
use futures::TryStreamExt;
use super::fake::FakeChat;

fn opts(window_ms: u64) -> ChatLogOptions {
    ChatLogOptions {
        straggler_window: Duration::from_millis(window_ms),
        head_retries: 2,
        head_retry_delay: Duration::from_millis(1),
    }
}

fn store(chat: &Arc<FakeChat>, window_ms: u64) -> Arc<ChatLogStore<FakeChat>> {
    Arc::new(ChatLogStore::new(chat.clone(), opts(window_ms)))
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
        let chat = Arc::new(FakeChat::default());
        move || {
            let chat = chat.clone();
            async move { Arc::new(ChatLogStore::new(chat, opts(0))) as Arc<dyn LogStore> }
        }
    })
    .await;
}

#[tokio::test]
async fn a_straggler_inside_the_claim_window_joins_the_history() {
    let chat = Arc::new(FakeChat::default());
    let a = store(&chat, 0);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();

    // B claims; while it waits out the window, A's in-flight batch lands.
    let b = store(&chat, 100);
    let claim = tokio::spawn({
        let b = b.clone();
        async move { b.claim(2).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
    let head = claim.await.unwrap().unwrap();

    // The straggler was never acknowledged, but it is on A's complete chain:
    // B takes it as part of the history (outcome unknown, as in Raft).
    assert_eq!(head.position, 2);
    b.append(2, &[entry(3)]).await.unwrap();
    assert_eq!(history(b.as_ref()).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_late_straggler_is_overridden_by_the_next_append() {
    let chat = Arc::new(FakeChat::default());
    let a = store(&chat, 0);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let b = store(&chat, 0);
    b.claim(2).await.unwrap();
    b.append(2, &[entry(2)]).await.unwrap();

    // A straggler past the window (the assumption broken): it tops the pin and
    // links to A's chain, without B's entry.
    assert!(a.append(1, &[entry(99)]).await.is_err());
    // B's next append notices it is not on top and pins a newer message.
    b.append(2, &[entry(3)]).await.unwrap();
    assert_eq!(history(b.as_ref()).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn racing_claimants_leave_one_writer() {
    let chat = Arc::new(FakeChat::default());
    let a = store(&chat, 0);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();

    // The description is overwritten blindly: whichever wrote last wins, the
    // other finds out after the window (or on its first append).
    let (b, c) = (store(&chat, 50), store(&chat, 50));
    let (rb, rc) = tokio::join!(b.claim(2), c.claim(3));
    let ((winner, wgen), (loser, lgen)) = match (&rb, &rc) {
        (Ok(_), Err(LogError::Fenced { .. })) => ((b.clone(), 2), (c.clone(), 3)),
        (Err(LogError::Fenced { .. }), Ok(_)) => ((c.clone(), 3), (b.clone(), 2)),
        other => panic!("exactly one claim must win: {other:?}"),
    };
    assert!(loser.append(lgen, &[entry(2)]).await.is_err());
    winner.append(wgen, &[entry(3)]).await.unwrap();
    assert_eq!(history(winner.as_ref()).await, vec![1, 3]);
}

#[tokio::test]
async fn refuses_a_chat_used_for_something_else() {
    let chat = Arc::new(FakeChat::default());
    chat.set_raw_description("our team chat");
    let err = store(&chat, 0).claim(1).await.unwrap_err();
    assert!(err.to_string().contains("dedicated chat"), "{err}");

    let chat = Arc::new(FakeChat::default());
    chat.post_text_and_pin();
    let err = store(&chat, 0).claim(1).await.unwrap_err();
    assert!(err.to_string().contains("dedicated chat"), "{err}");
}

#[tokio::test]
async fn a_changed_log_message_is_detected() {
    let chat = Arc::new(FakeChat::default());
    let a = store(&chat, 0);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    a.append(1, &[entry(2)]).await.unwrap();
    // Someone edits the older message's file in place.
    {
        let mut g = chat.inner.lock().unwrap();
        let first = *g.docs.keys().next().unwrap();
        g.docs.get_mut(&first).unwrap().1.push(0);
    }
    let head = a.discover().await.unwrap().unwrap().head;
    let read: Result<Vec<_>, _> = a.read(0, head).try_collect().await;
    assert!(matches!(read, Err(LogError::Lost(_))), "{read:?}");
}

#[tokio::test]
async fn a_rebase_deletes_the_messages_it_covers() {
    let chat = Arc::new(FakeChat::default());
    let a = store(&chat, 0);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    a.rebase(1, Base { at: 3, term: 1, snapshot: crate::snapshot::SnapshotRef("s3".into()) }).await.unwrap();
    // Batches 4 and 5 and the rebase message stay.
    assert_eq!(chat.inner.lock().unwrap().docs.len(), 3);
    a.append(1, &[entry(6)]).await.unwrap();
    let b = store(&chat, 0);
    assert_eq!(b.claim(2).await.unwrap().position, 6);
    let head = b.discover().await.unwrap().unwrap().head;
    let got: Vec<_> = b.read(3, head).try_collect().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5, 6]);
}
