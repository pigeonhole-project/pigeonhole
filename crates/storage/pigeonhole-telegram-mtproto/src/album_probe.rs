//! Spike for docs/proposals/albums.md, Telegram side: albums
//! (`messages.sendMultiMedia`) of documents in `TG_BLOBS_CHAT_ID`. Calls go
//! around our limiter so the flood waits seen are Telegram's. Deletes only
//! what it posted. Inside the crate: it uses the client's private calls.
//!
//! cargo test -p pigeonhole-telegram-mtproto --lib album_probe -- --ignored --nocapture
//! (`PROBE=shape` or `PROBE=rate` for one phase; the workspace `.env` as in
//! tests/live.rs).

use super::*;
use futures::FutureExt;
use std::time::Instant;

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

fn uploaded(file: enums::InputFile, name: &str) -> enums::InputMedia {
    types::InputMediaUploadedDocument {
        nosound_video: false,
        force_file: true,
        spoiler: false,
        file,
        thumb: None,
        mime_type: "application/octet-stream".into(),
        attributes: vec![types::DocumentAttributeFilename { file_name: name.into() }.into()],
        stickers: None,
        video_cover: None,
        video_timestamp: None,
        ttl_seconds: None,
    }
    .into()
}

fn multi(ch: &MtprotoChannel, media: Vec<enums::InputMedia>) -> functions::messages::SendMultiMedia {
    functions::messages::SendMultiMedia {
        silent: true,
        background: false,
        clear_draft: false,
        noforwards: false,
        update_stickersets_order: false,
        invert_media: false,
        allow_paid_floodskip: false,
        peer: ch.input_peer(),
        reply_to: None,
        multi_media: media
            .into_iter()
            .map(|media| {
                types::InputSingleMedia { media, random_id: random_i64(), message: String::new(), entities: None }
                    .into()
            })
            .collect(),
        schedule_date: None,
        send_as: None,
        quick_reply_shortcut: None,
        effect: None,
        allow_paid_stars: None,
    }
}

fn single(ch: &MtprotoChannel, media: enums::InputMedia) -> functions::messages::SendMedia {
    functions::messages::SendMedia {
        silent: true,
        background: false,
        clear_draft: false,
        noforwards: false,
        update_stickersets_order: false,
        invert_media: false,
        allow_paid_floodskip: false,
        peer: ch.input_peer(),
        reply_to: None,
        media,
        message: String::new(),
        random_id: random_i64(),
        reply_markup: None,
        entities: None,
        schedule_date: None,
        schedule_repeat_period: None,
        send_as: None,
        quick_reply_shortcut: None,
        effect: None,
        allow_paid_stars: None,
        suggested_post: None,
    }
}

/// (message id, grouped_id, file name) of the posts in an answer.
fn posts(u: &enums::Updates) -> Vec<(i32, Option<i64>, String)> {
    let list = match u {
        enums::Updates::Updates(u) => u.updates.clone(),
        enums::Updates::Combined(u) => u.updates.clone(),
        _ => Vec::new(),
    };
    list.into_iter()
        .filter_map(|u| match u {
            enums::Update::NewChannelMessage(m) => match m.message {
                enums::Message::Message(m) => {
                    let name = match channel_message(enums::Message::Message(m.clone())).1 {
                        ChannelMessage::Document { name, .. } => name,
                        other => format!("{other:?}"),
                    };
                    Some((m.id, m.grouped_id, name))
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// `request` raw, waiting out flood waits; the waits seen.
async fn raw<R: tl::RemoteCall>(
    ch: &MtprotoChannel,
    label: &str,
    request: &R,
) -> (Result<R::Return, String>, Vec<u32>) {
    let mut waits = Vec::new();
    loop {
        match ch.client.home(ch.lane, request).await {
            Ok(r) => return (Ok(r), waits),
            Err(InvocationError::Rpc(e)) if e.code == 420 || e.name.ends_with("_WAIT") => {
                let s = e.value.unwrap_or(1);
                println!("  {label}: {} {s}s", e.name);
                waits.push(s);
                tokio::time::sleep(Duration::from_secs(u64::from(s))).await;
            }
            Err(e) => return (Err(e.to_string()), waits),
        }
    }
}

async fn delete(ch: &MtprotoChannel, ids: &[i32]) {
    for page in ids.chunks(100) {
        let req = functions::channels::DeleteMessages { channel: ch.input_channel(), id: page.to_vec() };
        let (r, _) = raw(ch, "delete", &req).await;
        println!("deleted {} messages: {:?}", page.len(), r.map(|_| ()));
    }
}

async fn connect() -> MtprotoChannel {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_BLOBS_CHAT_ID").parse().unwrap();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-album.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let limiter = Arc::new(ChatLimiter::new(pigeonhole_storage_telegram::limits()));
    tg.channel(chat_id, Lane::Blobs, limiter).await.unwrap()
}

/// 10 small documents, uploaded once; uploadMedia turns each into an
/// InputMediaDocument that can be sent again and again.
async fn documents(ch: &MtprotoChannel) -> Vec<enums::InputMedia> {
    let mut docs = Vec::new();
    let mut upload_waits = Vec::new();
    let t = Instant::now();
    for i in 0..10 {
        let name = format!("ph-album-probe-{i}.bin");
        let data = Bytes::from(vec![i as u8; 64 * 1024 + i]);
        let file = ch.upload(&name, &data).await.unwrap();
        let req = functions::messages::UploadMedia {
            business_connection_id: None,
            peer: ch.input_peer(),
            media: uploaded(file, &name),
        };
        let (m, w) = raw(ch, "uploadMedia", &req).await;
        upload_waits.extend(w);
        let enums::MessageMedia::Document(types::MessageMediaDocument {
            document: Some(enums::Document::Document(d)),
            ..
        }) = m.unwrap()
        else {
            panic!("uploadMedia answered no document")
        };
        docs.push(enums::InputMedia::from(types::InputMediaDocument {
            spoiler: false,
            id: types::InputDocument { id: d.id, access_hash: d.access_hash, file_reference: d.file_reference }.into(),
            video_cover: None,
            video_timestamp: None,
            ttl_seconds: None,
            query: None,
        }));
    }
    println!("10 uploads + uploadMedia in {:?}, flood waits {upload_waits:?}", t.elapsed());
    docs
}

/// An album of `media` that may be refused: what came of it.
async fn try_album(ch: &MtprotoChannel, label: &str, media: Vec<enums::InputMedia>, posted: &mut Vec<i32>) {
    match raw(ch, label, &multi(ch, media)).await.0 {
        Ok(u) => {
            let p = posts(&u);
            println!("{label}: ok {p:?}");
            posted.extend(p.iter().map(|p| p.0));
        }
        Err(e) => println!("{label}: {e}"),
    }
}

/// What an album is: the refused shapes, then a proper one read back and
/// one of its messages deleted.
async fn shape(ch: &MtprotoChannel, docs: &[enums::InputMedia], posted: &mut Vec<i32>) {
    // Uploaded files straight into an album (no uploadMedia).
    let mut media = Vec::new();
    for i in 0..2 {
        let name = format!("ph-album-direct-{i}.bin");
        let data = Bytes::from(vec![7u8; 1000 + i]);
        media.push(uploaded(ch.upload(&name, &data).await.unwrap(), &name));
    }
    try_album(ch, "direct album (InputMediaUploadedDocument)", media, posted).await;
    let mut eleven = docs.to_vec();
    eleven.push(docs[0].clone());
    try_album(ch, "11-album", eleven, posted).await;
    // A proper 10-album: ids, grouped_id, names; then as read back.
    let (r, _) = raw(ch, "album", &multi(ch, docs.to_vec())).await;
    let p = posts(&r.unwrap());
    println!("10-album: {p:?}");
    posted.extend(p.iter().map(|p| p.0));
    let ids: Vec<i64> = p.iter().map(|p| i64::from(p.0)).collect();
    let documents =
        |back: &[ChannelMessage]| back.iter().filter(|m| matches!(m, ChannelMessage::Document { .. })).count();
    println!("read back: {} documents", documents(&ch.get_messages(&ids).await.unwrap()));
    // Delete one of the album's messages: the rest stay?
    delete(ch, &[p[3].0]).await;
    println!("after deleting {}: documents left {}", p[3].0, documents(&ch.get_messages(&ids).await.unwrap()));
}

/// Albums back to back, then as many single sends: does one album spend 1
/// or 10 sends?
async fn rate(ch: &MtprotoChannel, docs: &[enums::InputMedia], posted: &mut Vec<i32>) {
    tokio::time::sleep(Duration::from_secs(60)).await;
    let t = Instant::now();
    for k in 0..8 {
        let at = t.elapsed();
        let (r, w) = raw(ch, &format!("album {k}"), &multi(ch, docs.to_vec())).await;
        let p = posts(&r.unwrap());
        posted.extend(p.iter().map(|p| p.0));
        println!("album {k}: {} posts at {at:.1?}, waits {w:?}", p.len());
    }
    println!("8 albums (80 messages) in {:.1?}", t.elapsed());
    tokio::time::sleep(Duration::from_secs(90)).await;
    let t = Instant::now();
    for k in 0..16 {
        let at = t.elapsed();
        let (r, w) = raw(ch, &format!("single {k}"), &single(ch, docs[k % 10].clone())).await;
        posted.extend(posts(&r.unwrap()).iter().map(|p| p.0));
        println!("single {k}: at {at:.1?}, waits {w:?}");
    }
    println!("16 singles in {:.1?}", t.elapsed());
}

#[tokio::test]
#[ignore = "posts and deletes messages in the real blobs channel"]
async fn album_probe() {
    let ch = connect().await;
    let phases = std::env::var("PROBE").unwrap_or_else(|_| "shape,rate".into());
    let docs = documents(&ch).await;
    let mut posted: Vec<i32> = Vec::new();
    let result = std::panic::AssertUnwindSafe(Box::pin(async {
        if phases.contains("shape") {
            shape(&ch, &docs, &mut posted).await;
        }
        if phases.contains("rate") {
            rate(&ch, &docs, &mut posted).await;
        }
    }))
    .catch_unwind()
    .await;
    posted.sort();
    posted.dedup();
    delete(&ch, &posted).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
