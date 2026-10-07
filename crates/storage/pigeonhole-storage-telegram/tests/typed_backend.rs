//! Wiremock tests for TypedBlobBackend / Sweepable / TypedBootstrapPointer (no network).

use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, ChatLimiter, ChatLimiterConfig, LimitBudget, OpKind, Sweepable,
    TypedBlobBackend, TypedBootstrapPointer,
};
use pigeonhole_storage_telegram::{TelegramBlobStore, TelegramClient, DELETE_MESSAGES_MAX};
use pigeonhole_testkit::run_typed_conformance;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const TOKEN: &str = "123456:TEST-SECRET-TOKEN";
const CHAT: &str = "-100111";

fn extract_multipart_document(body: &[u8]) -> Vec<u8> {
    let markers: [&[u8]; 2] = [
        b"Content-Disposition: form-data; name=\"document\"",
        b"Content-Disposition: form-data; name=\"document\";",
    ];
    let start = markers
        .iter()
        .find_map(|m| body.windows(m.len()).position(|w| w == *m))
        .unwrap_or(0);
    let tail = &body[start..];
    let Some(hdr) = tail.windows(4).position(|w| w == b"\r\n\r\n") else {
        return body.to_vec();
    };
    let data = &tail[hdr + 4..];
    if let Some(end) = data.windows(4).position(|w| w == b"\r\n--") {
        return data[..end].to_vec();
    }
    data.to_vec()
}

struct TgState {
    next_msg: AtomicU64,
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    /// file_id → message_id
    msgs: Mutex<HashMap<i64, String>>,
    delete_batches: Mutex<Vec<Vec<i64>>>,
    pinned: Mutex<Option<(i64, String)>>,
}

fn form_field<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    // application/x-www-form-urlencoded
    for part in body.split('&') {
        let mut kv = part.splitn(2, '=');
        let k = kv.next()?;
        let v = kv.next().unwrap_or("");
        if k == name {
            return Some(v);
        }
    }
    None
}

fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '+' => out.push(' '),
            '%' => {
                let h1 = chars.next().unwrap_or('0');
                let h2 = chars.next().unwrap_or('0');
                let hex = format!("{h1}{h2}");
                if let Ok(b) = u8::from_str_radix(&hex, 16) {
                    out.push(b as char);
                }
            }
            other => out.push(other),
        }
    }
    out
}

async fn mount_mocks(server: &MockServer, state: Arc<TgState>) {
    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/sendDocument"))
        .respond_with(move |req: &Request| {
            let mid = st.next_msg.fetch_add(1, Ordering::SeqCst) as i64;
            let fid = format!("file-{mid}");
            let payload = extract_multipart_document(&req.body);
            st.blobs.lock().unwrap().insert(fid.clone(), payload);
            st.msgs.lock().unwrap().insert(mid, fid.clone());
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": {
                    "message_id": mid,
                    "document": { "file_id": fid }
                }
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/getFile"))
        .respond_with(move |req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let fid = form_field(&body, "file_id")
                .map(urldecode)
                .unwrap_or_default();
            if !st.blobs.lock().unwrap().contains_key(&fid) {
                return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": false,
                    "description": "file not found"
                }));
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": { "file_path": format!("docs/{fid}.bin") }
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    let uri = server.uri();
    Mock::given(method("GET"))
        .and(path_regex(r"/file/bot.+/docs/.+"))
        .respond_with(move |req: &Request| {
            let path = req.url.path();
            let name = path.rsplit('/').next().unwrap_or("");
            let fid = name.trim_end_matches(".bin");
            match st.blobs.lock().unwrap().get(fid) {
                Some(bytes) => ResponseTemplate::new(200).set_body_bytes(bytes.clone()),
                None => ResponseTemplate::new(404),
            }
        })
        .mount(server)
        .await;
    let _ = uri;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/deleteMessages"))
        .respond_with(move |req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let ids_raw = form_field(&body, "message_ids")
                .map(urldecode)
                .unwrap_or_else(|| "[]".into());
            let ids: Vec<i64> = serde_json::from_str(&ids_raw).unwrap_or_default();
            st.delete_batches.lock().unwrap().push(ids.clone());
            for mid in &ids {
                if let Some(fid) = st.msgs.lock().unwrap().remove(mid) {
                    st.blobs.lock().unwrap().remove(&fid);
                }
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": true
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/deleteMessage$"))
        .respond_with(move |req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let mid: i64 = form_field(&body, "message_id")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if let Some(fid) = st.msgs.lock().unwrap().remove(&mid) {
                st.blobs.lock().unwrap().remove(&fid);
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": true
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/sendMessage"))
        .respond_with(move |req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let text = form_field(&body, "text")
                .map(urldecode)
                .unwrap_or_default();
            let mid = st.next_msg.fetch_add(1, Ordering::SeqCst) as i64;
            *st.pinned.lock().unwrap() = Some((mid, text));
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": { "message_id": mid, "text": "" }
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/pinChatMessage"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": true
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/unpinChatMessage"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": true
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/bot.+/getChat"))
        .respond_with(move |_req: &Request| {
            let pinned = st.pinned.lock().unwrap().clone();
            let mut result = serde_json::json!({
                "id": -100111,
                "type": "channel",
                "title": "test"
            });
            if let Some((mid, text)) = pinned {
                result["pinned_message"] = serde_json::json!({
                    "message_id": mid,
                    "text": text
                });
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": result
            }))
        })
        .mount(server)
        .await;
}

fn fast_limiter() -> Arc<ChatLimiter> {
    Arc::new(ChatLimiter::new(ChatLimiterConfig {
        send_rate_per_sec: 1000.0,
        send_burst: 100.0,
        get_file_rate_per_sec: 1000.0,
        get_file_burst: 100.0,
        delete_rate_per_sec: 1000.0,
        delete_burst: 100.0,
        upload_concurrency: 4,
        download_concurrency: 4,
    }))
}

async fn make_store(server: &MockServer) -> (TelegramBlobStore, Arc<TgState>) {
    let state = Arc::new(TgState {
        next_msg: AtomicU64::new(100),
        blobs: Mutex::new(HashMap::new()),
        msgs: Mutex::new(HashMap::new()),
        delete_batches: Mutex::new(Vec::new()),
        pinned: Mutex::new(None),
    });
    mount_mocks(server, state.clone()).await;
    let tg = TelegramClient::with_api_base(TOKEN.into(), server.uri()).unwrap();
    let store = TelegramBlobStore::with_instance_id(tg, CHAT.into(), fast_limiter(), "tg-test");
    (store, state)
}

#[tokio::test]
async fn typed_put_get_delete_and_instance_info() {
    let server = MockServer::start().await;
    let (store, _st) = make_store(&server).await;

    assert_eq!(store.instance().fingerprint, "tg:123456:-100111");
    assert_eq!(store.instance().location, "tg:chat:-100111");

    let id = TypedBlobBackend::put(&store, Bytes::from_static(b"hello-typed"))
        .await
        .unwrap();
    let got = collect_stream(TypedBlobBackend::get(&store, &id, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(got.as_ref(), b"hello-typed");

    TypedBlobBackend::delete(&store, &[id.message_id])
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_conformance_suite() {
    let server = MockServer::start().await;
    let (store, _) = make_store(&server).await;
    run_typed_conformance(&store).await.unwrap();
}

#[tokio::test]
async fn delete_messages_batches_by_100() {
    let server = MockServer::start().await;
    let (store, st) = make_store(&server).await;

    let n = DELETE_MESSAGES_MAX + 3;
    let keys: Vec<i64> = (1..=n as i64).collect();
    TypedBlobBackend::delete(&store, &keys).await.unwrap();

    let batches = st.delete_batches.lock().unwrap().clone();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), DELETE_MESSAGES_MAX);
    assert_eq!(batches[1].len(), 3);
}

#[tokio::test]
async fn candidates_range_after_upto_limit() {
    let server = MockServer::start().await;
    let (store, _) = make_store(&server).await;

    let keys = Sweepable::candidates(&store, Some(10), 20, 5)
        .await
        .unwrap();
    assert_eq!(keys, vec![11, 12, 13, 14, 15]);

    let keys = Sweepable::candidates(&store, None, 3, 10).await.unwrap();
    assert_eq!(keys, vec![1, 2, 3]);
}

#[tokio::test]
async fn cost_get_zero_when_file_path_cached() {
    let server = MockServer::start().await;
    let (store, _) = make_store(&server).await;

    let id = TypedBlobBackend::put(&store, Bytes::from_static(b"cached-path"))
        .await
        .unwrap();
    // Prime getFile cache.
    let _ = collect_stream(TypedBlobBackend::get(&store, &id, None).await.unwrap())
        .await
        .unwrap();

    let hint = TypedBlobBackend::cost(&store, OpKind::Get, Some(&id));
    assert_eq!(hint.wait_secs, 0.0);

    // Exhaust getFile bucket; cached get must still report wait=0.
    store.limiter().penalize_get_file(std::time::Duration::from_secs(60));
    assert!(store.limiter().peek_wait(LimitBudget::GetFile) > std::time::Duration::ZERO);
    let hint2 = TypedBlobBackend::cost(&store, OpKind::Get, Some(&id));
    assert_eq!(hint2.wait_secs, 0.0);
}

#[tokio::test]
async fn typed_bootstrap_pin_swap_and_read() {
    let server = MockServer::start().await;
    let (store, _) = make_store(&server).await;

    assert!(TypedBootstrapPointer::read(&store).await.unwrap().is_none());
    TypedBootstrapPointer::swap(&store, Bytes::from_static(br#"{"gen":1}"#))
        .await
        .unwrap();
    let raw = TypedBootstrapPointer::read(&store).await.unwrap().unwrap();
    assert_eq!(raw.as_ref(), br#"{"gen":1}"#);
}

#[tokio::test]
async fn http_errors_do_not_leak_bot_token() {
    let server = MockServer::start().await;
    let tg = TelegramClient::with_api_base(TOKEN.into(), server.uri()).unwrap();
    // No mocks mounted → connection/HTTP failure.
    let err = tg
        .delete_messages(CHAT, &[1], None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        !err.contains(TOKEN),
        "error must not contain bot token: {err}"
    );
    assert!(!err.contains("TEST-SECRET"));
}
