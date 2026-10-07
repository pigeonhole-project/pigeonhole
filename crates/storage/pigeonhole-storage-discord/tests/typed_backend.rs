use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, ChatLimiter, ChatLimiterConfig, CostHint, OpKind, Sweepable, BlobBackend,
    TypedBootstrapPointer,
};
use pigeonhole_storage_discord::{
    discord_fingerprint, discord_location, snowflake_timestamp_ms, DiscordBlobStore, DiscordClient,
    DiscordId, DISCORD_EPOCH_MS,
};
use pigeonhole_testkit::run_typed_conformance;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const CHANNEL: &str = "999888777";
const TOKEN: &str = "app42.secret.part";

fn extract_multipart_payload(body: &[u8]) -> Vec<u8> {
    let marker = b"Content-Disposition: form-data; name=\"files[0]\";";
    let start = body
        .windows(marker.len())
        .position(|w| w == marker)
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

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build a snowflake for approximately `age_secs` ago.
fn snowflake_age_secs_ago(age_secs: u64) -> u64 {
    let now = unix_now_ms();
    let ts = now.saturating_sub(age_secs * 1000);
    ((ts.saturating_sub(DISCORD_EPOCH_MS)) << 22) | 1
}

struct MockState {
    next_msg: AtomicU64,
    blobs: Mutex<HashMap<u64, Vec<u8>>>,
    messages: Mutex<Vec<u64>>,
    /// message_id → text content (for bootstrap pins).
    message_text: Mutex<HashMap<u64, String>>,
    deleted: Mutex<Vec<u64>>,
    bulk_calls: Mutex<Vec<Vec<u64>>>,
    pins: Mutex<Vec<serde_json::Value>>,
}

async fn mount_core_mocks(server: &MockServer, state: Arc<MockState>, server_uri: &str) {
    let st = state.clone();
    let uri = server_uri.to_string();
    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages$"))
        .respond_with(move |req: &Request| {
            // JSON text send (bootstrap) vs multipart attachment.
            let ct = req
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if ct.starts_with("application/json") {
                let mid = st.next_msg.fetch_add(1, Ordering::SeqCst);
                st.messages.lock().unwrap().push(mid);
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).unwrap_or(serde_json::json!({}));
                let content = body
                    .get("content")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                st.message_text
                    .lock()
                    .unwrap()
                    .insert(mid, content.clone());
                return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": mid.to_string(),
                    "content": content,
                    "attachments": []
                }));
            }
            let blob = extract_multipart_payload(&req.body);
            let mid = st.next_msg.fetch_add(1, Ordering::SeqCst);
            let aid = mid * 10;
            st.blobs.lock().unwrap().insert(aid, blob);
            st.messages.lock().unwrap().push(mid);
            let url = format!("{uri}/cdn/{aid}");
            ResponseTemplate::new(200)
                .insert_header("x-ratelimit-remaining", "5")
                .insert_header("x-ratelimit-reset-after", "0")
                .set_body_json(serde_json::json!({
                    "id": mid.to_string(),
                    "attachments": [{
                        "id": aid.to_string(),
                        "url": url,
                        "filename": "a.bin"
                    }]
                }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    let uri = server_uri.to_string();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/messages/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid: u64 = req
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let aid = mid * 10;
            if !st.blobs.lock().unwrap().contains_key(&aid) {
                return ResponseTemplate::new(404);
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": mid.to_string(),
                "attachments": [{
                    "id": aid.to_string(),
                    "url": format!("{uri}/cdn/{aid}"),
                    "filename": "a.bin"
                }]
            }))
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/cdn/[0-9]+"))
        .respond_with(move |req: &Request| {
            let aid: u64 = req
                .url
                .path()
                .trim_start_matches("/cdn/")
                .parse()
                .unwrap_or(0);
            match st.blobs.lock().unwrap().get(&aid).cloned() {
                Some(data) => {
                    ResponseTemplate::new(200).set_body_raw(data, "application/octet-stream")
                }
                None => ResponseTemplate::new(404),
            }
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/messages$"))
        .respond_with(move |req: &Request| {
            let after: u64 = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse().ok())
                .unwrap_or(0);
            let limit: usize = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "limit")
                .and_then(|(_, v)| v.parse().ok())
                .unwrap_or(50);
            let mut ids: Vec<u64> = st
                .messages
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|&id| id > after)
                .collect();
            ids.sort_unstable();
            // API returns newest → oldest.
            ids.reverse();
            ids.truncate(limit);
            let body: Vec<_> = ids
                .into_iter()
                .map(|mid| {
                    serde_json::json!({
                        "id": mid.to_string(),
                        "content": "",
                        "attachments": []
                    })
                })
                .collect();
            ResponseTemplate::new(200)
                .insert_header("x-ratelimit-remaining", "0")
                .insert_header("x-ratelimit-reset-after", "0.25")
                .set_body_json(body)
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("DELETE"))
        .and(path_regex(r"/api/v10/channels/.+/messages/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid: u64 = req
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let mut deleted = st.deleted.lock().unwrap();
            if deleted.contains(&mid) || !st.messages.lock().unwrap().contains(&mid) {
                return ResponseTemplate::new(404);
            }
            deleted.push(mid);
            drop(deleted);
            st.messages.lock().unwrap().retain(|&m| m != mid);
            st.blobs.lock().unwrap().remove(&(mid * 10));
            ResponseTemplate::new(204)
                .insert_header("x-ratelimit-remaining", "0")
                .insert_header("x-ratelimit-reset-after", "0.5")
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages/bulk-delete$"))
        .respond_with(move |req: &Request| {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::json!({}));
            let ids: Vec<u64> = body
                .get("messages")
                .and_then(|m| m.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str()?.parse().ok())
                        .collect()
                })
                .unwrap_or_default();
            st.bulk_calls.lock().unwrap().push(ids.clone());
            for mid in &ids {
                st.deleted.lock().unwrap().push(*mid);
                st.messages.lock().unwrap().retain(|&m| m != *mid);
                st.blobs.lock().unwrap().remove(&(mid * 10));
            }
            ResponseTemplate::new(204)
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/pins$"))
        .respond_with(move |_req: &Request| {
            let pins = st.pins.lock().unwrap().clone();
            ResponseTemplate::new(200).set_body_json(pins)
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("PUT"))
        .and(path_regex(r"/api/v10/channels/.+/pins/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid_s = req
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap_or("0")
                .to_string();
            let mid: u64 = mid_s.parse().unwrap_or(0);
            let content = st
                .message_text
                .lock()
                .unwrap()
                .get(&mid)
                .cloned()
                .unwrap_or_default();
            let mut pins = st.pins.lock().unwrap();
            pins.clear();
            pins.push(serde_json::json!({
                "id": mid_s,
                "content": content,
                "attachments": []
            }));
            ResponseTemplate::new(204)
        })
        .mount(server)
        .await;

    let st = state.clone();
    Mock::given(method("DELETE"))
        .and(path_regex(r"/api/v10/channels/.+/pins/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid = req.url.path().rsplit('/').next().unwrap_or("");
            st.pins
                .lock()
                .unwrap()
                .retain(|p| p.get("id").and_then(|i| i.as_str()) != Some(mid));
            ResponseTemplate::new(204)
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
        upload_concurrency: 8,
        download_concurrency: 8,
    }))
}

#[tokio::test]
async fn typed_put_get_sweep_delete_and_fingerprint() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(snowflake_age_secs_ago(60)),
        blobs: Mutex::new(HashMap::new()),
        messages: Mutex::new(Vec::new()),
        message_text: Mutex::new(HashMap::new()),
        deleted: Mutex::new(Vec::new()),
        bulk_calls: Mutex::new(Vec::new()),
        pins: Mutex::new(Vec::new()),
    });
    mount_core_mocks(&server, state.clone(), &server.uri()).await;

    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);

    assert_eq!(
        store.instance().fingerprint,
        discord_fingerprint("app42", CHANNEL)
    );
    assert_eq!(store.instance().location, discord_location(CHANNEL));

    let id = BlobBackend::put(&store, Bytes::from_static(b"typed-dc"))
        .await
        .unwrap();
    assert!(id.message_id > 0);
    assert!(!id.attachment_id.is_empty());
    assert_eq!(DiscordBlobStore::key(&id), id.message_id);

    let got = collect_stream(BlobBackend::get(&store, &id, None).await.unwrap())
        .await
        .unwrap();
    assert_eq!(got.as_ref(), b"typed-dc");

    let keys = Sweepable::candidates(&store, None, u64::MAX, 10)
        .await
        .unwrap();
    assert!(keys.contains(&id.message_id));

    // List path observed remaining=0 → cost(List) reports wait.
    let list_cost = BlobBackend::cost(&store, OpKind::List, None);
    assert!(list_cost.wait_secs > 0.0);

    BlobBackend::delete(&store, &[id.message_id])
        .await
        .unwrap();
    // Single young key → one-by-one (bulk needs ≥2).
    assert!(state.bulk_calls.lock().unwrap().is_empty());
    assert!(state.deleted.lock().unwrap().contains(&id.message_id));

    // Not-found delete is success.
    BlobBackend::delete(&store, &[id.message_id])
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_bulk_delete_for_young_messages() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    let base = snowflake_age_secs_ago(120);
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(base),
        blobs: Mutex::new(HashMap::new()),
        messages: Mutex::new(Vec::new()),
        message_text: Mutex::new(HashMap::new()),
        deleted: Mutex::new(Vec::new()),
        bulk_calls: Mutex::new(Vec::new()),
        pins: Mutex::new(Vec::new()),
    });
    mount_core_mocks(&server, state.clone(), &server.uri()).await;

    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);

    let a = BlobBackend::put(&store, Bytes::from_static(b"a"))
        .await
        .unwrap();
    let b = BlobBackend::put(&store, Bytes::from_static(b"b"))
        .await
        .unwrap();
    let c = BlobBackend::put(&store, Bytes::from_static(b"c"))
        .await
        .unwrap();

    BlobBackend::delete(&store, &[a.message_id, b.message_id, c.message_id])
        .await
        .unwrap();

    let bulk = state.bulk_calls.lock().unwrap().clone();
    assert_eq!(bulk.len(), 1);
    assert_eq!(bulk[0].len(), 3);
    assert!(state.deleted.lock().unwrap().len() >= 3);
}

#[tokio::test]
async fn typed_old_messages_delete_one_by_one() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    // ~20 days old → above bulk-delete cutoff.
    let old_a = snowflake_age_secs_ago(20 * 24 * 60 * 60);
    let old_b = old_a + (1 << 22);
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(old_a),
        blobs: Mutex::new(HashMap::new()),
        messages: Mutex::new(vec![old_a, old_b]),
        message_text: Mutex::new(HashMap::new()),
        deleted: Mutex::new(Vec::new()),
        bulk_calls: Mutex::new(Vec::new()),
        pins: Mutex::new(Vec::new()),
    });
    // Seed blob map so get would work; delete only needs message endpoints.
    state.blobs.lock().unwrap().insert(old_a * 10, b"x".to_vec());
    state.blobs.lock().unwrap().insert(old_b * 10, b"y".to_vec());
    mount_core_mocks(&server, state.clone(), &server.uri()).await;

    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);

    assert!(snowflake_timestamp_ms(old_a) < unix_now_ms());
    BlobBackend::delete(&store, &[old_a, old_b])
        .await
        .unwrap();
    assert!(
        state.bulk_calls.lock().unwrap().is_empty(),
        "old messages must not use bulk-delete"
    );
    let deleted = state.deleted.lock().unwrap().clone();
    assert!(deleted.contains(&old_a) && deleted.contains(&old_b));
}

#[tokio::test]
async fn typed_bootstrap_pin_swap_read() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(snowflake_age_secs_ago(10)),
        blobs: Mutex::new(HashMap::new()),
        messages: Mutex::new(Vec::new()),
        message_text: Mutex::new(HashMap::new()),
        deleted: Mutex::new(Vec::new()),
        bulk_calls: Mutex::new(Vec::new()),
        pins: Mutex::new(Vec::new()),
    });

    // Override pin PUT to store real content from the last sent text message.
    let sent_text: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    let st = state.clone();
    let texts = sent_text.clone();
    Mock::given(method("POST"))
        .and(path_regex(r"/api/v10/channels/.+/messages$"))
        .respond_with(move |req: &Request| {
            let ct = req
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if ct.starts_with("application/json") {
                let mid = st.next_msg.fetch_add(1, Ordering::SeqCst);
                let body: serde_json::Value =
                    serde_json::from_slice(&req.body).unwrap_or(serde_json::json!({}));
                let content = body
                    .get("content")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                texts.lock().unwrap().insert(mid.to_string(), content.clone());
                st.messages.lock().unwrap().push(mid);
                return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": mid.to_string(),
                    "content": content,
                    "attachments": []
                }));
            }
            ResponseTemplate::new(400)
        })
        .mount(&server)
        .await;

    let st = state.clone();
    let texts = sent_text.clone();
    Mock::given(method("PUT"))
        .and(path_regex(r"/api/v10/channels/.+/pins/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid = req
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap_or("0")
                .to_string();
            let content = texts
                .lock()
                .unwrap()
                .get(&mid)
                .cloned()
                .unwrap_or_default();
            let mut pins = st.pins.lock().unwrap();
            pins.clear();
            pins.push(serde_json::json!({
                "id": mid,
                "content": content,
                "attachments": []
            }));
            ResponseTemplate::new(204)
        })
        .mount(&server)
        .await;

    let st = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v10/channels/.+/pins$"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_json(st.pins.lock().unwrap().clone())
        })
        .mount(&server)
        .await;

    let st = state.clone();
    Mock::given(method("DELETE"))
        .and(path_regex(r"/api/v10/channels/.+/pins/[0-9]+$"))
        .respond_with(move |req: &Request| {
            let mid = req.url.path().rsplit('/').next().unwrap_or("");
            st.pins
                .lock()
                .unwrap()
                .retain(|p| p.get("id").and_then(|i| i.as_str()) != Some(mid));
            ResponseTemplate::new(204)
        })
        .mount(&server)
        .await;

    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);

    assert!(TypedBootstrapPointer::read(&store).await.unwrap().is_none());

    let payload = Bytes::from_static(br#"{"format":1,"generation":7}"#);
    TypedBootstrapPointer::swap(&store, payload.clone())
        .await
        .unwrap();
    let got = TypedBootstrapPointer::read(&store).await.unwrap().unwrap();
    assert_eq!(got, payload);

    let payload2 = Bytes::from_static(br#"{"format":1,"generation":8}"#);
    TypedBootstrapPointer::swap(&store, payload2.clone())
        .await
        .unwrap();
    let got2 = TypedBootstrapPointer::read(&store).await.unwrap().unwrap();
    assert_eq!(got2, payload2);
    // Only one pin remains.
    assert_eq!(state.pins.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn typed_conformance_suite() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    let state = Arc::new(MockState {
        next_msg: AtomicU64::new(snowflake_age_secs_ago(60)),
        blobs: Mutex::new(HashMap::new()),
        messages: Mutex::new(Vec::new()),
        message_text: Mutex::new(HashMap::new()),
        deleted: Mutex::new(Vec::new()),
        bulk_calls: Mutex::new(Vec::new()),
        pins: Mutex::new(Vec::new()),
    });
    mount_core_mocks(&server, state, &server.uri()).await;
    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    // Smaller max keeps the near-boundary put cheap under wiremock.
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), Some(64 * 1024));
    run_typed_conformance(&store).await.unwrap();
}

#[tokio::test]
async fn cost_free_before_headers() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);
    assert_eq!(
        BlobBackend::cost(&store, OpKind::Put, None::<&DiscordId>),
        CostHint::free()
    );
}

#[tokio::test]
async fn delete_not_found_is_ok() {
    let server = MockServer::start().await;
    let api_base = format!("{}/api/v10", server.uri());
    Mock::given(method("DELETE"))
        .and(path_regex(r"/api/v10/channels/.+/messages/[0-9]+$"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let dc = DiscordClient::with_api_base(TOKEN.into(), api_base).unwrap();
    let store = DiscordBlobStore::new(dc, CHANNEL.into(), fast_limiter(), None);
    BlobBackend::delete(&store, &[12345]).await.unwrap();
}
