mod auth;
mod aws_chunked;
mod error;
mod xml;

use crate::chunker;
use crate::config::Config;
use crate::index::{parse_rfc3339, Index, OrphanMsg};
use crate::registry;
use crate::snapshot::{self, PushOutcome};
use crate::telegram::TelegramClient;
use auth::authorize;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use error::S3Error;
use futures::StreamExt;
use md5::{Digest, Md5};
use serde::Deserialize;
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use xml::*;

/// CreateBucket header: Telegram chat that stores this bucket's blobs.
const HEADER_CHAT_ID: &str = "x-s3gram-chat-id";

#[derive(Clone)]
pub struct AppState {
    pub cfg: Config,
    pub index: Index,
    pub tg: TelegramClient,
    pub snapshot_gate: Arc<Mutex<()>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", any(root))
        .route("/{bucket}", any(bucket_root))
        .route("/{bucket}/", any(bucket_root))
        .route("/{bucket}/{*key}", any(object_route))
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state))
}

async fn root(
    State(state): State<Arc<AppState>>,
    req: Request,
) -> Result<Response, S3Error> {
    authorize(&state.cfg, &req)?;
    match *req.method() {
        Method::GET => {
            let buckets = state.index.list_buckets().await?;
            Ok(xml_response(
                StatusCode::OK,
                &list_all_my_buckets(&buckets),
            ))
        }
        Method::POST => {
            // Internal: snapshot restore/export via query
            let q = req.uri().query().unwrap_or("");
            if q.contains("s3gram-snapshot=export") {
                return export_snapshot(&state).await;
            }
            if q.contains("s3gram-snapshot=import") {
                return import_snapshot(&state, req).await;
            }
            Err(S3Error::method_not_allowed())
        }
        _ => Err(S3Error::method_not_allowed()),
    }
}

async fn bucket_root(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<ListParams>,
    req: Request,
) -> Result<Response, S3Error> {
    authorize(&state.cfg, &req)?;
    validate_bucket_name(&bucket)?;

    let query = req.uri().query().unwrap_or("").to_string();
    let q = parse_query(&query);

    match *req.method() {
        Method::PUT => create_bucket(&state, &bucket, req.headers()).await,
        Method::DELETE => delete_bucket(&state, &bucket).await,
        Method::POST if q.contains_key("delete") => delete_objects(&state, &bucket, req).await,
        Method::GET | Method::HEAD if q.contains_key("versions") => {
            list_object_versions(&state, &bucket, params, req.method()).await
        }
        Method::GET | Method::HEAD => list_objects(&state, &bucket, params, req.method()).await,
        _ => Err(S3Error::method_not_allowed()),
    }
}

async fn object_route(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, S3Error> {
    authorize(&state.cfg, &req)?;
    validate_bucket_name(&bucket)?;
    if key.is_empty() {
        return Err(S3Error::invalid_argument("Empty key"));
    }

    let query = req.uri().query().unwrap_or("").to_string();
    let q = parse_query(&query);

    match *req.method() {
        Method::POST if q.contains_key("uploads") => {
            create_multipart_upload(&state, &bucket, &key, req).await
        }
        Method::POST if q.contains_key("uploadId") => {
            let upload_id = q.get("uploadId").cloned().unwrap_or_default();
            complete_multipart_upload(&state, &bucket, &key, &upload_id, req).await
        }
        Method::PUT if q.contains_key("uploadId") && q.contains_key("partNumber") => {
            let upload_id = q.get("uploadId").cloned().unwrap_or_default();
            let part_number: i64 = q
                .get("partNumber")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| S3Error::invalid_argument("Invalid partNumber"))?;
            upload_part(&state, &bucket, &key, &upload_id, part_number, req).await
        }
        Method::PUT => {
            if req.headers().contains_key("x-amz-copy-source") {
                copy_object(&state, &bucket, &key, req).await
            } else {
                put_object(&state, &bucket, &key, req).await
            }
        }
        Method::GET => {
            let range = req
                .headers()
                .get(header::RANGE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            get_object(&state, &bucket, &key, range.as_deref()).await
        }
        Method::HEAD => head_object(&state, &bucket, &key).await,
        Method::DELETE if q.contains_key("uploadId") => {
            let upload_id = q.get("uploadId").cloned().unwrap_or_default();
            abort_multipart_upload(&state, &upload_id).await
        }
        Method::DELETE => delete_object(&state, &bucket, &key).await,
        _ => Err(S3Error::method_not_allowed()),
    }
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    if query.is_empty() {
        return map;
    }
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let mut it = part.splitn(2, '=');
        let k = urlencoding::decode(it.next().unwrap_or(""))
            .unwrap_or_default()
            .into_owned();
        let v = urlencoding::decode(it.next().unwrap_or(""))
            .unwrap_or_default()
            .into_owned();
        map.insert(k, v);
    }
    map
}

#[derive(Debug, Deserialize, Default)]
struct ListParams {
    #[serde(rename = "list-type")]
    #[allow(dead_code)]
    list_type: Option<String>,
    prefix: Option<String>,
    delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    max_keys: Option<i64>,
    #[serde(rename = "start-after")]
    start_after: Option<String>,
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
}

async fn create_bucket(
    state: &AppState,
    bucket: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    if bucket == state.cfg.service_bucket {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::LOCATION, format!("/{bucket}"))
            .body(Body::empty())
            .unwrap());
    }

    // Already registered via admin — idempotent OK.
    if state.index.bucket_exists(bucket).await? {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::LOCATION, format!("/{bucket}"))
            .body(Body::empty())
            .unwrap());
    }

    // Data chat must be explicit: admin /bucket, header, or DEFAULT_DATA_CHAT_ID (tests).
    // Never the service chat.
    let chat_id = headers
        .get(HEADER_CHAT_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| state.cfg.default_data_chat_id.clone());

    let Some(chat_id) = chat_id else {
        return Err(S3Error::invalid_argument(format!(
            "bucket '{bucket}' is not registered; in the admin chat run: /bucket {bucket} \
(after adding the bot to a dedicated data chat). Service chat cannot hold object data."
        )));
    };

    if state.cfg.is_service_chat(&chat_id) {
        return Err(S3Error::invalid_argument(
            "cannot bind a data bucket to the service/admin chat; use a separate Telegram chat",
        ));
    }

    let orphans = registry::register_bucket(
        &state.index,
        &state.tg,
        &state.cfg.service_bucket,
        &state.cfg.service_chat_id,
        &state.cfg.admin_chat_id,
        bucket,
        &chat_id,
        false, // don't rename from S3 API path
    )
    .await
    .map_err(|e| S3Error::invalid_argument(e.to_string()))?;
    cleanup_orphans(state, orphans.0).await;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::LOCATION, format!("/{bucket}"))
        .body(Body::empty())
        .unwrap())
}

async fn delete_bucket(state: &AppState, bucket: &str) -> Result<Response, S3Error> {
    if bucket == state.cfg.service_bucket {
        return Err(S3Error::access_denied());
    }
    match registry::unregister_bucket(&state.index, &state.cfg.service_bucket, bucket).await {
        Ok(Some(orphans)) => {
            cleanup_orphans(state, orphans).await;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Ok(None) => Err(S3Error::no_such_bucket(bucket)),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("not empty") {
                Err(S3Error::bucket_not_empty(bucket))
            } else {
                Err(S3Error::internal(msg))
            }
        }
    }
}

async fn require_bucket_chat(state: &AppState, bucket: &str) -> Result<String, S3Error> {
    state
        .index
        .bucket_chat_id(bucket)
        .await?
        .ok_or_else(|| S3Error::no_such_bucket(bucket))
}

async fn list_objects(
    state: &AppState,
    bucket: &str,
    params: ListParams,
    method: &Method,
) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let prefix = params.prefix.unwrap_or_default();
    let max_keys = params.max_keys.unwrap_or(1000).clamp(1, 1000);
    let start_after = params
        .continuation_token
        .or(params.start_after);

    let (objects, common, truncated, next_token) = state
        .index
        .list_objects(
            bucket,
            &prefix,
            params.delimiter.as_deref(),
            max_keys,
            start_after.as_deref(),
        )
        .await?;

    let body = list_objects_v2(
        bucket,
        &prefix,
        params.delimiter.as_deref(),
        max_keys,
        &objects,
        &common,
        truncated,
        next_token.as_deref(),
    );

    if *method == Method::HEAD {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/xml")
            .body(Body::empty())
            .unwrap());
    }

    Ok(xml_response(StatusCode::OK, &body))
}

async fn list_object_versions(
    state: &AppState,
    bucket: &str,
    params: ListParams,
    method: &Method,
) -> Result<Response, S3Error> {
    // Non-versioned store: expose current objects as versions with VersionId "null"
    // so clients like s3-tests can empty buckets during cleanup.
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let prefix = params.prefix.unwrap_or_default();
    let max_keys = params.max_keys.unwrap_or(1000).clamp(1, 1000);
    let (objects, _common, truncated, _next) = state
        .index
        .list_objects(bucket, &prefix, None, max_keys, None)
        .await?;

    let body = list_versions_result(bucket, &prefix, max_keys, &objects, truncated);

    if *method == Method::HEAD {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/xml")
            .body(Body::empty())
            .unwrap());
    }
    Ok(xml_response(StatusCode::OK, &body))
}

async fn delete_objects(
    state: &AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let body = axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024)
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;
    let body_str = String::from_utf8_lossy(&body);
    let quiet = body_str.contains("<Quiet>true</Quiet>") || body_str.contains("<Quiet>True</Quiet>");
    let keys = parse_delete_objects_keys(&body_str)?;

    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    let mut orphans = Vec::new();

    for key in keys {
        match state.index.delete_object(bucket, &key).await {
            Ok(Some(o)) => {
                orphans.extend(o);
                deleted.push(key);
            }
            Ok(None) => {
                // S3 delete is idempotent — still report as deleted
                deleted.push(key);
            }
            Err(e) => {
                errors.push((key, e.to_string()));
            }
        }
    }

    cleanup_orphans(state, orphans).await;
    Ok(xml_response(
        StatusCode::OK,
        &delete_objects_result(&deleted, &errors, quiet),
    ))
}

fn parse_delete_objects_keys(xml: &str) -> Result<Vec<String>, S3Error> {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Object>") {
        let after = &rest[start + 8..];
        let end = after
            .find("</Object>")
            .ok_or_else(|| S3Error::invalid_argument("malformed Delete XML"))?;
        let block = &after[..end];
        if let Some(key) = extract_xml_text(block, "Key") {
            keys.push(key);
        }
        rest = &after[end + 9..];
    }
    Ok(keys)
}

async fn put_object(
    state: &AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_meta = extract_user_metadata(req.headers());

    let chat_id = require_bucket_chat(state, bucket).await?;
    let (etag, total_size, uploaded) = ingest_body_to_telegram(state, &chat_id, req).await?;
    let orphans = state
        .index
        .put_object(
            bucket,
            key,
            &etag,
            total_size,
            content_type.as_deref(),
            &uploaded,
            &chat_id,
            &user_meta,
        )
        .await?;

    cleanup_orphans(state, orphans).await;

    info!(bucket, key, size = total_size, parts = uploaded.len(), "PutObject ok");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{etag}\""))
        .body(Body::empty())
        .unwrap())
}

async fn copy_object(
    state: &AppState,
    dst_bucket: &str,
    dst_key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(dst_bucket).await? {
        return Err(S3Error::no_such_bucket(dst_bucket));
    }

    let src_raw = req
        .headers()
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::invalid_argument("Missing x-amz-copy-source"))?;

    let (src_bucket, src_key) = parse_copy_source(src_raw)?;
    if !state.index.bucket_exists(&src_bucket).await? {
        return Err(S3Error::no_such_bucket(&src_bucket));
    }
    if state
        .index
        .get_object(&src_bucket, &src_key)
        .await?
        .is_none()
    {
        return Err(S3Error::no_such_key(&src_bucket, &src_key));
    }

    let directive = req
        .headers()
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("COPY");

    let copy_source_meta = !directive.eq_ignore_ascii_case("REPLACE");
    let content_type = if copy_source_meta {
        None
    } else {
        Some(
            req.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string(),
        )
    };
    let user_meta = if copy_source_meta {
        vec![]
    } else {
        extract_user_metadata(req.headers())
    };

    let src_chat = require_bucket_chat(state, &src_bucket).await?;
    let dst_chat = require_bucket_chat(state, dst_bucket).await?;

    let (dst, orphans) = if src_chat == dst_chat {
        state
            .index
            .copy_object(
                &src_bucket,
                &src_key,
                dst_bucket,
                dst_key,
                content_type.as_deref(),
                &user_meta,
                copy_source_meta,
            )
            .await
            .map_err(|e| S3Error::internal(e.to_string()))?
    } else {
        // Different Telegram chats: deep copy (re-upload into destination chat).
        deep_copy_object(
            state,
            &src_bucket,
            &src_key,
            dst_bucket,
            dst_key,
            &dst_chat,
            content_type.as_deref(),
            &user_meta,
            copy_source_meta,
        )
        .await?
    };

    cleanup_orphans(state, orphans).await;

    info!(
        src_bucket,
        src_key,
        dst_bucket,
        dst_key,
        shallow = (src_chat == dst_chat),
        "CopyObject ok"
    );

    Ok(xml_response(
        StatusCode::OK,
        &copy_object_result(&dst.mtime, &dst.etag),
    ))
}

fn parse_copy_source(raw: &str) -> Result<(String, String), S3Error> {
    let decoded = urlencoding::decode(raw.trim_start_matches('/'))
        .map_err(|_| S3Error::invalid_argument("Invalid x-amz-copy-source encoding"))?
        .into_owned();
    let (bucket, key) = decoded
        .split_once('/')
        .ok_or_else(|| S3Error::invalid_argument("x-amz-copy-source must be bucket/key"))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(S3Error::invalid_argument("x-amz-copy-source must be bucket/key"));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn extract_user_metadata(headers: &axum::http::HeaderMap) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (name, value) in headers.iter() {
        let key = name.as_str();
        if let Some(rest) = key.strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                out.push((rest.to_ascii_lowercase(), v.to_string()));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

async fn deep_copy_object(
    state: &AppState,
    src_bucket: &str,
    src_key: &str,
    dst_bucket: &str,
    dst_key: &str,
    dst_chat: &str,
    content_type: Option<&str>,
    user_meta: &[(String, String)],
    copy_source_meta: bool,
) -> Result<(crate::index::ObjectMeta, Vec<OrphanMsg>), S3Error> {
    let src = state
        .index
        .get_object(src_bucket, src_key)
        .await?
        .ok_or_else(|| S3Error::no_such_key(src_bucket, src_key))?;
    let src_chunks = state.index.get_chunks(src_bucket, src_key).await?;

    let mut uploaded = Vec::new();
    for c in &src_chunks {
        let data = state
            .tg
            .download_file(&c.file_id)
            .await
            .map_err(|e| S3Error::internal(e.to_string()))?;
        let (file_id, message_id, size) = upload_blob(state, dst_chat, data).await?;
        uploaded.push((c.part_no, file_id, message_id, size));
    }

    let ct = content_type.or(src.content_type.as_deref());
    let meta: Vec<(String, String)> = if copy_source_meta {
        state
            .index
            .get_user_metadata(src_bucket, src_key)
            .await
            .map_err(|e| S3Error::internal(e.to_string()))?
    } else {
        user_meta.to_vec()
    };

    let orphans = state
        .index
        .put_object(
            dst_bucket,
            dst_key,
            &src.etag,
            src.size,
            ct,
            &uploaded,
            dst_chat,
            &meta,
        )
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;

    let dst = state
        .index
        .get_object(dst_bucket, dst_key)
        .await?
        .ok_or_else(|| S3Error::internal("destination missing after deep copy"))?;
    Ok((dst, orphans))
}

async fn cleanup_orphans(state: &AppState, orphans: Vec<OrphanMsg>) {
    let mut seen = std::collections::HashSet::new();
    for (chat_id, message_id) in orphans {
        if !seen.insert((chat_id.clone(), message_id)) {
            continue;
        }
        match state.tg.delete_message(&chat_id, message_id).await {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    chat_id,
                    message_id,
                    "Telegram deleteMessage did not remove message (age/rights?); queued for retry"
                );
                let _ = state.index.queue_tg_delete(&chat_id, message_id).await;
            }
            Err(e) => {
                warn!(error = %e, chat_id, message_id, "failed to delete orphaned telegram message");
                let _ = state.index.queue_tg_delete(&chat_id, message_id).await;
            }
        }
    }
}

fn apply_user_metadata(mut builder: axum::http::response::Builder, meta: &[(String, String)]) -> axum::http::response::Builder {
    for (name, value) in meta {
        if let Ok(h) = axum::http::HeaderName::from_bytes(format!("x-amz-meta-{name}").as_bytes()) {
            if let Ok(v) = axum::http::HeaderValue::from_str(value) {
                builder = builder.header(h, v);
            }
        }
    }
    builder
}

async fn get_object(
    state: &AppState,
    bucket: &str,
    key: &str,
    range_hdr: Option<&str>,
) -> Result<Response, S3Error> {
    let meta = state
        .index
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::no_such_key(bucket, key))?;

    let chunks = state.index.get_chunks(bucket, key).await?;
    let user_meta = state.index.get_user_metadata(bucket, key).await?;
    let total = meta.size as u64;

    let (start, end_inclusive) = match parse_byte_range(range_hdr, total)? {
        None => (0u64, total.saturating_sub(1)),
        Some((s, e)) => (s, e),
    };
    let is_range = range_hdr.is_some() && total > 0;
    let length = if total == 0 {
        0
    } else {
        end_inclusive.saturating_sub(start) + 1
    };

    let tg = state.tg.clone();
    let stream = stream_object_range(tg, chunks, start, length);

    let status = if is_range {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::CONTENT_LENGTH, length)
        .header(
            header::LAST_MODIFIED,
            httpdate(&parse_rfc3339(&meta.mtime)),
        )
        .header(header::ACCEPT_RANGES, "bytes");

    if is_range && total > 0 {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end_inclusive}/{total}"),
        );
    }

    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct);
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/octet-stream");
    }

    builder = apply_user_metadata(builder, &user_meta);

    Ok(builder.body(Body::from_stream(stream)).unwrap())
}

/// Returns Ok(None) for full object; Ok(Some(start,end_inclusive)) for a range.
fn parse_byte_range(header: Option<&str>, total: u64) -> Result<Option<(u64, u64)>, S3Error> {
    let Some(h) = header else {
        return Ok(None);
    };
    if total == 0 {
        return Ok(None);
    }
    let h = h.strip_prefix("bytes=").ok_or_else(|| {
        S3Error::invalid_argument("Only bytes ranges are supported")
    })?;
    // Single range only for MVP
    let spec = h.split(',').next().unwrap_or(h).trim();
    if let Some(start_str) = spec.strip_suffix('-') {
        // bytes=start-
        let start: u64 = start_str
            .parse()
            .map_err(|_| S3Error::invalid_argument("bad range"))?;
        if start >= total {
            return Err(S3Error::invalid_range());
        }
        return Ok(Some((start, total - 1)));
    }
    if let Some(suffix_str) = spec.strip_prefix('-') {
        // bytes=-suffix
        let suffix: u64 = suffix_str
            .parse()
            .map_err(|_| S3Error::invalid_argument("bad range"))?;
        if suffix == 0 {
            return Err(S3Error::invalid_argument("bad range"));
        }
        let start = total.saturating_sub(suffix);
        return Ok(Some((start, total - 1)));
    }
    let (a, b) = spec
        .split_once('-')
        .ok_or_else(|| S3Error::invalid_argument("bad range"))?;
    let start: u64 = a
        .parse()
        .map_err(|_| S3Error::invalid_argument("bad range"))?;
    let end: u64 = b
        .parse()
        .map_err(|_| S3Error::invalid_argument("bad range"))?;
    if start > end || start >= total {
        return Err(S3Error::invalid_range());
    }
    let end = end.min(total - 1);
    Ok(Some((start, end)))
}

fn stream_object_range(
    tg: TelegramClient,
    chunks: Vec<crate::index::Chunk>,
    start: u64,
    length: u64,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream_range(tg, chunks, start, length)
}

fn async_stream_range(
    tg: TelegramClient,
    chunks: Vec<crate::index::Chunk>,
    start: u64,
    length: u64,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
    Box::pin(futures::stream::unfold(
        RangeState {
            tg,
            chunks,
            idx: 0,
            offset: 0,
            start,
            remaining: length,
        },
        |mut st| async move {
            if st.remaining == 0 {
                return None;
            }
            while st.idx < st.chunks.len() {
                let chunk = &st.chunks[st.idx];
                let chunk_size = chunk.size as u64;
                let chunk_end = st.offset + chunk_size;
                if chunk_end <= st.start {
                    st.offset = chunk_end;
                    st.idx += 1;
                    continue;
                }

                let data = match st.tg.download_file(&chunk.file_id).await {
                    Ok(b) => b,
                    Err(e) => {
                        return Some((
                            Err(std::io::Error::other(e.to_string())),
                            st,
                        ));
                    }
                };

                let local_start = st.start.saturating_sub(st.offset) as usize;
                let take = (chunk_size - local_start as u64).min(st.remaining) as usize;
                let slice = data.slice(local_start..local_start + take);

                st.remaining -= take as u64;
                st.start += take as u64;
                st.offset = chunk_end;
                st.idx += 1;

                return Some((Ok(slice), st));
            }
            None
        },
    ))
}

struct RangeState {
    tg: TelegramClient,
    chunks: Vec<crate::index::Chunk>,
    idx: usize,
    offset: u64,
    start: u64,
    remaining: u64,
}

async fn head_object(state: &AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = state
        .index
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::no_such_key(bucket, key))?;
    let user_meta = state.index.get_user_metadata(bucket, key).await?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::CONTENT_LENGTH, meta.size)
        .header(
            header::LAST_MODIFIED,
            httpdate(&parse_rfc3339(&meta.mtime)),
        );

    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct);
    }

    builder = apply_user_metadata(builder, &user_meta);

    Ok(builder.body(Body::empty()).unwrap())
}

async fn delete_object(state: &AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    if let Some(orphans) = state.index.delete_object(bucket, key).await? {
        cleanup_orphans(state, orphans).await;
    }

    // S3 DeleteObject is idempotent — always 204
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn create_multipart_upload(
    state: &AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let user_meta = extract_user_metadata(req.headers());

    let upload_id = uuid::Uuid::new_v4().to_string();
    state
        .index
        .create_multipart_upload(
            &upload_id,
            bucket,
            key,
            content_type.as_deref(),
            &user_meta,
        )
        .await?;

    info!(bucket, key, %upload_id, "CreateMultipartUpload");
    Ok(xml_response(
        StatusCode::OK,
        &initiate_multipart_upload(bucket, key, &upload_id),
    ))
}

async fn upload_part(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
    req: Request,
) -> Result<Response, S3Error> {
    if !(1..=10000).contains(&part_number) {
        return Err(S3Error::invalid_argument("partNumber must be 1..10000"));
    }

    let upload = state
        .index
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::no_such_upload(upload_id))?;

    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::no_such_upload(upload_id));
    }

    let chat_id = require_bucket_chat(state, bucket).await?;
    let (etag, size, tg_chunks) = ingest_body_to_telegram(state, &chat_id, req).await?;

    let orphans = state
        .index
        .put_multipart_part(upload_id, part_number, &etag, size, &tg_chunks, &chat_id)
        .await?;
    cleanup_orphans(state, orphans).await;

    info!(bucket, key, part_number, size, "UploadPart ok");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{etag}\""))
        .body(Body::empty())
        .unwrap())
}

async fn complete_multipart_upload(
    state: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let upload = state
        .index
        .get_multipart_upload(upload_id)
        .await?
        .ok_or_else(|| S3Error::no_such_upload(upload_id))?;

    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::no_such_upload(upload_id));
    }

    let body = axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024)
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;
    let body_str = String::from_utf8_lossy(&body);
    let completed = parse_complete_parts(&body_str)?;
    if completed.is_empty() {
        return Err(S3Error::invalid_argument("CompleteMultipartUpload requires parts"));
    }

    let mut md5_concat = Md5::new();
    let mut total_size: i64 = 0;
    let mut part_numbers = Vec::new();

    for (part_number, client_etag) in &completed {
        let part = state
            .index
            .get_multipart_part(upload_id, *part_number)
            .await?
            .ok_or_else(|| S3Error::invalid_part(format!("part {part_number} not found")))?;

        let normalized = client_etag.trim_matches('"');
        if part.etag != normalized {
            return Err(S3Error::invalid_part(format!(
                "etag mismatch for part {part_number}"
            )));
        }

        let digest = hex::decode(&part.etag)
            .map_err(|_| S3Error::invalid_part(format!("bad etag for part {part_number}")))?;
        md5_concat.update(&digest);
        total_size += part.size;
        part_numbers.push(*part_number);
    }

    let etag = format!(
        "{}-{}",
        hex::encode(md5_concat.finalize()),
        part_numbers.len()
    );

    let orphans = state
        .index
        .complete_multipart_upload(&upload, &part_numbers, &etag, total_size)
        .await?;

    cleanup_orphans(state, orphans).await;

    info!(bucket, key, parts = part_numbers.len(), size = total_size, "CompleteMultipartUpload ok");

    let location = format!("/{bucket}/{key}");
    Ok(xml_response(
        StatusCode::OK,
        &complete_multipart_result(&location, bucket, key, &etag),
    ))
}

async fn abort_multipart_upload(state: &AppState, upload_id: &str) -> Result<Response, S3Error> {
    match state.index.abort_multipart_upload(upload_id).await? {
        None => Err(S3Error::no_such_upload(upload_id)),
        Some(orphans) => {
            cleanup_orphans(state, orphans).await;
            info!(%upload_id, "AbortMultipartUpload ok");
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }
}

/// Stream request body into Telegram documents (≤19 MiB each). Returns (md5_hex, size, chunks).
/// Telegram filenames are content-addressed: blobs may be shared across keys via shallow copy.
async fn ingest_body_to_telegram(
    state: &AppState,
    chat_id: &str,
    req: Request,
) -> Result<(String, i64, Vec<(i64, String, i64, i64)>), S3Error> {
    let expect_sha = req
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let chunked = aws_chunked::is_aws_chunked(req.headers());

    let body = req.into_body();
    let mut stream = body.into_data_stream();
    let mut raw: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| S3Error::internal(e.to_string()))?;
        raw.extend_from_slice(&chunk);
    }

    let payload = if chunked {
        aws_chunked::decode_aws_chunked(&raw).map_err(|e| S3Error::invalid_argument(e.to_string()))?
    } else {
        raw
    };

    if let Some(ref sha) = expect_sha {
        if sha.len() == 64 && sha.chars().all(|c| c.is_ascii_hexdigit()) {
            let got = {
                use sha2::Digest as _;
                hex::encode(Sha256::digest(&payload))
            };
            if !const_time_eq(&got, sha) {
                return Err(S3Error::signature_mismatch());
            }
        }
    }

    let mut hasher = Md5::new();
    hasher.update(&payload);
    let etag = format!("{:x}", hasher.finalize());
    let total_size = payload.len() as i64;

    let mut part_no: i64 = 0;
    let mut uploaded: Vec<(i64, String, i64, i64)> = Vec::new();
    let mut offset = 0;
    while offset < payload.len() {
        let end = (offset + chunker::CHUNK_SIZE).min(payload.len());
        let data = Bytes::copy_from_slice(&payload[offset..end]);
        let (file_id, message_id, chunk_size) = upload_blob(state, chat_id, data).await?;
        uploaded.push((part_no, file_id, message_id, chunk_size));
        part_no += 1;
        offset = end;
    }

    Ok((etag, total_size, uploaded))
}

fn const_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

async fn upload_blob(
    state: &AppState,
    chat_id: &str,
    data: Bytes,
) -> Result<(String, i64, i64), S3Error> {
    let chunk_size = data.len() as i64;
    let filename = format!("{:x}.bin", Md5::digest(&data));
    let (file_id, message_id) = state
        .tg
        .send_document(chat_id, data, &filename, "")
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;
    Ok((file_id, message_id, chunk_size))
}

fn parse_complete_parts(xml: &str) -> Result<Vec<(i64, String)>, S3Error> {
    let mut parts = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Part>") {
        let after = &rest[start + 6..];
        let end = after
            .find("</Part>")
            .ok_or_else(|| S3Error::invalid_argument("malformed CompleteMultipartUpload XML"))?;
        let block = &after[..end];
        let part_number = extract_xml_text(block, "PartNumber")
            .ok_or_else(|| S3Error::invalid_argument("missing PartNumber"))?
            .parse::<i64>()
            .map_err(|_| S3Error::invalid_argument("bad PartNumber"))?;
        let etag = extract_xml_text(block, "ETag")
            .ok_or_else(|| S3Error::invalid_argument("missing ETag"))?;
        parts.push((part_number, etag));
        rest = &after[end + 7..];
    }
    parts.sort_by_key(|(n, _)| *n);
    Ok(parts)
}

fn extract_xml_text(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(block[start..end].trim().to_string())
}

async fn export_snapshot(state: &AppState) -> Result<Response, S3Error> {
    let _guard = state.snapshot_gate.lock().await;
    let outcome = snapshot::push_if_changed(&state.index, &state.tg, &state.cfg.service_chat_id)
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;

    let body = match outcome {
        PushOutcome::Unchanged { hash } => serde_json::json!({
            "status": "unchanged",
            "hash": hash,
        }),
        PushOutcome::Uploaded {
            hash,
            file_id,
            message_id,
            replaced_message_id,
            parts,
        } => serde_json::json!({
            "status": "uploaded",
            "hash": hash,
            "file_id": file_id,
            "message_id": message_id,
            "replaced_message_id": replaced_message_id,
            "parts": parts,
            "filename": "s3gram-index.json.gz",
        }),
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap())
}

async fn import_snapshot(state: &AppState, req: Request) -> Result<Response, S3Error> {
    let bytes = axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;

    let snap = if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        if v.get("file_id").is_some() || v.get("parts").is_some() {
            let file_id = v.get("file_id").and_then(|x| x.as_str());
            let data = snapshot::download_snapshot_bytes(&state.tg, &state.index, file_id)
                .await
                .map_err(|e| S3Error::internal(e.to_string()))?;
            serde_json::from_slice(&data).map_err(|e| S3Error::invalid_argument(e.to_string()))?
        } else {
            serde_json::from_value(v).map_err(|e| S3Error::invalid_argument(e.to_string()))?
        }
    } else {
        // Raw gzip or JSON body
        let raw = snapshot::gunzip_bytes(&bytes)
            .map_err(|e| S3Error::invalid_argument(e.to_string()))?;
        serde_json::from_slice(&raw).map_err(|e| S3Error::invalid_argument(e.to_string()))?
    };

    state
        .index
        .import_snapshot(&snap)
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;

    Ok(StatusCode::OK.into_response())
}

fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    if name.len() < 3 || name.len() > 63 {
        return Err(S3Error::invalid_bucket_name(name));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return Err(S3Error::invalid_bucket_name(name));
    }
    Ok(())
}

fn xml_response(status: StatusCode, body: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn httpdate(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}
