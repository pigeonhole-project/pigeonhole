mod auth;
mod error;
mod xml;

use crate::chunker;
use crate::config::Config;
use crate::index::{parse_rfc3339, DeleteBucketResult, Index};
use crate::telegram::TelegramClient;
use auth::authorize;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use error::S3Error;
use futures::StreamExt;
use md5::{Digest, Md5};
use serde::Deserialize;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use xml::*;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Config,
    pub index: Index,
    pub tg: TelegramClient,
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

    match *req.method() {
        Method::PUT => create_bucket(&state, &bucket).await,
        Method::DELETE => delete_bucket(&state, &bucket).await,
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

    match *req.method() {
        Method::PUT => put_object(&state, &bucket, &key, req).await,
        Method::GET => get_object(&state, &bucket, &key).await,
        Method::HEAD => head_object(&state, &bucket, &key).await,
        Method::DELETE => delete_object(&state, &bucket, &key).await,
        _ => Err(S3Error::method_not_allowed()),
    }
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

async fn create_bucket(state: &AppState, bucket: &str) -> Result<Response, S3Error> {
    let created = state.index.create_bucket(bucket).await?;
    if !created {
        // AWS returns BucketAlreadyOwnedByYou for same account — treat as success for demo
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::LOCATION, format!("/{bucket}"))
            .body(Body::empty())
            .unwrap());
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::LOCATION, format!("/{bucket}"))
        .body(Body::empty())
        .unwrap())
}

async fn delete_bucket(state: &AppState, bucket: &str) -> Result<Response, S3Error> {
    match state.index.delete_bucket(bucket).await? {
        DeleteBucketResult::Deleted => Ok(StatusCode::NO_CONTENT.into_response()),
        DeleteBucketResult::NotFound => Err(S3Error::no_such_bucket(bucket)),
        DeleteBucketResult::NotEmpty => Err(S3Error::bucket_not_empty(bucket)),
    }
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

    let (objects, common, truncated) = state
        .index
        .list_objects(
            bucket,
            &prefix,
            params.delimiter.as_deref(),
            max_keys,
            start_after.as_deref(),
        )
        .await?;

    let next_token = if truncated {
        objects.last().map(|o| o.key.clone())
    } else {
        None
    };

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

    let body = req.into_body();
    let mut stream = body.into_data_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut hasher = Md5::new();
    let mut total_size: i64 = 0;
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<(i64, String, i64, i64)> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| S3Error::internal(e.to_string()))?;
        hasher.update(&chunk);
        buffer.extend_from_slice(&chunk);
        total_size += chunk.len() as i64;

        while buffer.len() >= chunker::CHUNK_SIZE {
            let data: Bytes = buffer.drain(..chunker::CHUNK_SIZE).collect::<Vec<u8>>().into();
            let chunk_size = data.len() as i64;
            let filename = format!("{bucket}_{}_{part_no}.part", key.replace('/', "_"));
            let caption = format!("{bucket}/{key}#{part_no}");
            let (file_id, message_id) = state
                .tg
                .send_document(data, &filename, &caption)
                .await
                .map_err(|e| S3Error::internal(e.to_string()))?;
            uploaded.push((part_no, file_id, message_id, chunk_size));
            part_no += 1;
        }
    }

    // Final (possibly empty) chunk — empty object still needs one telegram doc or we allow empty with zero chunks.
    // For empty objects, store a single empty chunk so Get works symmetrically.
    if uploaded.is_empty() || !buffer.is_empty() {
        let data: Bytes = std::mem::take(&mut buffer).into();
        let chunk_size = data.len() as i64;
        let filename = format!("{bucket}_{}_{part_no}.part", key.replace('/', "_"));
        let caption = format!("{bucket}/{key}#{part_no}");
        let (file_id, message_id) = state
            .tg
            .send_document(data, &filename, &caption)
            .await
            .map_err(|e| S3Error::internal(e.to_string()))?;
        uploaded.push((part_no, file_id, message_id, chunk_size));
    }

    let etag = format!("{:x}", hasher.finalize());
    let old = state
        .index
        .put_object(
            bucket,
            key,
            &etag,
            total_size,
            content_type.as_deref(),
            &uploaded,
        )
        .await?;

    // Best-effort cleanup of replaced object chunks
    for c in old {
        if let Err(e) = state.tg.delete_message(c.message_id).await {
            warn!(error = %e, "failed to delete old telegram message");
        }
    }

    info!(bucket, key, size = total_size, parts = uploaded.len(), "PutObject ok");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{etag}\""))
        .body(Body::empty())
        .unwrap())
}

async fn get_object(state: &AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = state
        .index
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::no_such_key(bucket, key))?;

    let chunks = state.index.get_chunks(bucket, key).await?;
    let tg = state.tg.clone();
    let stream = futures::stream::iter(chunks).then(move |c| {
        let tg = tg.clone();
        async move {
            tg.download_file(&c.file_id)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }
    });

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::CONTENT_LENGTH, meta.size)
        .header(
            header::LAST_MODIFIED,
            httpdate(&parse_rfc3339(&meta.mtime)),
        )
        .header(header::ACCEPT_RANGES, "bytes");

    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct);
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/octet-stream");
    }

    Ok(builder
        .body(Body::from_stream(stream))
        .unwrap())
}

async fn head_object(state: &AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = state
        .index
        .get_object(bucket, key)
        .await?
        .ok_or_else(|| S3Error::no_such_key(bucket, key))?;

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

    Ok(builder.body(Body::empty()).unwrap())
}

async fn delete_object(state: &AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    if !state.index.bucket_exists(bucket).await? {
        return Err(S3Error::no_such_bucket(bucket));
    }

    if let Some(chunks) = state.index.delete_object(bucket, key).await? {
        for c in chunks {
            if let Err(e) = state.tg.delete_message(c.message_id).await {
                warn!(error = %e, "failed to delete telegram message");
            }
        }
    }

    // S3 DeleteObject is idempotent — always 204
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn export_snapshot(state: &AppState) -> Result<Response, S3Error> {
    let snap = state.index.export_snapshot().await?;
    let json = serde_json::to_vec_pretty(&snap).map_err(|e| S3Error::internal(e.to_string()))?;
    let filename = format!("s3gram-index-{}.json", chrono::Utc::now().format("%Y%m%d%H%M%S"));
    let (file_id, message_id) = state
        .tg
        .send_document(Bytes::from(json), &filename, "s3gram-index-snapshot")
        .await
        .map_err(|e| S3Error::internal(e.to_string()))?;

    let body = serde_json::json!({
        "file_id": file_id,
        "message_id": message_id,
        "filename": filename,
    });
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

    // Body may be raw JSON snapshot, or {"file_id":"..."}
    let snap = if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        if let Some(file_id) = v.get("file_id").and_then(|x| x.as_str()) {
            let data = state
                .tg
                .download_file(file_id)
                .await
                .map_err(|e| S3Error::internal(e.to_string()))?;
            serde_json::from_slice(&data).map_err(|e| S3Error::invalid_argument(e.to_string()))?
        } else {
            serde_json::from_value(v).map_err(|e| S3Error::invalid_argument(e.to_string()))?
        }
    } else {
        return Err(S3Error::invalid_argument("expected JSON snapshot or {file_id}"));
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
