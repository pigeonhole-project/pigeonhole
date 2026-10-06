use crate::index::{Index, OrphanMsg};
use crate::telegram::TelegramClient;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use md5::{Digest, Md5};
use tracing::{info, warn};

fn registry_key(bucket: &str) -> String {
    format!("buckets/{bucket}.json")
}

pub fn validate_bucket_name(name: &str) -> Result<()> {
    if name.len() < 3 || name.len() > 63 {
        bail!("bucket name must be 3-63 characters");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        bail!("bucket name must be DNS-compatible (lowercase, digits, -, .)");
    }
    if name.starts_with('-') || name.ends_with('-') || name.starts_with('.') || name.ends_with('.')
    {
        bail!("bucket name cannot start/end with - or .");
    }
    Ok(())
}

pub fn chat_title_for_bucket(bucket: &str) -> String {
    // Telegram title limit 1–128; keep readable.
    let t = format!("s3:{bucket}");
    if t.len() <= 128 {
        t
    } else {
        t.chars().take(128).collect()
    }
}

/// Register or re-bind an S3 bucket to a Telegram chat and sync the service-bucket registry object.
/// One Telegram chat ↔ one data bucket. Service/admin chats are forbidden for data.
pub async fn register_bucket(
    index: &Index,
    tg: &TelegramClient,
    service_bucket: &str,
    service_chat: &str,
    admin_chat: &str,
    bucket: &str,
    data_chat_id: &str,
    rename_chat: bool,
) -> Result<(Vec<OrphanMsg>, Option<String>)> {
    validate_bucket_name(bucket)?;
    if bucket == service_bucket {
        bail!("'{service_bucket}' is reserved as the service bucket");
    }
    let data_chat_id = data_chat_id.trim();
    if data_chat_id.is_empty() {
        bail!("data chat_id is empty");
    }
    if data_chat_id == service_chat || data_chat_id == admin_chat {
        bail!("cannot store bucket data in the service/admin chat; create a separate Telegram chat");
    }

    if let Some(other) = index.bucket_using_chat(data_chat_id).await? {
        if other != bucket {
            bail!("chat {data_chat_id} is already bound to bucket '{other}'");
        }
    }

    index.upsert_bucket(bucket, data_chat_id).await?;

    let body = serde_json::json!({
        "bucket": bucket,
        "chat_id": data_chat_id,
    })
    .to_string();
    let data = Bytes::from(body.into_bytes());
    let etag = format!("{:x}", Md5::digest(&data));
    let size = data.len() as i64;
    let (file_id, message_id) = tg
        .send_document(service_chat, data, &format!("{bucket}.json"), "")
        .await
        .context("upload registry object")?;

    let key = registry_key(bucket);
    let orphans = index
        .put_object(
            service_bucket,
            &key,
            &etag,
            size,
            Some("application/json"),
            &[(0, file_id, message_id, size)],
            service_chat,
            &[],
        )
        .await?;

    let mut rename_note = None;
    if rename_chat {
        let title = chat_title_for_bucket(bucket);
        match tg.set_chat_title(data_chat_id, &title).await {
            Ok(()) => {
                rename_note = Some(format!("renamed chat to «{title}»"));
                info!(bucket, data_chat_id, %title, "data chat renamed");
            }
            Err(e) => {
                warn!(bucket, data_chat_id, error = %e, "setChatTitle failed");
                rename_note = Some(format!(
                    "could not rename chat (need admin + change info rights): {e}"
                ));
            }
        }
    }

    info!(bucket, data_chat_id, "bucket registered");
    Ok((orphans, rename_note))
}

pub async fn unregister_bucket(
    index: &Index,
    service_bucket: &str,
    bucket: &str,
) -> Result<Option<Vec<OrphanMsg>>> {
    if bucket == service_bucket {
        bail!("cannot delete the service bucket");
    }
    match index.delete_bucket(bucket).await? {
        crate::index::DeleteBucketResult::Deleted => {
            let key = registry_key(bucket);
            let orphans = index.delete_object(service_bucket, &key).await?;
            Ok(orphans)
        }
        crate::index::DeleteBucketResult::NotFound => Ok(None),
        crate::index::DeleteBucketResult::NotEmpty => bail!("bucket is not empty"),
    }
}
