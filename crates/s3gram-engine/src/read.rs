//! Frame-aware chunk reads with optional L1 [`FrameCache`] and readahead.

use crate::frame_cache::FrameCache;
use crate::ingest::decode_chunk_slice_async;
use anyhow::{Context, Result};
use bytes::Bytes;
use s3gram_blob::BlobStore;
use s3gram_chunk::{decode_frames_range, ChunkCodec, FrameRecord};
use s3gram_core::{BackendId, BlobKey, Locator};
use std::sync::Arc;

fn blob_key_for(file_id: &str) -> BlobKey {
    let (backend, loc) = if file_id.starts_with("mem-") {
        (BackendId::memory(), Locator::memory(file_id, 0))
    } else if let Some((message_id, attachment_id)) = Locator::parse_discord_store_file_id(file_id)
    {
        (
            BackendId::new("discord", "cached"),
            Locator::discord("cached", message_id, attachment_id, ""),
        )
    } else {
        (BackendId::new("tg", "cached"), Locator::telegram(file_id, 0))
    };
    BlobKey::new(backend, loc)
}

/// Decode `[from, to)` of a chunk, using L1 for individual frames when available.
pub async fn read_chunk_range_cached(
    store: Arc<dyn BlobStore>,
    file_id: &str,
    codec: ChunkCodec,
    frames: &[FrameRecord],
    from: usize,
    to: usize,
    logical_size: usize,
    frame_cache: Option<Arc<FrameCache>>,
    readahead: bool,
) -> Result<Bytes> {
    if codec != ChunkCodec::Frames || frames.is_empty() || frame_cache.is_none() {
        let data = store.get(file_id).await.context("blob get")?;
        return decode_chunk_slice_async(data, codec, frames, from, to, logical_size).await;
    }
    let cache = frame_cache.expect("checked");
    let key = blob_key_for(file_id);

    let mut needed = Vec::new();
    let mut cursor = 0usize;
    for (i, fr) in frames.iter().enumerate() {
        let flen = fr.logical_len as usize;
        let start = cursor;
        let end = cursor + flen;
        cursor = end;
        if end <= from || start >= to {
            continue;
        }
        needed.push((i as u32, fr.clone(), start));
    }

    let stored = store.get(file_id).await.context("blob get for frames")?;
    let mut out = Vec::with_capacity(to.saturating_sub(from));
    let mut last_frame_idx = None;
    for (frame_no, fr, frame_start) in &needed {
        let blob = key.clone();
        let fr_c = fr.clone();
        let stored_c = stored.clone();
        let decoded = cache
            .get_or_load(blob, *frame_no, || async move {
                let mut rec = fr_c.clone();
                let soff = rec.stored_off as usize;
                let slen = rec.stored_len as usize;
                if soff + slen > stored_c.len() {
                    anyhow::bail!("frame stored range outside blob");
                }
                let slice = stored_c.slice(soff..soff + slen);
                rec.stored_off = 0;
                decode_frames_range(slice.as_ref(), &[rec], 0, fr_c.logical_len as usize)
            })
            .await?;
        let flen = decoded.len();
        let local_from = from.saturating_sub(*frame_start).min(flen);
        let local_to = to.saturating_sub(*frame_start).min(flen);
        if local_from < local_to {
            out.extend_from_slice(&decoded[local_from..local_to]);
        }
        last_frame_idx = Some(*frame_no);
    }

    if readahead {
        if let Some(last) = last_frame_idx {
            spawn_readahead(
                store,
                file_id.to_string(),
                frames.to_vec(),
                key,
                last,
                cache,
            );
        }
    }

    if out.len() != to.saturating_sub(from) {
        return decode_chunk_slice_async(stored, codec, frames, from, to, logical_size).await;
    }
    Ok(Bytes::from(out))
}

fn spawn_readahead(
    store: Arc<dyn BlobStore>,
    file_id: String,
    frames: Vec<FrameRecord>,
    key: BlobKey,
    last: u32,
    cache: Arc<FrameCache>,
) {
    let n = cache.readahead_frames();
    if n == 0 {
        return;
    }
    let sem = cache.readahead_sem();
    tokio::spawn(async move {
        let Ok(_permit) = sem.try_acquire_owned() else {
            return;
        };
        let stored = match store.get(&file_id).await {
            Ok(b) => b,
            Err(_) => return,
        };
        let start = (last as usize) + 1;
        let end = (start + n).min(frames.len());
        for i in start..end {
            let fr = frames[i].clone();
            let frame_no = i as u32;
            let blob = key.clone();
            let stored_c = stored.clone();
            let _ = cache
                .get_or_load(blob, frame_no, || async move {
                    let mut rec = fr.clone();
                    let soff = rec.stored_off as usize;
                    let slen = rec.stored_len as usize;
                    if soff + slen > stored_c.len() {
                        anyhow::bail!("readahead frame out of range");
                    }
                    let slice = stored_c.slice(soff..soff + slen);
                    rec.stored_off = 0;
                    decode_frames_range(slice.as_ref(), &[rec], 0, fr.logical_len as usize)
                })
                .await;
        }
    });
}
