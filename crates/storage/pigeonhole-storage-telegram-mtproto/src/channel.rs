//! The few channel operations the backend, the log and the snapshots need,
//! as a trait: [`crate::MtprotoChannel`] talks MTProto, [`crate::fake`] is
//! an in-memory channel for tests.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// Most message ids per `channels.getMessages` / `channels.deleteMessages`.
pub const IDS_PER_CALL: usize = 100;

/// Largest `upload.getFile` answer we ask for: grammers 0.11 cannot take a
/// 1 MiB answer (it exceeds an MTProto container).
pub const WINDOW: u64 = 512 * 1024;

/// Where a document's bytes are. The file reference expires: a fresh one
/// comes with the message (`channels.getMessages`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLocation {
    pub id: i64,
    pub access_hash: i64,
    pub file_reference: Vec<u8>,
    pub dc_id: i32,
}

/// A channel message as far as we care.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMessage {
    /// Deleted, past the top of the channel, or (briefly) not visible yet:
    /// Telegram answers the same for all three.
    Empty,
    /// Anything that is not a document (text, a service message, a photo).
    Other,
    Document { name: String, size: u64, file: FileLocation },
}

/// A posted document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    pub message_id: i64,
    /// The document's location if the answer carried it.
    pub file: Option<FileLocation>,
}

/// `upload.getFile` refused an expired file reference: read the message again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileReferenceExpired;

impl std::fmt::Display for FileReferenceExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FILE_REFERENCE_EXPIRED")
    }
}

impl std::error::Error for FileReferenceExpired {}

/// One channel (or supergroup): message ids are per channel, dense and
/// assigned in sending order.
#[async_trait]
pub trait ChannelApi: Send + Sync + 'static {
    /// The channel's id (without the `-100` prefix of a Bot API chat id).
    fn channel_id(&self) -> i64;

    /// The messages `ids` (at most [`IDS_PER_CALL`]), in the order asked.
    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>>;

    /// Upload `data` and post it as a document named `name`. Every call with
    /// one `random_id` posts at most one message: a repeat returns it again,
    /// so a send may be retried after any failure.
    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent>;

    /// `limit` bytes of `file` at `offset`: one window, `offset` and `limit`
    /// multiples of 4 KiB, `limit` ≤ [`WINDOW`], not crossing a 1 MiB
    /// boundary. Shorter at the end of the file. [`FileReferenceExpired`]
    /// when the reference is stale.
    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes>;

    /// Delete messages (at most [`IDS_PER_CALL`]); missing ids are fine.
    async fn delete_messages(&self, ids: &[i64]) -> Result<()>;
}

/// `[start, end)` cut into the windows `get_file` takes: aligned to
/// [`WINDOW`], so none crosses a 1 MiB boundary. Each is `(offset, skip,
/// take)`: fetch the window at `offset`, keep `take` bytes after `skip`.
pub fn windows(start: u64, end: u64) -> Vec<(u64, usize, usize)> {
    let mut out = Vec::new();
    let mut pos = start;
    while pos < end {
        let offset = pos - pos % WINDOW;
        let take = (offset + WINDOW).min(end) - pos;
        out.push((offset, (pos - offset) as usize, take as usize));
        pos += take;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_cover_the_range_aligned() {
        assert!(windows(5, 5).is_empty());
        assert_eq!(windows(0, 10), vec![(0, 0, 10)]);
        let w = windows(WINDOW - 3, 2 * WINDOW + 1);
        assert_eq!(w, vec![(0, WINDOW as usize - 3, 3), (WINDOW, 0, WINDOW as usize), (2 * WINDOW, 0, 1)]);
        for (offset, _, _) in windows(1, 9 * WINDOW) {
            assert_eq!(offset % WINDOW, 0);
            assert_eq!(offset / (1 << 20), (offset + WINDOW - 1) / (1 << 20), "no 1 MiB crossing");
        }
    }
}
