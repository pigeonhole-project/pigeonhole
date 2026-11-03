//! Where a chat store's blob is: a blob that fits a text message goes as
//! text ([`crate::text`]: its bytes come with the message, no download),
//! anything bigger as a file (a Telegram document, a Discord attachment).
//! A store remembers where its recent blobs are ([`Locations`]), so a read
//! needs no lookup, and asks the channel again when the cache misses, or
//! when it must know the blob is still there ([`Locations::exists`]).

use crate::{NotFound, Owner};
use anyhow::Result;
use bytes::Bytes;
use lru::LruCache;
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Mutex;

/// Where a blob's bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Located<F> {
    /// A text message: the bytes themselves.
    Inline(Bytes),
    /// A file of `size` bytes, read through `file` (a document's location,
    /// an attachment's URL).
    File { size: u64, file: F },
}

impl<F> Located<F> {
    /// The blob a text message holds, if it is one of ours.
    pub fn of_text(text: &str) -> Option<Self> {
        crate::text::decode(text).map(|(_, data)| Located::Inline(data))
    }
}

/// Whether blob `name` of `len` bytes goes as a text message on a store
/// whose messages hold `max_chars` characters (`inline` off: never).
pub fn as_text(inline: bool, name: &str, len: usize, max_chars: usize) -> bool {
    inline && crate::text::fits(name, len, max_chars)
}

/// Recently seen blobs' locations, by message id (least recently used out).
pub struct Locations<K, F> {
    cache: Mutex<LruCache<K, Located<F>>>,
}

impl<K: Hash + Eq + Copy + std::fmt::Display, F: Clone> Locations<K, F> {
    pub fn new(capacity: usize) -> Self {
        Self { cache: Mutex::new(LruCache::new(NonZeroUsize::new(capacity.max(1)).unwrap())) }
    }

    pub fn remember(&self, id: K, located: Located<F>) {
        self.cache.lock().unwrap().put(id, located);
    }

    pub fn forget(&self, id: K) {
        self.cache.lock().unwrap().pop(&id);
    }

    /// Whether blob `id` is known (a read needs no lookup), without making
    /// it recent.
    pub fn holds(&self, id: K) -> bool {
        self.cache.lock().unwrap().contains(&id)
    }

    pub fn cached(&self, id: K) -> Option<Located<F>> {
        self.cache.lock().unwrap().get(&id).cloned()
    }

    /// Where blob `id` is: cached, unless `fresh` (a new file reference or
    /// URL is needed), else what `read` finds in the channel, remembered.
    /// `read` answers `None` for a message that is gone ([`NotFound`]), an
    /// error for one that is not a blob.
    pub async fn locate<Fut>(&self, id: K, fresh: bool, read: impl FnOnce() -> Fut) -> Result<Located<F>>
    where
        Fut: Future<Output = Result<Option<Located<F>>>>,
    {
        if !fresh {
            if let Some(l) = self.cached(id) {
                return Ok(l);
            }
        }
        let Some(located) = read().await? else {
            self.forget(id);
            return Err(NotFound(format!("blob message {id}")).into());
        };
        self.remember(id, located.clone());
        Ok(located)
    }

    /// Whether blob `id` is still there, asked of the channel (`read` as in
    /// [`Self::locate`]): a text blob's bytes stay in the cache after the
    /// message is deleted, so the cache never answers.
    pub async fn exists<Fut>(&self, id: K, read: impl FnOnce() -> Fut) -> Result<bool>
    where
        Fut: Future<Output = Result<Option<Located<F>>>>,
    {
        match self.locate(id, true, read).await {
            Ok(_) => Ok(true),
            Err(e) if crate::Failure::of(&e) == crate::Failure::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// What an owner marker says to a person reading the channel
/// (docs/store-owner.md).
pub const OWNER_MARKER: &[u8] = b"pigeonhole: the blobs in this channel are this deployment's";

/// The owner the pinned messages mark a channel for: of `pins` (blob
/// names, the **latest pinned first**), the first that is a marker. The
/// same rule on every store: a Telegram bot sees only the latest pin, a
/// Discord bot lists them latest first. Of two deployments racing to mark
/// a new channel, the last to pin keeps it; the other learns so when it
/// reads the pin back.
pub fn marked_owner<'a>(pins: impl IntoIterator<Item = Option<&'a str>>) -> Option<Owner> {
    pins.into_iter().flatten().find_map(Owner::from_marker)
}

/// Whether a blob named `name` is one of `owner`'s (a sweep candidate).
pub fn owned_by(name: Option<&str>, owner: Owner) -> bool {
    name.and_then(crate::BlobId::from_name).is_some_and(|b| b.owner() == owner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_miss_reads_once_and_a_gone_blob_is_forgotten() {
        let locations: Locations<u64, String> = Locations::new(2);
        let got = locations.locate(1, false, || async { Ok(Some(Located::File { size: 3, file: "u".into() })) });
        assert_eq!(got.await.unwrap(), Located::File { size: 3, file: "u".into() });
        let cached = locations.locate(1, false, || async { panic!("cached: no read") });
        assert!(matches!(cached.await.unwrap(), Located::File { .. }));
        assert!(!locations.exists(1, || async { Ok(None) }).await.unwrap());
        assert!(!locations.holds(1), "forgotten once gone");
        let gone = locations.locate(1, false, || async { Ok(None) }).await.unwrap_err();
        assert!(gone.is::<NotFound>());
    }

    #[test]
    fn the_latest_pinned_marker_marks_the_channel() {
        let (a, b) = (Owner(1).marker_name(), Owner(2).marker_name());
        assert_eq!(marked_owner([Some("photo.jpg"), None, Some(b.as_str()), Some(a.as_str())]), Some(Owner(2)));
        assert_eq!(marked_owner([Some("photo.jpg")]), None);
        assert!(owned_by(Some(&crate::BlobId::random_of(Owner(5)).name()), Owner(5)));
        assert!(!owned_by(Some(&crate::BlobId::random_of(Owner(5)).name()), Owner(6)));
        assert!(!owned_by(None, Owner(0)));
    }
}
