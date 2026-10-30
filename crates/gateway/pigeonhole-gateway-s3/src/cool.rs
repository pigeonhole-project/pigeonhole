//! Which inline objects the cooler moves out to chunks
//! (docs/inline-objects.md § The cooler).
//!
//! The choice is the proposer's: it travels in [`crate::S3Command::Cool`],
//! whose `apply` only checks that the object is still that inline version.

use crate::commands::{Body, S3State};
use crate::index::parse_rfc3339;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// When an inline object goes out (`[inline]` in the config).
#[derive(Debug, Clone, PartialEq)]
pub struct CoolPolicy {
    /// All inline bytes of the tablet; above it the oldest go out.
    pub budget_bytes: u64,
    /// Over budget, cool until the inline bytes are under this share of it.
    pub low_watermark: f64,
    /// Out once not rewritten for this long; `None`: only by budget.
    pub cool_after: Option<Duration>,
    /// Never out while written less than this long ago.
    pub min_inline: Duration,
    /// Most objects one pass moves out.
    pub batch: usize,
}

impl Default for CoolPolicy {
    fn default() -> Self {
        Self {
            budget_bytes: 64 << 20,
            low_watermark: 0.9,
            cool_after: Some(Duration::from_secs(600)),
            min_inline: Duration::from_secs(30),
            batch: 64,
        }
    }
}

/// Why an object is moved out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoolReason {
    /// Not rewritten for `cool_after`.
    Age,
    /// The inline bytes are over budget and it is among the oldest.
    Budget,
}

impl CoolReason {
    pub fn as_str(self) -> &'static str {
        match self {
            CoolReason::Age => "age",
            CoolReason::Budget => "budget",
        }
    }
}

/// An inline object to move out, as of the state it was picked from.
#[derive(Debug, Clone, PartialEq)]
pub struct CoolCandidate {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub bytes: Vec<u8>,
    pub reason: CoolReason,
}

/// The inline objects of `s` to move out at `now`, oldest first.
pub(crate) fn candidates(s: &S3State, now: DateTime<Utc>, p: &CoolPolicy) -> Vec<CoolCandidate> {
    let mut inline: Vec<_> = s
        .objects
        .iter()
        .filter_map(|((bucket, key), o)| match &o.body {
            Body::Inline(bytes) => {
                let age = (now - parse_rfc3339(&o.mtime)).to_std().unwrap_or_default();
                (age >= p.min_inline).then_some((age, bucket, key, o, bytes))
            }
            Body::Chunks(_) => None,
        })
        .collect();
    inline.sort_by_key(|c| std::cmp::Reverse(c.0));

    let over_budget = s.inline_bytes > p.budget_bytes;
    let target = (p.budget_bytes as f64 * p.low_watermark) as u64;
    let mut projected = s.inline_bytes;
    let mut out = Vec::new();
    for (age, bucket, key, o, bytes) in inline {
        if out.len() >= p.batch {
            break;
        }
        let reason = if p.cool_after.is_some_and(|after| age >= after) {
            CoolReason::Age
        } else if over_budget && projected > target {
            CoolReason::Budget
        } else {
            continue;
        };
        projected -= bytes.len() as u64;
        out.push(CoolCandidate {
            bucket: bucket.clone(),
            key: key.clone(),
            etag: o.etag.clone(),
            bytes: bytes.clone(),
            reason,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::S3Command;
    use pigeonhole_meta::StateMachine;

    fn state(objects: &[(&str, usize, i64)]) -> (S3State, DateTime<Utc>) {
        let now = DateTime::parse_from_rfc3339("2026-10-30T12:00:00Z").unwrap().with_timezone(&Utc);
        let mut s = S3State::default();
        s.apply(S3Command::CreateBucket { name: "b".into(), created_at: "t".into() }).unwrap().unwrap();
        for &(key, len, age_secs) in objects {
            let put = S3Command::PutObject {
                bucket: "b".into(),
                key: key.into(),
                etag: format!("e-{key}"),
                size: len as i64,
                content_type: None,
                body: Body::Inline(vec![0; len]),
                user_meta: vec![],
                tags: vec![],
                checksums_json: "{}".into(),
                mtime: (now - chrono::Duration::seconds(age_secs)).to_rfc3339(),
                expect: None,
            };
            s.apply(put).unwrap().unwrap();
        }
        (s, now)
    }

    fn picked(c: &[CoolCandidate]) -> Vec<(&str, CoolReason)> {
        c.iter().map(|c| (c.key.as_str(), c.reason)).collect()
    }

    #[test]
    fn cold_objects_go_by_age_and_hot_ones_stay() {
        let (s, now) = state(&[("lock", 200, 2), ("old", 100, 900), ("warm", 100, 300)]);
        let p = CoolPolicy { cool_after: Some(Duration::from_secs(600)), ..CoolPolicy::default() };
        assert_eq!(picked(&candidates(&s, now, &p)), [("old", CoolReason::Age)]);
        let p = CoolPolicy { cool_after: None, ..p };
        assert!(candidates(&s, now, &p).is_empty(), "under budget, no age rule: nothing");
    }

    #[test]
    fn over_budget_the_oldest_go_down_to_the_watermark_but_never_the_newest() {
        // 4 × 100 B, budget 250 B: cool to at most 125 B (0.5), oldest first;
        // the 10-second-old one is protected by min_inline.
        let (s, now) = state(&[("a", 100, 400), ("b", 100, 300), ("c", 100, 200), ("d", 100, 10)]);
        let p = CoolPolicy {
            budget_bytes: 250,
            low_watermark: 0.5,
            cool_after: None,
            min_inline: Duration::from_secs(30),
            batch: 64,
        };
        assert_eq!(picked(&candidates(&s, now, &p)), [("a", CoolReason::Budget), ("b", CoolReason::Budget), ("c", CoolReason::Budget)]);
        let p = CoolPolicy { low_watermark: 1.0, ..p };
        assert_eq!(picked(&candidates(&s, now, &p)), [("a", CoolReason::Budget), ("b", CoolReason::Budget)]);
        let p = CoolPolicy { batch: 1, ..p };
        assert_eq!(picked(&candidates(&s, now, &p)), [("a", CoolReason::Budget)]);
    }
}
