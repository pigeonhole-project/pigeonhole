//! The background loops as the UI shows them: each task's period, when it
//! passes next, and the work waiting for it.

use crate::Admin;
use anyhow::Result;

impl Admin {
    /// What runs in the background, task by task (the loops this node runs,
    /// `DynConfig::tasks`): its period, when it passes next, and the work
    /// waiting for it.
    pub async fn background(&self) -> Result<serde_json::Value> {
        use serde_json::json;
        let found = self.log.discover().await?;
        let (head, base) = found.map_or((0, 0), |d| (d.head.position, d.base.map_or(0, |b| b.at)));
        let jobs = self.store.db().repair_jobs().await;
        let zero_ref = self.store.db().list_zero_ref_chunks().await?;
        let policy = self.dynamic.cool.get().policy.with_inlining(self.inline_on);
        let cold = self.index.cool_candidates(chrono::Utc::now(), &policy);
        let work = |task: &str| {
            match task {
            "repair" => json!(jobs.iter().map(|(chunk, store)| json!({ "chunk": chunk, "store": store })).collect::<Vec<_>>()),
            "sweep" => json!(zero_ref.iter().map(|chunk| json!({ "chunk": chunk })).collect::<Vec<_>>()),
            "cool" => json!(cold
                .iter()
                .map(|c| json!({ "key": c.key, "bucket": c.bucket, "size": c.bytes.len(), "reason": format!("{:?}", c.reason).to_lowercase() }))
                .collect::<Vec<_>>()),
            _ => json!([]),
        }
        };
        let tasks: Vec<_> = self
            .dynamic
            .tasks()
            .into_iter()
            .map(|t| json!({ "task": t.task, "every": t.every, "next_in_ms": t.next_in_ms, "work": work(t.task) }))
            .collect();
        // `at`: the newest snapshot's position (the entries count from it); `base`: where the log starts.
        let at = self.meta.snapshot_position().unwrap_or(base);
        Ok(
            json!({ "tasks": tasks, "snapshot": { "head": head, "base": base, "at": at, "every": self.snapshot_every } }),
        )
    }
}
