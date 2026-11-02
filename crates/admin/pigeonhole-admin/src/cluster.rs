//! The `cluster` op: the tablet's Raft cluster as this node sees it, with
//! what the config says of each node (docs/cluster.md).

use crate::Admin;
use serde_json::{json, Value};

/// The ids of a node's own stores in `channels` (a cluster's node's: `-{id}` after).
pub const LOG_STORE: &str = "meta-log";
pub const SNAPSHOT_STORE: &str = "meta-snap";

/// A node's Raft outbox as `channels` lists it.
pub fn outbox_store(node: u64) -> String {
    format!("outbox-{node}")
}

impl Admin {
    /// This node's own `role` store (its log, its snapshots) as `channels`
    /// lists it: a fake channel goes by its own id, not by these constants.
    fn own_store(&self, role: &str) -> Option<&str> {
        self.stores.iter().find(|s| s.role == role).map(|s| s.store.as_str())
    }

    /// `{me, leader, term, nodes: [{id, role, grpc, outbox, log, snapshots,
    /// applied, lag}], links: [{a, b, via}], read_only: a leader without a
    /// quorum, timing: {heartbeat, election: [min, max]} (ms)}`. Only this node's own log and
    /// snapshots are known here; `applied` is this node's, `lag` known on
    /// the leader. A cluster of one: one leader, no links.
    pub fn cluster(&self) -> Value {
        let view = self.meta.cluster_view();
        let configured = |id: u64| self.cluster.as_ref().and_then(|c| c.nodes.iter().find(|n| n.id == id));
        let nodes: Vec<Value> = view
            .nodes
            .iter()
            .map(|n| {
                let node = configured(n.id);
                let mine = n.id == view.me;
                json!({
                    "id": n.id,
                    "role": n.role,
                    "grpc": node.and_then(|c| c.grpc.clone()),
                    "outbox": node.and_then(|c| c.outbox).map(|_| outbox_store(n.id)),
                    "chat": node.and_then(|c| c.outbox).map(|chat| chat.to_string()),
                    "log": mine.then(|| self.own_store("log")).flatten(),
                    "snapshots": mine.then(|| self.own_store("snapshots")).flatten(),
                    "applied": n.applied,
                    "lag": n.lag,
                })
            })
            .collect();
        let links: Vec<Value> = self
            .cluster
            .iter()
            .flat_map(|c| c.pairs())
            .map(|(a, b, link)| json!({ "a": a, "b": b, "via": link.as_str() }))
            .collect();
        // The clocks a follower's election runs by (ms): a heartbeat's period,
        // the timeout's range; none for a cluster of one.
        let timing = self.cluster.as_ref().map(|c| {
            let ms = |d: std::time::Duration| d.as_millis() as u64;
            json!({ "heartbeat": ms(c.heartbeat), "election": [ms(c.election_timeout.0), ms(c.election_timeout.1)] })
        });
        json!({
            "me": view.me, "leader": view.leader, "term": view.term, "nodes": nodes, "links": links,
            "read_only": self.meta.is_read_only(), "timing": timing,
        })
    }
}
