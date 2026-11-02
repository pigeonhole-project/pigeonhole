//! The cluster as this node sees it, for the admin UI and its graph.

use crate::raft::{Metrics, NodeId};
use openraft::ServerState;
use serde::Serialize;

/// The cluster from this node's side: who leads, in which term, and each
/// member's role; its lag only where this node knows it (on the leader).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterView {
    pub me: NodeId,
    /// `None` during an election.
    pub leader: Option<NodeId>,
    pub term: u64,
    pub nodes: Vec<NodeView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeView {
    pub id: NodeId,
    /// `leader`, `follower`, `candidate`, `learner` (`shutdown`: this node, stopped).
    pub role: &'static str,
    /// Entries applied: this node's own only.
    pub applied: Option<u64>,
    /// Entries behind the leader's log: known on the leader (0 for itself).
    pub lag: Option<u64>,
    /// Position its log holds up to (replicated to it, maybe not applied
    /// yet): known on the leader.
    pub matched: Option<u64>,
}

fn role(state: ServerState) -> &'static str {
    match state {
        ServerState::Leader => "leader",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Learner => "learner",
        ServerState::Shutdown => "shutdown",
    }
}

impl ClusterView {
    pub(crate) fn of(m: &Metrics, applied: u64) -> Self {
        let membership = m.membership_config.membership();
        let voters: Vec<NodeId> = membership.voter_ids().collect();
        let learners = membership.learner_ids().filter(|l| !voters.contains(l));
        let last = m.last_log_index.unwrap_or(0);
        // Raft's index n is position n + 1.
        let matched = |id: NodeId| Some(m.replication.as_ref()?.get(&id)?.as_ref().map_or(0, |l| l.index + 1));
        let lag = |id: NodeId| {
            let replication = m.replication.as_ref()?;
            match id == m.id {
                true => Some(0),
                false => Some(last.saturating_sub(replication.get(&id)?.as_ref().map_or(0, |l| l.index))),
            }
        };
        let view = |id: NodeId, learner: bool| NodeView {
            id,
            role: match (id == m.id, Some(id) == m.current_leader, learner) {
                (true, _, _) => role(m.state),
                (false, true, _) => "leader",
                (false, false, true) => "learner",
                (false, false, false) => "follower",
            },
            applied: (id == m.id).then_some(applied),
            lag: lag(id),
            matched: matched(id),
        };
        let mut nodes: Vec<NodeView> = voters.iter().map(|&id| view(id, false)).collect();
        nodes.extend(learners.map(|id| view(id, true)));
        nodes.sort_by_key(|n| n.id);
        Self { me: m.id, leader: m.current_leader, term: m.current_term, nodes }
    }
}
