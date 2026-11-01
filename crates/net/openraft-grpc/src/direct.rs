//! What gRPC is between two nodes that reach each other, in memory: a call
//! goes straight to the peer's [`Handler`], after an optional delay; a node
//! can be cut off. For tests, the simulation and the in-browser demo, where
//! no socket opens.

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Timeout, Unreachable};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{AnyError, RaftTypeConfig};
use openraft_chat::Handler;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The nodes of one direct network, and its faults.
pub struct Direct<C: RaftTypeConfig> {
    inner: Arc<Mutex<State<C>>>,
}

impl<C: RaftTypeConfig> Clone for Direct<C> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<C: RaftTypeConfig> Default for Direct<C> {
    fn default() -> Self {
        Self { inner: Arc::new(Mutex::new(State { nodes: HashMap::new(), cut: HashSet::new(), delay: None })) }
    }
}

struct State<C: RaftTypeConfig> {
    nodes: HashMap<C::NodeId, Arc<dyn Handler<C>>>,
    cut: HashSet<C::NodeId>,
    delay: Option<Duration>,
}

impl<C: RaftTypeConfig> Direct<C> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answer calls to `node` with `handler` (its `Raft`).
    pub fn serve(&self, node: C::NodeId, handler: Arc<dyn Handler<C>>) {
        self.inner.lock().unwrap().nodes.insert(node, handler);
    }

    /// Cut `node` off (its calls and the calls to it fail), or back.
    pub fn isolate(&self, node: C::NodeId, cut: bool) {
        let mut s = self.inner.lock().unwrap();
        match cut {
            true => s.cut.insert(node),
            false => s.cut.remove(&node),
        };
    }

    /// Each call takes this long to reach its peer; `None`: no time.
    pub fn set_delay(&self, delay: Option<Duration>) {
        self.inner.lock().unwrap().delay = delay;
    }

    /// Node `me`'s network factory.
    pub fn network(&self, me: C::NodeId) -> DirectNetwork<C> {
        DirectNetwork { net: self.clone(), me }
    }

    /// The peer's handler and the delay, if `me` reaches `target` now.
    fn route(&self, me: &C::NodeId, target: &C::NodeId) -> Option<(Arc<dyn Handler<C>>, Option<Duration>)> {
        let s = self.inner.lock().unwrap();
        if s.cut.contains(me) || s.cut.contains(target) {
            return None;
        }
        Some((s.nodes.get(target)?.clone(), s.delay))
    }
}

/// openraft's network factory for one node of a [`Direct`] network.
pub struct DirectNetwork<C: RaftTypeConfig> {
    net: Direct<C>,
    me: C::NodeId,
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for DirectNetwork<C> {
    type Network = DirectClient<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        DirectClient { net: self.net.clone(), me: self.me.clone(), target }
    }
}

/// One peer of a [`Direct`] network.
pub struct DirectClient<C: RaftTypeConfig> {
    net: Direct<C>,
    me: C::NodeId,
    target: C::NodeId,
}

impl<C: RaftTypeConfig> DirectClient<C> {
    /// `call` on the peer's handler, within `ttl`.
    #[allow(clippy::result_large_err)] // openraft's error type, as RaftNetwork returns it
    async fn call<T, E, F>(
        &self,
        action: RPCTypes,
        ttl: Duration,
        call: impl FnOnce(Arc<dyn Handler<C>>) -> F,
    ) -> Result<T, RPCError<C::NodeId, C::Node, RaftError<C::NodeId, E>>>
    where
        E: std::error::Error,
        F: Future<Output = Result<T, RaftError<C::NodeId, E>>>,
    {
        let Some((handler, delay)) = self.net.route(&self.me, &self.target) else {
            return Err(RPCError::Unreachable(Unreachable::new(&AnyError::error("cut off"))));
        };
        let answer = async {
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            call(handler).await
        };
        match tokio::time::timeout(ttl, answer).await {
            Ok(r) => r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target.clone(), e))),
            Err(_) => Err(RPCError::Timeout(Timeout {
                action,
                id: self.me.clone(),
                target: self.target.clone(),
                timeout: ttl,
            })),
        }
    }
}

impl<C: RaftTypeConfig> RaftNetwork<C> for DirectClient<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        self.call(RPCTypes::AppendEntries, option.hard_ttl(), |h| async move { h.append_entries(rpc).await }).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId, InstallSnapshotError>>,
    > {
        self.call(RPCTypes::InstallSnapshot, option.hard_ttl(), |h| async move { h.install_snapshot(rpc).await }).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C::NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        self.call(RPCTypes::Vote, option.hard_ttl(), |h| async move { h.vote(rpc).await }).await
    }
}
