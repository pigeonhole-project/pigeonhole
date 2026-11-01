//! A network of two transports, chosen per peer, and the client of either.

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::RaftTypeConfig;
use std::collections::HashSet;

/// A peer's client of one transport or the other.
pub enum Either<A, B> {
    A(A),
    B(B),
}

impl<C: RaftTypeConfig, A: RaftNetwork<C>, B: RaftNetwork<C>> RaftNetwork<C> for Either<A, B> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        match self {
            Either::A(a) => a.append_entries(rpc, option).await,
            Either::B(b) => b.append_entries(rpc, option).await,
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId, InstallSnapshotError>>,
    > {
        match self {
            Either::A(a) => a.install_snapshot(rpc, option).await,
            Either::B(b) => b.install_snapshot(rpc, option).await,
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C::NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        match self {
            Either::A(a) => a.vote(rpc, option).await,
            Either::B(b) => b.vote(rpc, option).await,
        }
    }

    fn backoff(&self) -> Backoff {
        match self {
            Either::A(a) => a.backoff(),
            Either::B(b) => b.backoff(),
        }
    }
}

/// A network of two transports: the peers in `via_b` over `b`, the rest
/// over `a` (gRPC to most, the chat to a node that only has channels).
pub struct Mixed<A, B, Id> {
    pub a: A,
    pub b: B,
    pub via_b: HashSet<Id>,
}

impl<C, A, B> RaftNetworkFactory<C> for Mixed<A, B, C::NodeId>
where
    C: RaftTypeConfig,
    A: RaftNetworkFactory<C>,
    B: RaftNetworkFactory<C>,
{
    type Network = Either<A::Network, B::Network>;

    async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network {
        match self.via_b.contains(&target) {
            true => Either::B(self.b.new_client(target, node).await),
            false => Either::A(self.a.new_client(target, node).await),
        }
    }
}
