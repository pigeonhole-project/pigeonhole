//! The real thing: one unary RPC per Raft call, over tonic.

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout, Unreachable};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{AnyError, RaftTypeConfig};
use openraft_chat::frame::{open, seal, Body, Envelope, Frame, Key, FRAME_VERSION};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[allow(clippy::all)] // generated
mod pb {
    tonic::include_proto!("openraft_grpc");
}

#[cfg(not(madsim))]
pub use server::serve;

/// Largest message either way: a snapshot chunk (openraft's are 3 MiB) or
/// a batch of entries.
pub const MAX_MESSAGE: usize = 64 << 20;

/// Who a node is to the others: the cluster it belongs to and its key.
#[derive(Clone, Debug)]
pub struct Identity<Id> {
    pub me: Id,
    /// Frames of another cluster are refused.
    pub cluster: u128,
    pub key: Key,
}

/// openraft's network factory for one node, its peers at `host:port`.
#[derive(Clone)]
pub struct GrpcNetwork<C: RaftTypeConfig> {
    id: Arc<Identity<C::NodeId>>,
    peers: Arc<HashMap<C::NodeId, String>>,
}

impl<C: RaftTypeConfig> GrpcNetwork<C> {
    pub fn new(id: Identity<C::NodeId>, peers: HashMap<C::NodeId, String>) -> Self {
        Self { id: Arc::new(id), peers: Arc::new(peers) }
    }
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for GrpcNetwork<C> {
    type Network = GrpcClient<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        // Lazy: it connects on the first call, again after a failure.
        let channel = self.peers.get(&target).and_then(|addr| Endpoint::from_shared(format!("http://{addr}")).ok());
        GrpcClient { id: self.id.clone(), target, channel: channel.map(|e| e.connect_lazy()) }
    }
}

/// One peer, as openraft's network sees it.
pub struct GrpcClient<C: RaftTypeConfig> {
    id: Arc<Identity<C::NodeId>>,
    target: C::NodeId,
    /// `None`: the peer has no address.
    channel: Option<Channel>,
}

/// Why a call got no answer.
enum Failure {
    Timeout,
    Unreachable(String),
    /// An answer, but not a valid one.
    Bad(String),
}

impl<C: RaftTypeConfig> GrpcClient<C> {
    async fn call(&self, body: Body<C>, ttl: Duration) -> Result<Body<C>, Failure> {
        let channel = self.channel.clone().ok_or_else(|| Failure::Unreachable("no address".into()))?;
        let id = &self.id;
        let kind = body.kind();
        let envelopes = vec![Envelope { to: self.target.clone(), rpc: 0, body }];
        let frame = Frame { version: FRAME_VERSION, cluster: id.cluster, from: id.me.clone(), seq: 0, envelopes };
        let request = pb::Sealed { frame: seal(&frame, &id.key).to_vec() };
        let (me, to, bytes) = (&id.me, &self.target, request.frame.len());
        tracing::debug!(node = %me, %to, kind, reply = false, via = "grpc", bytes, "raft frame sent");
        let mut client = pb::raft_client::RaftClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        let answer = match tokio::time::timeout(ttl, client.call(request)).await {
            Err(_) => return Err(Failure::Timeout),
            Ok(Err(status)) => return Err(Failure::Unreachable(format!("{}: {}", status.code(), status.message()))),
            Ok(Ok(answer)) => answer.into_inner(),
        };
        let frame: Frame<C> = open(&answer.frame, &id.key).map_err(|e| Failure::Bad(format!("{e:#}")))?;
        if frame.cluster != id.cluster || frame.from != self.target {
            return Err(Failure::Bad("an answer from another node or cluster".into()));
        }
        let envelope = frame.envelopes.into_iter().next().ok_or_else(|| Failure::Bad("an empty answer".into()))?;
        tracing::debug!(node = %id.me, from = %self.target, kind, reply = true, via = "grpc", "raft frame received");
        Ok(envelope.body)
    }

    fn failed<E: std::error::Error>(
        &self,
        action: RPCTypes,
        ttl: Duration,
        f: Failure,
    ) -> RPCError<C::NodeId, C::Node, E> {
        match f {
            Failure::Timeout => {
                RPCError::Timeout(Timeout { action, id: self.id.me.clone(), target: self.target.clone(), timeout: ttl })
            }
            // openraft backs off from an unreachable peer instead of retrying at once.
            Failure::Unreachable(why) => RPCError::Unreachable(Unreachable::new(&AnyError::error(why))),
            Failure::Bad(why) => RPCError::Network(NetworkError::new(&AnyError::error(why))),
        }
    }

    fn remote<E: std::error::Error>(
        &self,
        e: RaftError<C::NodeId, E>,
    ) -> RPCError<C::NodeId, C::Node, RaftError<C::NodeId, E>> {
        RPCError::RemoteError(RemoteError::new(self.target.clone(), e))
    }
}

impl<C: RaftTypeConfig> RaftNetwork<C> for GrpcClient<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        let ttl = option.hard_ttl();
        match self.call(Body::AppendEntries(rpc), ttl).await {
            Ok(Body::Appended(r)) => r.map_err(|e| self.remote(e)),
            Ok(_) => {
                Err(self.failed(RPCTypes::AppendEntries, ttl, Failure::Bad("a response of the wrong kind".into())))
            }
            Err(f) => Err(self.failed(RPCTypes::AppendEntries, ttl, f)),
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
        let ttl = option.hard_ttl();
        match self.call(Body::InstallSnapshot(rpc), ttl).await {
            Ok(Body::Installed(r)) => r.map_err(|e| self.remote(e)),
            Ok(_) => {
                Err(self.failed(RPCTypes::InstallSnapshot, ttl, Failure::Bad("a response of the wrong kind".into())))
            }
            Err(f) => Err(self.failed(RPCTypes::InstallSnapshot, ttl, f)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C::NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        let ttl = option.hard_ttl();
        match self.call(Body::Vote(rpc), ttl).await {
            Ok(Body::Voted(r)) => {
                if let Ok(v) = &r {
                    tracing::debug!(node = %self.target, to = %self.id.me, granted = v.vote_granted, via = "grpc", "raft vote answered");
                }
                r.map_err(|e| self.remote(e))
            }
            Ok(_) => Err(self.failed(RPCTypes::Vote, ttl, Failure::Bad("a response of the wrong kind".into()))),
            Err(f) => Err(self.failed(RPCTypes::Vote, ttl, f)),
        }
    }
}

#[cfg(not(madsim))] // a real TCP listener: not in the simulation
mod server {
    use super::{pb, Identity, MAX_MESSAGE};
    use openraft::RaftTypeConfig;
    use openraft_chat::frame::{open, seal, Body, Envelope, Frame, FRAME_VERSION};
    use openraft_chat::Handler;
    use std::sync::Arc;
    use tonic::{Request, Response, Status};

    /// Answer the peers' calls on `listener` with `handler` (the node's
    /// `Raft`) until the listener fails.
    pub async fn serve<C: RaftTypeConfig>(
        listener: tokio::net::TcpListener,
        id: Identity<C::NodeId>,
        handler: Arc<dyn Handler<C>>,
    ) -> anyhow::Result<()> {
        let service = pb::raft_server::RaftServer::new(Service { id, handler })
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await?;
        Ok(())
    }

    struct Service<C: RaftTypeConfig> {
        id: Identity<C::NodeId>,
        handler: Arc<dyn Handler<C>>,
    }

    #[tonic::async_trait]
    impl<C: RaftTypeConfig> pb::raft_server::Raft for Service<C> {
        async fn call(&self, request: Request<pb::Sealed>) -> Result<Response<pb::Sealed>, Status> {
            let id = &self.id;
            let frame: Frame<C> =
                open(&request.into_inner().frame, &id.key).map_err(|e| Status::unauthenticated(format!("{e:#}")))?;
            if frame.cluster != id.cluster {
                return Err(Status::permission_denied("a frame of another cluster"));
            }
            let Some(env) = frame.envelopes.into_iter().next().filter(|e| e.to == id.me && e.body.is_request()) else {
                return Err(Status::invalid_argument("not one request to this node"));
            };
            let kind = env.body.kind();
            tracing::debug!(node = %id.me, from = %frame.from, kind, reply = false, via = "grpc", "raft frame received");
            let body: Body<C> = match env.body {
                Body::AppendEntries(r) => Body::Appended(self.handler.append_entries(r).await),
                Body::Vote(r) => Body::Voted(self.handler.vote(r).await),
                Body::InstallSnapshot(r) => Body::Installed(self.handler.install_snapshot(r).await),
                _ => unreachable!("is_request"),
            };
            let envelopes = vec![Envelope { to: frame.from, rpc: env.rpc, body }];
            let answer = Frame { version: FRAME_VERSION, cluster: id.cluster, from: id.me.clone(), seq: 0, envelopes };
            let sealed = seal(&answer, &id.key).to_vec();
            let (to, bytes) = (&answer.envelopes[0].to, sealed.len());
            tracing::debug!(node = %id.me, %to, kind, reply = true, via = "grpc", bytes, "raft frame sent");
            Ok(Response::new(pb::Sealed { frame: sealed }))
        }
    }
}
