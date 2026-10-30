//! The wire format: a frame is one chat message, holding every envelope a
//! node sends in one tick, sealed with an HMAC of the cluster's key.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::RaftTypeConfig;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Version of [`Frame`]; a frame of another is dropped.
pub const FRAME_VERSION: u8 = 1;

/// The cluster's shared secret.
pub type Key = [u8; 32];

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Frame<C: RaftTypeConfig> {
    pub version: u8,
    /// Frames of another cluster sharing the channels are dropped.
    pub cluster: u128,
    pub from: C::NodeId,
    /// Per sender, strictly increasing: a frame posted twice is handled once.
    pub seq: u64,
    pub envelopes: Vec<Envelope<C>>,
}

/// One request or response, addressed to a node.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Envelope<C: RaftTypeConfig> {
    pub to: C::NodeId,
    /// The requester's id for the call; a response carries it back.
    pub rpc: u64,
    pub body: Body<C>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Body<C: RaftTypeConfig> {
    AppendEntries(AppendEntriesRequest<C>),
    Appended(Result<AppendEntriesResponse<C::NodeId>, RaftError<C::NodeId>>),
    Vote(VoteRequest<C::NodeId>),
    Voted(Result<VoteResponse<C::NodeId>, RaftError<C::NodeId>>),
    InstallSnapshot(InstallSnapshotRequest<C>),
    Installed(Result<InstallSnapshotResponse<C::NodeId>, RaftError<C::NodeId, InstallSnapshotError>>),
}

impl<C: RaftTypeConfig> Body<C> {
    pub fn is_request(&self) -> bool {
        matches!(self, Body::AppendEntries(_) | Body::Vote(_) | Body::InstallSnapshot(_))
    }

    /// The RPC a request or a response belongs to.
    pub fn rpc(&self) -> &'static str {
        match self {
            Body::AppendEntries(_) | Body::Appended(_) => "append_entries",
            Body::Vote(_) | Body::Voted(_) => "vote",
            Body::InstallSnapshot(_) | Body::Installed(_) => "install_snapshot",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Sealed {
    frame: Vec<u8>,
    mac: [u8; 32],
}

fn mac(key: &Key, data: &[u8]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key length");
    mac.update(data);
    mac
}

/// The frame as the bytes of one chat message.
pub fn seal<C: RaftTypeConfig>(frame: &Frame<C>, key: &Key) -> Bytes {
    let frame = postcard::to_stdvec(frame).expect("a frame always encodes");
    let mac = mac(key, &frame).finalize().into_bytes().into();
    Bytes::from(postcard::to_stdvec(&Sealed { frame, mac }).expect("a sealed frame always encodes"))
}

/// The frame in `bytes`, if it is sealed with `key` and of this version.
pub fn open<C: RaftTypeConfig>(bytes: &[u8], key: &Key) -> Result<Frame<C>> {
    let sealed: Sealed = postcard::from_bytes(bytes).context("not a sealed frame")?;
    mac(key, &sealed.frame).verify_slice(&sealed.mac).ok().context("the frame's MAC does not match")?;
    let frame: Frame<C> = postcard::from_bytes(&sealed.frame).context("decode frame")?;
    if frame.version != FRAME_VERSION {
        bail!("frame version {}, this build reads {FRAME_VERSION}", frame.version);
    }
    Ok(frame)
}
