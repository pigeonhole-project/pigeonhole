//! REAPI v2 remote cache (CAS + ActionCache + ByteStream) for Bazel/Buck2.

pub mod cas;
pub mod cas_index;
pub mod cas_snapshot;
pub mod config;
pub mod digest;
pub mod server;

pub mod google {
    pub mod bytestream {
        tonic::include_proto!("google.bytestream");
    }
    pub mod longrunning {
        tonic::include_proto!("google.longrunning");
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

pub mod build {
    pub mod bazel {
        pub mod semver {
            tonic::include_proto!("build.bazel.semver");
        }
        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    tonic::include_proto!("build.bazel.remote.execution.v2");
                }
            }
        }
    }
}

pub use build::bazel::remote::execution::v2 as reapi;
pub use cas_index::{CasEntry, CasIndex};
pub use cas_snapshot::{push_cas_snapshot, restore_cas_snapshot, ROOT_NAME as CAS_ROOT};
pub use google::bytestream as bytestream_pb;
