//! Generated gRPC types for the workflow matching service.
//!
//! The `proto/workflow.proto` file is compiled by `build.rs` via `tonic-build`;
//! the generated code lives in `OUT_DIR` and is pulled in here.

pub mod synapse {
    pub mod workflow {
        pub mod v1 {
            tonic::include_proto!("synapse.workflow.v1");
        }
    }
}
