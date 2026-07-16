//! Error model for the workflow matching service.

use thiserror::Error;

/// Errors surfaced by the matching-service layer (dispatch, ack, routing).
#[derive(Debug, Error)]
pub enum WorkflowError {
    /// The underlying `synapse-core` engine rejected the operation
    /// (quota, tenant charset, missing queue, ...).
    #[error("engine error: {0}")]
    Engine(#[from] synapse_core::EngineError),

    /// The caller supplied a malformed request (empty task queue, bad timeout).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The supplied task token does not correspond to an in-flight task.
    #[error("unknown task token")]
    UnknownToken,

    /// Any other internal failure (should not normally happen).
    #[error("internal: {0}")]
    Internal(String),
}

/// Convenience result alias used across the crate.
pub type WorkflowResult<T> = Result<T, WorkflowError>;
