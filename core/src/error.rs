//! Error model for the unified storage engine (spec.txt §3.1, §6 Phase 1).

use std::fmt;
use std::sync::Arc;

/// Classification of an [`EngineError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Requested entity (log, queue, key, offset, ...) does not exist.
    NotFound,
    /// Entity already exists and creation was requested.
    AlreadyExists,
    /// A per-tenant throughput/storage/object quota was exceeded.
    TenantQuotaExceeded,
    /// Caller supplied an invalid argument (bad topic, empty name, ...).
    InvalidArgument,
    /// Underlying storage I/O or corruption failure.
    Storage,
    /// Engine has been shut down.
    Closed,
    /// Any other internal failure.
    Internal,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorKind::NotFound => "not found",
            ErrorKind::AlreadyExists => "already exists",
            ErrorKind::TenantQuotaExceeded => "tenant quota exceeded",
            ErrorKind::InvalidArgument => "invalid argument",
            ErrorKind::Storage => "storage error",
            ErrorKind::Closed => "engine closed",
            ErrorKind::Internal => "internal error",
        };
        f.write_str(s)
    }
}

/// The unified error type returned by every storage/engine operation.
#[derive(Debug, Clone)]
pub struct EngineError {
    kind: ErrorKind,
    message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl EngineError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Arc::new(source)),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, msg)
    }

    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::AlreadyExists, msg)
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, msg)
    }

    pub fn storage(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Storage, msg)
    }

    pub fn quota_exceeded(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::TenantQuotaExceeded, msg)
    }

    pub fn closed(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Closed, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, msg)
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)?;
        if let Some(src) = &self.source {
            write!(f, " (caused by: {})", src)?;
        }
        Ok(())
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|s| s.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        let kind = match e.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            _ => ErrorKind::Storage,
        };
        EngineError::with_source(kind, "io error", Arc::new(e))
    }
}

/// Convenience result alias used across the crate.
pub type EngineResult<T> = Result<T, EngineError>;
