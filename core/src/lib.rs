//! Unified storage engine: `Log`, `Queue`, and `Map` primitives (spec.txt §3.1, §6 Phase 1).
//!
//! Empty scaffold — Phase 0 only wires up the crate and its dependency choices
//! (`openraft` for embedded consensus, `tokio-uring` for async I/O on Linux).

#[cfg(target_os = "linux")]
pub fn io_uring_supported() -> bool {
    true
}

#[cfg(not(target_os = "linux"))]
pub fn io_uring_supported() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_builds() {
        let _ = io_uring_supported();
    }
}
