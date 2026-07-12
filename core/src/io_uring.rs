//! Async network I/O engine for the data plane (TODO.md Phase 1: the
//! `tokio-uring`/`monoio` Linux backend picked in Phase 0 has not been wired
//! in).
//!
//! `IoEngine` selects the backend: on Linux with the `io-uring` feature the
//! storage engine's TCP accept path is served by a `tokio-uring` (io_uring)
//! runtime; elsewhere (or without the feature) a plain `tokio` runtime is
//! used, so the crate builds and tests on every platform. The accept loops
//! share the same connection-handler shape, so an adapter swaps its network
//! backend by calling the right `accept_loop_*` for its target.

use std::future::Future;

/// The async I/O backend currently in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoEngine {
    Tokio,
    #[allow(dead_code)]
    IoUring,
}

/// Return the engine selected by the build (Linux + `io-uring` feature =>
/// `IoUring`, otherwise `Tokio`).
pub fn current() -> IoEngine {
    if cfg!(all(target_os = "linux", feature = "io-uring")) {
        IoEngine::IoUring
    } else {
        IoEngine::Tokio
    }
}

/// tokio-backed accept loop. For every accepted connection `make_conn` is
/// spawned; it owns the connection and performs the broker protocol.
pub async fn accept_loop_tokio<F, Fut>(
    listener: tokio::net::TcpListener,
    mut make_conn: F,
) where
    F: FnMut(tokio::net::TcpStream) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let fut = make_conn(sock);
                tokio::spawn(fut);
            }
            Err(_) => break,
        }
    }
}

/// tokio-uring-backed accept loop (Linux + `io-uring` feature only). Same
/// connection-handler shape as [`accept_loop_tokio`], backed by an io_uring
/// runtime for lower per-connection syscall overhead.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub async fn accept_loop_uring<F, Fut>(
    listener: tokio_uring::net::TcpListener,
    mut make_conn: F,
) where
    F: FnMut(tokio_uring::net::TcpStream) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let fut = make_conn(sock);
                tokio_uring::spawn(fut);
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_is_tokio_off_linux_feature() {
        // This box isn't Linux+feature, so the selector must report Tokio.
        assert_eq!(current(), IoEngine::Tokio);
    }

    #[tokio::test]
    async fn tokio_accept_loop_serves_one_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(accept_loop_tokio(listener, |mut sock| async move {
            let mut buf = [0u8; 4];
            let _ = sock.read_exact(&mut buf).await;
            let _ = sock.write_all(b"pong").await;
        }));

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    }
}
