//! TLS / mTLS termination shared by the protocol adapters and the admin API
//! (TODO.md `tpt-identity` integration: mTLS across all protocol adapters).
//!
//! `TlsAcceptor` builds a rustls server config from PEM files. With a client CA
//! supplied it enforces mutual TLS (clients must present a certificate chained
//! to that CA); without one it terminates plain server TLS. The same acceptor
//! is reused by every adapter's accept loop ([`crate::io_uring::accept_loop_tls`])
//! and the admin server, so enabling mTLS is a single config decision per
//! listener rather than per-protocol code.

use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor as TokioTlsAcceptor;

use crate::error::{EngineError, EngineResult};

/// A configured TLS acceptor. Wrap an accepted [`tokio::net::TcpStream`] with
/// [`TlsAcceptor::accept`] to get a [`tokio_rustls::server::TlsStream`].
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: TokioTlsAcceptor,
}

impl TlsAcceptor {
    /// Build an acceptor from PEM-encoded `cert` (server chain) and `key`
    /// (server private key). When `client_ca` is `Some`, clients must present a
    /// certificate signed by that CA (mutual TLS).
    pub fn from_pem_files(
        cert_path: &str,
        key_path: &str,
        client_ca_path: Option<&str>,
    ) -> EngineResult<Self> {
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;

        let config = match client_ca_path {
            Some(ca) => {
                let ca_certs = load_certs(ca)?;
                let mut roots = RootCertStore::empty();
                for c in ca_certs {
                    roots.add(c).map_err(|e| {
                        EngineError::internal(format!("add client CA to root store: {e}"))
                    })?;
                }
                let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|e| EngineError::internal(format!("client verifier: {e}")))?;
                ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| EngineError::internal(format!("tls config: {e}")))?
            }
            None => ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| EngineError::internal(format!("tls config: {e}")))?,
        };

        Ok(Self {
            inner: TokioTlsAcceptor::from(Arc::new(config)),
        })
    }

    /// Perform the TLS handshake on an accepted TCP stream.
    pub async fn accept(
        &self,
        stream: tokio::net::TcpStream,
    ) -> EngineResult<tokio_rustls::server::TlsStream<tokio::net::TcpStream>> {
        self.inner
            .accept(stream)
            .await
            .map_err(|e| EngineError::internal(format!("tls handshake: {e}")))
    }
}

fn load_certs(path: &str) -> EngineResult<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| EngineError::internal(format!("open cert {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| EngineError::internal(format!("parse cert {path}: {e}")))
}

fn load_key(path: &str) -> EngineResult<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .map_err(|e| EngineError::internal(format!("open key {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    private_key(&mut reader)
        .map_err(|e| EngineError::internal(format!("parse key {path}: {e}")))?
        .ok_or_else(|| EngineError::internal(format!("no private key in {path}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // No certs ship in-repo; verify the loader rejects a missing file rather
    // than panicking, so misconfiguration fails loud and early.
    #[test]
    fn missing_cert_is_error() {
        let r = TlsAcceptor::from_pem_files("/no/such/cert.pem", "/no/such/key.pem", None);
        assert!(r.is_err());
    }
}
