//! `synapse-broker`: single-process entrypoint that boots every protocol
//! adapter (MQTT, Kafka, AMQP, RESP, native) against one shared
//! [`SynapseCore`], plus the admin API that backs Synapse Studio and the
//! Prometheus `/metrics` endpoint (TODO.md "Adoption & Tooling") — the binary
//! a container image and the Helm chart actually deploy.
//!
//! Each listener is independently configurable via a `SYNAPSE_<NAME>_ADDR`
//! env var and can be disabled by setting that var to `off`. Clustering
//! (the embedded Raft transport in `synapse_core::transport`) is not wired
//! into this binary yet; each instance runs standalone.

use std::sync::Arc;

use synapse_adapter_native::native_broker;
use synapse_adapter_native::{Codec, KeyRing, SALT_LEN};
use synapse_core::http::spawn_metrics_server;
use synapse_core::metrics::Metrics;
use synapse_core::{spawn_admin_server, SynapseCore};

/// Resolve a listener address from `env`, falling back to `default`, or
/// `None` if the operator explicitly set `env=off`.
fn addr(env: &str, default: &str) -> Option<String> {
    match std::env::var(env) {
        Ok(v) if v.eq_ignore_ascii_case("off") => None,
        Ok(v) => Some(v),
        Err(_) => Some(default.to_string()),
    }
}

#[tokio::main]
async fn main() {
    let core = Arc::new(SynapseCore::new());
    let metrics = Arc::new(Metrics::new());
    let mut listening = Vec::new();

    if let Some(a) = addr("SYNAPSE_MQTT_ADDR", "0.0.0.0:1883") {
        let broker = Arc::new(synapse_adapter_mqtt::Broker::new(core.clone()));
        tokio::spawn(async move {
            if let Err(e) = synapse_adapter_mqtt::serve(broker, a.clone()).await {
                eprintln!("mqtt server on {a} exited: {e}");
            }
        });
        listening.push("mqtt");
    }

    if let Some(a) = addr("SYNAPSE_KAFKA_ADDR", "0.0.0.0:9092") {
        let broker = Arc::new(synapse_adapter_kafka::Broker::new(core.clone()));
        tokio::spawn(async move {
            if let Err(e) = synapse_adapter_kafka::serve(broker, a.clone()).await {
                eprintln!("kafka server on {a} exited: {e}");
            }
        });
        listening.push("kafka");
    }

    if let Some(a) = addr("SYNAPSE_AMQP_ADDR", "0.0.0.0:5672") {
        let broker = Arc::new(synapse_adapter_amqp::Broker::new(core.clone()));
        tokio::spawn(async move {
            if let Err(e) = synapse_adapter_amqp::serve(broker, a.clone()).await {
                eprintln!("amqp server on {a} exited: {e}");
            }
        });
        listening.push("amqp");
    }

    if let Some(a) = addr("SYNAPSE_RESP_ADDR", "0.0.0.0:6379") {
        let broker = Arc::new(synapse_adapter_resp::RespBroker::new(core.clone()));
        tokio::spawn(async move {
            if let Err(e) = synapse_adapter_resp::serve(broker, a.clone()).await {
                eprintln!("resp server on {a} exited: {e}");
            }
        });
        listening.push("resp");
    }

    if let Some(a) = addr("SYNAPSE_NATIVE_ADDR", "0.0.0.0:7900") {
        spawn_native(a, core.clone());
        listening.push("native");
    }

    if let Some(a) = addr("SYNAPSE_ADMIN_ADDR", "0.0.0.0:9091") {
        let socket_addr = a.parse().unwrap_or_else(|e| {
            panic!("invalid SYNAPSE_ADMIN_ADDR {a:?}: {e}");
        });
        let (bound, _handle) = spawn_admin_server(socket_addr, core.clone(), metrics.clone())
            .await
            .expect("bind admin listener");
        println!("synapse-broker: admin API listening on {bound}");
        listening.push("admin");
    }

    if let Some(a) = addr("SYNAPSE_METRICS_ADDR", "0.0.0.0:9090") {
        let socket_addr = a.parse().unwrap_or_else(|e| {
            panic!("invalid SYNAPSE_METRICS_ADDR {a:?}: {e}");
        });
        let (bound, _handle) = spawn_metrics_server(socket_addr, metrics.clone())
            .await
            .expect("bind metrics listener");
        println!("synapse-broker: metrics listening on {bound}");
        listening.push("metrics");
    }

    if listening.is_empty() {
        eprintln!("synapse-broker: every listener disabled via SYNAPSE_*_ADDR=off, exiting");
        return;
    }
    println!("synapse-broker: running with {listening:?}");

    // Adapters run in detached tasks and serve forever; the process exits on
    // the container runtime's SIGTERM/SIGINT (default Rust disposition is to
    // terminate), so there is nothing further for `main` to drive.
    std::future::pending::<()>().await;
}

/// Bind the native-protocol listener and spawn one `native_broker::serve`
/// task per connection. Every frame is AEAD-encrypted by design (see
/// `adapters/native`'s module docs), so a key must exist before any client
/// can connect: `SYNAPSE_NATIVE_KEY` (32 bytes, base64) if set, otherwise an
/// ephemeral random key generated at boot and logged as a warning — fine for
/// local evaluation, not for a real deployment where the key must be
/// provisioned out of band to clients.
fn spawn_native(addr: String, core: Arc<SynapseCore>) {
    use rand::RngCore;

    let mut keyring = KeyRing::new();
    let key: [u8; 32] = match std::env::var("SYNAPSE_NATIVE_KEY") {
        Ok(b64) => {
            let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                .expect("SYNAPSE_NATIVE_KEY must be valid base64");
            bytes
                .try_into()
                .unwrap_or_else(|v: Vec<u8>| panic!("SYNAPSE_NATIVE_KEY must decode to 32 bytes, got {}", v.len()))
        }
        Err(_) => {
            eprintln!(
                "synapse-broker: SYNAPSE_NATIVE_KEY not set, generating an ephemeral key \
                 (fine for local evaluation; provision a real key for production)"
            );
            let mut k = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut k);
            k
        }
    };
    keyring.insert(0, key);
    let mut salt = [0u8; SALT_LEN];
    rand::thread_rng().fill_bytes(&mut salt);

    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("native server on {addr} failed to bind: {e}");
                return;
            }
        };
        loop {
            match listener.accept().await {
                Ok((sock, _peer)) => {
                    let codec = Codec::new(salt, keyring.clone());
                    let core = core.clone();
                    tokio::spawn(async move {
                        let _ = native_broker::serve(sock, codec, core, 0).await;
                    });
                }
                Err(e) => {
                    eprintln!("native server on {addr} accept error: {e}");
                    break;
                }
            }
        }
    });
}
