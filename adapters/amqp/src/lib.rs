//! AMQP 0-9-1 "Lite" wire protocol adapter (spec.txt §3.3, §6 Phase 3).
//!
//! Turns the unified storage core + routing engine into a wire-compatible
//! AMQP broker: exchanges/bindings/queues map to the [`GraphRouter`], and
//! `basic.publish`/`consume`/`get`/`ack` drive the core [`Queue`] primitive.
//! Run it with [`server::serve`] over TCP. [`parse`] is the untrusted-input
//! entry point fuzzed by `fuzz/fuzz_targets/parse.rs`.
//!
//! Excluded by design (per TODO.md): distributed XA transactions and complex
//! message prioritization.

pub mod broker;
pub mod codec;
pub mod server;

pub use broker::{Broker, DeliverMsg, ServerEvent};
pub use codec::{
    decode_frame, encode_basic_consume_ok, encode_basic_deliver, encode_basic_get_empty,
    encode_basic_get_ok, encode_basic_qos_ok, encode_channel_close_ok, encode_channel_open_ok,
    encode_connection_close_ok, encode_connection_open_ok, encode_connection_start,
    encode_connection_tune, encode_exchange_declare_ok, encode_header, encode_method,
    encode_queue_bind_ok, encode_queue_declare_ok, parse, Frame, ProtocolError, Reader, Writer,
    CLASS_BASIC, CLASS_CHANNEL, CLASS_CONNECTION, CLASS_EXCHANGE, CLASS_QUEUE, PROTOCOL_HEADER,
    METHOD_BASIC_ACK, METHOD_BASIC_CONSUME, METHOD_BASIC_DELIVER, METHOD_BASIC_GET,
    METHOD_BASIC_PUBLISH, METHOD_CHANNEL_CLOSE, METHOD_CHANNEL_OPEN, METHOD_CONNECTION_CLOSE,
    METHOD_CONNECTION_OPEN, METHOD_CONNECTION_START, METHOD_CONNECTION_START_OK,
    METHOD_CONNECTION_TUNE, METHOD_CONNECTION_TUNE_OK, METHOD_EXCHANGE_DECLARE,
    METHOD_QUEUE_BIND, METHOD_QUEUE_DECLARE,
};
pub use server::{broker_for, serve, serve_with_listener};
