#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The untrusted network entry point: never panics, returns None on a
    // partial frame and Err on malformed input. On a fully-decoded packet we
    // also exercise the encoder to keep decode/encode symmetric under fuzzing.
    if let Ok(Some((pkt, consumed))) = synapse_adapter_mqtt::parse(data) {
        let mut buf = Vec::new();
        synapse_adapter_mqtt::encode_packet(&pkt, synapse_adapter_mqtt::ProtocolVersion::V311, &mut buf);
        debug_assert!(buf.len() >= consumed || consumed == 0);
    }
});
