#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The untrusted network entry point: never panics, returns None on a
    // partial frame and Err on malformed input. On a fully-decoded frame we
    // also exercise the property-section skipper to keep decode/parse
    // symmetric under fuzzing.
    if let Ok(Some((frame, _consumed))) = synapse_adapter_amqp::parse(data) {
        if let synapse_adapter_amqp::Frame::Header { properties, .. } = &frame {
            let mut r = synapse_adapter_amqp::Reader::new(properties);
            let _ = synapse_adapter_amqp::skip_properties(&mut r);
        }
    }
});
