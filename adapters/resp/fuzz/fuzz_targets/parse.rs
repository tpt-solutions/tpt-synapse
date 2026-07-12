#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The untrusted network entry point: never panics, returns None on a
    // partial frame and Err on malformed input. On a fully-decoded value we
    // also exercise the encoder to keep decode/encode symmetric under fuzzing.
    if let Ok(Some((val, consumed))) = synapse_adapter_resp::parse(data) {
        let mut buf = Vec::new();
        synapse_adapter_resp::encode_value(&val, &mut buf);
        debug_assert!(buf.len() >= consumed || consumed == 0);
    }
});
