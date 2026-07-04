#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    synapse_adapter_mqtt::parse(data);
});
