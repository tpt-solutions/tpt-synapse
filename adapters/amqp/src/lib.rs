//! AMQP 0-9-1 "Lite" adapter: Exchanges/Bindings/Queues → Graph Router and
//! Queue primitive (spec.txt §6 Phase 3). XA transactions and complex message
//! prioritization are explicitly out of scope.
//!
//! `parse` is the untrusted-input entry point fuzzed by `fuzz/fuzz_targets/parse.rs`.
//! It's a no-op stub until the real frame parser lands in Phase 3.

pub fn parse(_input: &[u8]) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_does_not_panic_on_empty_input() {
        parse(&[]);
    }
}
