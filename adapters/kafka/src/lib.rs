//! Kafka wire protocol adapter: produce/fetch → Log writes/reads (spec.txt §6 Phase 3).
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
