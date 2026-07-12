//! Shared backpressure signal (TODO.md Phase 1).
//!
//! One normalized pressure level (0 = idle, 100 = saturated) that every
//! adapter reads and translates into its own native flow-control primitive.
//! This is the *single internal representation* the TODO.md calls for so MQTT
//! inflight windows, Kafka fetch/produce quotas, and AMQP prefetch/credit all
//! share one source of truth.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;

/// Normalized backpressure level, clamped to `0..=100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pressure(u8);

impl Pressure {
    pub fn level(&self) -> u8 {
        self.0
    }

    pub fn is_saturated(&self) -> bool {
        self.0 >= 100
    }

    pub fn from_level(v: u8) -> Self {
        Pressure(v.min(100))
    }
}

impl Default for Pressure {
    fn default() -> Self {
        Pressure(0)
    }
}

/// A single shared backpressure signal plus per-adapter capacity hints.
#[derive(Debug)]
pub struct Backpressure {
    level: AtomicU8,
    adapters: Mutex<HashMap<String, u32>>,
    throttle_at: u8,
}

impl Backpressure {
    pub fn new() -> Self {
        Self {
            level: AtomicU8::new(0),
            adapters: Mutex::new(HashMap::new()),
            throttle_at: 90,
        }
    }

    /// Register an adapter and its nominal max in-flight capacity, so the
    /// per-protocol mappers below can scale proportionally to pressure.
    pub fn register_adapter(&self, name: &str, max_inflight: u32) {
        self.adapters
            .lock()
            .unwrap()
            .insert(name.to_string(), max_inflight);
    }

    /// Update the normalized pressure level (typically from an engine gauge
    /// like queue depth or producer lag).
    pub fn set_level(&self, level: u8) {
        self.level.store(level.min(100), Ordering::SeqCst);
    }

    pub fn current(&self) -> Pressure {
        Pressure::from_level(self.level.load(Ordering::SeqCst))
    }

    /// Whether a new operation should be admitted. Above `throttle_at` we
    /// throttle; at 100 we reject.
    pub fn admit(&self) -> bool {
        let lvl = self.level.load(Ordering::SeqCst);
        lvl < 100 && lvl < self.throttle_at
    }

    /// Translate the normalized level into a concrete MQTT subscribe inflight
    /// window: full capacity at zero pressure, shrinking linearly to 1 at
    /// saturation.
    pub fn mqtt_inflight_window(&self, subscriber_max: u16) -> u16 {
        let lvl = self.level.load(Ordering::SeqCst) as f32 / 100.0;
        let scaled = (subscriber_max as f32 * (1.0 - lvl)).max(1.0);
        scaled as u16
    }

    /// Translate into a Kafka produce/fetch quota (messages per permit window).
    pub fn kafka_quota(&self, full: u32) -> u32 {
        let lvl = self.level.load(Ordering::SeqCst) as f32 / 100.0;
        ((full as f32 * (1.0 - lvl)).max(0.0)) as u32
    }

    /// Translate into an AMQP prefetch credit.
    pub fn amqp_credit(&self, full: u16) -> u16 {
        let lvl = self.level.load(Ordering::SeqCst) as f32 / 100.0;
        ((full as f32 * (1.0 - lvl)).max(1.0)) as u16
    }
}

impl Default for Backpressure {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_scales_protocols() {
        let bp = Backpressure::new();
        bp.register_adapter("mqtt", 16);
        assert_eq!(bp.mqtt_inflight_window(16), 16);
        assert_eq!(bp.kafka_quota(1000), 1000);
        assert_eq!(bp.amqp_credit(100), 100);

        bp.set_level(50);
        assert_eq!(bp.mqtt_inflight_window(16), 8);
        assert_eq!(bp.kafka_quota(1000), 500);

        bp.set_level(100);
        assert_eq!(bp.mqtt_inflight_window(16), 1); // never below 1
        assert!(!bp.admit());
    }
}
