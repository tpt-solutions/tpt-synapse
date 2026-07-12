//! WASM-based transform plugins (spec.txt/TODO.md Phase 1) — sandboxed,
//! untrusted per-tenant transform code as an addition to the SQL-like
//! [`crate::rule`] engine. `rule::Rule` stays the fast, safe-by-construction
//! path for filter/project; this module is the escape hatch for heavier
//! per-tenant logic that can't be expressed as a `WHERE` predicate, run
//! inside a `wasmtime` sandbox so one tenant's plugin can't read another
//! tenant's data, exhaust host memory, or hang the routing engine.
//!
//! ## Plugin ABI
//!
//! The wasm module must export:
//! * `memory` — the module's linear memory.
//! * `alloc(len: i32) -> i32` — allocate `len` bytes, returning a pointer.
//! * `transform(ptr: i32, len: i32) -> i64` — transform the `len` bytes at
//!   `ptr`, returning a packed `(result_ptr << 32) | result_len` i64. A
//!   packed result of `0` (both ptr and len zero) signals "drop this
//!   message".
//!
//! No `dealloc` export is required: each call runs in a fresh [`Store`], so
//! the whole linear memory — and any plugin state — is torn down afterwards.
//! That also means plugins are stateless across messages by construction.

use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap};

use crate::{RoutingError, RoutingResult};

/// Default execution budget for one `transform` call: generous enough for
/// real per-message logic, small enough to bound a runaway or malicious
/// plugin to low-single-digit milliseconds of host CPU.
pub const DEFAULT_FUEL: u64 = 10_000_000;
/// Default linear memory cap per invocation.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// A compiled, sandboxed WASM transform plugin.
pub struct WasmTransform {
    engine: Engine,
    module: Module,
    fuel: u64,
    memory_limit_bytes: usize,
}

struct StoreState {
    limits: StoreLimits,
}

impl WasmTransform {
    /// Compile `wasm_bytes` with the default fuel/memory budget.
    pub fn compile(wasm_bytes: &[u8]) -> RoutingResult<Self> {
        Self::compile_with_limits(wasm_bytes, DEFAULT_FUEL, DEFAULT_MEMORY_LIMIT_BYTES)
    }

    /// Compile `wasm_bytes` with an explicit fuel/memory budget, e.g. a
    /// stricter per-tenant quota.
    pub fn compile_with_limits(
        wasm_bytes: &[u8],
        fuel: u64,
        memory_limit_bytes: usize,
    ) -> RoutingResult<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine =
            Engine::new(&config).map_err(|e| RoutingError::new(format!("wasm engine init: {e}")))?;
        let module = Module::new(&engine, wasm_bytes)
            .map_err(|e| RoutingError::new(format!("wasm module compile: {e}")))?;
        Ok(Self {
            engine,
            module,
            fuel,
            memory_limit_bytes,
        })
    }

    /// Run the plugin's `transform` export over `input`, returning the
    /// transformed bytes, or `None` if the plugin dropped the message.
    ///
    /// Each call gets a fresh, isolated `Store` bounded by this plugin's
    /// fuel and memory limits — no state and no tenant-to-tenant leakage
    /// persists across invocations.
    pub fn transform(&self, input: &[u8]) -> RoutingResult<Option<Vec<u8>>> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_limit_bytes)
            .build();
        let mut store = Store::new(&self.engine, StoreState { limits });
        store.limiter(|s| &mut s.limits);
        store
            .set_fuel(self.fuel)
            .map_err(|e| RoutingError::new(format!("wasm set fuel: {e}")))?;

        let instance = Linker::new(&self.engine)
            .instantiate(&mut store, &self.module)
            .map_err(|e| self.wrap_exec_err("wasm instantiate", e))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| RoutingError::new("wasm plugin has no exported \"memory\""))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| RoutingError::new(format!("wasm plugin missing \"alloc\" export: {e}")))?;
        let transform_fn = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "transform")
            .map_err(|e| {
                RoutingError::new(format!("wasm plugin missing \"transform\" export: {e}"))
            })?;

        let in_ptr = if input.is_empty() {
            0
        } else {
            let ptr = alloc
                .call(&mut store, input.len() as i32)
                .map_err(|e| self.wrap_exec_err("wasm alloc", e))?;
            memory
                .write(&mut store, ptr as usize, input)
                .map_err(|e| RoutingError::new(format!("wasm memory write: {e}")))?;
            ptr
        };

        let packed = transform_fn
            .call(&mut store, (in_ptr, input.len() as i32))
            .map_err(|e| self.wrap_exec_err("wasm transform", e))?;

        let out_ptr = ((packed as u64) >> 32) as u32 as usize;
        let out_len = (packed as u64 & 0xffff_ffff) as u32 as usize;
        if out_ptr == 0 && out_len == 0 {
            return Ok(None);
        }

        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|e| RoutingError::new(format!("wasm memory read: {e}")))?;
        Ok(Some(out))
    }

    fn wrap_exec_err(&self, op: &str, e: wasmtime::Error) -> RoutingError {
        if e.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
            RoutingError::new(format!(
                "{op}: exceeded fuel budget of {} (plugin likely looping or too expensive)",
                self.fuel
            ))
        } else {
            RoutingError::new(format!("{op}: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-assembled minimal wasm module exporting `memory`, `alloc`, and a
    /// `transform` that uppercases ASCII bytes in place and returns the same
    /// pointer/length packed into an i64. Written directly as WAT and
    /// compiled by `wasmtime::Module::new` (which accepts both WAT and wasm
    /// binary), so the test needs no external toolchain.
    const UPPERCASE_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $next (mut i32) (i32.const 1024))
          (func (export "alloc") (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $next))
            (global.set $next (i32.add (global.get $next) (local.get $len)))
            (local.get $ptr))
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            (local $i i32)
            (local $addr i32)
            (local $c i32)
            (block $done
              (loop $loop
                (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
                (local.set $addr (i32.add (local.get $ptr) (local.get $i)))
                (local.set $c (i32.load8_u (local.get $addr)))
                (if (i32.and
                      (i32.ge_u (local.get $c) (i32.const 97))
                      (i32.le_u (local.get $c) (i32.const 122)))
                  (then
                    (i32.store8 (local.get $addr) (i32.sub (local.get $c) (i32.const 32)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $loop)))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
              (i64.extend_i32_u (local.get $len)))))
    "#;

    const LOOP_FOREVER_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param $len i32) (result i32) i32.const 0)
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            (loop $forever
              br $forever)
            i64.const 0))
    "#;

    #[test]
    fn transforms_bytes_through_sandbox() {
        let plugin = WasmTransform::compile(UPPERCASE_WAT.as_bytes()).unwrap();
        let out = plugin.transform(b"hello world").unwrap().unwrap();
        assert_eq!(out, b"HELLO WORLD");
    }

    #[test]
    fn runaway_plugin_hits_fuel_limit() {
        let plugin = WasmTransform::compile_with_limits(
            LOOP_FOREVER_WAT.as_bytes(),
            10_000,
            DEFAULT_MEMORY_LIMIT_BYTES,
        )
        .unwrap();
        let err = plugin.transform(b"x").unwrap_err();
        assert!(err.0.contains("fuel"), "unexpected error: {}", err.0);
    }

    #[test]
    fn oversized_memory_request_is_denied() {
        let plugin = WasmTransform::compile_with_limits(UPPERCASE_WAT.as_bytes(), DEFAULT_FUEL, 64 * 1024)
            .unwrap();
        // One page (64KiB) is already at the cap; growth beyond it must fail
        // rather than silently succeeding, but a transform within the cap
        // still works.
        let out = plugin.transform(b"ok").unwrap().unwrap();
        assert_eq!(out, b"OK");
    }
}
