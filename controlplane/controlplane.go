// Package controlplane implements the Go-based control plane: embedded Raft
// coordination for multi-node HA and cluster membership (spec.txt §6 Phase 4).
//
// Empty scaffold for Phase 0 — the control plane lands in Phase 4.
package controlplane

// Version is the control plane's own version, independent of the data-plane
// (Rust core) version.
const Version = "0.1.0"
