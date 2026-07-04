// Command synapsectl is the tpt-synapse CLI: cluster admin, topic/queue/key
// inspection, and control-plane operations against a running broker.
//
// Empty scaffold for Phase 0 — real subcommands land alongside the
// controlplane package in Phase 4.
package main

import (
	"fmt"
	"os"

	"tpt-synapse/controlplane"
)

func main() {
	fmt.Printf("synapsectl %s (control plane not yet implemented)\n", controlplane.Version)
	os.Exit(0)
}
