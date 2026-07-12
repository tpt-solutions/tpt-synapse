// Command synapsectl is the tpt-synapse CLI: cluster admin, membership, and
// control-plane operations against a running control plane (spec.txt §6
// Phase 4).
//
// Usage:
//
//	synapsectl cluster status
//	synapsectl cluster members
//	synapsectl cluster join <id> <addr>
//	synapsectl cluster leave <id>
//	synapsectl node info
//
// The control plane address defaults to http://127.0.0.1:8080 and can be
// overridden with SYNAPSE_CP_ADDR.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"tpt-synapse/controlplane"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	cpAddr := os.Getenv("SYNAPSE_CP_ADDR")
	if cpAddr == "" {
		cpAddr = "http://127.0.0.1:8080"
	}
	client := &http.Client{}

	switch args[0] {
	case "cluster":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		switch args[1] {
		case "status":
			getJSON(client, cpAddr+"/cluster")
		case "members":
			getJSON(client, cpAddr+"/nodes")
		case "join":
			if len(args) < 4 {
				fmt.Fprintln(os.Stderr, "usage: synapsectl cluster join <id> <addr>")
				os.Exit(2)
			}
			joinNode(client, cpAddr, args[2], args[3])
		case "leave":
			if len(args) < 3 {
				fmt.Fprintln(os.Stderr, "usage: synapsectl cluster leave <id>")
				os.Exit(2)
			}
			leaveNode(client, cpAddr, args[2])
		default:
			usage()
			os.Exit(2)
		}
	case "node":
		if len(args) >= 2 && args[1] == "info" {
			fmt.Printf("synapsectl %s (control plane %s)\n", controlplane.Version, controlplane.Version)
			return
		}
		usage()
		os.Exit(2)
	default:
		usage()
		os.Exit(2)
	}
}

func getJSON(client *http.Client, url string) {
	resp, err := client.Get(url)
	if err != nil {
		fail(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		fail(fmt.Errorf("control plane returned %s: %s", resp.Status, strings.TrimSpace(string(body))))
	}
	fmt.Println(string(body))
}

func joinNode(client *http.Client, cpAddr, id, addr string) {
	payload, _ := json.Marshal(map[string]string{"id": id, "addr": addr})
	resp, err := client.Post(cpAddr+"/nodes", "application/json", strings.NewReader(string(payload)))
	if err != nil {
		fail(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		fail(fmt.Errorf("join failed (%s): %s", resp.Status, strings.TrimSpace(string(body))))
	}
	fmt.Printf("joined %s -> %s\n", id, addr)
	fmt.Println(string(body))
}

func leaveNode(client *http.Client, cpAddr, id string) {
	req, err := http.NewRequest(http.MethodDelete, cpAddr+"/nodes?id="+id, nil)
	if err != nil {
		fail(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		fail(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		fail(fmt.Errorf("leave failed (%s): %s", resp.Status, strings.TrimSpace(string(body))))
	}
	fmt.Printf("left %s\n", id)
	fmt.Println(string(body))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, `synapsectl — tpt-synapse control plane CLI

Usage:
  synapsectl cluster status
  synapsectl cluster members
  synapsectl cluster join <id> <addr>
  synapsectl cluster leave <id>
  synapsectl node info

Env:
  SYNAPSE_CP_ADDR   control plane base URL (default http://127.0.0.1:8080)`)
}
