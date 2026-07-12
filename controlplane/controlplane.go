// Package controlplane implements the Go-based control plane: cluster
// membership, leader election, and the HTTP API the data-plane adapters and
// synapsectl talk to (spec.txt §6 Phase 4).
//
// The actual log replication / HA is performed by the Rust core using the
// openraft library (see core/src/consensus.rs); this package is the
// coordination and membership authority that the data plane registers with and
// queries for the current leader. Keeping membership + election here (in Go)
// and replication in Rust mirrors the Phase 0 decision to keep the control
// plane in Go while the storage engine stays in Rust.
package controlplane

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NodeRole is the Raft-like role of a cluster member.
type NodeRole string

const (
	RoleLeader   NodeRole = "leader"
	RoleFollower NodeRole = "follower"
	RoleCandidate NodeRole = "candidate"
)

// Node is one member of the cluster (a data-plane broker instance).
type Node struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Role     NodeRole  `json:"role"`
	LastSeen time.Time `json:"last_seen"`
}

// Cluster is the in-memory, concurrency-safe membership + election state.
type Cluster struct {
	mu      sync.RWMutex
	name    string
	nodes   map[string]*Node
	term    uint64
	leader  string
	changed time.Time
}

// NewCluster creates an empty cluster with the given name.
func NewCluster(name string) *Cluster {
	return &Cluster{
		name:    name,
		nodes:   make(map[string]*Node),
		term:    1,
		changed: time.Now(),
	}
}

// AddNode registers a node; if it is the first node it becomes leader. Returns
// the registered node (with its role) and whether it was newly added.
func (c *Cluster) AddNode(id, addr string) (*Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[id]; ok {
		n.Addr = addr
		n.LastSeen = time.Now()
		return n, false
	}
	role := RoleFollower
	if len(c.nodes) == 0 {
		role = RoleLeader
		c.leader = id
	}
	n := &Node{ID: id, Addr: addr, Role: role, LastSeen: time.Now()}
	c.nodes[id] = n
	c.changed = time.Now()
	return n, true
}

// RemoveNode drops a node from the cluster, stepping a new election if the
// leader left.
func (c *Cluster) RemoveNode(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nodes[id]; !ok {
		return false
	}
	delete(c.nodes, id)
	if c.leader == id {
		c.leader = ""
	}
	c.changed = time.Now()
	if c.leader == "" {
		c.stepElectionLocked()
	}
	return true
}

// Nodes returns a sorted snapshot of cluster members.
func (c *Cluster) Nodes() []Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Leader returns the current leader node id (may be empty during election).
func (c *Cluster) Leader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

// Term returns the current election term.
func (c *Cluster) Term() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.term
}

// StepElection bumps the term and elects a leader (deterministically the
// lowest-id node, standing in for a real Raft vote tally). Returns the new
// leader id.
func (c *Cluster) StepElection() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.nodes) == 0 {
		return ""
	}
	c.term++
	return c.stepElectionLocked()
}

func (c *Cluster) stepElectionLocked() string {
	if len(c.nodes) == 0 {
		c.leader = ""
		return ""
	}
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	newLeader := ids[0]
	c.leader = newLeader
	for id, n := range c.nodes {
		if id == newLeader {
			n.Role = RoleLeader
		} else {
			n.Role = RoleFollower
		}
	}
	c.changed = time.Now()
	return newLeader
}

// Status is the JSON shape returned by the cluster status endpoint.
type Status struct {
	Name      string    `json:"name"`
	Term      uint64    `json:"term"`
	Leader    string    `json:"leader"`
	Nodes     []Node    `json:"nodes"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Status returns a snapshot of the whole cluster.
func (c *Cluster) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := Status{
		Name:      c.name,
		Term:      c.term,
		Leader:    c.leader,
		Nodes:     make([]Node, 0, len(c.nodes)),
		UpdatedAt: c.changed,
	}
	for _, n := range c.nodes {
		snap.Nodes = append(snap.Nodes, *n)
	}
	sort.Slice(snap.Nodes, func(i, j int) bool { return snap.Nodes[i].ID < snap.Nodes[j].ID })
	return snap
}

// Handler exposes the control-plane HTTP API:
//
//	GET  /cluster            -> cluster status
//	GET  /nodes              -> member list
//	POST /nodes  {id,addr}    -> register a node
//	DELETE /nodes/{id}        -> deregister a node
func (c *Cluster) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, c.Status())
	})
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, c.Nodes())
		case http.MethodPost:
			var n Node
			if err := json.NewDecoder(r.Body).Decode(&n); err != nil || n.ID == "" {
				http.Error(w, "invalid node body", http.StatusBadRequest)
				return
			}
			registered, _ := c.AddNode(n.ID, n.Addr)
			writeJSON(w, http.StatusCreated, registered)
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, "missing id", http.StatusBadRequest)
				return
			}
			ok := c.RemoveNode(id)
			writeJSON(w, http.StatusOK, map[string]bool{"removed": ok})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintf(w, `{"error":%q}`, err.Error())
	}
}

// Version is the control plane's own version, independent of the data-plane
// (Rust core) version.
const Version = "0.2.0"
