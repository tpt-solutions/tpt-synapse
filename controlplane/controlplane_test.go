package controlplane

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClusterMembershipAndLeadership(t *testing.T) {
	c := NewCluster("test")
	if got := c.Leader(); got != "" {
		t.Fatalf("empty cluster should have no leader, got %q", got)
	}

	n1, added := c.AddNode("n1", "127.0.0.1:9001")
	if !added {
		t.Fatal("first node should be newly added")
	}
	if n1.Role != RoleLeader {
		t.Fatalf("first node should be leader, got %q", n1.Role)
	}
	if c.Leader() != "n1" {
		t.Fatalf("leader should be n1, got %q", c.Leader())
	}

	c.AddNode("n2", "127.0.0.1:9002")
	c.AddNode("n3", "127.0.0.1:9003")
	if len(c.Nodes()) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(c.Nodes()))
	}

	// Removing the leader forces a new election.
	if !c.RemoveNode("n1") {
		t.Fatal("RemoveNode(n1) should succeed")
	}
	if c.Leader() == "n1" {
		t.Fatal("leader should have changed after removing n1")
	}
	if c.Leader() == "" {
		t.Fatal("a new leader should have been elected")
	}
	if len(c.Nodes()) != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", len(c.Nodes()))
	}
}

func TestStepElectionBumpsTerm(t *testing.T) {
	c := NewCluster("test")
	c.AddNode("n1", "a:1")
	c.AddNode("n2", "b:2")
	before := c.Term()
	c.StepElection()
	if c.Term() != before+1 {
		t.Fatalf("term should bump by 1, before=%d after=%d", before, c.Term())
	}
	if c.Leader() == "" {
		t.Fatal("election should produce a leader")
	}
}

func TestHandlerEndpoints(t *testing.T) {
	c := NewCluster("api")
	srv := httptest.NewServer(c.Handler())
	defer srv.Close()

	// Register a node.
	body, _ := json.Marshal(Node{ID: "n1", Addr: "127.0.0.1:9001"})
	resp, err := http.Post(srv.URL+"/nodes", "application/json", jsonReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Status reflects the node and a leader.
	st := getStatus(t, srv.URL+"/cluster")
	if len(st.Nodes) != 1 {
		t.Fatalf("expected 1 node in status, got %d", len(st.Nodes))
	}
	if st.Leader != "n1" {
		t.Fatalf("expected leader n1, got %q", st.Leader)
	}

	// Deregister.
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/nodes?id=n1", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(getStatus(t, srv.URL+"/cluster").Nodes) != 0 {
		t.Fatal("node should be removed")
	}
}

func getStatus(t *testing.T, url string) Status {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func jsonReader(s string) io.Reader {
	return strings.NewReader(s)
}
