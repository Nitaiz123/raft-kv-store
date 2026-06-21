package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Nitaiz123/raft-kv-store/internal/raft"
	"github.com/Nitaiz123/raft-kv-store/internal/transport"
)

// cluster is a helper that manages a set of Raft nodes for testing.
type cluster struct {
	nodes    []*raft.Node
	network  *transport.MemoryNetwork
	applyChans []chan raft.ApplyMsg
}

// newCluster creates a fully connected Raft cluster of the given size.
func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	network := transport.NewMemoryNetwork()
	nodes := make([]*raft.Node, size)
	applyChans := make([]chan raft.ApplyMsg, size)

	for i := 0; i < size; i++ {
		peers := make([]int, 0, size-1)
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		cfg := raft.Config{
			ID:              i,
			Peers:           peers,
			ElectionTimeout: 100 * time.Millisecond,
			HeartbeatTick:   30 * time.Millisecond,
		}
		applyChans[i] = make(chan raft.ApplyMsg, 256)
		trans := transport.NewMemoryTransport(i, network)
		nodes[i] = raft.NewNode(cfg, trans, applyChans[i])
	}

	// Register all nodes before starting (so RPCs can be delivered)
	for i, node := range nodes {
		network.Register(i, &nodeAdapter{node})
	}

	for _, node := range nodes {
		node.Start()
	}

	return &cluster{nodes: nodes, network: network, applyChans: applyChans}
}

func (c *cluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

// waitForLeader polls until a single leader is elected or the deadline passes.
func (c *cluster) waitForLeader(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaderCount := 0
		leaderID := -1
		for i, node := range c.nodes {
			_, isLeader := node.GetState()
			if isLeader {
				leaderCount++
				leaderID = i
			}
		}
		if leaderCount == 1 {
			return leaderID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %v", timeout)
	return -1
}

// nodeAdapter adapts raft.Node to transport.RaftNode.
type nodeAdapter struct{ node *raft.Node }

func (a *nodeAdapter) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
	a.node.RequestVote(args, reply)
}
func (a *nodeAdapter) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
	a.node.AppendEntries(args, reply)
}

// ---- Tests ----

// TestLeaderElection verifies that a 3-node cluster elects exactly one leader.
func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(t, 2*time.Second)
	t.Logf("Leader elected: node %d", leaderID)

	// Verify exactly one leader
	leaderCount := 0
	for _, node := range c.nodes {
		_, isLeader := node.GetState()
		if isLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected 1 leader, got %d", leaderCount)
	}
}

// TestLogReplication verifies that a command proposed to the leader is
// replicated to all followers.
func TestLogReplication(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(t, 2*time.Second)
	leader := c.nodes[leaderID]

	// Propose a command
	cmd := []byte(`{"op":"PUT","key":"hello","value":"world"}`)
	index, term, isLeader := leader.Propose(cmd)
	if !isLeader {
		t.Fatal("expected leader to accept proposal")
	}
	t.Logf("Proposed at index=%d term=%d", index, term)

	// Wait for the command to be applied on all nodes
	deadline := time.Now().Add(2 * time.Second)
	applied := make([]bool, len(c.nodes))
	for time.Now().Before(deadline) {
		allApplied := true
		for i, ch := range c.applyChans {
			if applied[i] {
				continue
			}
			select {
			case msg := <-ch:
				if msg.CommandIndex == index {
					applied[i] = true
					t.Logf("Node %d applied index %d", i, index)
				}
			default:
				allApplied = false
			}
		}
		if allApplied {
			t.Log("All nodes applied the command — replication successful")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("not all nodes applied the command within deadline")
}

// TestLeaderFailover verifies that the cluster elects a new leader after
// the current leader is partitioned from the network.
func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(t, 2*time.Second)
	t.Logf("Initial leader: node %d", leaderID)

	// Partition the leader
	c.network.Partition(leaderID)
	t.Logf("Partitioned node %d", leaderID)

	// Wait for a new leader among the remaining nodes
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, node := range c.nodes {
			if i == leaderID {
				continue
			}
			_, isLeader := node.GetState()
			if isLeader {
				t.Logf("New leader elected: node %d", i)
				c.network.Heal(leaderID)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("no new leader elected after partitioning the original leader")
}

// TestFiveNodeCluster verifies leader election in a 5-node cluster.
func TestFiveNodeCluster(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leaderID := c.waitForLeader(t, 3*time.Second)
	t.Logf("5-node cluster leader: node %d", leaderID)

	// Propose multiple commands
	leader := c.nodes[leaderID]
	for i := 0; i < 10; i++ {
		cmd := []byte(fmt.Sprintf(`{"op":"PUT","key":"key%d","value":"val%d"}`, i, i))
		_, _, isLeader := leader.Propose(cmd)
		if !isLeader {
			t.Fatalf("node %d is no longer leader at iteration %d", leaderID, i)
		}
	}
	t.Log("All 10 commands proposed successfully")
}
