// Package transport provides RPC transport implementations for Raft nodes.
// The MemoryTransport is used for unit testing and local cluster simulation.
// For production, replace with a gRPC-based implementation.
package transport

import (
	"fmt"
	"sync"

	"github.com/Nitaiz123/raft-kv-store/internal/raft"
)

// RaftNode is the interface that a Raft node must implement to receive RPCs.
type RaftNode interface {
	RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply)
	AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply)
}

// MemoryTransport implements raft.Transport using direct in-process function calls.
// It simulates network communication within a single process, making it ideal
// for integration tests and benchmarks without network overhead.
type MemoryTransport struct {
	mu      sync.RWMutex
	nodeID  int
	network *MemoryNetwork
}

// MemoryNetwork is a shared registry of all in-process Raft nodes.
// All nodes in the simulated cluster register themselves here.
type MemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[int]RaftNode

	// Fault injection: set to true to simulate network partition for a node
	partitioned map[int]bool
}

// NewMemoryNetwork creates a new in-memory network for cluster simulation.
func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		nodes:       make(map[int]RaftNode),
		partitioned: make(map[int]bool),
	}
}

// Register adds a node to the network so others can send it RPCs.
func (net *MemoryNetwork) Register(id int, node RaftNode) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
}

// Partition simulates a network partition for the given node ID.
// Partitioned nodes cannot send or receive RPCs.
func (net *MemoryNetwork) Partition(id int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.partitioned[id] = true
}

// Heal removes a simulated network partition for the given node ID.
func (net *MemoryNetwork) Heal(id int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.partitioned, id)
}

// NewMemoryTransport creates a transport for a specific node within the network.
func NewMemoryTransport(nodeID int, network *MemoryNetwork) *MemoryTransport {
	return &MemoryTransport{nodeID: nodeID, network: network}
}

// SendRequestVote delivers a RequestVote RPC to the target peer.
func (t *MemoryTransport) SendRequestVote(peerID int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	t.network.mu.RLock()
	partitioned := t.network.partitioned[t.nodeID] || t.network.partitioned[peerID]
	peer, ok := t.network.nodes[peerID]
	t.network.mu.RUnlock()

	if partitioned || !ok {
		return nil, fmt.Errorf("node %d: cannot reach peer %d (partitioned or not registered)", t.nodeID, peerID)
	}

	reply := &raft.RequestVoteReply{}
	peer.RequestVote(args, reply)
	return reply, nil
}

// SendAppendEntries delivers an AppendEntries RPC to the target peer.
func (t *MemoryTransport) SendAppendEntries(peerID int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	t.network.mu.RLock()
	partitioned := t.network.partitioned[t.nodeID] || t.network.partitioned[peerID]
	peer, ok := t.network.nodes[peerID]
	t.network.mu.RUnlock()

	if partitioned || !ok {
		return nil, fmt.Errorf("node %d: cannot reach peer %d (partitioned or not registered)", t.nodeID, peerID)
	}

	reply := &raft.AppendEntriesReply{}
	peer.AppendEntries(args, reply)
	return reply, nil
}
