// Command server starts a single node in the distributed Raft KV cluster.
// For local testing, use the cluster package to spin up a 3 or 5 node cluster
// within a single process using the MemoryTransport.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nitaiz123/raft-kv-store/internal/api"
	"github.com/Nitaiz123/raft-kv-store/internal/raft"
	"github.com/Nitaiz123/raft-kv-store/internal/store"
	"github.com/Nitaiz123/raft-kv-store/internal/transport"
)

func main() {
	nodeID := flag.Int("id", 0, "Node ID (unique integer in the cluster)")
	httpAddr := flag.String("http", ":8080", "HTTP API listen address")
	clusterSize := flag.Int("cluster", 3, "Total number of nodes in the cluster (for local simulation)")
	flag.Parse()

	fmt.Printf("=== Raft KV Store — Node %d ===\n", *nodeID)
	fmt.Printf("Cluster size: %d nodes\n", *clusterSize)
	fmt.Printf("HTTP API: %s\n\n", *httpAddr)

	// Build peer list (all nodes except self)
	peers := make([]int, 0, *clusterSize-1)
	for i := 0; i < *clusterSize; i++ {
		if i != *nodeID {
			peers = append(peers, i)
		}
	}

	// Create in-memory network (for local multi-node simulation)
	// In production, replace with a gRPC transport implementation
	network := transport.NewMemoryNetwork()

	cfg := raft.Config{
		ID:              *nodeID,
		Peers:           peers,
		ElectionTimeout: 150 * time.Millisecond,
		HeartbeatTick:   50 * time.Millisecond,
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	trans := transport.NewMemoryTransport(*nodeID, network)
	node := raft.NewNode(cfg, trans, applyCh)

	// Register node in the shared network so peers can reach it
	network.Register(*nodeID, &raftAdapter{node})

	kvStore := store.NewKVStore(node, applyCh)
	apiServer := api.NewServer(*nodeID, *httpAddr, kvStore, node)

	node.Start()
	log.Printf("Raft node %d started. Waiting for leader election...", *nodeID)

	// Start HTTP API server in background
	go func() {
		if err := apiServer.Start(); err != nil {
			log.Fatalf("API server error: %v", err)
		}
	}()

	// Wait for OS signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("Shutting down node %d...", *nodeID)
	node.Stop()
}

// raftAdapter wraps raft.Node to implement transport.RaftNode interface.
type raftAdapter struct {
	node *raft.Node
}

func (a *raftAdapter) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
	a.node.RequestVote(args, reply)
}

func (a *raftAdapter) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
	a.node.AppendEntries(args, reply)
}
