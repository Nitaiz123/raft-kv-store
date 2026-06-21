// Package store implements the replicated key-value state machine.
// It applies committed Raft log entries to an in-memory map and provides
// linearizable read/write semantics through the Raft consensus layer.
package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Nitaiz123/raft-kv-store/internal/raft"
	"github.com/Nitaiz123/raft-kv-store/pkg/logger"
)

// OpType represents the type of key-value operation.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpDelete OpType = "DELETE"
	OpGet    OpType = "GET"
)

// Command is the serialized form of a state machine operation stored in the Raft log.
type Command struct {
	Op        OpType `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	RequestID string `json:"request_id"` // Idempotency key for deduplication
	ClientID  int64  `json:"client_id"`
}

// Result holds the outcome of a committed command.
type Result struct {
	Value string
	Err   error
}

// pendingOp tracks a client request waiting for Raft commitment.
type pendingOp struct {
	index  int
	term   int
	respCh chan Result
}

// KVStore is a linearizable key-value store backed by Raft consensus.
// All writes go through Raft; reads can optionally be served locally
// (for performance) or through Raft (for strict linearizability).
type KVStore struct {
	mu sync.RWMutex

	data     map[string]string // The actual key-value state
	lastSeen map[string]string // Deduplication: clientID+requestID -> last result

	raftNode *raft.Node
	applyCh  chan raft.ApplyMsg

	// Pending operations waiting for their log index to be committed
	pending   map[int]*pendingOp
	pendingMu sync.Mutex

	log_ *logger.Logger
}

// NewKVStore creates a new key-value store and starts the apply loop.
func NewKVStore(node *raft.Node, applyCh chan raft.ApplyMsg) *KVStore {
	kv := &KVStore{
		data:     make(map[string]string),
		lastSeen: make(map[string]string),
		raftNode: node,
		applyCh:  applyCh,
		pending:  make(map[int]*pendingOp),
		log_:     logger.New("store", 0),
	}
	go kv.applyLoop()
	return kv
}

// Put inserts or updates a key with the given value.
// Blocks until the operation is committed by a Raft majority or times out.
func (kv *KVStore) Put(key, value, requestID string, clientID int64) error {
	cmd := Command{Op: OpPut, Key: key, Value: value, RequestID: requestID, ClientID: clientID}
	_, err := kv.propose(cmd)
	return err
}

// Delete removes a key from the store.
// Blocks until the operation is committed by a Raft majority or times out.
func (kv *KVStore) Delete(key, requestID string, clientID int64) error {
	cmd := Command{Op: OpDelete, Key: key, RequestID: requestID, ClientID: clientID}
	_, err := kv.propose(cmd)
	return err
}

// Get retrieves the value for a key. This is a local read (not linearizable).
// For strict linearizability, implement a read-index mechanism through Raft.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

// Snapshot returns a copy of the entire key-value state for debugging/inspection.
func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	snap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snap[k] = v
	}
	return snap
}

// propose serializes a command and submits it to Raft, then waits for commitment.
func (kv *KVStore) propose(cmd Command) (string, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return "", fmt.Errorf("marshal command: %w", err)
	}

	index, term, isLeader := kv.raftNode.Propose(data)
	if !isLeader {
		return "", fmt.Errorf("not leader: redirect request to the cluster leader")
	}

	respCh := make(chan Result, 1)
	kv.pendingMu.Lock()
	kv.pending[index] = &pendingOp{index: index, term: term, respCh: respCh}
	kv.pendingMu.Unlock()

	select {
	case result := <-respCh:
		return result.Value, result.Err
	case <-time.After(5 * time.Second):
		kv.pendingMu.Lock()
		delete(kv.pending, index)
		kv.pendingMu.Unlock()
		return "", fmt.Errorf("timeout: operation did not commit within 5 seconds")
	}
}

// applyLoop processes committed log entries from the Raft applyCh and applies
// them to the state machine. It also notifies any pending client operations.
func (kv *KVStore) applyLoop() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		var cmd Command
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			kv.log_.Errorf("Failed to unmarshal command at index %d: %v", msg.CommandIndex, err)
			continue
		}

		result := kv.apply(cmd)

		// Notify the pending operation, if any
		kv.pendingMu.Lock()
		op, ok := kv.pending[msg.CommandIndex]
		if ok {
			delete(kv.pending, msg.CommandIndex)
		}
		kv.pendingMu.Unlock()

		if ok {
			// Verify the term hasn't changed (leadership may have changed)
			if _, isLeader := kv.raftNode.GetState(); isLeader {
				op.respCh <- result
			} else {
				op.respCh <- Result{Err: fmt.Errorf("leadership changed during commit")}
			}
		}
	}
}

// apply executes a single command against the state machine.
func (kv *KVStore) apply(cmd Command) Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Deduplication: skip if we've already applied this request
	dedupKey := fmt.Sprintf("%d:%s", cmd.ClientID, cmd.RequestID)
	if lastVal, seen := kv.lastSeen[dedupKey]; seen {
		return Result{Value: lastVal}
	}

	var result Result
	switch cmd.Op {
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
		kv.lastSeen[dedupKey] = cmd.Value
		result = Result{Value: cmd.Value}
		kv.log_.Infof("Applied PUT %s=%s", cmd.Key, cmd.Value)

	case OpDelete:
		delete(kv.data, cmd.Key)
		kv.lastSeen[dedupKey] = ""
		result = Result{}
		kv.log_.Infof("Applied DELETE %s", cmd.Key)

	case OpGet:
		val := kv.data[cmd.Key]
		result = Result{Value: val}

	default:
		result = Result{Err: fmt.Errorf("unknown operation: %s", cmd.Op)}
	}

	return result
}
