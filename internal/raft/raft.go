// Package raft implements the Raft consensus algorithm for distributed coordination.
// Based on the original paper: "In Search of an Understandable Consensus Algorithm"
// by Diego Ongaro and John Ousterhout (2014).
package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Nitaiz123/raft-kv-store/pkg/logger"
)

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower  NodeState = iota // Default state; accepts log entries from leader
	Candidate                  // Actively soliciting votes to become leader
	Leader                     // Manages log replication across the cluster
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single command in the replicated log.
type LogEntry struct {
	Term    int    // The term when this entry was received by the leader
	Index   int    // Position in the log (1-indexed)
	Command []byte // Serialized state machine command
}

// RequestVoteArgs contains the arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         int // Candidate's current term
	CandidateID  int // ID of the candidate requesting the vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// RequestVoteReply contains the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True if candidate received the vote
}

// AppendEntriesArgs contains arguments for the AppendEntries RPC (also used as heartbeat).
type AppendEntriesArgs struct {
	Term         int        // Leader's current term
	LeaderID     int        // Leader's node ID (so followers can redirect clients)
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply contains the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          int  // Current term, for leader to update itself
	Success       bool // True if follower contained entry matching PrevLogIndex/PrevLogTerm
	ConflictIndex int  // Optimistic conflict index for fast log backtracking
	ConflictTerm  int  // Term of the conflicting entry
}

// ApplyMsg is sent on the applyCh channel when a log entry is committed and
// ready to be applied to the state machine.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int
}

// Config holds the configuration for a Raft node.
type Config struct {
	ID              int           // Unique node identifier
	Peers           []int         // IDs of all other nodes in the cluster
	ElectionTimeout time.Duration // Base election timeout (jitter applied automatically)
	HeartbeatTick   time.Duration // Interval between leader heartbeats
}

// Transport defines the interface for sending RPCs between nodes.
// Implementations can use gRPC, HTTP, or in-memory channels (for testing).
type Transport interface {
	SendRequestVote(peerID int, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peerID int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Node is a single participant in the Raft cluster.
type Node struct {
	mu sync.Mutex

	// Persistent state (must be saved to stable storage before responding to RPCs)
	currentTerm int
	votedFor    int // -1 if not voted in current term
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[int]int // For each peer, index of next log entry to send
	matchIndex map[int]int // For each peer, highest log entry known to be replicated

	// Node metadata
	id    int
	peers []int
	state NodeState

	// Election timer management
	electionTimeout  time.Duration
	heartbeatTick    time.Duration
	lastHeartbeat    time.Time
	electionDeadline time.Time

	// Communication channels
	applyCh chan ApplyMsg
	stopCh  chan struct{}

	// RPC transport layer
	transport Transport

	log_ *logger.Logger
}

// NewNode creates and initializes a new Raft node. It does not start the node;
// call Start() to begin participating in the cluster.
func NewNode(cfg Config, transport Transport, applyCh chan ApplyMsg) *Node {
	n := &Node{
		currentTerm:     0,
		votedFor:        -1,
		log:             []LogEntry{{Term: 0, Index: 0}}, // sentinel entry at index 0
		commitIndex:     0,
		lastApplied:     0,
		id:              cfg.ID,
		peers:           cfg.Peers,
		state:           Follower,
		electionTimeout: cfg.ElectionTimeout,
		heartbeatTick:   cfg.HeartbeatTick,
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		transport:       transport,
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		log_:            logger.New("raft", cfg.ID),
	}
	n.resetElectionTimer()
	return n
}

// Start begins the Raft node's background goroutines for election management
// and log application.
func (n *Node) Start() {
	go n.ticker()
	go n.applyCommitted()
	n.log_.Infof("Node %d started as %s", n.id, n.state)
}

// Stop gracefully shuts down the Raft node.
func (n *Node) Stop() {
	close(n.stopCh)
}

// Propose submits a command to the Raft log. Returns the log index, term,
// and whether this node is the current leader. Only the leader can accept proposals.
func (n *Node) Propose(command []byte) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, n.currentTerm, false
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   len(n.log),
		Command: command,
	}
	n.log = append(n.log, entry)
	n.log_.Infof("Leader %d appended entry at index %d (term %d)", n.id, entry.Index, entry.Term)

	// Immediately broadcast to peers (don't wait for heartbeat tick)
	go n.broadcastAppendEntries()

	return entry.Index, entry.Term, true
}

// GetState returns the current term and whether this node believes it is the leader.
func (n *Node) GetState() (term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state == Leader
}

// GetLeaderID returns the ID of the current known leader (-1 if unknown).
func (n *Node) GetLeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.id
	}
	return -1
}

// ---- RPC Handlers ----

// RequestVote handles an incoming vote request from a candidate.
// Implements §5.2 and §5.4 of the Raft paper.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// Rule: if RPC term > currentTerm, convert to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	if args.Term < n.currentTerm {
		return // Stale request; reject
	}

	// Grant vote only if we haven't voted yet (or already voted for this candidate)
	// AND the candidate's log is at least as up-to-date as ours (§5.4.1)
	alreadyVoted := n.votedFor != -1 && n.votedFor != args.CandidateID
	if alreadyVoted {
		return
	}

	if !n.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	n.votedFor = args.CandidateID
	reply.VoteGranted = true
	n.resetElectionTimer()
	n.log_.Infof("Node %d granted vote to %d for term %d", n.id, args.CandidateID, args.Term)
}

// AppendEntries handles an incoming AppendEntries RPC from the leader.
// Also serves as the heartbeat mechanism (§5.2).
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	if args.Term < n.currentTerm {
		return // Stale leader
	}

	// Valid leader contact — reset election timer
	n.resetElectionTimer()
	if n.state == Candidate {
		n.state = Follower
	}

	// Log consistency check (§5.3): verify PrevLogIndex/PrevLogTerm
	if args.PrevLogIndex >= len(n.log) {
		reply.ConflictIndex = len(n.log)
		reply.ConflictTerm = -1
		return
	}

	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm
		// Find the first index with this conflicting term
		for i := 1; i < len(n.log); i++ {
			if n.log[i].Term == conflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// Append new entries, overwriting any conflicting entries
	insertIdx := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIdx := insertIdx + i
		if logIdx < len(n.log) {
			if n.log[logIdx].Term != entry.Term {
				// Conflict: truncate and append
				n.log = append(n.log[:logIdx], args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	// Update commit index
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
}

// ---- Internal Helpers ----

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		state := n.state
		now := time.Now()
		n.mu.Unlock()

		switch state {
		case Follower, Candidate:
			n.mu.Lock()
			timedOut := now.After(n.electionDeadline)
			n.mu.Unlock()
			if timedOut {
				n.startElection()
			}
		case Leader:
			n.broadcastAppendEntries()
			time.Sleep(n.heartbeatTick)
			continue
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastLogIndex := len(n.log) - 1
	lastLogTerm := n.log[lastLogIndex].Term
	peers := make([]int, len(n.peers))
	copy(peers, n.peers)
	n.resetElectionTimer()
	n.mu.Unlock()

	n.log_.Infof("Node %d starting election for term %d", n.id, term)

	votes := 1 // Vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peerID := range peers {
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				currentVotes := votes
				voteMu.Unlock()

				// Check for majority
				if currentVotes > (len(peers)+1)/2 {
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
				}
			}
		}(peerID)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	// Initialize nextIndex and matchIndex for all peers
	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log)
		n.matchIndex[peer] = 0
	}
	n.log_.Infof("Node %d became LEADER for term %d", n.id, n.currentTerm)
}

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.resetElectionTimer()
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	peers := make([]int, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	for _, peerID := range peers {
		go func(peer int) {
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			nextIdx := n.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			prevLogTerm := n.log[prevLogIndex].Term
			entries := make([]LogEntry, len(n.log)-nextIdx)
			copy(entries, n.log[nextIdx:])
			n.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			reply, err := n.transport.SendAppendEntries(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if n.state != Leader || n.currentTerm != term {
				return
			}

			if reply.Success {
				newMatchIndex := prevLogIndex + len(entries)
				if newMatchIndex > n.matchIndex[peer] {
					n.matchIndex[peer] = newMatchIndex
					n.nextIndex[peer] = newMatchIndex + 1
				}
				n.advanceCommitIndex()
			} else {
				// Fast backtracking using ConflictTerm/ConflictIndex
				if reply.ConflictTerm == -1 {
					n.nextIndex[peer] = reply.ConflictIndex
				} else {
					found := false
					for i := len(n.log) - 1; i >= 1; i-- {
						if n.log[i].Term == reply.ConflictTerm {
							n.nextIndex[peer] = i + 1
							found = true
							break
						}
					}
					if !found {
						n.nextIndex[peer] = reply.ConflictIndex
					}
				}
				if n.nextIndex[peer] < 1 {
					n.nextIndex[peer] = 1
				}
			}
		}(peerID)
	}
}

// advanceCommitIndex updates the leader's commitIndex based on majority replication.
// A log entry is committed when it is stored on a majority of servers (§5.3, §5.4).
func (n *Node) advanceCommitIndex() {
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break // Only commit entries from the current term (§5.4.2)
		}
		replicated := 1 // Count self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicated++
			}
		}
		if replicated > (len(n.peers)+1)/2 {
			n.commitIndex = idx
			n.log_.Infof("Leader %d advanced commitIndex to %d", n.id, idx)
			break
		}
	}
}

// applyCommitted sends committed log entries to the state machine via applyCh.
func (n *Node) applyCommitted() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.log[n.lastApplied]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			n.mu.Unlock()
			n.applyCh <- msg
			n.mu.Lock()
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// isCandidateLogUpToDate returns true if the candidate's log is at least as
// up-to-date as ours, per §5.4.1 of the Raft paper.
func (n *Node) isCandidateLogUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	myLastIndex := len(n.log) - 1
	myLastTerm := n.log[myLastIndex].Term
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}

// resetElectionTimer sets a new randomized election deadline.
// Randomization prevents split votes (§5.2).
func (n *Node) resetElectionTimer() {
	jitter := time.Duration(rand.Int63n(int64(n.electionTimeout)))
	n.electionDeadline = time.Now().Add(n.electionTimeout + jitter)
}
