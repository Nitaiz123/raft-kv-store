# raft-kv-store

A fault-tolerant, distributed key-value store built from scratch using the **Raft consensus algorithm**. This project implements the complete Raft protocol — leader election, log replication, and safety guarantees — without relying on any third-party Raft library.

[![CI](https://github.com/Nitaiz123/raft-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/Nitaiz123/raft-kv-store/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.22-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
│              HTTP PUT/GET/DELETE /kv/{key}                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────▼──────────────┐
        │         HTTP API Layer      │  ← internal/api
        │   /kv/{key}  /status        │
        └──────────────┬──────────────┘
                       │ Propose(cmd)
        ┌──────────────▼──────────────┐
        │       KV State Machine      │  ← internal/store
        │  map[string]string + dedup  │
        └──────────────┬──────────────┘
                       │ applyCh
        ┌──────────────▼──────────────┐
        │       Raft Consensus Node   │  ← internal/raft
        │  Leader election + Log rep  │
        └──────────────┬──────────────┘
                       │ RPCs
        ┌──────────────▼──────────────┐
        │         Transport Layer     │  ← internal/transport
        │  MemoryTransport / gRPC     │
        └─────────────────────────────┘
```

### Key Design Decisions

| Component | Decision | Rationale |
|-----------|----------|-----------|
| Consensus | Raft (from scratch) | Understandable, well-specified; no external deps |
| Transport | Pluggable interface | Swap between in-memory (tests) and gRPC (prod) |
| Deduplication | Client ID + Request ID | Exactly-once semantics for write operations |
| Log backtracking | ConflictTerm/ConflictIndex | O(1) catch-up instead of O(n) one-by-one |
| State machine | In-memory map | Simplicity; swap for RocksDB for persistence |

---

## Raft Implementation Details

This implementation follows the original Raft paper closely:

- **Leader Election** (§5.2): Randomized election timeouts prevent split votes. Nodes transition Follower → Candidate → Leader.
- **Log Replication** (§5.3): The leader appends entries and replicates them via `AppendEntries` RPCs. Entries commit once a majority acknowledges.
- **Safety** (§5.4): A candidate can only win an election if its log is at least as up-to-date as any majority member's log.
- **Fast Log Backtracking**: When a follower rejects `AppendEntries`, it returns `ConflictTerm` and `ConflictIndex` so the leader can skip entire terms rather than decrementing `nextIndex` one step at a time.
- **Deduplication**: The state machine tracks `(clientID, requestID)` pairs to ensure idempotent writes even if a request is retried after a leader change.

---

## Getting Started

### Prerequisites

- Go 1.22+
- Docker & Docker Compose (optional, for containerized cluster)

### Run a Local 3-Node Cluster (Single Process)

```bash
git clone https://github.com/Nitaiz123/raft-kv-store.git
cd raft-kv-store
go run ./cmd/server --id=0 --http=:8080 --cluster=3
```

### Run with Docker Compose

```bash
docker-compose up --build
```

This starts three nodes:
- Node 0: `http://localhost:8080`
- Node 1: `http://localhost:8081`
- Node 2: `http://localhost:8082`

### API Usage

```bash
# Write a key
curl -X PUT http://localhost:8080/kv/hello \
     -H 'Content-Type: application/json' \
     -d '{"value":"world","request_id":"req-1","client_id":1}'

# Read a key
curl http://localhost:8080/kv/hello

# Delete a key
curl -X DELETE http://localhost:8080/kv/hello

# Check node status (term, role, leader ID)
curl http://localhost:8080/status

# View full snapshot of the KV state
curl http://localhost:8080/snapshot
```

### Run Tests

```bash
# Run all tests with race detector
go test -v -race ./...

# Run only Raft consensus tests
go test -v -race ./internal/raft/...
```

---

## Project Structure

```
raft-kv-store/
├── cmd/
│   └── server/          # Main entry point
│       └── main.go
├── internal/
│   ├── raft/            # Core Raft consensus implementation
│   │   ├── raft.go      # Node, election, log replication
│   │   └── raft_test.go # Unit & integration tests
│   ├── store/           # KV state machine
│   │   └── store.go
│   ├── transport/       # RPC transport layer
│   │   └── memory.go    # In-memory transport for testing
│   └── api/             # HTTP REST API
│       └── server.go
├── pkg/
│   └── logger/          # Structured logger
│       └── logger.go
├── docs/                # Architecture diagrams
├── Dockerfile
├── docker-compose.yml
└── .github/workflows/ci.yml
```

---

## Fault Tolerance

The cluster tolerates up to `⌊(n-1)/2⌋` node failures:

| Cluster Size | Max Failures |
|:---:|:---:|
| 3 | 1 |
| 5 | 2 |
| 7 | 3 |

To simulate a network partition in tests, use `MemoryNetwork.Partition(nodeID)` and `Heal(nodeID)`.

---

## References

- Ongaro, D., & Ousterhout, J. (2014). [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf). USENIX ATC.
- [The Raft Consensus Algorithm](https://raft.github.io/) — interactive visualization
- [etcd](https://github.com/etcd-io/etcd) — production Raft implementation for reference

---

## License

MIT License. See [LICENSE](LICENSE).
