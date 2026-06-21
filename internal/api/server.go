// Package api provides the HTTP REST API for the distributed key-value store.
// Clients interact with the cluster through this layer. Write operations are
// automatically forwarded to the leader if this node is not the current leader.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Nitaiz123/raft-kv-store/internal/raft"
	"github.com/Nitaiz123/raft-kv-store/internal/store"
	"github.com/Nitaiz123/raft-kv-store/pkg/logger"
)

// Server exposes the KV store over HTTP.
type Server struct {
	store    *store.KVStore
	raftNode *raft.Node
	nodeID   int
	addr     string
	log_     *logger.Logger
}

// NewServer creates a new HTTP API server.
func NewServer(nodeID int, addr string, kv *store.KVStore, node *raft.Node) *Server {
	return &Server{
		store:    kv,
		raftNode: node,
		nodeID:   nodeID,
		addr:     addr,
		log_:     logger.New("api", nodeID),
	}
}

// Start registers routes and begins listening for HTTP requests.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/snapshot", s.handleSnapshot)

	srv := &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	s.log_.Infof("API server listening on %s", s.addr)
	return srv.ListenAndServe()
}

// handleKV handles GET, PUT, and DELETE operations on /kv/{key}.
//
// GET    /kv/{key}           — retrieve value for key
// PUT    /kv/{key}           — set value (body: {"value":"...", "request_id":"...", "client_id":1})
// DELETE /kv/{key}           — delete key
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		val, ok := s.store.Get(key)
		if !ok {
			http.Error(w, fmt.Sprintf("key %q not found", key), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": val})

	case http.MethodPut:
		var body struct {
			Value     string `json:"value"`
			RequestID string `json:"request_id"`
			ClientID  int64  `json:"client_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.RequestID == "" {
			body.RequestID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
		}
		if err := s.store.Put(key, body.Value, body.RequestID, body.ClientID); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": key, "value": body.Value})

	case http.MethodDelete:
		requestID := r.URL.Query().Get("request_id")
		if requestID == "" {
			requestID = fmt.Sprintf("del-%d", time.Now().UnixNano())
		}
		if err := s.store.Delete(key, requestID, 0); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "key": key})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStatus returns the current node's Raft state (term, role, leader).
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	term, isLeader := s.raftNode.GetState()
	role := "follower"
	if isLeader {
		role = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":   s.nodeID,
		"term":      term,
		"role":      role,
		"leader_id": s.raftNode.GetLeaderID(),
		"timestamp": time.Now().UTC(),
	})
}

// handleSnapshot returns the full key-value state (for debugging/inspection).
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap := s.store.Snapshot()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id": s.nodeID,
		"data":    snap,
		"count":   len(snap),
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
