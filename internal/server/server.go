// Package server is the HTTP gateway of a node.
//
// Two read paths with two different contracts live side by side:
//
//	GET /v1/kv/{key}     linearizable; "Cache-Control: no-store"; never cached
//	GET /v1/cache/{key}  bounded staleness; cacheable by a CDN for the key's TTL
//
// Keeping them on different URL prefixes is deliberate. A CDN decides what to
// cache per path pattern, so the consistency contract is visible in the URL and
// can be enforced twice: by the CDN's behaviour rules and by the origin's
// response headers.
package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/cdn"
	"github.com/Tr3nt-Xie/EdgeKV/internal/metrics"
	"github.com/Tr3nt-Xie/EdgeKV/internal/node"
	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
	"github.com/Tr3nt-Xie/EdgeKV/internal/store"
)

const (
	maxBodyBytes = 1 << 20

	headerForwarded = "X-Edgekv-Forwarded"  // set when a node proxies to the leader
	headerNoForward = "X-Edgekv-No-Forward" // set by leader-aware clients
	headerRequestID = "X-Request-Id"
	headerVersion   = "X-Edgekv-Version"
	headerServedBy  = "X-Edgekv-Served-By"
	headerShard     = "X-Edgekv-Shard" // lets leader-aware clients learn the key -> shard -> leader mapping
)

// Config configures the gateway.
type Config struct {
	NodeID string
	Node   *node.Node
	// PeerHTTP maps every node ID to its HTTP base URL, e.g. http://n2:8080.
	PeerHTTP map[string]string

	// CacheableNamespaces lists the key prefixes that may ever be served from
	// /v1/cache. A key outside them cannot be made cacheable by any request.
	CacheableNamespaces []string
	DefaultTTLSeconds   uint32
	MaxTTLSeconds       uint32

	Invalidations  *cdn.Queue
	Metrics        *metrics.Registry
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

// Server handles the public HTTP API.
type Server struct {
	cfg    Config
	mux    *http.ServeMux
	client *http.Client
	log    *slog.Logger
}

func New(cfg Config) *Server {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.DefaultTTLSeconds == 0 {
		cfg.DefaultTTLSeconds = 30
	}
	if cfg.MaxTTLSeconds == 0 {
		cfg.MaxTTLSeconds = 300
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		cfg: cfg, log: cfg.Logger, mux: http.NewServeMux(),
		client: &http.Client{
			Timeout: cfg.RequestTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	s.mux.HandleFunc("PUT /v1/kv/{key...}", s.instrument("put", s.handlePut))
	s.mux.HandleFunc("GET /v1/kv/{key...}", s.instrument("get", s.handleGet))
	s.mux.HandleFunc("DELETE /v1/kv/{key...}", s.instrument("delete", s.handleDelete))
	s.mux.HandleFunc("GET /v1/cache/{key...}", s.instrument("cache_get", s.handleCacheGet))
	s.mux.HandleFunc("GET /v1/status", s.handleStatus)
	s.mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		cfg.Metrics.Write(w)
	})
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	s.registerCollectors()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

type putRequest struct {
	RequestID   string `json:"request_id"`
	Value       string `json:"value"`
	CachePolicy *struct {
		Cacheable  bool   `json:"cacheable"`
		TTLSeconds uint32 `json:"ttl_seconds"`
	} `json:"cache_policy"`
}

type writeResponse struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
	Shard   uint32 `json:"shard"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 1 MiB")
		return
	}
	var req putRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}

	cmd := &pb.Command{Op: pb.Op_OP_PUT, Key: key, Value: []byte(req.Value)}
	cmd.RequestId = firstNonEmpty(req.RequestID, r.Header.Get(headerRequestID), newRequestID())

	if ev := r.URL.Query().Get("expectedVersion"); ev != "" {
		n, err := strconv.ParseUint(ev, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_expected_version", "expectedVersion must be a non-negative integer")
			return
		}
		cmd.HasExpectedVersion, cmd.ExpectedVersion = true, n
	}

	if p := req.CachePolicy; p != nil && p.Cacheable {
		// Refuse loudly rather than silently storing the key as non-cacheable:
		// a caller who believes task state is being cached has a design bug
		// that they need to hear about.
		if !s.namespaceCacheable(key) {
			writeError(w, http.StatusBadRequest, "namespace_not_cacheable",
				fmt.Sprintf("keys outside %v can never be cached", s.cfg.CacheableNamespaces))
			return
		}
		cmd.Cacheable = true
		cmd.TtlSeconds = min(max(p.TTLSeconds, 1), s.cfg.MaxTTLSeconds)
		if p.TTLSeconds == 0 {
			cmd.TtlSeconds = s.cfg.DefaultTTLSeconds
		}
	}
	s.write(w, r, body, cmd)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	cmd := &pb.Command{Op: pb.Op_OP_DELETE, Key: r.PathValue("key")}
	cmd.RequestId = firstNonEmpty(r.URL.Query().Get("request_id"), r.Header.Get(headerRequestID), newRequestID())
	s.write(w, r, nil, cmd)
}

func (s *Server) write(w http.ResponseWriter, r *http.Request, body []byte, cmd *pb.Command) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()

	res, err := s.cfg.Node.Write(ctx, cmd)
	if err != nil {
		s.handleRaftError(w, r, body, cmd.Key, cmd.RequestId, err)
		return
	}
	w.Header().Set(headerServedBy, s.cfg.NodeID)
	w.Header().Set(headerShard, strconv.Itoa(int(s.cfg.Node.ShardFor(cmd.Key).ID)))
	switch res.Status {
	case pb.Status_STATUS_OK:
		// Invalidate only after the write is committed. The other order would
		// let the CDN re-fetch and re-cache the old value in between.
		if s.cfg.Invalidations != nil && s.namespaceCacheable(cmd.Key) {
			s.cfg.Invalidations.Enqueue("/v1/cache/" + cmd.Key)
		}
		writeJSON(w, http.StatusOK, writeResponse{Key: cmd.Key, Version: res.Version, Shard: s.cfg.Node.ShardFor(cmd.Key).ID})
	case pb.Status_STATUS_VERSION_MISMATCH:
		writeJSON(w, http.StatusPreconditionFailed, map[string]any{
			"error": "version_mismatch", "current_version": res.Version,
		})
	case pb.Status_STATUS_NOT_FOUND:
		writeError(w, http.StatusNotFound, "not_found", "key does not exist")
	}
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

type readResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	UpdatedAt string `json:"updated_at"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	// Whatever happens below, no cache may keep this response.
	w.Header().Set("Cache-Control", "no-store")
	key := r.PathValue("key")
	entry, ok := s.read(w, r, key)
	if !ok {
		return
	}
	w.Header().Set("ETag", etag(entry.Version))
	w.Header().Set(headerVersion, strconv.FormatUint(entry.Version, 10))
	writeJSON(w, http.StatusOK, toReadResponse(key, entry))
}

func (s *Server) handleCacheGet(w http.ResponseWriter, r *http.Request) {
	// Errors and refusals must not be cached; only the success path below
	// replaces this header.
	w.Header().Set("Cache-Control", "no-store")
	key := r.PathValue("key")

	if !s.namespaceCacheable(key) {
		writeError(w, http.StatusForbidden, "not_cacheable", "this key's namespace is never served from the cache path")
		return
	}
	// The origin answers with a linearizable read. Staleness is therefore
	// introduced only by the cache in front, and is bounded by the TTL below.
	entry, ok := s.read(w, r, key)
	if !ok {
		return
	}
	if !entry.Cacheable {
		writeError(w, http.StatusForbidden, "not_cacheable", "this key was not written with cache_policy.cacheable")
		return
	}

	// Versioned URL: /v1/cache/{key}?v=N names one immutable version. It can be
	// cached forever, because a new version has a new URL and can never be
	// confused with this one.
	if v := r.URL.Query().Get("v"); v != "" {
		if want, err := strconv.ParseUint(v, 10, 64); err != nil || want != entry.Version {
			writeError(w, http.StatusNotFound, "version_not_found", "only the current version is retained")
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", entry.TTLSeconds))
	}
	w.Header().Set("ETag", etag(entry.Version))
	w.Header().Set(headerVersion, strconv.FormatUint(entry.Version, 10))
	w.Header().Set("Last-Modified", time.UnixMilli(entry.UpdatedUnixMs).UTC().Format(http.TimeFormat))

	// Revalidation: when the TTL runs out the CDN asks "is my copy still
	// good?". If so, a 304 renews it without sending the body again.
	if match := r.Header.Get("If-None-Match"); match != "" && match == etag(entry.Version) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, toReadResponse(key, entry))
}

// read performs a linearizable read, forwarding to the leader if necessary. It
// reports false if it has already written a response.
func (s *Server) read(w http.ResponseWriter, r *http.Request, key string) (store.Entry, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()

	entry, err := s.cfg.Node.Get(ctx, key)
	w.Header().Set(headerShard, strconv.Itoa(int(s.cfg.Node.ShardFor(key).ID)))
	switch {
	case err == nil:
		w.Header().Set(headerServedBy, s.cfg.NodeID)
		return entry, true
	case errors.Is(err, store.ErrNotFound):
		w.Header().Set(headerServedBy, s.cfg.NodeID)
		writeError(w, http.StatusNotFound, "not_found", "key does not exist")
	default:
		s.handleRaftError(w, r, nil, key, "", err)
	}
	return store.Entry{}, false
}

// ---------------------------------------------------------------------------
// Routing to the leader
// ---------------------------------------------------------------------------

// handleRaftError deals with "this node cannot serve the request itself".
//
// Plain HTTP clients and the CDN know nothing about shards or leaders, so by
// default the node proxies the request to the leader on their behalf. A
// leader-aware client sets X-Edgekv-No-Forward and gets a 421 with the leader's
// address instead, which saves a network hop on every later request.
func (s *Server) handleRaftError(w http.ResponseWriter, r *http.Request, body []byte, key, requestID string, err error) {
	var notLeader *raft.NotLeaderError
	if errors.As(err, &notLeader) {
		leaderURL := s.cfg.PeerHTTP[notLeader.Leader]
		alreadyForwarded := r.Header.Get(headerForwarded) != ""
		wantsRedirect := r.Header.Get(headerNoForward) != ""

		if leaderURL != "" && notLeader.Leader != s.cfg.NodeID && !alreadyForwarded && !wantsRedirect {
			s.forward(w, r, body, leaderURL, requestID)
			return
		}
		w.Header().Set("Retry-After", "0")
		writeJSON(w, http.StatusMisdirectedRequest, map[string]any{
			"error": "not_leader", "shard": s.cfg.Node.ShardFor(key).ID,
			"leader": notLeader.Leader, "leader_url": leaderURL,
		})
		return
	}
	// Timeout, shutdown, no leader elected. For a write the outcome is unknown:
	// it may yet be committed. The only safe reaction is to retry with the same
	// request_id, which the state machine deduplicates.
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"error": "unavailable", "detail": err.Error(), "retry_with_same_request_id": true,
	})
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, body []byte, leaderURL, requestID string) {
	out, err := http.NewRequestWithContext(r.Context(), r.Method, leaderURL+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "forward_failed", err.Error())
		return
	}
	for _, h := range []string{"Content-Type", "If-None-Match"} {
		if v := r.Header.Get(h); v != "" {
			out.Header.Set(h, v)
		}
	}
	// One hop only: if the leader moved again meanwhile, the next node answers
	// 421 instead of forwarding, so a request can never loop.
	out.Header.Set(headerForwarded, s.cfg.NodeID)
	if requestID != "" {
		// Carry the ID we generated, so that our attempt and the leader's
		// attempt are recognised as the same operation.
		out.Header.Set(headerRequestID, requestID)
	}

	resp, err := s.client.Do(out)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "unavailable", "detail": "forward to leader failed: " + err.Error(), "retry_with_same_request_id": true,
		})
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ---------------------------------------------------------------------------
// Status and metrics
// ---------------------------------------------------------------------------

type shardStatus struct {
	Shard           uint32            `json:"shard"`
	State           string            `json:"state"`
	Term            uint64            `json:"term"`
	Leader          string            `json:"leader"`
	LeaderURL       string            `json:"leader_url"`
	PreferredLeader string            `json:"preferred_leader"`
	CommitIndex     uint64            `json:"commit_index"`
	LastApplied     uint64            `json:"last_applied"`
	LastLogIndex    uint64            `json:"last_log_index"`
	SnapshotIndex   uint64            `json:"snapshot_index"`
	WALBytes        int64             `json:"wal_bytes"`
	Keys            int               `json:"keys"`
	MatchIndex      map[string]uint64 `json:"match_index,omitempty"`
}

type statusResponse struct {
	Node      string            `json:"node"`
	NumShards int               `json:"num_shards"`
	Nodes     map[string]string `json:"nodes"`
	Shards    []shardStatus     `json:"shards"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	resp := statusResponse{Node: s.cfg.NodeID, NumShards: s.cfg.Node.NumShards(), Nodes: s.cfg.PeerHTTP}
	for _, sh := range s.cfg.Node.Shards() {
		st := sh.Raft.Status()
		resp.Shards = append(resp.Shards, shardStatus{
			Shard: sh.ID, State: st.State.String(), Term: st.Term,
			Leader: st.Leader, LeaderURL: s.cfg.PeerHTTP[st.Leader],
			PreferredLeader: s.cfg.Node.PreferredLeader(sh.ID),
			CommitIndex:     st.CommitIndex, LastApplied: st.LastApplied,
			LastLogIndex: st.LastLogIndex, SnapshotIndex: st.SnapshotIndex,
			WALBytes: sh.WALSize(), Keys: sh.Store.Len(), MatchIndex: st.MatchIndex,
		})
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) registerCollectors() {
	m := s.cfg.Metrics
	m.Help("edgekv_http_requests_total", "HTTP requests by operation and status code.")
	m.Help("edgekv_http_request_duration_seconds", "HTTP request latency by operation.")
	m.Help("edgekv_raft_is_leader", "1 if this node leads the shard.")
	m.Help("edgekv_raft_replication_lag_entries", "Leader only: entries the follower is behind.")
	m.Collect(func(emit func(string, map[string]string, float64)) {
		for _, sh := range s.cfg.Node.Shards() {
			st := sh.Raft.Status()
			l := map[string]string{"shard": strconv.Itoa(int(sh.ID))}
			emit("edgekv_raft_term", l, float64(st.Term))
			emit("edgekv_raft_is_leader", l, boolFloat(st.State == raft.Leader))
			emit("edgekv_raft_commit_index", l, float64(st.CommitIndex))
			emit("edgekv_raft_last_applied", l, float64(st.LastApplied))
			emit("edgekv_raft_last_log_index", l, float64(st.LastLogIndex))
			emit("edgekv_raft_snapshot_index", l, float64(st.SnapshotIndex))
			emit("edgekv_wal_size_bytes", l, float64(sh.WALSize()))
			for peer, match := range st.MatchIndex {
				emit("edgekv_raft_replication_lag_entries",
					map[string]string{"shard": l["shard"], "peer": peer}, float64(st.LastLogIndex-match))
			}
		}
	})
}

// instrument records request count and latency for a handler.
func (s *Server) instrument(op string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next(rec, r)
		s.cfg.Metrics.Inc("edgekv_http_requests_total", map[string]string{"op": op, "code": strconv.Itoa(rec.code)})
		s.cfg.Metrics.Observe("edgekv_http_request_duration_seconds", map[string]string{"op": op}, time.Since(start).Seconds())
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Server) namespaceCacheable(key string) bool {
	for _, prefix := range s.cfg.CacheableNamespaces {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func toReadResponse(key string, e store.Entry) readResponse {
	return readResponse{
		Key: key, Value: string(e.Value), Version: e.Version,
		UpdatedAt: time.UnixMilli(e.UpdatedUnixMs).UTC().Format(time.RFC3339Nano),
	}
}

func etag(version uint64) string { return fmt.Sprintf(`"v%d"`, version) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, detail string) {
	writeJSON(w, code, map[string]string{"error": kind, "detail": detail})
}

func newRequestID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
