// Package client is a leader-aware Go client for EdgeKV.
//
// It remembers which node leads each shard, retries with the same request ID
// on timeouts and leader changes, and asks nodes not to proxy on its behalf,
// so that after the first request per shard every write goes straight to the
// leader.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotFound        = errors.New("edgekv: key not found")
	ErrVersionMismatch = errors.New("edgekv: version mismatch")
	ErrNotCacheable    = errors.New("edgekv: key is not cacheable")
)

// Entry is a value read from the store.
type Entry struct {
	Value     string
	Version   uint64
	UpdatedAt time.Time
	// FromCache reports whether an intermediate cache served the response
	// (only meaningful for CachedGet).
	FromCache bool
	CacheAge  time.Duration
}

// CachePolicy controls whether a key may be served from /v1/cache.
type CachePolicy struct {
	Cacheable  bool
	TTLSeconds uint32
}

// Client talks to an EdgeKV cluster.
type Client struct {
	nodes    []string // base URLs
	edge     string   // base URL of the CDN / edge cache for CachedGet; "" = origin
	http     *http.Client
	retries  int
	backoff  time.Duration
	mu       sync.RWMutex
	leaders  map[uint32]string // shard -> base URL
	shardOf  map[string]uint32 // key -> shard, learned from responses
	numNodes int
}

// Option configures a Client.
type Option func(*Client)

// WithEdge sets the URL used for CachedGet (a CloudFront distribution or the
// local edge simulator). Without it, CachedGet goes to the origin.
func WithEdge(baseURL string) Option { return func(c *Client) { c.edge = baseURL } }

// WithHTTPClient replaces the underlying HTTP client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithRetries sets how many attempts a request gets before giving up.
func WithRetries(n int) Option { return func(c *Client) { c.retries = n } }

// New creates a client for the given node base URLs.
func New(nodes []string, opts ...Option) *Client {
	c := &Client{
		nodes: nodes, numNodes: len(nodes),
		http: &http.Client{
			Timeout:   3 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 512, IdleConnTimeout: 90 * time.Second},
		},
		retries: 8, backoff: 20 * time.Millisecond,
		leaders: make(map[uint32]string), shardOf: make(map[string]uint32),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Put writes a value. It returns the new version.
func (c *Client) Put(ctx context.Context, key, value string, policy *CachePolicy) (uint64, error) {
	return c.put(ctx, key, value, policy, nil)
}

// CAS writes a value only if the key is at expectedVersion (0 = must not
// exist). It returns ErrVersionMismatch otherwise.
func (c *Client) CAS(ctx context.Context, key, value string, expectedVersion uint64, policy *CachePolicy) (uint64, error) {
	return c.put(ctx, key, value, policy, &expectedVersion)
}

func (c *Client) put(ctx context.Context, key, value string, policy *CachePolicy, expected *uint64) (uint64, error) {
	body := map[string]any{"request_id": newRequestID(), "value": value}
	if policy != nil {
		body["cache_policy"] = map[string]any{"cacheable": policy.Cacheable, "ttl_seconds": policy.TTLSeconds}
	}
	raw, _ := json.Marshal(body)
	path := "/v1/kv/" + url.PathEscape(key)
	if expected != nil {
		path += "?expectedVersion=" + strconv.FormatUint(*expected, 10)
	}
	var out struct {
		Version uint64 `json:"version"`
	}
	if err := c.do(ctx, http.MethodPut, key, path, raw, nil, &out); err != nil {
		return 0, err
	}
	return out.Version, nil
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) (uint64, error) {
	path := "/v1/kv/" + url.PathEscape(key) + "?request_id=" + newRequestID()
	var out struct {
		Version uint64 `json:"version"`
	}
	if err := c.do(ctx, http.MethodDelete, key, path, nil, nil, &out); err != nil {
		return 0, err
	}
	return out.Version, nil
}

// Get performs a linearizable read.
func (c *Client) Get(ctx context.Context, key string) (Entry, error) {
	var out readBody
	if err := c.do(ctx, http.MethodGet, key, "/v1/kv/"+url.PathEscape(key), nil, nil, &out); err != nil {
		return Entry{}, err
	}
	return out.entry(), nil
}

// CachedGet reads through the edge cache, accepting bounded staleness.
func (c *Client) CachedGet(ctx context.Context, key string) (Entry, error) {
	path := "/v1/cache/" + url.PathEscape(key)
	var out readBody
	var hdr http.Header
	var err error
	if c.edge != "" {
		hdr, err = c.request(ctx, c.edge, http.MethodGet, path, nil, nil, &out)
	} else {
		err = c.do(ctx, http.MethodGet, key, path, nil, &hdr, &out)
	}
	if err != nil {
		return Entry{}, err
	}
	e := out.entry()
	if hdr != nil {
		// Both CloudFront ("Hit from cloudfront") and the simulator set X-Cache.
		e.FromCache = bytes.HasPrefix(bytes.ToLower([]byte(hdr.Get("X-Cache"))), []byte("hit"))
		if age, err := strconv.Atoi(hdr.Get("Age")); err == nil {
			e.CacheAge = time.Duration(age) * time.Second
		}
	}
	return e, nil
}

type readBody struct {
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	UpdatedAt string `json:"updated_at"`
}

func (b readBody) entry() Entry {
	t, _ := time.Parse(time.RFC3339Nano, b.UpdatedAt)
	return Entry{Value: b.Value, Version: b.Version, UpdatedAt: t}
}

// do sends a request to the node believed to lead the key's shard, following
// 421 hints and retrying on failures. Because the body carries a request ID,
// retrying a write is safe: the state machine executes it at most once.
func (c *Client) do(ctx context.Context, method, key, path string, body []byte, hdrOut *http.Header, out any) error {
	target := c.pickNode(key)
	var lastErr error
	for attempt := 0; attempt < c.retries; attempt++ {
		hdr, err := c.request(ctx, target, method, path, body, nil, out)
		if hdrOut != nil {
			*hdrOut = hdr
		}
		if err == nil {
			c.learn(key, hdr, target)
			return nil
		}
		var re *redirectError
		switch {
		case errors.As(err, &re):
			c.setLeader(key, re.shard, re.leaderURL)
			if re.leaderURL != "" {
				target = re.leaderURL
				continue // no backoff: we were just told where to go
			}
		case errors.Is(err, ErrNotFound), errors.Is(err, ErrVersionMismatch), errors.Is(err, ErrNotCacheable):
			return err // definitive answers are not retried
		case ctx.Err() != nil:
			return ctx.Err()
		}
		lastErr = err
		c.forget(key)
		target = c.nodes[attempt%len(c.nodes)]
		select {
		case <-time.After(c.backoff << min(attempt, 5)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("edgekv: giving up after %d attempts: %w", c.retries, lastErr)
}

type redirectError struct {
	shard     uint32
	leaderURL string
}

func (e *redirectError) Error() string { return "edgekv: not leader, redirected to " + e.leaderURL }

func (c *Client) request(ctx context.Context, base, method, path string, body []byte, _ http.Header, out any) (http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Edgekv-No-Forward", "1")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.Header, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if out != nil {
			return resp.Header, json.Unmarshal(raw, out)
		}
		return resp.Header, nil
	case http.StatusNotFound:
		return resp.Header, ErrNotFound
	case http.StatusPreconditionFailed:
		return resp.Header, ErrVersionMismatch
	case http.StatusForbidden, http.StatusBadRequest:
		var e struct{ Error, Detail string }
		json.Unmarshal(raw, &e)
		if e.Error == "not_cacheable" || e.Error == "namespace_not_cacheable" {
			return resp.Header, ErrNotCacheable
		}
		return resp.Header, fmt.Errorf("edgekv: %s: %s", e.Error, e.Detail)
	case http.StatusMisdirectedRequest:
		var e struct {
			Shard     uint32 `json:"shard"`
			LeaderURL string `json:"leader_url"`
		}
		json.Unmarshal(raw, &e)
		return resp.Header, &redirectError{shard: e.Shard, leaderURL: e.LeaderURL}
	default:
		return resp.Header, fmt.Errorf("edgekv: %s: %s", resp.Status, bytes.TrimSpace(raw))
	}
}

// --- leader cache ---------------------------------------------------------

func (c *Client) pickNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if shard, ok := c.shardOf[key]; ok {
		if leader, ok := c.leaders[shard]; ok {
			return leader
		}
	}
	return c.nodes[0]
}

func (c *Client) learn(key string, hdr http.Header, target string) {
	// A successful write response names the shard; remember that this node
	// leads it. Reads served locally are also proof of leadership.
	shard := hdr.Get("X-Edgekv-Shard")
	if shard == "" {
		return
	}
	if n, err := strconv.ParseUint(shard, 10, 32); err == nil {
		c.setLeader(key, uint32(n), target)
	}
}

func (c *Client) setLeader(key string, shard uint32, leaderURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shardOf[key] = shard
	if leaderURL != "" {
		c.leaders[shard] = leaderURL
	} else {
		delete(c.leaders, shard)
	}
}

func (c *Client) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if shard, ok := c.shardOf[key]; ok {
		delete(c.leaders, shard)
	}
}

func newRequestID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
