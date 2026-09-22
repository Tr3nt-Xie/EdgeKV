// Command edgekv-edge is a small caching reverse proxy that behaves like one
// CloudFront edge location. It exists so that cache hits, misses, TTL expiry,
// invalidation and stale reads can be demonstrated and measured on a laptop,
// with the same origin headers CloudFront would see.
//
// Behaviour, modelled on CloudFront:
//   - only GET /v1/cache/* is ever cached; everything else is proxied as is
//   - a response is cached for the max-age in its Cache-Control header
//   - "no-store", "private" and non-200 responses are never cached
//   - the query string is part of the cache key (so ?v=N versioned URLs work)
//   - after expiry the edge revalidates with If-None-Match; a 304 renews the entry
//   - POST /_edge/invalidate {"paths": [...]} evicts entries, optionally after
//     a configurable propagation delay, or fails if -fail-invalidations is set
//   - X-Cache: Hit / Miss / RefreshHit and Age headers on every response
//   - GET /_edge/stats reports hit ratio and origin requests
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	status  int
	header  http.Header
	body    []byte
	etag    string
	fetched time.Time
	expires time.Time
}

type edge struct {
	origins   []string
	next      atomic.Uint64
	client    *http.Client
	delay     time.Duration
	failInval bool
	log       *slog.Logger

	mu    sync.Mutex
	cache map[string]*entry

	hits, misses, refreshHits, originRequests, invalidations, failedInvalidations atomic.Uint64
}

func main() {
	listen := flag.String("listen", ":8000", "listen address")
	origins := flag.String("origins", "http://127.0.0.1:8081", "comma-separated origin base URLs (round-robin)")
	delay := flag.Duration("invalidation-delay", 0, "simulated propagation delay before an invalidation takes effect")
	failInval := flag.Bool("fail-invalidations", false, "reject every invalidation request (simulates a CDN API outage)")
	flag.Parse()

	e := &edge{
		origins:   strings.Split(*origins, ","),
		client:    &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}},
		delay:     *delay,
		failInval: *failInval,
		cache:     make(map[string]*entry),
		log:       slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_edge/invalidate", e.handleInvalidate)
	mux.HandleFunc("GET /_edge/stats", e.handleStats)
	mux.HandleFunc("POST /_edge/flush", func(w http.ResponseWriter, _ *http.Request) {
		e.mu.Lock()
		e.cache = make(map[string]*entry)
		e.mu.Unlock()
	})
	mux.HandleFunc("/", e.handle)
	e.log.Info("edge cache started", "listen", *listen, "origins", e.origins, "invalidation_delay", *delay, "fail_invalidations", *failInval)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		e.log.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func (e *edge) handle(w http.ResponseWriter, r *http.Request) {
	cacheable := r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/cache/")
	if !cacheable {
		e.proxy(w, r, nil)
		return
	}
	key := r.URL.RequestURI()
	now := time.Now()

	e.mu.Lock()
	ent := e.cache[key]
	e.mu.Unlock()

	if ent != nil && now.Before(ent.expires) {
		e.hits.Add(1)
		e.serve(w, ent, "Hit", now)
		return
	}

	// Miss or expired: go to the origin, revalidating if we have an ETag.
	var conditional http.Header
	if ent != nil && ent.etag != "" {
		conditional = http.Header{"If-None-Match": []string{ent.etag}}
	}
	resp, body, err := e.fetch(r, conditional)
	if err != nil {
		if ent != nil {
			// CloudFront's "stale-if-error" behaviour: better a stale answer
			// than an error while the origin is down.
			e.serve(w, ent, "Stale", now)
			return
		}
		http.Error(w, "origin unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}

	if resp.StatusCode == http.StatusNotModified && ent != nil {
		// Renew the existing entry for another TTL.
		ttl := maxAge(resp.Header)
		if ttl == 0 {
			ttl = maxAge(ent.header)
		}
		e.mu.Lock()
		ent.fetched, ent.expires = now, now.Add(ttl)
		e.mu.Unlock()
		e.refreshHits.Add(1)
		e.serve(w, ent, "RefreshHit", now)
		return
	}

	e.misses.Add(1)
	ttl := maxAge(resp.Header)
	if resp.StatusCode == http.StatusOK && ttl > 0 && !strings.Contains(resp.Header.Get("Cache-Control"), "no-store") {
		ent = &entry{
			status: resp.StatusCode, header: resp.Header.Clone(), body: body,
			etag: resp.Header.Get("ETag"), fetched: now, expires: now.Add(ttl),
		}
		e.mu.Lock()
		e.cache[key] = ent
		e.mu.Unlock()
		e.serve(w, ent, "Miss", now)
		return
	}
	copyHeader(w.Header(), resp.Header)
	w.Header().Set("X-Cache", "Miss")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func (e *edge) serve(w http.ResponseWriter, ent *entry, result string, now time.Time) {
	copyHeader(w.Header(), ent.header)
	w.Header().Set("X-Cache", result)
	w.Header().Set("Age", strconv.Itoa(int(now.Sub(ent.fetched).Seconds())))
	w.WriteHeader(ent.status)
	w.Write(ent.body)
}

func (e *edge) proxy(w http.ResponseWriter, r *http.Request, extra http.Header) {
	resp, body, err := e.fetch(r, extra)
	if err != nil {
		http.Error(w, "origin unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	copyHeader(w.Header(), resp.Header)
	w.Header().Set("X-Cache", "Pass")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func (e *edge) fetch(r *http.Request, extra http.Header) (*http.Response, []byte, error) {
	e.originRequests.Add(1)
	origin := e.origins[e.next.Add(1)%uint64(len(e.origins))]
	target, err := url.Parse(origin + r.URL.RequestURI())
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), r.Body)
	if err != nil {
		return nil, nil, err
	}
	copyHeader(req.Header, r.Header)
	copyHeader(req.Header, extra)
	req.Header.Del("X-Edgekv-No-Forward") // the edge is a dumb client; let the origin route
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

type invalidateRequest struct {
	Paths []string `json:"paths"`
}

func (e *edge) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	if e.failInval {
		e.failedInvalidations.Add(1)
		http.Error(w, "invalidation API unavailable (simulated)", http.StatusServiceUnavailable)
		return
	}
	var req invalidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	e.invalidations.Add(1)
	apply := func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, p := range req.Paths {
			// A path invalidates every variant (query strings) of that key, as
			// CloudFront does for "/path*".
			for key := range e.cache {
				if key == p || strings.HasPrefix(key, p+"?") {
					delete(e.cache, key)
				}
			}
		}
	}
	if e.delay > 0 {
		time.AfterFunc(e.delay, apply)
	} else {
		apply()
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"paths":%d,"effective_in":%q}`, len(req.Paths), e.delay.String())
}

func (e *edge) handleStats(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	entries := len(e.cache)
	e.mu.Unlock()
	hits, misses, refresh := e.hits.Load(), e.misses.Load(), e.refreshHits.Load()
	total := hits + misses + refresh
	var ratio float64
	if total > 0 {
		ratio = float64(hits+refresh) / float64(total)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"entries": entries, "hits": hits, "misses": misses, "refresh_hits": refresh,
		"hit_ratio": ratio, "origin_requests": e.originRequests.Load(),
		"invalidations": e.invalidations.Load(), "failed_invalidations": e.failedInvalidations.Load(),
	})
}

func maxAge(h http.Header) time.Duration {
	for _, part := range strings.Split(h.Get("Cache-Control"), ",") {
		part = strings.TrimSpace(part)
		if v, ok := strings.CutPrefix(part, "max-age="); ok {
			if n, err := strconv.Atoi(v); err == nil {
				return time.Duration(n) * time.Second
			}
		}
	}
	return 0
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
}
