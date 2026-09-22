// Package cdn invalidates cached copies of a key at the edge after the key
// has been changed.
//
// Invalidation is strictly best effort. The bound on how stale a cached read
// can be never depends on it: that bound is the TTL the origin put in
// Cache-Control. A successful invalidation only makes the common case better
// than the bound; a failed one leaves the system exactly at the bound.
package cdn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Invalidator removes paths from an edge cache.
type Invalidator interface {
	Name() string
	Invalidate(ctx context.Context, paths []string) error
}

// Noop does nothing. With it the system runs in "TTL only" mode.
type Noop struct{}

func (Noop) Name() string                               { return "none" }
func (Noop) Invalidate(context.Context, []string) error { return nil }

// HTTPPurge invalidates the local edge cache simulator (cmd/edgekv-edge).
type HTTPPurge struct {
	Endpoints []string // base URLs of the edge caches
	Client    *http.Client
}

func (h *HTTPPurge) Name() string { return "edge-sim" }

func (h *HTTPPurge) Invalidate(ctx context.Context, paths []string) error {
	body, _ := json.Marshal(map[string][]string{"paths": paths})
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	var firstErr error
	for _, ep := range h.Endpoints {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep+"/_edge/invalidate", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 300 {
				err = fmt.Errorf("cdn: edge %s answered %s", ep, resp.Status)
			}
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Queue decouples writes from invalidation. A write is acknowledged as soon as
// Raft commits it; the invalidation happens afterwards, in the background,
// batched and retried. Making the write wait for the CDN would tie write
// latency and availability to a third-party API.
type Queue struct {
	inv      Invalidator
	log      *slog.Logger
	onResult func(ok bool, paths int)

	mu      sync.Mutex
	pending map[string]struct{}
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

// NewQueue starts the background worker. onResult, if set, is called once per
// batch for metrics.
func NewQueue(inv Invalidator, log *slog.Logger, onResult func(ok bool, paths int)) *Queue {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	q := &Queue{
		inv: inv, log: log, onResult: onResult,
		pending: make(map[string]struct{}),
		wake:    make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go q.run()
	return q
}

// Enqueue schedules a path. Duplicates collapse: if a key is written ten times
// before the worker runs, it is invalidated once.
func (q *Queue) Enqueue(path string) {
	if _, ok := q.inv.(Noop); ok {
		return
	}
	q.mu.Lock()
	q.pending[path] = struct{}{}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) Stop() {
	close(q.stop)
	<-q.done
}

func (q *Queue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.stop:
			return
		case <-q.wake:
		}
		// A short pause lets a burst of writes share one API call. CloudFront
		// bills per invalidated path and rate-limits requests.
		select {
		case <-q.stop:
			return
		case <-time.After(50 * time.Millisecond):
		}

		q.mu.Lock()
		paths := make([]string, 0, len(q.pending))
		for p := range q.pending {
			paths = append(paths, p)
		}
		q.pending = make(map[string]struct{})
		q.mu.Unlock()
		if len(paths) == 0 {
			continue
		}

		var err error
		for attempt, backoff := 0, 100*time.Millisecond; attempt < 4; attempt, backoff = attempt+1, backoff*2 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = q.inv.Invalidate(ctx, paths)
			cancel()
			if err == nil {
				break
			}
			select {
			case <-q.stop:
				return
			case <-time.After(backoff):
			}
		}
		if err != nil {
			// Give up. Cached copies now simply live out their TTL, which is
			// the staleness the API promised in the first place.
			q.log.Warn("invalidation failed; falling back to TTL expiry", "paths", len(paths), "err", err)
		}
		if q.onResult != nil {
			q.onResult(err == nil, len(paths))
		}
	}
}
