// Package metrics is a small Prometheus-compatible metrics registry: counters,
// gauges and histograms, rendered in the text exposition format.
//
// It is hand-written to keep the dependency tree small and because the format
// is simple. Anything that scrapes Prometheus endpoints (Prometheus itself, the
// CloudWatch agent, Grafana Agent) can read it.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultBuckets are latency buckets in seconds, from 100µs to 10s.
var DefaultBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds metrics and renders them.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*atomic.Uint64
	histograms map[string]*histogram
	help       map[string]string
	collectors []func(emit func(name string, labels map[string]string, value float64))
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*atomic.Uint64),
		histograms: make(map[string]*histogram),
		help:       make(map[string]string),
	}
}

type histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []uint64 // counts[i] = observations <= buckets[i]; last slot is +Inf
	sum     float64
}

// Inc adds 1 to a counter.
func (r *Registry) Inc(name string, labels map[string]string) { r.Add(name, labels, 1) }

// Add adds n to a counter.
func (r *Registry) Add(name string, labels map[string]string, n uint64) {
	key := seriesKey(name, labels)
	r.mu.Lock()
	c, ok := r.counters[key]
	if !ok {
		c = &atomic.Uint64{}
		r.counters[key] = c
	}
	r.mu.Unlock()
	c.Add(n)
}

// Observe records a value (for latencies: seconds) into a histogram.
func (r *Registry) Observe(name string, labels map[string]string, v float64) {
	key := seriesKey(name, labels)
	r.mu.Lock()
	h, ok := r.histograms[key]
	if !ok {
		h = &histogram{buckets: DefaultBuckets, counts: make([]uint64, len(DefaultBuckets)+1)}
		r.histograms[key] = h
	}
	r.mu.Unlock()

	h.mu.Lock()
	idx := sort.SearchFloat64s(h.buckets, v)
	h.counts[idx]++
	h.sum += v
	h.mu.Unlock()
}

// Collect registers a function that is called on every scrape to emit gauges
// whose value is read from live state (Raft term, commit index, ...).
func (r *Registry) Collect(fn func(emit func(name string, labels map[string]string, value float64))) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, fn)
}

// Help sets the HELP text of a metric.
func (r *Registry) Help(name, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.help[name] = text
}

// Write renders every metric in the Prometheus text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	counters := make(map[string]uint64, len(r.counters))
	for k, c := range r.counters {
		counters[k] = c.Load()
	}
	histograms := make(map[string]*histogram, len(r.histograms))
	for k, h := range r.histograms {
		histograms[k] = h
	}
	collectors := append([]func(func(string, map[string]string, float64)){}, r.collectors...)
	help := r.help
	r.mu.Unlock()

	var lines []string
	for key, v := range counters {
		lines = append(lines, fmt.Sprintf("%s %d", key, v))
	}
	for _, fn := range collectors {
		fn(func(name string, labels map[string]string, value float64) {
			lines = append(lines, fmt.Sprintf("%s %s", seriesKey(name, labels), formatFloat(value)))
		})
	}
	for key, h := range histograms {
		name, labels := splitKey(key)
		h.mu.Lock()
		var cumulative uint64
		for i, upper := range h.buckets {
			cumulative += h.counts[i]
			lines = append(lines, fmt.Sprintf("%s_bucket%s %d", name, withLabel(labels, "le", formatFloat(upper)), cumulative))
		}
		cumulative += h.counts[len(h.buckets)]
		lines = append(lines, fmt.Sprintf("%s_bucket%s %d", name, withLabel(labels, "le", "+Inf"), cumulative))
		lines = append(lines, fmt.Sprintf("%s_sum%s %s", name, labels, formatFloat(h.sum)))
		lines = append(lines, fmt.Sprintf("%s_count%s %d", name, labels, cumulative))
		h.mu.Unlock()
	}
	sort.Strings(lines)

	seen := map[string]bool{}
	for _, line := range lines {
		name := line[:strings.IndexAny(line, "{ ")]
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			name = strings.TrimSuffix(name, suffix)
		}
		if text, ok := help[name]; ok && !seen[name] {
			seen[name] = true
			fmt.Fprintf(w, "# HELP %s %s\n", name, text)
		}
		fmt.Fprintln(w, line)
	}
}

func seriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, labels[k])
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func splitKey(key string) (name, labels string) {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		return key[:i], key[i:]
	}
	return key, ""
}

func withLabel(labels, k, v string) string {
	extra := fmt.Sprintf("%s=%q", k, v)
	if labels == "" {
		return "{" + extra + "}"
	}
	return labels[:len(labels)-1] + "," + extra + "}"
}

func formatFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
