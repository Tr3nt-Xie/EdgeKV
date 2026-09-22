// Command edgekv-bench is a closed-loop load generator for EdgeKV.
//
// Each of -clients goroutines runs a loop: pick an operation according to the
// mix, pick a key according to the distribution, issue the request, record the
// latency, repeat. Throughput is therefore bounded by latency × clients, which
// is how a real service with a fixed number of callers behaves.
//
// Output is one JSON line per run so that runs can be collected and plotted.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/pkg/client"
)

type result struct {
	Name           string             `json:"name"`
	Clients        int                `json:"clients"`
	Duration       float64            `json:"duration_s"`
	Ops            uint64             `json:"ops"`
	Errors         uint64             `json:"errors"`
	Throughput     float64            `json:"ops_per_s"`
	Latency        map[string]latency `json:"latency_ms"`
	CacheHits      uint64             `json:"cache_hits,omitempty"`
	CacheReads     uint64             `json:"cache_reads,omitempty"`
	StaleReads     uint64             `json:"stale_reads,omitempty"`
	MaxStalenessMs float64            `json:"max_staleness_ms,omitempty"`
	Config         map[string]any     `json:"config"`
}

type latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
	N   int     `json:"n"`
}

type sample struct {
	op string
	ms float64
}

func main() {
	var (
		nodes     = flag.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083", "node base URLs")
		edge      = flag.String("edge", "", "edge cache base URL for cached reads (empty: read from origin)")
		name      = flag.String("name", "run", "label for this run")
		clients   = flag.Int("clients", 10, "concurrent clients")
		duration  = flag.Duration("duration", 10*time.Second, "how long to run")
		keys      = flag.Int("keys", 100_000, "key space size")
		valueSize = flag.Int("value-size", 512, "value size in bytes")
		mix       = flag.String("mix", "get=80,put=15,delete=5", "operation mix in percent; ops: get, put, delete, cas, cget (cached get)")
		dist      = flag.String("dist", "uniform", "key distribution: uniform | zipf")
		zipfS     = flag.Float64("zipf-s", 1.1, "Zipf skew (higher = hotter hot keys)")
		cacheable = flag.Float64("cacheable", 0.2, "fraction of keys written as cacheable (namespace config:)")
		ttl       = flag.Uint("ttl", 30, "TTL for cacheable keys, seconds")
		preload   = flag.Bool("preload", true, "write every key once before measuring")
		warmup    = flag.Duration("warmup", 2*time.Second, "warm-up period excluded from results")
		out       = flag.String("out", "", "append JSON result to this file (default: stdout only)")
	)
	flag.Parse()

	ops, err := parseMix(*mix)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	nodeList := strings.Split(*nodes, ",")
	c := client.New(nodeList, client.WithEdge(*edge))
	value := strings.Repeat("x", *valueSize)

	keyName := func(i int) string {
		if float64(i%1000)/1000 < *cacheable {
			return fmt.Sprintf("config:item:%d", i)
		}
		return fmt.Sprintf("task:%d:status", i)
	}
	policyFor := func(key string) *client.CachePolicy {
		if strings.HasPrefix(key, "config:") {
			return &client.CachePolicy{Cacheable: true, TTLSeconds: uint32(*ttl)}
		}
		return nil
	}

	if *preload {
		fmt.Fprintf(os.Stderr, "preloading %d keys...\n", *keys)
		var wg sync.WaitGroup
		work := make(chan int, 1024)
		for w := 0; w < 64; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					key := keyName(i)
					if _, err := c.Put(context.Background(), key, value, policyFor(key)); err != nil {
						fmt.Fprintln(os.Stderr, "preload:", err)
					}
				}
			}()
		}
		for i := 0; i < *keys; i++ {
			work <- i
		}
		close(work)
		wg.Wait()
	}

	// Track the latest version written for each cacheable key so that stale
	// cached reads can be detected: a read returning an older version than
	// one acknowledged before the read started is stale.
	var latest sync.Map // key -> struct{version uint64; at time.Time}
	type stamp struct {
		version uint64
		at      time.Time
	}

	var (
		samplesMu                                      sync.Mutex
		samples                                        []sample
		total, errs, cacheHits, cacheReads, staleReads atomic.Uint64
		maxStaleMu                                     sync.Mutex
		maxStale                                       time.Duration
		measuring                                      atomic.Bool
	)
	record := func(op string, d time.Duration, err error) {
		if !measuring.Load() {
			return
		}
		total.Add(1)
		if err != nil {
			errs.Add(1)
			return
		}
		samplesMu.Lock()
		samples = append(samples, sample{op, float64(d.Microseconds()) / 1000})
		samplesMu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*duration)
	defer cancel()
	go func() {
		time.Sleep(*warmup)
		measuring.Store(true)
	}()

	var wg sync.WaitGroup
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			var zipf *rand.Zipf
			if *dist == "zipf" {
				zipf = rand.NewZipf(rng, *zipfS, 1, uint64(*keys-1))
			}
			nextKey := func() int {
				if zipf != nil {
					return int(zipf.Uint64())
				}
				return rng.IntN(*keys)
			}
			for ctx.Err() == nil {
				op := ops[rng.IntN(len(ops))]
				key := keyName(nextKey())
				start := time.Now()
				var err error
				switch op {
				case "get":
					_, err = c.Get(ctx, key)
				case "cget":
					// Cached reads only make sense on cacheable keys; keep drawing
					// until one comes up so the key distribution is preserved.
					for !strings.HasPrefix(key, "config:") {
						key = keyName(nextKey())
					}
					var e client.Entry
					e, err = c.CachedGet(ctx, key)
					if err == nil && measuring.Load() {
						cacheReads.Add(1)
						if e.FromCache {
							cacheHits.Add(1)
						}
						if v, ok := latest.Load(key); ok {
							if s := v.(stamp); e.Version < s.version && start.After(s.at) {
								staleReads.Add(1)
								if age := start.Sub(s.at); age > 0 {
									maxStaleMu.Lock()
									maxStale = max(maxStale, age)
									maxStaleMu.Unlock()
								}
							}
						}
					}
				case "put":
					var v uint64
					v, err = c.Put(ctx, key, value, policyFor(key))
					if err == nil && strings.HasPrefix(key, "config:") {
						latest.Store(key, stamp{version: v, at: time.Now()})
					}
				case "cas":
					var e client.Entry
					if e, err = c.Get(ctx, key); err == nil {
						_, err = c.CAS(ctx, key, value, e.Version, policyFor(key))
					}
				case "delete":
					_, err = c.Delete(ctx, key)
				}
				if err == client.ErrNotFound || err == client.ErrVersionMismatch || err == client.ErrNotCacheable {
					err = nil // expected outcomes, not failures
				}
				if ctx.Err() != nil {
					break
				}
				record(op, time.Since(start), err)
			}
		}(uint64(w + 1))
	}
	wg.Wait()

	res := result{
		Name: *name, Clients: *clients, Duration: duration.Seconds(),
		Ops: total.Load(), Errors: errs.Load(),
		Throughput: float64(total.Load()-errs.Load()) / duration.Seconds(),
		Latency:    map[string]latency{},
		CacheHits:  cacheHits.Load(), CacheReads: cacheReads.Load(), StaleReads: staleReads.Load(),
		MaxStalenessMs: float64(maxStale.Milliseconds()),
		Config: map[string]any{
			"mix": *mix, "dist": *dist, "keys": *keys, "value_size": *valueSize,
			"cacheable": *cacheable, "ttl": *ttl, "edge": *edge, "nodes": nodeList,
		},
	}
	byOp := map[string][]float64{}
	var all []float64
	for _, s := range samples {
		byOp[s.op] = append(byOp[s.op], s.ms)
		all = append(all, s.ms)
	}
	res.Latency["all"] = percentiles(all)
	for op, xs := range byOp {
		res.Latency[op] = percentiles(xs)
	}

	line, _ := json.Marshal(res)
	fmt.Println(string(line))
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(line, '\n'))
			f.Close()
		}
	}
	fmt.Fprintf(os.Stderr, "%s: %d clients, %.0f ops/s, p50 %.2fms p95 %.2fms p99 %.2fms, %d errors\n",
		*name, *clients, res.Throughput, res.Latency["all"].P50, res.Latency["all"].P95, res.Latency["all"].P99, res.Errors)
}

func percentiles(xs []float64) latency {
	if len(xs) == 0 {
		return latency{}
	}
	sort.Float64s(xs)
	at := func(p float64) float64 { return xs[min(len(xs)-1, int(float64(len(xs))*p))] }
	return latency{P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: xs[len(xs)-1], N: len(xs)}
}

// parseMix expands "get=80,put=15,delete=5" into a 100-element slice to draw from.
func parseMix(spec string) ([]string, error) {
	var ops []string
	for _, part := range strings.Split(spec, ",") {
		name, pct, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad mix entry %q", part)
		}
		var n int
		if _, err := fmt.Sscanf(pct, "%d", &n); err != nil {
			return nil, fmt.Errorf("bad percentage in %q", part)
		}
		for i := 0; i < n; i++ {
			ops = append(ops, name)
		}
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("empty mix")
	}
	return ops, nil
}
