// Command edgekv runs one EdgeKV node: a replica of every shard, the gRPC
// endpoint for Raft traffic, and the public HTTP API.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Tr3nt-Xie/EdgeKV/internal/cdn"
	"github.com/Tr3nt-Xie/EdgeKV/internal/metrics"
	"github.com/Tr3nt-Xie/EdgeKV/internal/node"
	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
	"github.com/Tr3nt-Xie/EdgeKV/internal/server"
	"github.com/Tr3nt-Xie/EdgeKV/internal/transport"
)

func main() {
	var (
		id         = flag.String("id", env("EDGEKV_ID", ""), "this node's ID; must appear in -cluster")
		cluster    = flag.String("cluster", env("EDGEKV_CLUSTER", ""), "comma-separated id=raftHost:port|httpBaseURL for every node")
		httpAddr   = flag.String("http", env("EDGEKV_HTTP", ":8080"), "listen address of the public HTTP API")
		raftAddr   = flag.String("raft", env("EDGEKV_RAFT", ":9090"), "listen address for Raft gRPC traffic")
		dataDir    = flag.String("data", env("EDGEKV_DATA", "./data"), "directory for WAL and snapshots")
		shards     = flag.Int("shards", envInt("EDGEKV_SHARDS", 3), "number of shards (Raft groups); must be identical on every node, forever")
		fsync      = flag.Bool("fsync", envBool("EDGEKV_FSYNC", true), "fsync the Raft log before acknowledging; false is for benchmarks only")
		snapEvery  = flag.Uint64("snapshot-threshold", uint64(envInt("EDGEKV_SNAPSHOT_THRESHOLD", 10000)), "applied entries between snapshots; 0 disables")
		heartbeat  = flag.Duration("heartbeat", 50*time.Millisecond, "Raft heartbeat interval")
		election   = flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout; the maximum is twice this")
		rebalance  = flag.Duration("rebalance-interval", 3*time.Second, "how often leaders are moved back to their preferred node; 0 disables")
		namespaces = flag.String("cacheable-namespaces", env("EDGEKV_CACHEABLE_NAMESPACES", "config:,feature:,agent-template:,prompt:"), "key prefixes that may be served from /v1/cache")
		invalidate = flag.String("invalidator", env("EDGEKV_INVALIDATOR", "none"), "none | edge:<url>[,<url>] | cloudfront:<distribution-id>")
		verbose    = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	raftAddrs, httpURLs, err := parseCluster(*cluster)
	if err != nil {
		fatal(log, "bad -cluster", err)
	}
	if _, ok := raftAddrs[*id]; !ok {
		fatal(log, "bad -id", fmt.Errorf("node %q is not listed in -cluster", *id))
	}
	peers := make([]string, 0, len(raftAddrs))
	for p := range raftAddrs {
		peers = append(peers, p)
	}
	sort.Strings(peers)

	reg := metrics.NewRegistry()
	reg.Help("edgekv_raft_leader_changes_total", "Times this node saw a shard's leader change.")
	reg.Help("edgekv_cdn_invalidations_total", "Invalidation batches by result.")

	rpc := transport.NewClient(raftAddrs)
	defer rpc.Close()

	n, err := node.New(node.Config{
		ID: *id, Peers: peers, NumShards: *shards, DataDir: *dataDir,
		Transport: rpc, Sync: *fsync, SnapshotThreshold: *snapEvery,
		HeartbeatInterval: *heartbeat, ElectionTimeoutMin: *election, ElectionTimeoutMax: 2 * *election,
		RebalanceInterval: *rebalance,
		Logger:            log,
		OnLeaderChange: func(shardID uint32, st raft.Status) {
			reg.Inc("edgekv_raft_leader_changes_total", map[string]string{"shard": strconv.Itoa(int(shardID))})
			log.Info("raft state change", "shard", shardID, "state", st.State.String(), "term", st.Term, "leader", st.Leader)
		},
	})
	if err != nil {
		fatal(log, "cannot start node", err)
	}

	// Raft traffic.
	lis, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		fatal(log, "cannot listen for raft", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, &transport.Server{Resolve: n.Handler})
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc server stopped", "err", err)
		}
	}()

	// Cache invalidation.
	inv, err := newInvalidator(*invalidate)
	if err != nil {
		fatal(log, "bad -invalidator", err)
	}
	queue := cdn.NewQueue(inv, log, func(ok bool, paths int) {
		result := "ok"
		if !ok {
			result = "failed"
		}
		reg.Inc("edgekv_cdn_invalidations_total", map[string]string{"result": result})
	})
	defer queue.Stop()

	n.Start()

	// Public API.
	api := server.New(server.Config{
		NodeID: *id, Node: n, PeerHTTP: httpURLs,
		CacheableNamespaces: splitNonEmpty(*namespaces),
		Invalidations:       queue, Metrics: reg, Logger: log,
	})
	httpServer := &http.Server{Addr: *httpAddr, Handler: api, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal(log, "http server failed", err)
		}
	}()
	log.Info("edgekv started", "id", *id, "http", *httpAddr, "raft", *raftAddr,
		"shards", *shards, "fsync", *fsync, "invalidator", inv.Name(), "peers", peers)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)
	grpcServer.Stop()
	n.Stop()
}

// parseCluster parses "n1=host:9090|http://host:8080,n2=...".
func parseCluster(spec string) (raftAddrs, httpURLs map[string]string, err error) {
	raftAddrs, httpURLs = map[string]string{}, map[string]string{}
	for _, item := range splitNonEmpty(spec) {
		id, rest, ok := strings.Cut(item, "=")
		if !ok {
			return nil, nil, fmt.Errorf("%q: want id=raftAddr|httpURL", item)
		}
		raftAddr, httpURL, ok := strings.Cut(rest, "|")
		if !ok {
			return nil, nil, fmt.Errorf("%q: want id=raftAddr|httpURL", item)
		}
		raftAddrs[id], httpURLs[id] = raftAddr, strings.TrimRight(httpURL, "/")
	}
	if len(raftAddrs) == 0 {
		return nil, nil, fmt.Errorf("no nodes given")
	}
	return raftAddrs, httpURLs, nil
}

func newInvalidator(spec string) (cdn.Invalidator, error) {
	kind, arg, _ := strings.Cut(spec, ":")
	switch kind {
	case "", "none":
		return cdn.Noop{}, nil
	case "edge":
		return &cdn.HTTPPurge{Endpoints: splitNonEmpty(arg)}, nil
	case "cloudfront":
		if arg == "" {
			return nil, fmt.Errorf("cloudfront needs a distribution ID")
		}
		return cdn.NewCloudFront(context.Background(), arg)
	}
	return nil, fmt.Errorf("unknown invalidator %q", spec)
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if b, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return b
	}
	return fallback
}

func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}
