package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestElectsExactlyOneLeader(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()

	time.Sleep(500 * time.Millisecond) // leadership must be stable when nothing fails
	if got := c.leaders(); len(got) != 1 || got[0] != leader {
		t.Fatalf("leadership changed without any failure: %d leaders", len(got))
	}
	term := leader.Status().Term
	for _, id := range c.ids {
		if st := c.nodes[id].Status(); st.Term != term || st.Leader != leader.id {
			t.Fatalf("%s: term %d leader %q, want term %d leader %q", id, st.Term, st.Leader, term, leader.id)
		}
	}
}

func TestSingleNodeGroup(t *testing.T) {
	c := newCluster(t, 1, 0)
	c.waitLeader()
	c.propose("solo")
	c.waitApplied(1)
	if err := c.nodes["n1"].ReadIndex(context.Background()); err != nil {
		t.Fatalf("ReadIndex on single node: %v", err)
	}
}

func TestReplicatesToAllNodes(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitLeader()
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
	}
	c.waitApplied(20)
	c.checkConsistent()
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.waitLeader()
	c.propose("before")

	c.crash(old.id)
	rest := others(c.ids, old.id)
	next := c.waitLeader(rest...)
	if next.Status().Term <= old.Status().Term {
		t.Fatalf("new leader's term %d is not above the old one's %d", next.Status().Term, old.Status().Term)
	}
	c.propose("after", rest...)
	c.waitApplied(2, rest...)

	// The old leader comes back, finds a higher term, and follows.
	c.start(old.id)
	c.waitApplied(2)
	c.checkConsistent()
	if c.nodes[old.id].IsLeader() && next.IsLeader() {
		t.Fatal("two leaders after the old one rejoined")
	}
}

// A leader on the minority side of a partition must not commit anything, and
// what it appended meanwhile must be erased once it sees the real leader.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.waitLeader()
	c.propose("committed-before")
	c.waitApplied(1)

	rest := others(c.ids, old.id)
	c.net.Partition([]string{old.id}, rest)

	// This write reaches only the isolated leader's own log.
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	_, err := old.Propose(ctx, []byte("lost-write"))
	cancel()
	if err == nil {
		t.Fatal("a leader without a majority committed a write")
	}

	c.waitLeader(rest...)
	c.propose("majority-write", rest...)
	c.waitApplied(2, rest...)

	c.net.Heal()
	c.waitApplied(2)
	c.checkConsistent()
	for _, id := range c.ids {
		for _, cmd := range c.sms[id].log() {
			if cmd == "lost-write" {
				t.Fatalf("%s applied a write that was never committed", id)
			}
		}
	}
}

func TestNoQuorumNoProgress(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()
	for _, id := range others(c.ids, leader.id) {
		c.crash(id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, []byte("x")); err == nil {
		t.Fatal("committed with only one of three nodes alive")
	}
	// Check-quorum: it should also notice and give up leadership.
	deadline := time.Now().Add(2 * time.Second)
	for leader.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if leader.IsLeader() {
		t.Fatal("leader kept its role after losing its majority")
	}
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()
	lagging := others(c.ids, leader.id)[0]

	c.net.SetDown(lagging, true)
	rest := others(c.ids, lagging)
	for i := 0; i < 200; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i), rest...)
	}
	c.net.SetDown(lagging, false)
	c.waitApplied(200)
	c.checkConsistent()
}

// Pre-vote: a node that was cut off must not come back with an inflated term
// and knock out a healthy leader.
func TestIsolatedNodeDoesNotDisruptLeader(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()
	term := leader.Status().Term
	isolated := others(c.ids, leader.id)[0]

	c.net.Partition([]string{isolated}, others(c.ids, isolated))
	time.Sleep(time.Second) // several election timeouts
	if got := c.nodes[isolated].Status().Term; got != term {
		t.Fatalf("isolated node raised its term from %d to %d", term, got)
	}

	c.net.Heal()
	time.Sleep(500 * time.Millisecond)
	if st := leader.Status(); st.State != Leader || st.Term != term {
		t.Fatalf("leader was disrupted: now %v in term %d (was term %d)", st.State, st.Term, term)
	}
}

func TestRestartRecoversFromStorage(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.waitLeader()
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
	}
	c.waitApplied(10)
	before := c.sms["n1"].log()

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id) // fresh state machines, same "disks"
	}
	c.waitLeader()
	c.propose("after-restart")
	c.waitApplied(11)
	c.checkConsistent()

	after := c.sms["n1"].log()
	for i, cmd := range before {
		if after[i] != cmd {
			t.Fatalf("position %d changed across restart: %q -> %q", i, cmd, after[i])
		}
	}
}

func TestSnapshotCompactsLogAndCatchesUpFollower(t *testing.T) {
	c := newCluster(t, 3, 10)
	leader := c.waitLeader()
	lagging := others(c.ids, leader.id)[0]

	c.net.SetDown(lagging, true)
	rest := others(c.ids, lagging)
	for i := 0; i < 100; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i), rest...)
	}
	c.waitApplied(100, rest...)

	leader = c.waitLeader(rest...)
	st := leader.Status()
	if st.SnapshotIndex == 0 {
		t.Fatal("leader never took a snapshot")
	}
	if kept := st.LastLogIndex - st.SnapshotIndex; kept > 30 {
		t.Fatalf("log was not compacted: %d entries still in memory", kept)
	}

	// The entries the follower needs are gone, so it must receive a snapshot.
	c.net.SetDown(lagging, false)
	c.waitApplied(100, lagging)
	c.checkConsistent()
	if c.nodes[lagging].Status().SnapshotIndex == 0 {
		t.Fatal("follower caught up without installing a snapshot")
	}

	// And it must be able to restart from that snapshot.
	c.crash(lagging)
	c.start(lagging)
	c.propose("after")
	c.waitApplied(101)
	c.checkConsistent()
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()
	c.propose("x")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex on leader: %v", err)
	}
	follower := c.nodes[others(c.ids, leader.id)[0]]
	if err := follower.ReadIndex(ctx); !isNotLeader(err) {
		t.Fatalf("ReadIndex on follower: got %v, want NotLeaderError", err)
	}

	// A deposed leader that has not noticed yet must not serve reads. This is
	// exactly the stale read that ReadIndex exists to prevent.
	c.net.Partition([]string{leader.id}, others(c.ids, leader.id))
	if err := leader.ReadIndex(ctx); err == nil {
		t.Fatal("partitioned leader served a linearizable read")
	}
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.waitLeader()
	c.propose("x")
	target := others(c.ids, leader.id)[0]

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leader.TransferLeadership(ctx, target); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if got := c.waitLeader(); got.id != target {
		t.Fatalf("leader is %s, want %s", got.id, target)
	}
	c.propose("y")
	c.waitApplied(2)
	c.checkConsistent()
}

// Everything at once: lossy, slow network, concurrent writers, leader crashes.
// Whatever happens, no two state machines may disagree.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 5, 50)
	c.waitLeader()
	c.net.SetUnreliable(0.1, 0, 15*time.Millisecond)

	var wg sync.WaitGroup
	const writers, perWriter = 4, 40
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				c.propose(fmt.Sprintf("w%d-%d", w, i))
			}
		}()
	}

	stop := make(chan struct{})
	var chaos sync.WaitGroup
	chaos.Add(1)
	go func() {
		defer chaos.Done()
		for round := 0; ; round++ {
			select {
			case <-stop:
				return
			case <-time.After(400 * time.Millisecond):
			}
			victim := c.ids[round%len(c.ids)]
			c.net.SetDown(victim, true)
			time.Sleep(300 * time.Millisecond)
			c.net.SetDown(victim, false)
		}
	}()

	wg.Wait()
	close(stop)
	chaos.Wait()
	c.net.SetUnreliable(0, 0, 0)

	c.propose("final")
	time.Sleep(time.Second)
	c.checkConsistent()

	// Every write was retried until acknowledged, so each must appear at least
	// once. (Exactly-once is the state machine's job, via request IDs.)
	seen := map[string]bool{}
	for _, cmd := range c.sms[c.waitLeader().id].log() {
		seen[cmd] = true
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			if cmd := fmt.Sprintf("w%d-%d", w, i); !seen[cmd] {
				t.Fatalf("acknowledged write %q is missing", cmd)
			}
		}
	}
}
