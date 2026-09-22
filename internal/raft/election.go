package raft

import (
	"context"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// campaign runs one election attempt: a pre-vote round, and if a majority says
// it would vote for us, a real round.
//
// Why pre-vote: a node cut off from the group keeps timing out. Without
// pre-vote each timeout bumps its term, and when the partition heals that
// inflated term forces the healthy leader to step down for no reason. With
// pre-vote the isolated node never gets a majority of "yes" and so never
// touches its term.
func (r *Raft) campaign(skipPreVote bool) {
	defer func() {
		r.mu.Lock()
		r.campaigning = false
		r.mu.Unlock()
	}()

	r.mu.Lock()
	if r.stopped || r.state == Leader {
		r.mu.Unlock()
		return
	}
	r.resetElectionTimerLocked()

	if !skipPreVote {
		req := r.voteRequestLocked(r.term+1, true)
		r.mu.Unlock()
		if !r.collectVotes(req) {
			return
		}
		r.mu.Lock()
		// The world may have moved while we were asking.
		if r.stopped || r.state == Leader || r.term+1 != req.Term {
			r.mu.Unlock()
			return
		}
	}

	// Real election: new term, vote for ourselves, and make both durable
	// before asking anyone else. Otherwise a crash and restart could let us
	// vote a second time in the same term.
	r.term++
	r.votedFor = r.id
	r.state = Candidate
	r.leaderID = ""
	r.persistLocked(true, nil)
	r.resetElectionTimerLocked()
	r.notifyStateChangeLocked()
	req := r.voteRequestLocked(r.term, false)
	r.log.Debug("starting election", "term", r.term)
	r.mu.Unlock()

	if !r.collectVotes(req) {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Only become leader if this is still the election we won.
	if !r.stopped && r.state == Candidate && r.term == req.Term {
		r.becomeLeaderLocked()
	}
}

func (r *Raft) voteRequestLocked(term uint64, preVote bool) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		Group:        r.cfg.Group,
		Term:         term,
		CandidateId:  r.id,
		LastLogIndex: r.rlog.lastIndex(),
		LastLogTerm:  r.rlog.lastTerm(),
		PreVote:      preVote,
	}
}

// collectVotes sends req to every peer in parallel and reports whether a
// majority, counting our own vote, granted it.
func (r *Raft) collectVotes(req *pb.RequestVoteRequest) bool {
	if r.quorum == 1 {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	defer cancel()

	results := make(chan bool, len(r.others))
	for _, peer := range r.others {
		go func() {
			resp, err := r.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil {
				results <- false
				return
			}
			r.mu.Lock()
			if resp.Term > r.term {
				r.becomeFollowerLocked(resp.Term, "")
			}
			r.mu.Unlock()
			results <- resp.VoteGranted
		}()
	}

	granted, answered := 1, 0
	for answered < len(r.others) {
		select {
		case ok := <-results:
			answered++
			if ok {
				granted++
			}
			if granted >= r.quorum {
				return true
			}
			// Stop early once a majority is out of reach.
			if granted+(len(r.others)-answered) < r.quorum {
				return false
			}
		case <-r.stopCh:
			return false
		}
	}
	return false
}

// HandleRequestVote implements the receiver side of RequestVote (Raft §5.2, §5.4.1).
func (r *Raft) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.PreVote {
		return r.handlePreVoteLocked(req)
	}

	if req.Term < r.term {
		return &pb.RequestVoteResponse{Term: r.term}
	}
	if req.Term > r.term {
		r.becomeFollowerLocked(req.Term, "")
	}

	// One vote per term (first come, first served), and only for a candidate
	// whose log is at least as up to date as ours. The second condition is the
	// election restriction: since a committed entry lives on a majority, and a
	// winner needs a majority of votes, at least one voter holds the entry and
	// will refuse any candidate that lacks it.
	canVote := r.votedFor == "" || r.votedFor == req.CandidateId
	if !canVote || !r.logUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
		return &pb.RequestVoteResponse{Term: r.term}
	}

	r.votedFor = req.CandidateId
	r.persistLocked(true, nil) // durable before the reply leaves
	r.resetElectionTimerLocked()
	return &pb.RequestVoteResponse{Term: r.term, VoteGranted: true}
}

// handlePreVoteLocked answers "would you vote for me?". It never changes any
// state, which is the whole point.
func (r *Raft) handlePreVoteLocked(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	resp := &pb.RequestVoteResponse{Term: r.term}
	if req.Term <= r.term {
		return resp
	}
	// Leader stickiness: if we are the leader, or heard from one recently, the
	// group is healthy from where we stand and an election would only disrupt it.
	if r.state == Leader || time.Since(r.lastLeaderContact) < r.cfg.ElectionTimeoutMin {
		return resp
	}
	resp.VoteGranted = r.logUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	return resp
}

// logUpToDateLocked is the comparison from Raft §5.4.1: the later last term
// wins; with equal last terms the longer log wins.
func (r *Raft) logUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myTerm := r.rlog.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= r.rlog.lastIndex()
}

// HandleTimeoutNow makes this node campaign immediately, skipping pre-vote. It
// is sent by a leader that has chosen us as its successor.
func (r *Raft) HandleTimeoutNow(req *pb.TimeoutNowRequest) *pb.TimeoutNowResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term == r.term && r.state == Follower && !r.campaigning && !r.stopped {
		r.campaigning = true
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.campaign(true)
		}()
	}
	return &pb.TimeoutNowResponse{Term: r.term}
}
