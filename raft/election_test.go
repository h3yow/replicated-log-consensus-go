package raft

import (
	"fmt"
	"testing"
)

func newCluster(t *testing.T, seed int64, n int, faults Faults) *Harness {
	t.Helper()
	h := NewHarness(seed, faults)
	peers := make([]NodeID, n)
	for i := 0; i < n; i++ {
		peers[i] = NodeID(fmt.Sprintf("N%d", i+1))
	}
	for _, id := range peers {
		h.AddNode(Config{
			ID:                id,
			Peers:             peers,
			ElectionTimeout:   10,
			HeartbeatTimeout:  2,
			MaxInflight:       8,
			SnapshotThreshold: 16,
		})
	}
	return h
}

// leaders 返回当前处于 Leader 角色的节点。
func leaders(h *Harness) []NodeID {
	var out []NodeID
	for _, id := range h.sortedIDs() {
		if h.Node(id).Status().Role == Leader {
			out = append(out, id)
		}
	}
	return out
}

// TestSingleLeaderPerTerm 同一任期不得出现两个被多数派承认的领导者。
func TestSingleLeaderPerTerm(t *testing.T) {
	h := newCluster(t, 42, 5, Faults{})
	leaderPerTerm := map[Term]NodeID{}
	for round := 0; round < 30; round++ {
		h.Run(5)
		ls := leaders(h)
		if len(ls) > 1 {
			t.Fatalf("tick=%d 出现多个领导者: %v", h.tick, ls)
		}
		if len(ls) == 1 {
			term := h.Node(ls[0]).Status().Term
			if prev, ok := leaderPerTerm[term]; ok && prev != ls[0] {
				t.Fatalf("任期 %d 出现两个领导者: %s 与 %s", term, prev, ls[0])
			}
			leaderPerTerm[term] = ls[0]
			t.Logf("tick=%d 领导者=%s 任期=%d", h.tick, ls[0], term)
		}
	}
	if len(leaderPerTerm) == 0 {
		t.Fatal("30 轮内未选出任何领导者")
	}
}

// TestVoteRejectReasons 投票拒绝原因必须可区分。
func TestVoteRejectReasons(t *testing.T) {
	peers := []NodeID{"A", "B", "C"}

	// 场景 1：候选人任期落后 -> TermStale。
	a := NewNode(Config{ID: "A", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	a.currentTerm = 5
	a.persist()
	a.Step(Message{Type: MsgRequestVote, From: "B", To: "A", Term: 3})
	resp := a.TakeMessages()[0]
	if resp.VoteGranted || resp.VoteRejectReason != VoteRejectTermStale {
		t.Fatalf("场景1 期望 TermStale 拒绝, 实际 granted=%v reason=%v", resp.VoteGranted, resp.VoteRejectReason)
	}
	t.Logf("场景1: 候选人任期=3 本地任期=5 -> 拒绝原因=%v", resp.VoteRejectReason)

	// 场景 2：本任期已投给他人 -> AlreadyVoted。
	b := NewNode(Config{ID: "B", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	b.Step(Message{Type: MsgRequestVote, From: "A", To: "B", Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	first := b.TakeMessages()[0]
	if !first.VoteGranted {
		t.Fatalf("场景2 首票应投出, 实际 reason=%v", first.VoteRejectReason)
	}
	b.Step(Message{Type: MsgRequestVote, From: "C", To: "B", Term: 1, LastLogIndex: 0, LastLogTerm: 0})
	second := b.TakeMessages()[0]
	if second.VoteGranted || second.VoteRejectReason != VoteRejectAlreadyVoted {
		t.Fatalf("场景2 期望 AlreadyVoted 拒绝, 实际 granted=%v reason=%v", second.VoteGranted, second.VoteRejectReason)
	}
	t.Logf("场景2: 任期=1 已投给 A, C 来拉票 -> 拒绝原因=%v", second.VoteRejectReason)

	// 场景 3：候选人日志落后 -> LogNotUpToDate。
	c := NewNode(Config{ID: "C", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	c.entries = []LogEntry{{Term: 2, Command: []byte("x")}}
	c.currentTerm = 2
	c.persist()
	c.Step(Message{Type: MsgRequestVote, From: "A", To: "C", Term: 3, LastLogIndex: 0, LastLogTerm: 0})
	third := c.TakeMessages()[0]
	if third.VoteGranted || third.VoteRejectReason != VoteRejectLogNotUpToDate {
		t.Fatalf("场景3 期望 LogNotUpToDate 拒绝, 实际 granted=%v reason=%v", third.VoteGranted, third.VoteRejectReason)
	}
	t.Logf("场景3: 本地(lastTerm=2,lastIndex=1) 候选人(lastTerm=0,lastIndex=0) -> 拒绝原因=%v", third.VoteRejectReason)
}

// TestOldLeaderStepsDownAfterPartition 分区恢复后旧领导者必须让位。
func TestOldLeaderStepsDownAfterPartition(t *testing.T) {
	h := newCluster(t, 7, 5, Faults{})
	h.Run(30) // 选出首个领导者
	old := leaders(h)
	if len(old) != 1 {
		t.Fatalf("期望 1 个领导者, 实际 %v", old)
	}
	oldID := old[0]
	oldTerm := h.Node(oldID).Status().Term
	t.Logf("分区前: 领导者=%s 任期=%d", oldID, oldTerm)

	// 隔离旧领导者，多数派选出新领导者。
	h.Partition(oldID)
	h.Run(60)
	// 旧领导者在分区中仍自以为是 Leader（符合协议），
	// 因此只在未隔离的多数派中找新领导者。
	var newLs []NodeID
	for _, id := range h.sortedIDs() {
		if id != oldID && h.Node(id).Status().Role == Leader {
			newLs = append(newLs, id)
		}
	}
	if len(newLs) != 1 {
		t.Fatalf("分区后期望多数派选出唯一新领导者, 实际 %v", newLs)
	}
	newTerm := h.Node(newLs[0]).Status().Term
	t.Logf("分区中: 新领导者=%s 任期=%d (旧领导者 %s 仍停留在任期 %d)",
		newLs[0], newTerm, oldID, h.Node(oldID).Status().Term)
	if newTerm <= oldTerm {
		t.Fatalf("新领导者任期 %d 应大于旧任期 %d", newTerm, oldTerm)
	}

	// 恢复分区：旧领导者看到更高任期后必须让位。
	h.Heal()
	h.Run(20)
	st := h.Node(oldID).Status()
	t.Logf("恢复后: 旧领导者角色=%v 任期=%d (让位原因=看到更高任期)", st.Role, st.Term)
	if st.Role == Leader {
		t.Fatalf("旧领导者 %s 恢复后仍为 Leader", oldID)
	}
	if st.Term < newTerm {
		t.Fatalf("旧领导者任期 %d 应至少推进到 %d", st.Term, newTerm)
	}
	// 全集群只应有一个领导者。
	if ls := leaders(h); len(ls) != 1 || ls[0] != newLs[0] {
		t.Fatalf("恢复后期望唯一领导者 %s, 实际 %v", newLs[0], ls)
	}
}

// TestElectionTimeoutDistinctFromRejection 选举超时重新发起选举时任期推进，
// 与投票拒绝是两条可区分路径。
func TestElectionTimeoutDistinctFromRejection(t *testing.T) {
	h := newCluster(t, 99, 3, Faults{DropProb: 1.0}) // 全部丢包：无人能获得多数票
	h.Run(50)
	for _, id := range h.sortedIDs() {
		st := h.Node(id).Status()
		t.Logf("节点=%s 角色=%v 任期=%d (丢包下选举超时反复自增任期)", id, st.Role, st.Term)
		if st.Role == Leader {
			t.Fatalf("全丢包下不应出现领导者: %s", id)
		}
		if st.Term == 0 {
			t.Fatalf("选举超时应推进任期: %s 任期仍为 0", id)
		}
	}
}
