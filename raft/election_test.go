package raft

import "testing"

func testConfig() Config {
	return Config{
		ElectionTimeout:  10,
		HeartbeatTick:    3,
		MaxEntriesPerMsg: 16,
	}
}

// TestSingleLeaderPerTerm 验证：同一任期内不会出现两个被多数派承认的领导者。
func TestSingleLeaderPerTerm(t *testing.T) {
	sim := NewSim(3, testConfig(), 42)
	// 记录每个任期出现过的领导者。
	leadersByTerm := map[Term]NodeID{}
	for i := 0; i < 200; i++ {
		sim.Tick()
		for _, id := range []NodeID{1, 2, 3} {
			n := sim.Node(id)
			if n.Role() == Leader {
				if prev, ok := leadersByTerm[n.Term()]; ok && prev != id {
					t.Fatalf("term %d 出现两个领导者: %d 与 %d\n%s", n.Term(), prev, id, sim.DumpState())
				}
				leadersByTerm[n.Term()] = id
			}
		}
	}
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("200 tick 后无唯一领导者\n%s", sim.DumpState())
	}
	t.Logf("最终领导者=%d term=%d", leader, sim.Node(leader).Term())
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestOldLeaderStepsDownAfterPartition 验证：分区恢复后旧领导者必须让位。
func TestOldLeaderStepsDownAfterPartition(t *testing.T) {
	sim := NewSim(3, testConfig(), 7)
	sim.Run(50)
	oldLeader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	oldTerm := sim.Node(oldLeader).Term()
	t.Logf("旧领导者=%d term=%d", oldLeader, oldTerm)

	// 隔离旧领导者，其余两节点应选出新领导者（更高任期）。
	sim.Partition(oldLeader)
	sim.Run(80)
	var newLeader NodeID
	for _, id := range []NodeID{1, 2, 3} {
		if id != oldLeader && sim.Node(id).Role() == Leader {
			newLeader = id
		}
	}
	if newLeader == 0 {
		t.Fatalf("分区后未选出新领导者\n%s", sim.DumpState())
	}
	newTerm := sim.Node(newLeader).Term()
	t.Logf("新领导者=%d term=%d (旧 term=%d)", newLeader, newTerm, oldTerm)
	if newTerm <= oldTerm {
		t.Fatalf("新领导者任期未推进: new=%d old=%d", newTerm, oldTerm)
	}

	// 恢复分区：旧领导者收到更高任期心跳后必须让位。
	sim.Heal()
	sim.Run(20)
	n := sim.Node(oldLeader)
	t.Logf("恢复后旧领导者: role=%s term=%d stepDown=%s", n.Role(), n.Term(), n.LastStepDown())
	if n.Role() != Follower {
		t.Fatalf("旧领导者未让位: role=%s\n%s", n.Role(), sim.DumpState())
	}
	if n.Term() < newTerm {
		t.Fatalf("旧领导者任期未跟上: %d < %d", n.Term(), newTerm)
	}
	if n.LastStepDown() != StepDownHigherTerm && n.LastStepDown() != StepDownLeaderDiscovered {
		t.Fatalf("让位原因不可解释: %s", n.LastStepDown())
	}
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestVoteRejectReasons 验证：投票拒绝原因可区分。
func TestVoteRejectReasons(t *testing.T) {
	sim := NewSim(3, testConfig(), 99)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者")
	}
	// 领导者追加条目，使某节点日志落后。
	sim.Node(leader).Propose([]byte("cmd-1"))
	sim.Run(20)

	follower := NodeID(0)
	third := NodeID(0)
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			if follower == 0 {
				follower = id
			} else {
				third = id
			}
		}
	}
	f := sim.Node(follower)

	// 场景1：过期任期的投票请求（来自成员）→ RejectStaleTerm。
	f.Step(Message{Type: MsgRequestVote, From: third, To: follower, Term: f.Term() - 1})
	if f.LastVoteDecision() != RejectStaleTerm {
		t.Fatalf("期望 RejectStaleTerm，得到 %s", f.LastVoteDecision())
	}
	t.Logf("node=%d 拒绝过期任期投票: reason=%s", follower, f.LastVoteDecision())

	// 场景2：本任期已投票 → RejectAlreadyVoted。
	// 用与本地日志完全一致的信息请求更高任期，应先被批准。
	lastIdx := f.LastLogIndex()
	lastEntry, _ := f.EntryAt(lastIdx)
	f.Step(Message{Type: MsgRequestVote, From: third, To: follower, Term: f.Term() + 1,
		LastLogIndex: lastIdx, LastLogTerm: lastEntry.Term})
	if f.LastVoteDecision() != VoteGranted {
		t.Fatalf("期望 VoteGranted，得到 %s", f.LastVoteDecision())
	}
	f.Step(Message{Type: MsgRequestVote, From: leader, To: follower, Term: f.Term(),
		LastLogIndex: lastIdx, LastLogTerm: lastEntry.Term})
	if f.LastVoteDecision() != RejectAlreadyVoted {
		t.Fatalf("期望 RejectAlreadyVoted，得到 %s", f.LastVoteDecision())
	}
	t.Logf("node=%d 拒绝重复投票: reason=%s", follower, f.LastVoteDecision())

	// 场景3：候选人日志落后（来自成员）→ RejectLogNotUpToDate。
	f2 := sim.Node(leader) // 领导者日志最新
	f2.Step(Message{Type: MsgRequestVote, From: follower, To: leader, Term: f2.Term() + 1,
		LastLogIndex: 0, LastLogTerm: 0})
	if f2.LastVoteDecision() != RejectLogNotUpToDate {
		t.Fatalf("期望 RejectLogNotUpToDate，得到 %s", f2.LastVoteDecision())
	}
	t.Logf("node=%d 拒绝日志落后候选人: reason=%s", leader, f2.LastVoteDecision())

	// 场景4：非成员候选人 → RejectNotMember，且其更高任期不被采纳
	// （被摘除节点不得凭旧认知扰乱集群）。
	before := f.Term()
	f.Step(Message{Type: MsgRequestVote, From: 99, To: follower, Term: before + 5,
		LastLogIndex: lastIdx, LastLogTerm: lastEntry.Term})
	if f.LastVoteDecision() != RejectNotMember {
		t.Fatalf("期望 RejectNotMember，得到 %s", f.LastVoteDecision())
	}
	if f.Term() != before {
		t.Fatalf("非成员候选人的任期被采纳: term %d -> %d", before, f.Term())
	}
	t.Logf("node=%d 拒绝非成员候选人: reason=%s 本地任期保持=%d", follower, f.LastVoteDecision(), f.Term())
}
