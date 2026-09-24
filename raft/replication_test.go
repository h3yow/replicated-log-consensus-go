package raft

import (
	"fmt"
	"testing"
)

// mustLeader 运行直到选出唯一领导者。
func mustLeader(t *testing.T, h *Harness) NodeID {
	t.Helper()
	for i := 0; i < 50; i++ {
		h.Run(5)
		if ls := leaders(h); len(ls) == 1 {
			return ls[0]
		}
	}
	t.Fatal("未能选出领导者")
	return ""
}

// TestReplicateAndCommit 多数派确认后条目提交并仅应用一次。
func TestReplicateAndCommit(t *testing.T) {
	h := newCluster(t, 1, 3, Faults{})
	leader := mustLeader(t, h)
	if !h.Propose(leader, []byte("set x=1")) {
		t.Fatal("领导者 Propose 应成功")
	}
	h.Run(10)
	got := h.Committed()
	if len(got) != 3 {
		t.Fatalf("期望 3 个节点都提交, 实际 %d: %v", len(got), got)
	}
	for id, c := range got {
		st := h.Node(id).Status()
		t.Logf("节点=%s 任期=%d 提交索引=%d 起始索引=%d 条目数=%d 命令=%s",
			id, st.Term, st.CommitIndex, c.From, len(c.Entries), c.Entries[0].Command)
		if c.From != 1 || len(c.Entries) != 1 || string(c.Entries[0].Command) != "set x=1" {
			t.Fatalf("节点 %s 提交内容不符: from=%d entries=%v", id, c.From, c.Entries)
		}
	}
	// 再次取出应为空：不得重复应用。
	if again := h.Committed(); len(again) != 0 {
		t.Fatalf("已提交内容被重复应用: %v", again)
	}
}

// TestNoCommitWithoutMajority 少数派不可提交，未提交条目不得对外生效。
func TestNoCommitWithoutMajority(t *testing.T) {
	h := newCluster(t, 2, 5, Faults{})
	leader := mustLeader(t, h)
	// 隔离 3 个节点，领导者只能到达 1 个追随者（2/5 不足多数）。
	others := []NodeID{}
	for _, id := range h.sortedIDs() {
		if id != leader {
			others = append(others, id)
		}
	}
	h.Partition(others[0], others[1], others[2])
	h.Propose(leader, []byte("cmd-no-majority"))
	h.Run(20)
	st := h.Node(leader).Status()
	t.Logf("领导者=%s 任期=%d lastIndex=%d commitIndex=%d (不足多数不得提交)",
		leader, st.Term, st.LastIndex, st.CommitIndex)
	if st.CommitIndex != 0 {
		t.Fatalf("不足多数派时提交了条目: commitIndex=%d", st.CommitIndex)
	}
	if got := h.Committed(); len(got) != 0 {
		t.Fatalf("未提交条目对外生效: %v", got)
	}
}

// TestOldTermEntryNotDirectlyCommitted 旧任期条目即使复制到多数派，
// 也不能直接提交，只能随本任期条目间接提交（Raft §5.4.2）。
func TestOldTermEntryNotDirectlyCommitted(t *testing.T) {
	peers := []NodeID{"A", "B", "C"}
	// 手工构造：A 是任期 2 的领导者，日志里有一条任期 1 的条目。
	a := NewNode(Config{ID: "A", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	a.currentTerm = 2
	a.entries = []LogEntry{{Term: 1, Command: []byte("old")}}
	a.role = Leader
	a.matchIndex = map[NodeID]Index{"A": 1, "B": 1, "C": 0} // 多数派已复制旧任期条目
	a.nextIndex = map[NodeID]Index{"A": 2, "B": 2, "C": 1}
	a.persist()
	a.maybeCommit()
	if a.commitIndex != 0 {
		t.Fatalf("旧任期条目被直接提交: commitIndex=%d", a.commitIndex)
	}
	t.Logf("旧任期条目复制到多数派后 commitIndex=%d (不可直接提交)", a.commitIndex)
	// 追加本任期条目并复制到多数派后，旧条目随之间接提交。
	a.entries = append(a.entries, LogEntry{Term: 2, Command: []byte("new")})
	a.matchIndex["A"] = 2
	a.matchIndex["B"] = 2
	a.persist()
	a.maybeCommit()
	if a.commitIndex != 2 {
		t.Fatalf("本任期条目应提交并带动旧条目: commitIndex=%d", a.commitIndex)
	}
	t.Logf("本任期条目复制到多数派后 commitIndex=%d (旧条目间接提交)", a.commitIndex)
}

// TestRestartNoLossNoReapply 重启后已提交内容不丢失、不重复应用。
func TestRestartNoLossNoReapply(t *testing.T) {
	h := newCluster(t, 3, 3, Faults{})
	leader := mustLeader(t, h)
	h.Propose(leader, []byte("k1"))
	h.Run(10)
	before := h.Committed()
	if len(before) != 3 {
		t.Fatalf("期望 3 节点提交, 实际 %d", len(before))
	}

	// 重启所有节点。
	for _, id := range h.sortedIDs() {
		h.Restart(id)
	}
	// 重启后不得重复交付已应用条目。
	if got := h.Committed(); len(got) != 0 {
		t.Fatalf("重启后重复应用已提交条目: %v", got)
	}
	// 已提交内容不得丢失。
	for _, id := range h.sortedIDs() {
		meta, entries := h.Node(id).CommittedView()
		st := h.Node(id).Status()
		t.Logf("重启后 节点=%s 任期=%d 快照点=%d 已提交条目数=%d", id, st.Term, meta.Index, len(entries))
		if len(entries) != 1 || string(entries[0].Command) != "k1" {
			t.Fatalf("节点 %s 重启后已提交内容丢失: %v", id, entries)
		}
	}
	// 集群继续工作：新提案照常提交。
	leader2 := mustLeader(t, h)
	h.Propose(leader2, []byte("k2"))
	h.Run(10)
	got := h.Committed()
	if len(got) != 3 {
		t.Fatalf("重启后期望 3 节点提交新条目, 实际 %d", len(got))
	}
	for id, c := range got {
		if string(c.Entries[len(c.Entries)-1].Command) != "k2" {
			t.Fatalf("节点 %s 新提交内容不符: %v", id, c.Entries)
		}
	}
}

// TestDuplicateDeliveryIdempotent 重复投递不得导致重复应用或日志膨胀。
func TestDuplicateDeliveryIdempotent(t *testing.T) {
	h := newCluster(t, 4, 3, Faults{DuplicateProb: 1.0}) // 每条消息都重复一次
	leader := mustLeader(t, h)
	for i := 0; i < 5; i++ {
		h.Propose(leader, []byte(fmt.Sprintf("cmd-%d", i)))
		h.Run(5)
	}
	h.Run(10)
	applied := map[NodeID]int{}
	total := h.Committed()
	for id, c := range total {
		applied[id] = len(c.Entries)
		st := h.Node(id).Status()
		t.Logf("节点=%s 任期=%d lastIndex=%d commitIndex=%d 本次应用=%d",
			id, st.Term, st.LastIndex, st.CommitIndex, len(c.Entries))
	}
	for id, n := range applied {
		if n != 5 {
			t.Fatalf("节点 %s 应用条目数=%d, 期望 5 (重复投递应幂等)", id, n)
		}
		if st := h.Node(id).Status(); st.LastIndex != 5 {
			t.Fatalf("节点 %s lastIndex=%d, 期望 5 (重复投递不得造成日志膨胀)", id, st.LastIndex)
		}
	}
}
