package raft

import (
	"fmt"
	"testing"
)

// TestSnapshotCatchUp 落后节点通过快照追赶上被压缩的历史。
func TestSnapshotCatchUp(t *testing.T) {
	h := newCluster(t, 11, 3, Faults{})
	leader := mustLeader(t, h)

	// 找出一个追随者并隔离它。
	var lagging NodeID
	for _, id := range h.sortedIDs() {
		if id != leader {
			lagging = id
			break
		}
	}
	h.Partition(lagging)

	// 领导者在多数派中提交 10 条并压缩前 8 条。
	for i := 0; i < 10; i++ {
		h.Propose(leader, []byte(fmt.Sprintf("cmd-%d", i)))
		h.Run(3)
	}
	h.Run(5)
	h.Committed() // 推进多数派的 lastApplied
	ln := h.Node(leader)
	ln.Compact(8, []byte("state-up-to-8"))
	st := ln.Status()
	t.Logf("领导者=%s 任期=%d 快照点=8 lastIndex=%d commitIndex=%d",
		leader, st.Term, st.LastIndex, st.CommitIndex)

	// 恢复分区：落后节点应通过 InstallSnapshot 追赶。
	h.Heal()
	h.Run(30)
	got := h.Committed()
	c, ok := got[lagging]
	lst := h.Node(lagging).Status()
	meta, entries := h.Node(lagging).CommittedView()
	t.Logf("落后节点=%s 任期=%d 快照点=%d commitIndex=%d 已提交条目数=%d 本次应用起始=%d 条数=%d",
		lagging, lst.Term, meta.Index, lst.CommitIndex, len(entries), c.From, len(c.Entries))
	if !ok && lst.CommitIndex < 10 {
		t.Fatalf("落后节点未追赶: commitIndex=%d", lst.CommitIndex)
	}
	if meta.Index != 8 {
		t.Fatalf("落后节点快照点=%d, 期望 8", meta.Index)
	}
	if lst.CommitIndex != 10 {
		t.Fatalf("落后节点 commitIndex=%d, 期望 10", lst.CommitIndex)
	}
	// 追赶后已提交前缀必须与领导者一致。
	lmeta, lentries := h.Node(leader).CommittedView()
	if lmeta != meta || len(lentries) != len(entries) {
		t.Fatalf("追赶后与领导者不一致: 领导者(%v,%d) 落后者(%v,%d)",
			lmeta, len(lentries), meta, len(entries))
	}
	for i := range entries {
		if string(entries[i].Command) != string(lentries[i].Command) {
			t.Fatalf("索引 %d 处条目不一致: %s vs %s",
				meta.Index+1+Index(i), entries[i].Command, lentries[i].Command)
		}
	}
}

// TestSnapshotNotOverwriteLongerCommittedSuffix 快照安装不得覆盖更长的已提交后缀。
func TestSnapshotNotOverwriteLongerCommittedSuffix(t *testing.T) {
	peers := []NodeID{"A", "B", "C"}
	b := NewNode(Config{ID: "B", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	// B 已有 5 条已提交条目。
	b.currentTerm = 3
	b.entries = []LogEntry{
		{Term: 1, Command: []byte("e1")},
		{Term: 2, Command: []byte("e2")},
		{Term: 2, Command: []byte("e3")},
		{Term: 3, Command: []byte("e4")},
		{Term: 3, Command: []byte("e5")},
	}
	b.commitIndex = 5
	b.lastApplied = 5
	b.persist()

	// 收到索引 3 处的快照（比本地已提交前缀短）：安装后已提交后缀必须保留。
	b.Step(Message{
		Type: MsgInstallSnapshot,
		From: "A", To: "B", Term: 3,
		Snapshot: Snapshot{Meta: SnapshotMeta{Index: 3, Term: 2}, Data: []byte("snap-3")},
	})
	meta, entries := b.CommittedView()
	t.Logf("安装快照点=3 后: 快照点=%d 已提交后缀条目数=%d commitIndex=%d",
		meta.Index, len(entries), b.commitIndex)
	if meta.Index != 3 {
		t.Fatalf("快照点=%d, 期望 3", meta.Index)
	}
	if b.commitIndex != 5 || len(entries) != 2 {
		t.Fatalf("已提交后缀被覆盖: commitIndex=%d 后缀=%d", b.commitIndex, len(entries))
	}
	if string(entries[0].Command) != "e4" || string(entries[1].Command) != "e5" {
		t.Fatalf("已提交后缀内容被篡改: %v", entries)
	}
}

// TestCompactOnlyApplied 只能压缩已应用的前缀。
func TestCompactOnlyApplied(t *testing.T) {
	peers := []NodeID{"A", "B", "C"}
	a := NewNode(Config{ID: "A", Peers: peers, ElectionTimeout: 10, HeartbeatTimeout: 2}, NewMemoryStorage())
	a.entries = []LogEntry{
		{Term: 1, Command: []byte("e1")},
		{Term: 1, Command: []byte("e2")},
		{Term: 1, Command: []byte("e3")},
	}
	a.commitIndex = 2
	a.lastApplied = 2
	a.persist()
	a.Compact(3, []byte("d")) // 请求压缩到 3，但只应用到 2
	if a.snapshot.Meta.Index != 2 {
		t.Fatalf("压缩点=%d, 期望被钳制到 lastApplied=2", a.snapshot.Meta.Index)
	}
	t.Logf("请求压缩到 3, lastApplied=2 -> 实际快照点=%d", a.snapshot.Meta.Index)
}

// TestLaggingNodeReadableDuringCatchUp 追赶中的节点对外仍提供一致可读视图：
// 已提交前缀在任意时刻都与领导者已提交前缀相容（互为前缀）。
func TestLaggingNodeReadableDuringCatchUp(t *testing.T) {
	h := newCluster(t, 12, 5, Faults{ReorderWindow: 4})
	leader := mustLeader(t, h)
	var lagging NodeID
	for _, id := range h.sortedIDs() {
		if id != leader {
			lagging = id
			break
		}
	}
	h.Partition(lagging)
	for i := 0; i < 8; i++ {
		h.Propose(leader, []byte(fmt.Sprintf("v%d", i)))
		h.Run(3)
	}
	h.Run(5)
	h.Committed()
	h.Node(leader).Compact(6, []byte("snap"))
	h.Heal()
	// 恢复过程中逐步检查：落后节点的已提交前缀始终是领导者已提交前缀的前缀。
	for step := 0; step < 10; step++ {
		h.Run(2)
		lmeta, lentries := h.Node(leader).CommittedView()
		fmeta, fentries := h.Node(lagging).CommittedView()
		// 将两者展开为 (index,command) 序列比较公共部分。
		lseq := map[Index]string{}
		for i, e := range lentries {
			lseq[lmeta.Index+1+Index(i)] = string(e.Command)
		}
		for i, e := range fentries {
			idx := fmeta.Index + 1 + Index(i)
			if lv, ok := lseq[idx]; ok && lv != string(e.Command) {
				t.Fatalf("step=%d 索引 %d 处不一致: 落后者=%s 领导者=%s", step, idx, e.Command, lv)
			}
		}
		t.Logf("step=%d 落后者快照点=%d 已提交=%d 条, 与领导者已提交前缀相容",
			step, fmeta.Index, len(fentries))
	}
	// 快照与后续条目可能仍在投递途中，继续运行至追赶完成。
	h.Run(40)
	if st := h.Node(lagging).Status(); st.CommitIndex != 8 {
		t.Fatalf("最终 commitIndex=%d, 期望 8", st.CommitIndex)
	}
}
