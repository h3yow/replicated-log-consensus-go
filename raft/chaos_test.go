package raft

import (
	"fmt"
	"testing"
)

// committedSeq 展开节点的已提交前缀为 index->command 序列（含快照点）。
func committedSeq(n *Node) (Index, map[Index]string) {
	meta, entries := n.CommittedView()
	seq := map[Index]string{}
	for i, e := range entries {
		seq[meta.Index+1+Index(i)] = string(e.Command)
	}
	return meta.Index, seq
}

// assertStateMachineSafety 状态机安全性：任意两节点在同一索引处的已提交命令必须相同。
func assertStateMachineSafety(t *testing.T, h *Harness) {
	t.Helper()
	seqs := map[NodeID]map[Index]string{}
	snaps := map[NodeID]Index{}
	for _, id := range h.sortedIDs() {
		snap, seq := committedSeq(h.Node(id))
		seqs[id] = seq
		snaps[id] = snap
	}
	for _, a := range h.sortedIDs() {
		for _, b := range h.sortedIDs() {
			for idx, cmd := range seqs[a] {
				if cmd2, ok := seqs[b][idx]; ok && cmd != cmd2 {
					t.Fatalf("状态机安全性违反: 节点 %s 与 %s 在索引 %d 处分别为 %q 与 %q",
						a, b, idx, cmd, cmd2)
				}
			}
		}
	}
}

// TestChaosSafetyAndConvergence 丢包+重复+乱序+分区+重启组合故障下，
// 安全性不变量始终成立，故障消除后最终一致。
func TestChaosSafetyAndConvergence(t *testing.T) {
	faults := Faults{DropProb: 0.15, DuplicateProb: 0.2, ReorderWindow: 4}
	h := newCluster(t, 2026, 5, faults)
	ids := h.sortedIDs()

	applied := map[NodeID]map[Index]string{} // 各节点已应用序列
	for _, id := range ids {
		applied[id] = map[Index]string{}
	}
	collect := func() {
		for id, c := range h.Committed() {
			for i, e := range c.Entries {
				idx := c.From + Index(i)
				if prev, ok := applied[id][idx]; ok && prev != string(e.Command) {
					t.Fatalf("节点 %s 在索引 %d 处重复应用了不同命令: %q vs %q", id, idx, prev, e.Command)
				}
				applied[id][idx] = string(e.Command)
			}
		}
	}

	cmdN := 0
	for phase := 0; phase < 12; phase++ {
		// 向当前领导者提案（若无领导者则跳过）。
		if ls := leaders(h); len(ls) == 1 {
			h.Propose(ls[0], []byte(fmt.Sprintf("cmd-%d", cmdN)))
			cmdN++
		}
		h.Run(8)
		collect()
		assertStateMachineSafety(t, h)

		// 注入分区与重启。
		switch phase % 4 {
		case 1:
			h.Partition(ids[0], ids[1])
			t.Logf("phase=%d 注入分区: 隔离 %s,%s", phase, ids[0], ids[1])
		case 2:
			h.Heal()
			h.Restart(ids[3])
			t.Logf("phase=%d 恢复分区并重启 %s", phase, ids[3])
		case 3:
			h.Partition(ids[4])
			t.Logf("phase=%d 注入分区: 隔离 %s", phase, ids[4])
		default:
			h.Heal()
		}
	}

	// 故障消除：恢复分区、关闭丢包，最终必须收敛。
	h.Heal()
	h.faults = Faults{}
	h.Run(80)
	collect()
	assertStateMachineSafety(t, h)

	// 最终一致性：所有存活节点已提交序列收敛到同一前缀。
	var ref NodeID
	var refLen int
	for _, id := range ids {
		if len(applied[id]) > refLen {
			refLen = len(applied[id])
			ref = id
		}
	}
	for _, id := range ids {
		st := h.Node(id).Status()
		t.Logf("最终 节点=%s 角色=%v 任期=%d commitIndex=%d 已应用=%d 条",
			id, st.Role, st.Term, st.CommitIndex, len(applied[id]))
		for idx, cmd := range applied[id] {
			if applied[ref][idx] != cmd {
				t.Fatalf("最终一致性违反: 节点 %s 索引 %d 为 %q, 参考节点 %s 为 %q",
					id, idx, cmd, ref, applied[ref][idx])
			}
		}
		if len(applied[id]) != refLen {
			t.Fatalf("最终一致性违反: 节点 %s 已应用 %d 条, 参考节点 %s 已应用 %d 条",
				id, len(applied[id]), ref, refLen)
		}
	}
	t.Logf("收敛完成: 共提交 %d 条命令, 全部节点一致", refLen)
}

// TestDeterministicReplay 相同种子两次运行，逐 tick 状态完全一致。
func TestDeterministicReplay(t *testing.T) {
	run := func() []Status {
		faults := Faults{DropProb: 0.2, DuplicateProb: 0.3, ReorderWindow: 3}
		h := newCluster(t, 777, 5, faults)
		ids := h.sortedIDs()
		for phase := 0; phase < 8; phase++ {
			if ls := leaders(h); len(ls) == 1 {
				h.Propose(ls[0], []byte(fmt.Sprintf("p-%d", phase)))
			}
			h.Run(7)
			if phase == 3 {
				h.Partition(ids[0])
			}
			if phase == 5 {
				h.Heal()
				h.Restart(ids[2])
			}
		}
		var out []Status
		for _, id := range ids {
			out = append(out, h.Node(id).Status())
		}
		return out
	}
	first := run()
	second := run()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("确定性重放失败: 节点 %s 两次运行状态不同: %+v vs %+v",
				first[i].ID, first[i], second[i])
		}
		t.Logf("节点=%s 两次运行一致: 角色=%v 任期=%d commitIndex=%d lastIndex=%d",
			first[i].ID, first[i].Role, first[i].Term, first[i].CommitIndex, first[i].LastIndex)
	}
}

// TestRestartUnderFaults 故障环境下反复重启，已提交内容不丢失。
func TestRestartUnderFaults(t *testing.T) {
	faults := Faults{DropProb: 0.1, DuplicateProb: 0.1, ReorderWindow: 2}
	h := newCluster(t, 555, 3, faults)
	ids := h.sortedIDs()
	for round := 0; round < 6; round++ {
		// 给领导者足够时间窗口提交，再注入重启。
		for attempt := 0; attempt < 3; attempt++ {
			if ls := leaders(h); len(ls) == 1 {
				h.Propose(ls[0], []byte(fmt.Sprintf("r-%d-%d", round, attempt)))
			}
			h.Run(10)
		}
		h.Restart(ids[round%3])
		t.Logf("round=%d 重启 %s", round, ids[round%3])
	}
	h.faults = Faults{}
	h.Run(80)
	assertStateMachineSafety(t, h)
	// 所有节点收敛到相同的已提交序列。
	seqs := map[NodeID]map[Index]string{}
	for _, id := range ids {
		_, seqs[id] = committedSeq(h.Node(id))
		t.Logf("节点=%s 已提交序列长度=%d", id, len(seqs[id]))
	}
	if len(seqs[ids[0]]) == 0 {
		t.Fatal("测试无效: 没有任何条目被提交")
	}
	for _, a := range ids {
		for _, b := range ids {
			if len(seqs[a]) != len(seqs[b]) {
				t.Fatalf("重启后未收敛: %s 有 %d 条, %s 有 %d 条", a, len(seqs[a]), b, len(seqs[b]))
			}
			for idx, cmd := range seqs[a] {
				if seqs[b][idx] != cmd {
					t.Fatalf("重启后不一致: 索引 %d %s=%q %s=%q", idx, a, cmd, b, seqs[b][idx])
				}
			}
		}
	}
}
