package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// runFaultInjection 在固定种子下执行一次可重放的故障注入运行。
// 每拍以注入的随机源决定：丢包/重复/乱序/分区/重启/提案。
// 全程断言安全性不变量，结束后断言最终一致性。
func runFaultInjection(t *testing.T, seed int64, ticks int) {
	t.Helper()
	sim := NewSim(5, testConfig(), seed)
	faultRnd := rand.New(rand.NewSource(seed ^ 0x5eed))

	// 全程记录：每个任期出现过的领导者（选举安全性）。
	leadersByTerm := map[Term]NodeID{}
	// 每个节点累计已应用命令（状态机安全性）。
	applied := map[NodeID][]string{}
	for _, id := range []NodeID{1, 2, 3, 4, 5} {
		applied[id] = nil
	}
	checkApplied := func() {
		for _, id := range sim.sortedIDs() {
			for _, am := range sim.Applied(id) {
				if am.Snapshot {
					// 快照内容编码了前缀，这里用占位符保持索引对齐。
					for len(applied[id]) < int(am.Index) {
						applied[id] = append(applied[id], fmt.Sprintf("snap-prefix@%d", am.Index))
					}
					continue
				}
				idx := int(am.Index)
				for len(applied[id]) < idx {
					applied[id] = append(applied[id], "")
				}
				if prev := applied[id][idx-1]; prev != "" && prev != string(am.Command) {
					t.Fatalf("node=%d 索引=%d 被重复应用为不同值: %q -> %q\n%s",
						id, am.Index, prev, am.Command, sim.DumpState())
				}
				applied[id][idx-1] = string(am.Command)
			}
		}
		// 任意两节点：已应用序列不得冲突（前缀一致）。
		ids := sim.sortedIDs()
		for i := 0; i < len(ids); i++ {
			for j := i + 1; j < len(ids); j++ {
				a, b := applied[ids[i]], applied[ids[j]]
				for k := 0; k < len(a) && k < len(b); k++ {
					if a[k] != "" && b[k] != "" && a[k] != b[k] {
						t.Fatalf("状态机安全性违反: node=%d 与 node=%d 在索引=%d 应用了 %q 与 %q\n%s",
							ids[i], ids[j], k+1, a[k], b[k], sim.DumpState())
					}
				}
			}
		}
	}

	proposalSeq := 0
	for tick := 0; tick < ticks; tick++ {
		// 随机故障注入。
		switch faultRnd.Intn(40) {
		case 0:
			ids := sim.sortedIDs()
			victim := ids[faultRnd.Intn(len(ids))]
			sim.Partition(victim)
		case 1:
			sim.Heal()
		case 2:
			sim.SetLoss(faultRnd.Float64() * 0.5)
		case 3:
			sim.SetLoss(0)
		case 4:
			sim.SetDup(faultRnd.Float64() * 0.5)
		case 5:
			sim.SetDup(0)
		case 6:
			sim.SetReorder(faultRnd.Intn(2) == 0)
		case 7:
			ids := sim.sortedIDs()
			if len(ids) > 0 && faultRnd.Intn(3) == 0 {
				victim := ids[faultRnd.Intn(len(ids))]
				sim.Crash(victim)
				sim.Restart(victim)
			}
		}
		// 随机提案（发给当前领导者，若有）。
		if faultRnd.Intn(10) == 0 {
			if leader, ok := sim.Leader(); ok {
				cmd := fmt.Sprintf("seed=%d-cmd=%d", seed, proposalSeq)
				if sim.Node(leader).Propose([]byte(cmd)) {
					proposalSeq++
				}
			}
		}
		sim.Tick()
		// 选举安全性：同任期不得有两个领导者。
		for _, id := range sim.sortedIDs() {
			n := sim.Node(id)
			if n.Role() == Leader {
				if prev, ok := leadersByTerm[n.Term()]; ok && prev != id {
					t.Fatalf("选举安全性违反: term=%d 领导者 %d 与 %d\n%s",
						n.Term(), prev, id, sim.DumpState())
				}
				leadersByTerm[n.Term()] = id
			}
		}
		checkApplied()
	}

	// 收尾：清除全部故障，给集群足够时间收敛。
	sim.Heal()
	sim.SetLoss(0)
	sim.SetDup(0)
	sim.SetReorder(false)
	sim.Run(300)
	checkApplied()

	// 最终一致性：所有存活节点的已应用序列两两前缀一致，
	// 且提交点收敛到相同值。
	ids := sim.sortedIDs()
	var maxCommit Index
	for _, id := range ids {
		if c := sim.Node(id).CommitIndex(); c > maxCommit {
			maxCommit = c
		}
	}
	for _, id := range ids {
		n := sim.Node(id)
		if n.CommitIndex() != maxCommit {
			t.Fatalf("最终一致性违反: node=%d commit=%d 期望=%d\n%s\n事件:\n%v",
				id, n.CommitIndex(), maxCommit, sim.DumpState(), sim.Events())
		}
	}
	t.Logf("seed=%d 收敛: commit=%d 提案总数=%d 任期数=%d", seed, maxCommit, proposalSeq, len(leadersByTerm))
	for id, cmds := range applied {
		t.Logf("node=%d 已应用 %d 条 (term=%d): %v", id, len(cmds), sim.Node(id).Term(), cmds)
	}
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestFaultInjectionDeterministic 多种子故障注入：安全性 + 最终一致性。
func TestFaultInjectionDeterministic(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runFaultInjection(t, seed, 400)
		})
	}
}

// TestDeterministicReplay 验证：相同种子两次运行产生完全相同的事件序列。
func TestDeterministicReplay(t *testing.T) {
	run := func() []string {
		sim := NewSim(3, testConfig(), 777)
		sim.SetLoss(0.2)
		sim.SetDup(0.2)
		sim.SetReorder(true)
		for i := 0; i < 100; i++ {
			if i == 30 {
				sim.Partition(1)
			}
			if i == 60 {
				sim.Heal()
			}
			if leader, ok := sim.Leader(); ok && i%10 == 0 {
				sim.Node(leader).Propose([]byte(fmt.Sprintf("x%d", i)))
			}
			sim.Tick()
		}
		return append(sim.Events(), sim.DumpState())
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("重放长度不一致: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("重放分歧 @%d:\nA: %s\nB: %s", i, a[i], b[i])
		}
	}
	t.Logf("两次运行产生完全相同的 %d 条事件", len(a))
}
