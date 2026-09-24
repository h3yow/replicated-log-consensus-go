package raft

import (
	"fmt"
	"testing"
)

// collectApplied 排空所有节点的应用队列，返回 nodeID -> 命令序列。
func collectApplied(sim *Sim) map[NodeID][]string {
	out := map[NodeID][]string{}
	for _, id := range sim.sortedIDs() {
		for _, am := range sim.Applied(id) {
			if am.Snapshot {
				out[id] = append(out[id], fmt.Sprintf("SNAP@%d", am.Index))
			} else if am.Command != nil {
				out[id] = append(out[id], string(am.Command))
			}
		}
	}
	return out
}

// TestReplicationCommit 验证：多数派确认后条目被提交并按序应用。
func TestReplicationCommit(t *testing.T) {
	sim := NewSim(3, testConfig(), 5)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者")
	}
	for i := 0; i < 5; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i))) {
			t.Fatalf("Propose 失败")
		}
	}
	sim.Run(30)
	n := sim.Node(leader)
	t.Logf("leader=%d term=%d commit=%d log=%s", leader, n.Term(), n.CommitIndex(), n.LogString())
	// 5 条命令 + 1 条当选空条目 = 6 条应全部提交。
	if n.CommitIndex() < n.LastLogIndex() {
		t.Fatalf("存在未提交条目: commit=%d last=%d\n%s", n.CommitIndex(), n.LastLogIndex(), sim.DumpState())
	}
	applied := collectApplied(sim)
	want := []string{"cmd-0", "cmd-1", "cmd-2", "cmd-3", "cmd-4"}
	for id, cmds := range applied {
		if len(cmds) != len(want) {
			t.Fatalf("node=%d 应用条数=%d 期望=%d: %v", id, len(cmds), len(want), cmds)
		}
		for i, c := range cmds {
			if c != want[i] {
				t.Fatalf("node=%d 第%d条=%q 期望=%q", id, i, c, want[i])
			}
		}
		t.Logf("node=%d 已按序应用: %v", id, cmds)
	}
}

// TestOldTermEntryNotCommittedByMinority 验证：旧任期条目即使复制到多数派，
// 也不能仅凭计数提交——必须由本任期条目顺带提交（Raft §5.4.2 场景）。
func TestOldTermEntryNotCommittedByMinority(t *testing.T) {
	sim := NewSim(5, testConfig(), 11)
	sim.Run(60)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者")
	}
	// 领导者追加一条后立刻被隔离（条目只到部分节点）。
	sim.Node(leader).Propose([]byte("risky"))
	sim.Tick() // 让消息入队但尚未全部投递
	sim.Partition(leader)
	sim.Run(100) // 其余节点选出新领导者
	sim.Heal()
	sim.Run(60)

	// 最终：所有节点已应用的命令序列必须一致（状态机安全性）。
	applied := collectApplied(sim)
	var ref []string
	var refID NodeID
	for id, cmds := range applied {
		if ref == nil {
			ref, refID = cmds, id
			continue
		}
		if len(cmds) != len(ref) {
			t.Fatalf("node=%d 与 node=%d 应用序列长度不一致: %v vs %v\n%s",
				id, refID, cmds, ref, sim.DumpState())
		}
		for i := range cmds {
			if cmds[i] != ref[i] {
				t.Fatalf("node=%d 与 node=%d 第%d条不一致: %q vs %q", id, refID, i, cmds[i], ref[i])
			}
		}
	}
	t.Logf("所有节点应用序列一致: %v", ref)
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestRestartNoLossNoDuplicate 验证：重启与重复投递后，
// 已提交内容不丢失、不重复应用。
func TestRestartNoLossNoDuplicate(t *testing.T) {
	sim := NewSim(3, testConfig(), 23)
	sim.Run(50)
	leader, _ := sim.Leader()
	for i := 0; i < 3; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("k=%d", i)))
	}
	sim.Run(30)
	before := collectApplied(sim)
	t.Logf("重启前已应用: %v", before)

	// 开启重复投递，崩溃并重启一个跟随者。
	sim.SetDup(0.5)
	victim := NodeID(0)
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			victim = id
			break
		}
	}
	sim.Crash(victim)
	sim.Restart(victim)
	sim.Node(leader).Propose([]byte("k=3"))
	sim.Run(60)

	after := sim.Applied(victim)
	var got []string
	for _, am := range after {
		if am.Command != nil {
			got = append(got, string(am.Command))
		}
	}
	t.Logf("node=%d 重启后新应用: %v (重启前: %v)", victim, got, before[victim])
	// 重启后不得重复应用重启前已提交的条目。
	seen := map[string]bool{}
	for _, c := range before[victim] {
		seen[c] = true
	}
	for _, c := range got {
		if seen[c] {
			t.Fatalf("node=%d 重启后重复应用 %q", victim, c)
		}
	}
	// 新命令 k=3 必须最终被应用到。
	found := false
	for _, c := range got {
		if c == "k=3" {
			found = true
		}
	}
	if !found {
		t.Fatalf("node=%d 重启后未应用新命令 k=3\n%s", victim, sim.DumpState())
	}
}
