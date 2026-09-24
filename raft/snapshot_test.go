package raft

import (
	"fmt"
	"testing"
)

// TestSnapshotCatchUp 验证：落后节点通过快照恢复被压缩的历史。
func TestSnapshotCatchUp(t *testing.T) {
	sim := NewSim(3, testConfig(), 3)
	sim.Run(50)
	leader, _ := sim.Leader()

	// 隔离一个跟随者，让领导者继续提交并压缩。
	lagging := NodeID(0)
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			lagging = id
			break
		}
	}
	sim.Partition(lagging)
	for i := 0; i < 8; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("v=%d", i)))
		sim.Run(5)
	}
	ln := sim.Node(leader)
	t.Logf("压缩前 leader log=%s commit=%d", ln.LogString(), ln.CommitIndex())
	// 压缩前 6 条（含当选空条目），快照数据记录压缩点。
	ln.Compact(6, []byte("state@6"))
	t.Logf("压缩后 leader log=%s", ln.LogString())

	// 恢复分区：落后节点应先收到快照，再补齐后缀。
	sim.Heal()
	sim.Run(60)

	n := sim.Node(lagging)
	t.Logf("追赶后 node=%d log=%s commit=%d", lagging, n.LogString(), n.CommitIndex())
	if n.CommitIndex() != ln.CommitIndex() {
		t.Fatalf("落后节点提交点未追平: %d != %d\n%s", n.CommitIndex(), ln.CommitIndex(), sim.DumpState())
	}
	// 落后节点应收到快照应用消息，且随后命令与领导者一致。
	var got []string
	for _, am := range sim.Applied(lagging) {
		if am.Snapshot {
			got = append(got, fmt.Sprintf("SNAP:%s", am.SnapshotData))
		} else if am.Command != nil {
			got = append(got, string(am.Command))
		}
	}
	t.Logf("node=%d 应用序列: %v", lagging, got)
	if len(got) == 0 || got[0] != "SNAP:state@6" {
		t.Fatalf("落后节点未通过快照恢复: %v", got)
	}
	// 快照@6 覆盖了 v=0..v=4（索引2..6），其后必须按序跟上 v=5..v=7。
	wantTail := []string{"v=5", "v=6", "v=7"}
	if len(got) < 1+len(wantTail) {
		t.Fatalf("快照后命令不足: %v", got)
	}
	for i, w := range wantTail {
		if got[1+i] != w {
			t.Fatalf("快照后第%d条=%q 期望=%q", i, got[1+i], w)
		}
	}
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestSnapshotDoesNotOverwriteLongerSuffix 验证：
// 快照安装不得覆盖更长的已提交后缀。
func TestSnapshotDoesNotOverwriteLongerSuffix(t *testing.T) {
	sim := NewSim(3, testConfig(), 17)
	sim.Run(50)
	leader, _ := sim.Leader()
	for i := 0; i < 4; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("e=%d", i)))
	}
	sim.Run(30)

	// 任选一个跟随者，其日志已含全部 5 条（1 空 + 4 命令）。
	var f *Node
	var fid NodeID
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			f, fid = sim.Node(id), id
			break
		}
	}
	if f.LastLogIndex() != 5 {
		t.Fatalf("前置条件失败: follower last=%d", f.LastLogIndex())
	}
	// 伪造一个只覆盖到索引 2 的快照（来自当前任期领导者）。
	f.Step(Message{
		Type:              MsgInstallSnapshot,
		From:              leader,
		To:                fid,
		Term:              sim.Node(leader).Term(),
		SnapshotLastIndex: 2,
		SnapshotLastTerm:  mustTerm(t, f, 2),
		SnapshotData:      []byte("partial@2"),
	})
	t.Logf("安装快照后 node=%d log=%s commit=%d", fid, f.LogString(), f.CommitIndex())
	// 后缀 [3..5] 必须保留。
	for idx := Index(3); idx <= 5; idx++ {
		e, ok := f.EntryAt(idx)
		if !ok {
			t.Fatalf("索引 %d 的已提交后缀被快照覆盖\nlog=%s", idx, f.LogString())
		}
		want := fmt.Sprintf("e=%d", idx-2)
		if string(e.Command) != want {
			t.Fatalf("索引 %d 内容=%q 期望=%q", idx, e.Command, want)
		}
	}
	if f.CommitIndex() < 5 {
		t.Fatalf("提交点回退: %d", f.CommitIndex())
	}
}

func mustTerm(t *testing.T, n *Node, idx Index) Term {
	t.Helper()
	e, ok := n.EntryAt(idx)
	if !ok {
		t.Fatalf("索引 %d 无条目", idx)
	}
	return e.Term
}

// TestLaggingNodeConsistentView 验证：追赶中的节点对外仍提供一致视图——
// 已应用序列始终是领导者已提交序列的前缀。
func TestLaggingNodeConsistentView(t *testing.T) {
	sim := NewSim(3, testConfig(), 29)
	sim.Run(50)
	leader, _ := sim.Leader()
	sim.SetLoss(0.3)
	sim.SetDup(0.3)
	sim.SetReorder(true)

	var leaderApplied []string
	followerApplied := map[NodeID][]string{}
	for round := 0; round < 10; round++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("r%d", round)))
		sim.Run(8)
		for _, am := range sim.Applied(leader) {
			if am.Command != nil {
				leaderApplied = append(leaderApplied, string(am.Command))
			}
		}
		for _, id := range []NodeID{1, 2, 3} {
			if id == leader {
				continue
			}
			for _, am := range sim.Applied(id) {
				if am.Command != nil {
					followerApplied[id] = append(followerApplied[id], string(am.Command))
				}
			}
			// 一致性断言：跟随者已应用序列是领导者序列的前缀。
			fa := followerApplied[id]
			for i, c := range fa {
				if i >= len(leaderApplied) || leaderApplied[i] != c {
					t.Fatalf("node=%d 视图与领导者不一致: %v 不是 %v 的前缀", id, fa, leaderApplied)
				}
			}
		}
	}
	sim.SetLoss(0)
	sim.SetDup(0)
	sim.SetReorder(false)
	sim.Run(60)
	t.Logf("领导者已应用 %d 条: %v", len(leaderApplied), leaderApplied)
	t.Logf("最终状态:\n%s", sim.DumpState())
}
