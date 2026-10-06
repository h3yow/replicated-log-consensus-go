package raft

import (
	"fmt"
	"testing"
)

// 本文件覆盖"带上一版本持久化状态的节点重启"（升级路径）：
// 旧版本不支持动态成员，其持久化状态有历史任期/日志/提交点（甚至快照），
// 但没有任何成员信息（无快照成员、无变更条目）。这类节点重启后必须
// 按启动时给定的初始成员配置恢复投票成员身份，正常参与选举与投票，
// 并与集群其余成员收敛到同一份配置。

// stripMembershipToLegacy 把节点的持久化状态改写成旧版本格式：
// 清掉快照成员元数据；旧版本日志里本就不存在变更条目（有则无法模拟，直接失败）。
func stripMembershipToLegacy(t *testing.T, sim *Sim, id NodeID) {
	t.Helper()
	ps, err := sim.storage[id].Load()
	if err != nil {
		t.Fatalf("node %d 读取存储失败: %v", id, err)
	}
	for _, e := range ps.Log {
		if e.Type == EntryConfig {
			t.Fatalf("node %d 日志含变更条目，无法模拟旧版本状态", id)
		}
	}
	ps.SnapVoters = nil
	ps.SnapLearners = nil
	if err := sim.storage[id].Save(ps); err != nil {
		t.Fatalf("node %d 写回存储失败: %v", id, err)
	}
	sim.logf("UPGRADE 模拟旧版本持久化状态: node=%d term=%d commit=%d snap@%d(t%d) logEntries=%d",
		id, ps.CurrentTerm, ps.CommitIndex, ps.SnapIndex, ps.SnapTerm, len(ps.Log))
}

// assertRecoveredMembership 断言升级重启后的成员身份与落盘结果。
func assertRecoveredMembership(t *testing.T, sim *Sim, ids []NodeID, wantVoters []NodeID, votersBefore map[NodeID][]NodeID) {
	t.Helper()
	want := fmt.Sprint(wantVoters)
	for _, id := range ids {
		n := sim.Node(id)
		if !n.IsVoter(id) {
			t.Fatalf("升级重启后 node %d 未认出自己的投票成员身份: voters=%v\n%s\n事件:\n%v",
				id, n.Voters(), sim.DumpState(), sim.Events())
		}
		if got := fmt.Sprint(n.Voters()); got != want {
			t.Fatalf("升级重启后 node %d 成员视图=%v 期望=%v\n%s", id, got, want, sim.DumpState())
		}
		if before := fmt.Sprint(votersBefore[id]); before != want {
			t.Fatalf("node %d 重启前视图=%v 与期望=%v 不一致（用例构造有误）", id, before, want)
		}
		// 恢复结果必须已落盘：之后反复重启不再依赖引导配置。
		ps, _ := sim.storage[id].Load()
		if got := fmt.Sprint(ps.SnapVoters); got != want {
			t.Fatalf("node %d 恢复的成员配置未落盘: SnapVoters=%v 期望=%v", id, got, want)
		}
		t.Logf("node %d 升级恢复: term=%d commit=%d voters=%v（重启前=%v，已落盘）",
			id, n.Term(), n.CommitIndex(), n.Voters(), votersBefore[id])
	}
}

// runLegacyUpgradeScenario 构造"旧版本状态重启"场景并验证恢复行为。
// compact=true 时先在所有节点上压缩日志产生快照，再降级为旧版本状态。
func runLegacyUpgradeScenario(t *testing.T, seed int64, compact bool) {
	sim := NewSim(3, testConfig(), seed)
	ids := []NodeID{1, 2, 3}

	waitFor(t, sim, 200, "升级前首次选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	leader, _ := sim.Leader()
	for i := 0; i < 5; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("pre-upgrade-%d", i))) {
			t.Fatalf("领导者 %d 拒绝写入 pre-upgrade-%d", leader, i)
		}
	}
	sim.Run(30)

	commitBefore := map[NodeID]Index{}
	votersBefore := map[NodeID][]NodeID{}
	for _, id := range ids {
		n := sim.Node(id)
		if n.CommitIndex() == 0 {
			t.Fatalf("node %d 提交点未推进，无法构造升级场景\n%s", id, sim.DumpState())
		}
		if n.LastLogIndex() == 0 {
			t.Fatalf("node %d 日志为空，无法构造升级场景\n%s", id, sim.DumpState())
		}
		commitBefore[id] = n.CommitIndex()
		votersBefore[id] = n.Voters()
	}
	t.Logf("升级前状态（旧版本运行中）:\n%s", sim.DumpState())

	if compact {
		// 产生快照后再降级：旧版本的快照同样不含成员元数据。
		for _, id := range ids {
			n := sim.Node(id)
			n.Compact(n.CommitIndex(), []byte("legacy-snap"))
		}
		sim.Run(5)
		for _, id := range ids {
			ps, _ := sim.storage[id].Load()
			if ps.SnapIndex == 0 {
				t.Fatalf("node %d 未产生快照，无法构造快照升级场景\n%s", id, sim.DumpState())
			}
		}
		t.Logf("已压缩产生快照:\n%s", sim.DumpState())
	}

	// 模拟从旧版本升级：持久化状态保留，但成员信息不存在。
	for _, id := range ids {
		stripMembershipToLegacy(t, sim, id)
	}
	for _, id := range ids {
		sim.Crash(id)
	}
	for _, id := range ids {
		sim.Restart(id)
	}

	// 重启后立即断言：按初始成员配置认出身份，视图与重启前一致，且已落盘。
	assertRecoveredMembership(t, sim, ids, []NodeID{1, 2, 3}, votersBefore)

	// 升级后的集群必须能重新选出领导者。
	waitFor(t, sim, 300, "升级后重新选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	newLeader, _ := sim.Leader()
	t.Logf("升级后领导者=%d term=%d\n%s", newLeader, sim.Node(newLeader).Term(), sim.DumpState())

	// 继续接受写入并推进提交点。
	if !sim.Node(newLeader).Propose([]byte("post-upgrade")) {
		t.Fatalf("升级后领导者 %d 拒绝写入", newLeader)
	}
	waitFor(t, sim, 300, "升级后提交点继续推进", func() bool {
		for _, id := range ids {
			if sim.Node(id).CommitIndex() <= commitBefore[id] {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		t.Logf("node %d 提交点推进: %d -> %d", id, commitBefore[id], sim.Node(id).CommitIndex())
	}

	// 反复重启：成员视图保持稳定（恢复结果不依赖再次给对引导配置）。
	for _, id := range ids {
		sim.Crash(id)
	}
	for _, id := range ids {
		sim.Restart(id)
	}
	for _, id := range ids {
		if got := fmt.Sprint(sim.Node(id).Voters()); got != "[1 2 3]" {
			t.Fatalf("二次重启后 node %d 成员视图=%v 期望=[1 2 3]\n%s", id, got, sim.DumpState())
		}
	}
	t.Logf("二次重启后成员视图保持稳定:\n%s", sim.DumpState())
}

// TestUpgradeLegacyRestartWithLog 旧版本状态（日志非空 + 已有提交点）重启后可恢复。
func TestUpgradeLegacyRestartWithLog(t *testing.T) {
	runLegacyUpgradeScenario(t, 20261006, false)
}

// TestUpgradeLegacyRestartWithSnapshot 旧版本状态（已产生快照）重启后可恢复。
func TestUpgradeLegacyRestartWithSnapshot(t *testing.T) {
	runLegacyUpgradeScenario(t, 20261007, true)
}

// TestUpgradeMixedClusterConverges 新旧状态混跑：一部分节点带旧版本状态、
// 一部分带新版本状态，重启后自动收敛到同一份成员配置，
// 且升级节点能继续跟随后续的动态成员变更。
func TestUpgradeMixedClusterConverges(t *testing.T) {
	sim := NewSim(3, testConfig(), 20261008)
	ids := []NodeID{1, 2, 3}

	waitFor(t, sim, 200, "首次选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	leader, _ := sim.Leader()
	sim.Node(leader).Propose([]byte("mixed-1"))
	sim.Run(30)

	// 只有 node 1 是旧版本状态；node 2、3 保留新版本格式（含持久化成员）。
	stripMembershipToLegacy(t, sim, 1)
	for _, id := range ids {
		sim.Crash(id)
	}
	for _, id := range ids {
		sim.Restart(id)
	}
	t.Logf("混合重启后状态:\n%s", sim.DumpState())

	if got := fmt.Sprint(sim.Node(1).Voters()); got != "[1 2 3]" {
		t.Fatalf("旧状态 node 1 重启后成员视图=%v 期望=[1 2 3]\n%s", got, sim.DumpState())
	}
	waitFor(t, sim, 300, "混合集群重新选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})

	// 混合集群继续推进动态成员变更：扩容 node 4，
	// 升级恢复的老节点必须与新节点收敛到同一份新配置。
	waitFor(t, sim, 300, "add(4) 被接受", func() bool { return sim.AddNode(4) })
	waitFor(t, sim, 300, "node 4 提升为 voter", func() bool {
		l, ok := sim.Leader()
		return ok && sim.Node(l).IsVoter(4) && sim.Node(4).IsVoter(4)
	})
	sim.Run(30)
	for _, id := range []NodeID{1, 2, 3, 4} {
		if got := fmt.Sprint(sim.Node(id).Voters()); got != "[1 2 3 4]" {
			t.Fatalf("收敛后 node %d 成员视图=%v 期望=[1 2 3 4]\n%s\n事件:\n%v",
				id, got, sim.DumpState(), sim.Events())
		}
	}
	t.Logf("新旧状态混跑收敛到同一配置 voters=[1 2 3 4]:\n%s", sim.DumpState())
}

// TestUpgradeDoesNotResurrectRemovedNode 升级回退不得误判：
// 确实被动态摘除过的节点，其持久化状态里必有变更条目或快照成员，
// 重启后仍是非成员，不得竞选、不得扰乱集群选举。
func TestUpgradeDoesNotResurrectRemovedNode(t *testing.T) {
	sim := NewSim(3, testConfig(), 20261009)

	waitFor(t, sim, 200, "首次选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	waitFor(t, sim, 300, "remove(3) 被接受", func() bool { return sim.RemoveNode(3) })
	// 等被摘除者收到摘除条目（其日志中出现变更条目），并让变更在集群提交。
	waitFor(t, sim, 300, "node 3 收到摘除条目", func() bool {
		return !sim.Node(3).IsVoter(3)
	})
	sim.Run(50)
	t.Logf("摘除提交后状态:\n%s", sim.DumpState())

	// 被摘除者的持久化状态必须携带成员信息（变更条目），不得走升级回退。
	ps, _ := sim.storage[3].Load()
	if ps.lacksMembershipInfo() {
		t.Fatalf("node 3 被摘除后持久化状态不含成员信息，存在误判风险: %+v", ps)
	}

	sim.Crash(3)
	sim.Restart(3)
	if sim.Node(3).IsVoter(3) || sim.Node(3).IsLearner(3) {
		t.Fatalf("被摘除的 node 3 重启后被误判为成员: voters=%v learners=%v\n%s",
			sim.Node(3).Voters(), sim.Node(3).Learners(), sim.DumpState())
	}

	// 重启后长时间运行：node 3 不得竞选/当选；集群其余成员正常工作。
	for i := 0; i < 150; i++ {
		sim.Tick()
		if r := sim.Node(3).Role(); r != Follower {
			t.Fatalf("被摘除的 node 3 重启后角色变为 %s（应始终为 Follower）\n%s\n事件:\n%v",
				r, sim.DumpState(), sim.Events())
		}
	}
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("摘除 node 3 后集群无领导者\n%s\n事件:\n%v", sim.DumpState(), sim.Events())
	}
	commitBefore := sim.Node(leader).CommitIndex()
	sim.Node(leader).Propose([]byte("after-remove"))
	waitFor(t, sim, 300, "摘除后提交点继续推进", func() bool {
		return sim.Node(leader).CommitIndex() > commitBefore
	})
	t.Logf("被摘除节点保持非成员，集群正常: leader=%d term=%d commit=%d\n%s",
		leader, sim.Node(leader).Term(), sim.Node(leader).CommitIndex(), sim.DumpState())
}
