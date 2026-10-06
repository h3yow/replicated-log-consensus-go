package raft

import (
	"fmt"
	"testing"
)

// stripMembershipToV1 把节点的持久化状态改写成"上一版本"的格式：
// 上一版本的 PersistentState 没有 SnapVoters/SnapLearners 字段，
// 日志中也不会出现成员变更条目（成员是静态的 Config.Peers）。
// 这里等价地清空成员元数据，模拟"带历史数据从旧版本升级上来"的节点。
func stripMembershipToV1(t *testing.T, sim *Sim, id NodeID) {
	t.Helper()
	st := sim.storage[id]
	ps, err := st.Load()
	if err != nil {
		t.Fatalf("load node %d: %v", id, err)
	}
	for _, e := range ps.Log {
		if e.Type == EntryConfig {
			t.Fatalf("前置条件不满足：旧版本日志不应含成员变更条目 node=%d", id)
		}
	}
	ps.SnapVoters = nil
	ps.SnapLearners = nil
	if err := st.Save(ps); err != nil {
		t.Fatalf("save node %d: %v", id, err)
	}
	t.Logf("node %d 持久化状态已改写为旧版本格式: term=%d commit=%d snap@%d logEntries=%d",
		id, ps.CurrentTerm, ps.CommitIndex, ps.SnapIndex, len(ps.Log))
}

// restartAll 崩溃并重启全部节点（模拟全集群滚动升级）。
func restartAll(sim *Sim, ids ...NodeID) {
	for _, id := range ids {
		sim.Crash(id)
	}
	for _, id := range ids {
		sim.Restart(id)
	}
}

// TestUpgradeRestartElectsLeader 复现并验证升级路径：
// 三个节点都带着旧版本格式的历史状态（任期、日志、提交点均非初始值）重启，
// 重启后必须能重新认出成员身份、选出领导者并继续推进提交。
func TestUpgradeRestartElectsLeader(t *testing.T) {
	sim := NewSim(3, testConfig(), 2026)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("升级前未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 5; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("pre-upgrade-%d", i)))
	}
	sim.Run(30)
	commitBefore := sim.Node(leader).CommitIndex()
	t.Logf("升级前: leader=%d commit=%d\n%s", leader, commitBefore, sim.DumpState())

	// 全部节点改写为旧版本格式并滚动重启。
	for _, id := range []NodeID{1, 2, 3} {
		stripMembershipToV1(t, sim, id)
	}
	restartAll(sim, 1, 2, 3)
	t.Logf("升级重启后:\n%s", sim.DumpState())

	// 重启后每个节点都应认出自己的成员身份。
	for _, id := range []NodeID{1, 2, 3} {
		if !sim.Node(id).IsVoter(id) {
			t.Fatalf("node %d 升级重启后未认出自己的投票成员身份 voters=%v",
				id, sim.Node(id).Voters())
		}
	}

	// 集群应能重新选出领导者。
	waitFor(t, sim, 200, "升级后重新选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	leader, _ = sim.Leader()
	t.Logf("升级后 leader=%d", leader)

	// 继续写入并推进提交点。
	for i := 0; i < 3; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("post-upgrade-%d", i))) {
			t.Fatalf("升级后写入被拒绝")
		}
	}
	sim.Run(30)
	for _, id := range []NodeID{1, 2, 3} {
		if sim.Node(id).CommitIndex() <= commitBefore {
			t.Fatalf("node %d 提交点未推进: before=%d after=%d\n%s",
				id, commitBefore, sim.Node(id).CommitIndex(), sim.DumpState())
		}
	}
	assertAppliedConsistent(t, sim, 1, 2, 3)
	t.Logf("升级后状态:\n%s", sim.DumpState())
}

// TestUpgradeRestartWithSnapshot 验证：老节点升级前已做过日志压缩
// （持久化状态里有 SnapIndex/SnapTerm/SnapData，但没有旧版本不存在的
// SnapVoters/SnapLearners），重启后仍能从 Config.Peers 恢复成员基底，
// 且快照点与提交点不丢失、集群继续推进。
func TestUpgradeRestartWithSnapshot(t *testing.T) {
	sim := NewSim(3, testConfig(), 777)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("升级前未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 8; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	sim.Run(30)
	// 全部节点压缩历史（旧版本快照不含成员元数据）。
	for _, id := range []NodeID{1, 2, 3} {
		n := sim.Node(id)
		n.Compact(n.CommitIndex(), []byte(fmt.Sprintf("snap-of-%d", id)))
	}
	sim.Run(10)
	commitBefore := sim.Node(leader).CommitIndex()
	t.Logf("升级前(已压缩): leader=%d commit=%d\n%s", leader, commitBefore, sim.DumpState())

	for _, id := range []NodeID{1, 2, 3} {
		stripMembershipToV1(t, sim, id)
	}
	restartAll(sim, 1, 2, 3)
	t.Logf("升级重启后:\n%s", sim.DumpState())

	// 快照点与提交点必须保留。
	for _, id := range []NodeID{1, 2, 3} {
		n := sim.Node(id)
		if n.CommitIndex() < commitBefore {
			t.Fatalf("node %d 重启后提交点回退: before=%d after=%d", id, commitBefore, n.CommitIndex())
		}
		if !n.IsVoter(id) {
			t.Fatalf("node %d 升级重启后未认出成员身份 voters=%v", id, n.Voters())
		}
	}

	waitFor(t, sim, 200, "带快照升级后重新选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	leader, _ = sim.Leader()
	for i := 8; i < 11; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i))) {
			t.Fatalf("升级后写入被拒绝")
		}
	}
	sim.Run(30)
	for _, id := range []NodeID{1, 2, 3} {
		if sim.Node(id).CommitIndex() <= commitBefore {
			t.Fatalf("node %d 提交点未推进: before=%d after=%d\n%s",
				id, commitBefore, sim.Node(id).CommitIndex(), sim.DumpState())
		}
	}
	assertAppliedConsistent(t, sim, 1, 2, 3)
	t.Logf("升级后状态:\n%s", sim.DumpState())
}

// assertSameMembership 断言各节点的成员视图完全一致。
func assertSameMembership(t *testing.T, sim *Sim, ids ...NodeID) {
	t.Helper()
	base := sim.Node(ids[0])
	for _, id := range ids[1:] {
		n := sim.Node(id)
		if fmt.Sprint(n.Voters()) != fmt.Sprint(base.Voters()) ||
			fmt.Sprint(n.Learners()) != fmt.Sprint(base.Learners()) {
			t.Fatalf("成员视图不一致: node=%d voters=%v learners=%v vs node=%d voters=%v learners=%v\n%s",
				ids[0], base.Voters(), base.Learners(), id, n.Voters(), n.Learners(), sim.DumpState())
		}
	}
	t.Logf("成员视图一致: voters=%v learners=%v", base.Voters(), base.Learners())
}

// TestUpgradeMixedClusterConverges 验证混合部署：
// 一部分节点是新格式状态、一部分是旧格式状态，混跑时集群照常选举与提交，
// 后续动态扩容后所有节点（含升级上来的老节点）收敛到同一份成员配置。
func TestUpgradeMixedClusterConverges(t *testing.T) {
	sim := NewSim(3, testConfig(), 31337)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 4; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("mix-%d", i)))
	}
	sim.Run(30)

	// 只有 node 3 是"带旧版本状态升级"的节点；1、2 保持新格式。
	stripMembershipToV1(t, sim, 3)
	sim.Crash(3)
	sim.Restart(3)
	t.Logf("混合状态: node3 旧格式重启后 voters=%v\n%s", sim.Node(3).Voters(), sim.DumpState())

	waitFor(t, sim, 200, "混合集群选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	assertSameMembership(t, sim, 1, 2, 3)

	// 混合状态下继续写入并提交。
	leader, _ = sim.Leader()
	for i := 4; i < 7; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("mix-%d", i))) {
			t.Fatalf("混合集群写入被拒绝")
		}
	}
	sim.Run(30)
	assertAppliedConsistent(t, sim, 1, 2, 3)

	// 混合状态下发起动态扩容，所有节点应收敛到同一份新配置。
	if !sim.AddNode(4) {
		sim.Run(20)
		if !sim.AddNode(4) {
			t.Fatalf("扩容提议未被接受\n%s", sim.DumpState())
		}
	}
	waitFor(t, sim, 300, "node 4 提升为 voter", func() bool {
		l, ok := sim.Leader()
		return ok && sim.Node(l).IsVoter(4)
	})
	waitFor(t, sim, 300, "全部节点收敛到 [1 2 3 4]", func() bool {
		for _, id := range []NodeID{1, 2, 3, 4} {
			if !sim.Node(id).IsVoter(4) {
				return false
			}
		}
		return true
	})
	assertSameMembership(t, sim, 1, 2, 3, 4)
	assertAppliedConsistent(t, sim, 1, 2, 3, 4)
	t.Logf("收敛后状态:\n%s", sim.DumpState())
}

// TestRemovedNodeStaysNonMemberAfterRestart 是回退逻辑的保护性用例：
// 被动态摘除的节点，其持久化状态里带有成员信息（日志中的变更条目），
// 重启时绝不能回退到 Config.Peers 把自己误判回成员。
func TestRemovedNodeStaysNonMemberAfterRestart(t *testing.T) {
	sim := NewSim(3, testConfig(), 555)
	sim.Run(50)
	if _, ok := sim.Leader(); !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	if !sim.RemoveNode(3) {
		sim.Run(20)
		if !sim.RemoveNode(3) {
			t.Fatalf("摘除提议未被接受\n%s", sim.DumpState())
		}
	}
	waitFor(t, sim, 200, "node 3 看到自己被摘除", func() bool {
		return !sim.Node(3).IsVoter(3) && !sim.Node(3).IsLearner(3)
	})
	t.Logf("摘除后:\n%s", sim.DumpState())

	// 被摘除节点重启：成员认知来自持久化日志，不得回退到引导配置。
	sim.Crash(3)
	sim.Restart(3)
	if sim.Node(3).IsVoter(3) || sim.Node(3).IsLearner(3) {
		t.Fatalf("被摘除节点重启后被误判回成员: voters=%v learners=%v",
			sim.Node(3).Voters(), sim.Node(3).Learners())
	}

	// 被摘除节点不得竞选、不得成为领导者；集群其余成员照常工作。
	sim.Run(200)
	if sim.Node(3).Role() != Follower {
		t.Fatalf("被摘除节点重启后角色异常: %s", sim.Node(3).Role())
	}
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("摘除重启后集群无唯一领导者\n%s", sim.DumpState())
	}
	if leader == 3 {
		t.Fatalf("被摘除节点不应成为领导者")
	}
	if !sim.Node(leader).Propose([]byte("after-remove")) {
		t.Fatalf("摘除后写入被拒绝")
	}
	sim.Run(30)
	assertAppliedConsistent(t, sim, 1, 2)
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestUpgradeRestartMembershipStableAcrossRestarts 验证升级后的成员认知
// 会被正确写回持久化存储：重启前后成员视图一致，且反复重启不漂移。
func TestUpgradeRestartMembershipStableAcrossRestarts(t *testing.T) {
	sim := NewSim(3, testConfig(), 9090)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 3; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("stab-%d", i)))
	}
	sim.Run(30)

	for _, id := range []NodeID{1, 2, 3} {
		stripMembershipToV1(t, sim, id)
	}
	restartAll(sim, 1, 2, 3)
	waitFor(t, sim, 200, "升级后选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	sim.Run(20) // 让各节点至少持久化一次（心跳/投票都会触发落盘）

	// 重启后成员视图与升级前一致，且已写回持久化存储。
	for _, id := range []NodeID{1, 2, 3} {
		if got := fmt.Sprint(sim.Node(id).Voters()); got != "[1 2 3]" {
			t.Fatalf("node %d 升级后成员视图异常: voters=%s", id, got)
		}
		ps, err := sim.storage[id].Load()
		if err != nil {
			t.Fatalf("load node %d: %v", id, err)
		}
		if fmt.Sprint(ps.SnapVoters) != "[1 2 3]" {
			t.Fatalf("node %d 持久化的快照成员错误: SnapVoters=%v", id, ps.SnapVoters)
		}
		t.Logf("node %d 持久化成员已写回: SnapVoters=%v term=%d commit=%d",
			id, ps.SnapVoters, ps.CurrentTerm, ps.CommitIndex)
	}

	// 再次滚动重启：成员视图不得漂移。
	restartAll(sim, 1, 2, 3)
	for _, id := range []NodeID{1, 2, 3} {
		if got := fmt.Sprint(sim.Node(id).Voters()); got != "[1 2 3]" {
			t.Fatalf("node %d 二次重启后成员漂移: voters=%s", id, got)
		}
	}
	waitFor(t, sim, 200, "二次重启后重新选出领导者", func() bool {
		_, ok := sim.Leader()
		return ok
	})
	assertSameMembership(t, sim, 1, 2, 3)
	t.Logf("二次重启后状态:\n%s", sim.DumpState())
}
