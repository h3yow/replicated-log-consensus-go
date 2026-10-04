package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// waitFor 逐拍推进仿真直到条件满足或超时（超时即失败并转储状态）。
func waitFor(t *testing.T, sim *Sim, maxTicks int, desc string, cond func() bool) {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		if cond() {
			t.Logf("tick 条件满足: %s (用了<%d拍)", desc, maxTicks)
			return
		}
		sim.Tick()
	}
	t.Fatalf("等待超时: %s\n%s\n事件:\n%v", desc, sim.DumpState(), sim.Events())
}

// appliedSeq 收集节点已应用序列。快照覆盖的前缀对本测试不可见
// （内容由快照正确性用例保证），用 "" 占位保持索引对齐，
// 使逐位比较只在双方都确切应用过命令的位置上生效。
func appliedSeq(sim *Sim, id NodeID) []string {
	var out []string
	for _, am := range sim.Applied(id) {
		if am.Snapshot {
			for len(out) < int(am.Index) {
				out = append(out, "")
			}
			continue
		}
		for len(out) < int(am.Index) {
			out = append(out, "")
		}
		if am.Command != nil {
			out[int(am.Index)-1] = string(am.Command)
		}
	}
	return out
}

// assertAppliedConsistent 断言任意两节点已应用序列在非空位置上不冲突。
func assertAppliedConsistent(t *testing.T, sim *Sim, ids ...NodeID) {
	t.Helper()
	seqs := map[NodeID][]string{}
	for _, id := range ids {
		seqs[id] = appliedSeq(sim, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := seqs[ids[i]], seqs[ids[j]]
			for k := 0; k < len(a) && k < len(b); k++ {
				if a[k] != "" && b[k] != "" && a[k] != b[k] {
					t.Fatalf("状态机安全性违反: node=%d 与 node=%d 索引=%d: %q vs %q\n%s",
						ids[i], ids[j], k+1, a[k], b[k], sim.DumpState())
				}
			}
		}
	}
}

// TestAddNodeCatchUpAndPromote 验证扩容全流程：
// 空节点以学习者加入 → 历史已压缩 → 通过快照+日志追平 → 自动提升为投票成员。
// 全程集群可持续写入，扩容前后选举与提交正常。
func TestAddNodeCatchUpAndPromote(t *testing.T) {
	sim := NewSim(3, testConfig(), 101)
	sim.Run(50)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}

	// 写入一批命令并提交。
	for i := 0; i < 10; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	sim.Run(30)
	// 各节点压缩历史（模拟"历史早就被压缩掉"的场景）。
	for _, id := range []NodeID{1, 2, 3} {
		n := sim.Node(id)
		n.Compact(n.CommitIndex(), []byte(fmt.Sprintf("snap-of-%d", id)))
	}
	// 压缩后继续写入。
	for i := 10; i < 15; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	sim.Run(30)
	t.Logf("扩容前状态:\n%s", sim.DumpState())

	// 发起扩容：node 4 是全新空节点。
	if !sim.AddNode(4) {
		sim.Run(20) // 领导者可能尚未提交本任期条目，稍后重试
		if !sim.AddNode(4) {
			t.Fatalf("扩容提议未被接受\n%s", sim.DumpState())
		}
	}

	// 等待 node 4 被提升为投票成员（以领导者视角为准）。
	waitFor(t, sim, 300, "node 4 提升为 voter", func() bool {
		l, ok := sim.Leader()
		return ok && sim.Node(l).IsVoter(4)
	})
	// 等待 node 4 自己也看到新配置并追平提交点。
	waitFor(t, sim, 300, "node 4 视角含自己为 voter 且提交点收敛", func() bool {
		l, lok := sim.Leader()
		return lok && sim.Node(4).IsVoter(4) &&
			sim.Node(4).CommitIndex() == sim.Node(l).CommitIndex()
	})

	// 扩容后集群继续接受写入。
	leader, ok = sim.Leader()
	if !ok {
		t.Fatalf("扩容后无唯一领导者\n%s", sim.DumpState())
	}
	for i := 15; i < 20; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i))) {
			t.Fatalf("扩容后写入被拒绝")
		}
	}
	sim.Run(60)

	// 最终一致：四个节点提交点相同，已应用序列互不冲突。
	assertAppliedConsistent(t, sim, 1, 2, 3, 4)
	c := sim.Node(leader).CommitIndex()
	for _, id := range []NodeID{1, 2, 3, 4} {
		if sim.Node(id).CommitIndex() != c {
			t.Fatalf("node=%d 提交点=%d 期望=%d\n%s", id, sim.Node(id).CommitIndex(), c, sim.DumpState())
		}
	}
	// node 4 必须持有快照（历史被压缩过，只能经快照追赶）。
	if sim.Node(4).LastLogIndex() < c {
		t.Fatalf("node 4 日志不完整\n%s", sim.DumpState())
	}
	t.Logf("扩容完成: node4 commit=%d voters=%v\n最终状态:\n%s",
		sim.Node(4).CommitIndex(), sim.Node(leader).Voters(), sim.DumpState())
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestRemoveNode 验证缩容：被摘除节点退出多数派判定；
// 集群在摘除后继续正常选主与写入。
func TestRemoveNode(t *testing.T) {
	sim := NewSim(5, testConfig(), 202)
	sim.Run(60)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	// 选一个非领导者节点摘除。
	victim := NodeID(0)
	for _, id := range []NodeID{1, 2, 3, 4, 5} {
		if id != leader {
			victim = id
			break
		}
	}
	for i := 0; i < 5; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("pre-%d", i)))
	}
	sim.Run(30)

	if !sim.RemoveNode(victim) {
		sim.Run(20)
		if !sim.RemoveNode(victim) {
			t.Fatalf("缩容提议未被接受\n%s", sim.DumpState())
		}
	}
	// 等待新配置（不含 victim）在领导者处生效并提交。
	waitFor(t, sim, 200, "领导者配置不再含 victim", func() bool {
		l, ok := sim.Leader()
		return ok && !sim.Node(l).IsVoter(victim) && !sim.Node(l).IsLearner(victim)
	})
	// 等待 victim 自己也学到（它仍在线，只是不再参与）。
	waitFor(t, sim, 200, "victim 自己学到被摘除", func() bool {
		n := sim.Node(victim)
		return !n.IsVoter(victim) && !n.IsLearner(victim)
	})

	// 摘除后集群继续写入并提交。
	leader, ok = sim.Leader()
	if !ok {
		t.Fatalf("缩容后无唯一领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 5; i++ {
		if !sim.Node(leader).Propose([]byte(fmt.Sprintf("post-%d", i))) {
			t.Fatalf("缩容后写入被拒绝")
		}
	}
	sim.Run(60)

	// 留下的 4 个节点收敛且互不冲突；victim 不再收到新条目。
	others := []NodeID{}
	for _, id := range []NodeID{1, 2, 3, 4, 5} {
		if id != victim {
			others = append(others, id)
		}
	}
	assertAppliedConsistent(t, sim, others...)
	c := sim.Node(leader).CommitIndex()
	for _, id := range others {
		if sim.Node(id).CommitIndex() != c {
			t.Fatalf("node=%d 提交点=%d 期望=%d\n%s", id, sim.Node(id).CommitIndex(), c, sim.DumpState())
		}
	}
	if sim.Node(victim).CommitIndex() >= c {
		t.Fatalf("victim 不应继续推进提交点: victim=%d cluster=%d\n%s",
			sim.Node(victim).CommitIndex(), c, sim.DumpState())
	}
	if sim.Node(victim).Role() == Leader {
		t.Fatalf("victim 不得成为领导者\n%s", sim.DumpState())
	}
	t.Logf("缩容完成: victim=%d 剩余成员 voters=%v\n%s", victim, sim.Node(leader).Voters(), sim.DumpState())
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestRemoveLeaderSelf 验证摘除领导者自身：
// 变更提交后旧领导者让位，其余节点选出新领导者，集群继续服务。
func TestRemoveLeaderSelf(t *testing.T) {
	sim := NewSim(3, testConfig(), 303)
	sim.Run(60)
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 3; i++ {
		sim.Node(leader).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	sim.Run(30)

	if !sim.RemoveNode(leader) {
		sim.Run(20)
		if !sim.RemoveNode(leader) {
			t.Fatalf("摘除领导者的提议未被接受\n%s", sim.DumpState())
		}
	}
	// 旧领导者应在变更提交后让位。
	waitFor(t, sim, 200, "旧领导者让位", func() bool {
		return sim.Node(leader).Role() != Leader
	})
	if got := sim.Node(leader).LastStepDown(); got != StepDownRemoved && got != StepDownHigherTerm && got != StepDownLeaderDiscovered {
		t.Fatalf("旧领导者让位原因不可解释: %s", got)
	}
	t.Logf("旧领导者=%d 让位原因=%s", leader, sim.Node(leader).LastStepDown())

	// 剩余两节点选出新领导者（不含旧领导者）。
	waitFor(t, sim, 200, "剩余成员选出新领导者", func() bool {
		l, ok := sim.Leader()
		return ok && l != leader
	})
	newLeader, _ := sim.Leader()
	if !sim.Node(newLeader).IsVoter(newLeader) || sim.Node(newLeader).IsVoter(leader) {
		t.Fatalf("新领导者配置不正确: voters=%v", sim.Node(newLeader).Voters())
	}
	// 新配置下继续写入。
	for i := 3; i < 6; i++ {
		if !sim.Node(newLeader).Propose([]byte(fmt.Sprintf("cmd-%d", i))) {
			t.Fatalf("新领导者写入被拒绝")
		}
	}
	sim.Run(60)
	assertAppliedConsistent(t, sim, 1, 2, 3)
	t.Logf("新领导者=%d voters=%v\n%s", newLeader, sim.Node(newLeader).Voters(), sim.DumpState())
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// TestRemovedNodeCannotDisruptAfterReconnect 验证关键安全性质：
// 被摘除的节点（此处直接摘除分区中的旧领导者）恢复连接后，
// 不得凭旧的成员认知重新当选，也不得用过期日志影响集群。
func TestRemovedNodeCannotDisruptAfterReconnect(t *testing.T) {
	sim := NewSim(5, testConfig(), 404)
	sim.Run(60)
	oldLeader, ok := sim.Leader()
	if !ok {
		t.Fatalf("未选出领导者\n%s", sim.DumpState())
	}
	for i := 0; i < 3; i++ {
		sim.Node(oldLeader).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
	}
	sim.Run(30)

	// 分区旧领导者，剩余 4 节点选出新领导者并把它摘除。
	sim.Partition(oldLeader)
	sim.Run(80)
	// 注意：被分区的旧领导者仍自认为是 Leader，不能用 sim.Leader() 判定。
	waitFor(t, sim, 200, "剩余成员选出新领导者", func() bool {
		for _, id := range sim.sortedIDs() {
			if id != oldLeader && sim.Node(id).Role() == Leader {
				return true
			}
		}
		return false
	})
	if !sim.RemoveNode(oldLeader) {
		sim.Run(20)
		if !sim.RemoveNode(oldLeader) {
			t.Fatalf("摘除提议未被接受\n%s", sim.DumpState())
		}
	}
	// 成员侧领导者（排除被分区的旧领导者）提交摘除变更。
	memberLeaderID := func() (NodeID, bool) {
		for _, id := range sim.sortedIDs() {
			if id != oldLeader && sim.Node(id).Role() == Leader {
				return id, true
			}
		}
		return 0, false
	}
	waitFor(t, sim, 200, "摘除提交", func() bool {
		l, ok := memberLeaderID()
		return ok && !sim.Node(l).IsVoter(oldLeader) &&
			sim.Node(l).CommitIndex() >= sim.Node(l).LastLogIndex()
	})
	memberLeader, _ := memberLeaderID()
	t.Logf("新领导者=%d 被摘除者=%d(分区中) voters=%v",
		memberLeader, oldLeader, sim.Node(memberLeader).Voters())

	// 恢复分区：旧领导者带着旧认知（自己还是 5 节点集群的成员/领导者）回来。
	sim.Heal()
	// 它应先因更高任期让位，之后的竞选一率被成员以 NotMember 拒绝。
	waitFor(t, sim, 100, "被摘除的旧领导者让位", func() bool {
		return sim.Node(oldLeader).Role() != Leader
	})
	// 全程记录每个任期的领导者，断言选举安全性。
	leadersByTerm := map[Term]NodeID{}
	for tick := 0; tick < 300; tick++ {
		// 集群在干扰下持续接受写入。
		if tick%20 == 0 {
			if l, ok := sim.Leader(); ok {
				sim.Node(l).Propose([]byte(fmt.Sprintf("post-%d", tick)))
			}
		}
		sim.Tick()
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
	}

	// 被摘除者绝不得重新当选。
	for term, id := range leadersByTerm {
		if id == oldLeader {
			t.Fatalf("被摘除节点在 term=%d 重新当选\n%s", term, sim.DumpState())
		}
	}
	if sim.Node(oldLeader).Role() == Leader {
		t.Fatalf("被摘除节点仍是领导者\n%s", sim.DumpState())
	}
	// 集群仍有唯一领导者且写入全部提交。
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("干扰后无唯一领导者\n%s", sim.DumpState())
	}
	n := sim.Node(leader)
	if n.CommitIndex() < n.LastLogIndex() {
		t.Fatalf("存在未提交条目: commit=%d last=%d\n%s", n.CommitIndex(), n.LastLogIndex(), sim.DumpState())
	}
	// 成员间最终一致；被摘除者的过期日志没有污染任何人。
	members := []NodeID{}
	for _, id := range sim.sortedIDs() {
		if id != oldLeader {
			members = append(members, id)
		}
	}
	assertAppliedConsistent(t, sim, members...)
	t.Logf("被摘除者=%d role=%s term=%d 集群领导者=%d term=%d commit=%d\n%s",
		oldLeader, sim.Node(oldLeader).Role(), sim.Node(oldLeader).Term(),
		leader, n.Term(), n.CommitIndex(), sim.DumpState())
	for _, e := range sim.Events() {
		t.Log(e)
	}
}

// --- 全程不变量检查器 ---

// safetyChecker 在扩缩容全程逐拍断言安全不变量：
//   - 选举安全性：同一任期至多一个领导者；
//   - 状态机安全性：任意两节点已应用序列在非空位置不冲突。
type safetyChecker struct {
	t             *testing.T
	sim           *Sim
	leadersByTerm map[Term]NodeID
	applied       map[NodeID][]string
}

func newSafetyChecker(t *testing.T, sim *Sim) *safetyChecker {
	return &safetyChecker{
		t:             t,
		sim:           sim,
		leadersByTerm: map[Term]NodeID{},
		applied:       map[NodeID][]string{},
	}
}

func (c *safetyChecker) check() {
	for _, id := range c.sim.sortedIDs() {
		n := c.sim.Node(id)
		if n.Role() == Leader {
			if prev, ok := c.leadersByTerm[n.Term()]; ok && prev != id {
				c.t.Fatalf("选举安全性违反: term=%d 领导者 %d 与 %d\n%s",
					n.Term(), prev, id, c.sim.DumpState())
			}
			c.leadersByTerm[n.Term()] = id
		}
		for _, am := range c.sim.Applied(id) {
			seq := c.applied[id]
			if am.Snapshot {
				for len(seq) < int(am.Index) {
					seq = append(seq, "")
				}
			} else {
				for len(seq) < int(am.Index) {
					seq = append(seq, "")
				}
				if am.Command != nil {
					idx := int(am.Index) - 1
					if prev := seq[idx]; prev != "" && prev != string(am.Command) {
						c.t.Fatalf("状态机安全性违反: node=%d 索引=%d 被应用为 %q 与 %q\n%s",
							id, am.Index, prev, am.Command, c.sim.DumpState())
					}
					seq[idx] = string(am.Command)
				}
			}
			c.applied[id] = seq
		}
	}
	ids := c.sim.sortedIDs()
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := c.applied[ids[i]], c.applied[ids[j]]
			for k := 0; k < len(a) && k < len(b); k++ {
				if a[k] != "" && b[k] != "" && a[k] != b[k] {
					c.t.Fatalf("状态机安全性违反: node=%d 与 node=%d 索引=%d: %q vs %q\n%s",
						ids[i], ids[j], k+1, a[k], b[k], c.sim.DumpState())
				}
			}
		}
	}
}

// waitForChecked 与 waitFor 相同，但每拍先运行不变量检查。
func waitForChecked(t *testing.T, sim *Sim, sc *safetyChecker, maxTicks int, desc string, cond func() bool) {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		if cond() {
			t.Logf("tick 条件满足: %s", desc)
			return
		}
		sim.Tick()
		sc.check()
	}
	t.Fatalf("等待超时: %s\n%s\n事件:\n%v", desc, sim.DumpState(), sim.Events())
}

// TestConsecutiveMembershipChanges 验证连续来回扩缩容：
// 加 4 → 摘 2 → 加 5 → 摘 4 → 加 6 → 摘 6，每一步收敛后才进行下一步，
// 全程断言安全不变量，且集群持续可写。
func TestConsecutiveMembershipChanges(t *testing.T) {
	sim := NewSim(3, testConfig(), 505)
	sc := newSafetyChecker(t, sim)
	sim.Run(60)
	sc.check()

	writeCmds := func(tag string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			leader, ok := sim.Leader()
			if !ok {
				sim.Run(10)
				sc.check()
				leader, ok = sim.Leader()
				if !ok {
					t.Fatalf("阶段 %s: 无领导者\n%s", tag, sim.DumpState())
				}
			}
			sim.Node(leader).Propose([]byte(fmt.Sprintf("%s-%d", tag, i)))
		}
		sim.Run(20)
		sc.check()
	}
	writeCmds("init", 3)

	steps := []struct {
		op string
		id NodeID
	}{
		{"add", 4},
		{"remove", 2},
		{"add", 5},
		{"remove", 4},
		{"add", 6},
		{"remove", 6},
	}
	for _, st := range steps {
		desc := fmt.Sprintf("%s(%d)", st.op, st.id)
		// 发起变更（领导者忙时重试）。
		waitForChecked(t, sim, sc, 200, desc+" 提议被接受", func() bool {
			if st.op == "add" {
				return sim.AddNode(st.id)
			}
			return sim.RemoveNode(st.id)
		})
		// 等待变更在领导者视角生效。
		waitForChecked(t, sim, sc, 300, desc+" 生效", func() bool {
			leader, ok := sim.Leader()
			if !ok {
				return false
			}
			if st.op == "add" {
				return sim.Node(leader).IsVoter(st.id)
			}
			return !sim.Node(leader).IsVoter(st.id) && !sim.Node(leader).IsLearner(st.id)
		})
		writeCmds("after-"+desc, 2)
		t.Logf("阶段 %s 完成:\n%s", desc, sim.DumpState())
	}

	// 全部变更后收敛：成员一致、提交点一致。
	sim.Run(200)
	sc.check()
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("最终无唯一领导者\n%s", sim.DumpState())
	}
	wantVoters := sim.Node(leader).Voters()
	t.Logf("最终 voters=%v", wantVoters)
	for _, id := range wantVoters {
		n := sim.Node(id)
		if got := n.Voters(); fmt.Sprint(got) != fmt.Sprint(wantVoters) {
			t.Fatalf("node=%d 成员视图=%v 期望=%v\n%s", id, got, wantVoters, sim.DumpState())
		}
		if n.CommitIndex() != sim.Node(leader).CommitIndex() {
			t.Fatalf("node=%d 提交点=%d 期望=%d\n%s",
				id, n.CommitIndex(), sim.Node(leader).CommitIndex(), sim.DumpState())
		}
	}
	// 被摘除的节点（2、4、6）不得是领导者，也不得在最终配置中。
	for _, id := range []NodeID{2, 4, 6} {
		if sim.Node(id).Role() == Leader {
			t.Fatalf("被摘除节点 %d 仍是领导者\n%s", id, sim.DumpState())
		}
		if sim.Node(leader).IsVoter(id) || sim.Node(leader).IsLearner(id) {
			t.Fatalf("被摘除节点 %d 仍在最终配置中\n%s", id, sim.DumpState())
		}
	}
	sc.check()
	t.Logf("连续扩缩容完成，最终状态:\n%s", sim.DumpState())
}

// runMembershipFaultInjection 在固定种子下执行一次可重放的
// "动态扩缩容 + 全故障注入"运行：丢包/重复/乱序/分区/重启随机注入，
// 同时随机发起扩容/缩容与写入。全程断言安全不变量，收尾断言最终一致。
func runMembershipFaultInjection(t *testing.T, seed int64, ticks int) {
	t.Helper()
	sim := NewSim(4, testConfig(), seed)
	sc := newSafetyChecker(t, sim)
	faultRnd := rand.New(rand.NewSource(seed ^ 0x5eed))

	nextID := NodeID(5)
	spawned := map[NodeID]bool{} // 运行期加入过的节点
	removed := map[NodeID]bool{} // 已被接受摘除的节点
	confChanges := 0
	proposalSeq := 0

	randomID := func() NodeID {
		ids := sim.sortedIDs()
		return ids[faultRnd.Intn(len(ids))]
	}

	for tick := 0; tick < ticks; tick++ {
		switch faultRnd.Intn(48) {
		case 0:
			sim.Partition(randomID())
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
			if faultRnd.Intn(3) == 0 {
				victim := randomID()
				sim.Crash(victim)
				sim.Restart(victim)
			}
		case 8:
			// 扩容：新节点以空配置加入。
			if confChanges < 10 && sim.AddNode(nextID) {
				t.Logf("tick=%d 扩容提议被接受: node %d", sim.tick, nextID)
				spawned[nextID] = true
				delete(removed, nextID)
				nextID++
				confChanges++
			}
		case 9:
			// 缩容：从当前成员中随机挑选（保留至少 2 个投票成员，避免无法收敛）。
			if confChanges < 10 {
				leader, ok := sim.Leader()
				if ok && len(sim.Node(leader).Voters()) > 2 {
					victim := randomID()
					if sim.RemoveNode(victim) {
						t.Logf("tick=%d 缩容提议被接受: node %d", sim.tick, victim)
						removed[victim] = true
						confChanges++
					}
				}
			}
		}
		// 随机压缩历史（快照元数据携带成员配置，落后节点经快照学习配置）。
		if faultRnd.Intn(25) == 0 {
			for _, id := range sim.sortedIDs() {
				n := sim.Node(id)
				if c := n.CommitIndex(); c > 0 {
					n.Compact(c, []byte(fmt.Sprintf("snap-%d@%d", id, c)))
				}
			}
		}
		// 随机写入。
		if faultRnd.Intn(10) == 0 {
			if leader, ok := sim.Leader(); ok {
				cmd := fmt.Sprintf("seed=%d-cmd=%d", seed, proposalSeq)
				if sim.Node(leader).Propose([]byte(cmd)) {
					proposalSeq++
				}
			}
		}
		sim.Tick()
		sc.check()
	}

	// 收尾：清除全部故障，重试未完成的扩容，给集群足够时间收敛。
	sim.Heal()
	sim.SetLoss(0)
	sim.SetDup(0)
	sim.SetReorder(false)
	settled := false
	for i := 0; i < 1500 && !settled; i++ {
		if i%50 == 0 {
			// 重试尚未生效的扩容（故障期间可能未被接受）。
			for id := range spawned {
				if removed[id] {
					continue
				}
				if leader, ok := sim.Leader(); ok && !sim.Node(leader).IsVoter(id) {
					sim.AddNode(id)
				}
			}
		}
		sim.Tick()
		sc.check()
		leader, ok := sim.Leader()
		if !ok {
			continue
		}
		settled = true
		// 收敛判定：无学习者滞留、所有应加入的节点都已成为投票成员、
		// 全体成员提交点一致。
		if len(sim.Node(leader).Learners()) != 0 {
			settled = false
			continue
		}
		for id := range spawned {
			if !removed[id] && !sim.Node(leader).IsVoter(id) {
				settled = false
			}
		}
		c := sim.Node(leader).CommitIndex()
		for _, id := range sim.Node(leader).Voters() {
			if sim.Node(id).CommitIndex() != c {
				settled = false
			}
		}
	}
	if !settled {
		t.Fatalf("收尾未收敛\n%s\n事件:\n%v", sim.DumpState(), sim.Events())
	}

	// 最终一致性与成员一致性。
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("最终无唯一领导者\n%s", sim.DumpState())
	}
	voters := sim.Node(leader).Voters()
	c := sim.Node(leader).CommitIndex()
	for _, id := range voters {
		n := sim.Node(id)
		if got := n.Voters(); fmt.Sprint(got) != fmt.Sprint(voters) {
			t.Fatalf("node=%d 成员视图=%v 期望=%v\n%s", id, got, voters, sim.DumpState())
		}
		if n.CommitIndex() != c {
			t.Fatalf("node=%d 提交点=%d 期望=%d\n%s", id, n.CommitIndex(), c, sim.DumpState())
		}
	}
	// 被摘除节点：不得是领导者，不得出现在最终配置中。
	for id := range removed {
		if sim.Node(id).Role() == Leader {
			t.Fatalf("被摘除节点 %d 仍是领导者\n%s", id, sim.DumpState())
		}
		if sim.Node(leader).IsVoter(id) || sim.Node(leader).IsLearner(id) {
			t.Fatalf("被摘除节点 %d 仍在最终配置中\n%s", id, sim.DumpState())
		}
	}
	sc.check()
	t.Logf("seed=%d 收敛: 提案=%d 变更=%d 最终 voters=%v commit=%d 任期数=%d",
		seed, proposalSeq, confChanges, voters, c, len(sc.leadersByTerm))
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestMembershipFaultInjection 固定种子下的动态扩缩容故障注入。
func TestMembershipFaultInjection(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runMembershipFaultInjection(t, seed, 500)
		})
	}
}

// TestMembershipSurvivesRestart 验证成员配置随持久化状态跨重启恢复：
// 重启后节点从"快照元数据 + 日志条目"重新推导配置，
// 与集群其余成员一致；压缩后的重启同样正确。
func TestMembershipSurvivesRestart(t *testing.T) {
	sim := NewSim(3, testConfig(), 606)
	sc := newSafetyChecker(t, sim)
	sim.Run(60)
	sc.check()

	// 扩容 node 4 并等待提升完成。
	waitForChecked(t, sim, sc, 300, "add(4) 被接受", func() bool { return sim.AddNode(4) })
	waitForChecked(t, sim, sc, 300, "node 4 成为 voter", func() bool {
		l, ok := sim.Leader()
		return ok && sim.Node(l).IsVoter(4) && sim.Node(4).IsVoter(4)
	})
	leader, _ := sim.Leader()
	sim.Node(leader).Propose([]byte("before-restart"))
	sim.Run(30)
	sc.check()

	// 压缩全部历史后重启新节点：配置必须从快照元数据恢复。
	for _, id := range sim.sortedIDs() {
		n := sim.Node(id)
		n.Compact(n.CommitIndex(), []byte("snap"))
	}
	sim.Crash(4)
	sim.Restart(4)
	if !sim.Node(4).IsVoter(4) {
		t.Fatalf("重启后 node 4 丢失成员身份: voters=%v\n%s", sim.Node(4).Voters(), sim.DumpState())
	}
	if got := sim.Node(4).Voters(); fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("重启后 node 4 成员视图=%v 期望=[1 2 3 4]", got)
	}
	t.Logf("node 4 重启后配置恢复: voters=%v commit=%d", sim.Node(4).Voters(), sim.Node(4).CommitIndex())

	// 变更进行中重启另一个节点：未提交变更不得残留，已提交变更不得丢失。
	sim.Node(leader).Propose([]byte("x"))
	sim.Run(20)
	if !sim.RemoveNode(2) {
		sim.Run(20)
		if !sim.RemoveNode(2) {
			t.Fatalf("remove(2) 未被接受\n%s", sim.DumpState())
		}
	}
	sim.Run(5) // 变更已提议但未必提交
	victim := NodeID(3)
	if victim == leader {
		victim = 1
	}
	sim.Crash(victim)
	sim.Restart(victim)
	sim.Run(100)
	sc.check()

	// 收敛后：所有成员对最终配置达成一致。
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("重启后无领导者\n%s", sim.DumpState())
	}
	want := sim.Node(leader).Voters()
	for _, id := range want {
		if got := sim.Node(id).Voters(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node=%d 成员视图=%v 期望=%v\n%s", id, got, want, sim.DumpState())
		}
	}
	t.Logf("重启恢复后最终 voters=%v\n%s", want, sim.DumpState())
}

// TestMembershipDeterministicReplay 验证含扩缩容的运行在相同种子下逐字节重放。
func TestMembershipDeterministicReplay(t *testing.T) {
	run := func() []string {
		sim := NewSim(3, testConfig(), 888)
		sim.SetLoss(0.15)
		sim.SetDup(0.15)
		sim.SetReorder(true)
		sim.Run(60)
		sim.AddNode(4)
		for i := 0; i < 120; i++ {
			sim.Tick()
			if i == 40 {
				sim.RemoveNode(2)
			}
			if i == 80 {
				sim.AddNode(5)
			}
			if i%25 == 0 {
				if l, ok := sim.Leader(); ok {
					sim.Node(l).Propose([]byte(fmt.Sprintf("cmd-%d", i)))
				}
			}
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
	t.Logf("含扩缩容的两次运行产生完全相同的 %d 条事件", len(a))
}
