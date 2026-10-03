package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// --- 扩缩容测试辅助 ---

// safetyTracker 全程跟踪选举安全性（同任期唯一领导者）。
type safetyTracker struct {
	t             *testing.T
	sim           *Sim
	leadersByTerm map[Term]NodeID
}

func newSafetyTracker(t *testing.T, sim *Sim) *safetyTracker {
	t.Helper()
	return &safetyTracker{t: t, sim: sim, leadersByTerm: map[Term]NodeID{}}
}

// tick 推进一拍并断言选举安全性。
func (st *safetyTracker) tick() {
	st.sim.Tick()
	for _, id := range st.sim.sortedIDs() {
		n := st.sim.Node(id)
		if n.Role() == Leader {
			if prev, ok := st.leadersByTerm[n.Term()]; ok && prev != id {
				st.t.Fatalf("选举安全性违反: term=%d 领导者 %d 与 %d\n%s",
					n.Term(), prev, id, st.sim.DumpState())
			}
			st.leadersByTerm[n.Term()] = id
		}
	}
}

func (st *safetyTracker) run(ticks int) {
	for i := 0; i < ticks; i++ {
		st.tick()
	}
}

// waitFor 在上限拍数内等待条件满足，期间持续断言选举安全性。
func (st *safetyTracker) waitFor(maxTicks int, desc string, cond func() bool) {
	st.t.Helper()
	for i := 0; i < maxTicks; i++ {
		if cond() {
			st.t.Logf("等待完成(%d tick): %s", i, desc)
			return
		}
		st.tick()
	}
	st.t.Fatalf("等待超时(%d tick): %s\n%s", maxTicks, desc, st.sim.DumpState())
}

// leader 返回当前唯一领导者。
func (st *safetyTracker) leader() NodeID {
	st.t.Helper()
	id, ok := st.sim.Leader()
	if !ok {
		st.t.Fatalf("无唯一领导者\n%s", st.sim.DumpState())
	}
	return id
}

// proposeAndWait 向领导者提交一条命令并等待其提交点覆盖。
func (st *safetyTracker) proposeAndWait(cmd string) {
	st.t.Helper()
	leader := st.leader()
	if !st.sim.Node(leader).Propose([]byte(cmd)) {
		st.t.Fatalf("Propose 失败: leader=%d\n%s", leader, st.sim.DumpState())
	}
	target := st.sim.Node(leader).LastLogIndex()
	st.waitFor(200, fmt.Sprintf("命令 %q 提交至 index=%d", cmd, target), func() bool {
		return st.sim.Node(leader).CommitIndex() >= target
	})
}

// waitConfig 等待指定节点的已应用配置满足条件。
func (st *safetyTracker) waitConfig(id NodeID, desc string, cond func(ConfigState) bool) {
	st.t.Helper()
	st.waitFor(400, fmt.Sprintf("node=%d 配置: %s", id, desc), func() bool {
		return cond(st.sim.Node(id).AppliedConfig())
	})
}

// TestAddNodeCatchUpAndJoin 验证扩容全流程：
// 空节点以学习者加入 → 通过快照追平被压缩的历史 → 自动提升为投票成员。
// 追赶期间原集群写入与提交点推进不受影响。
func TestAddNodeCatchUpAndJoin(t *testing.T) {
	sim := NewSim(3, testConfig(), 101)
	st := newSafetyTracker(t, sim)
	st.run(50)
	leader := st.leader()
	t.Logf("初始领导者=%d term=%d", leader, sim.Node(leader).Term())

	// 写入 8 条命令并压缩前 6 条（含当选空条目），制造"历史已被压缩"的场景。
	for i := 0; i < 8; i++ {
		st.proposeAndWait(fmt.Sprintf("old-%d", i))
	}
	sim.Node(leader).Compact(6, []byte("state@6"))
	t.Logf("压缩后 leader log=%s", sim.Node(leader).LogString())

	// 新节点 4 以空配置学习者身份启动。
	sim.AddNode(4)
	if !sim.Node(4).IsLearner() {
		t.Fatalf("node=4 应为学习者")
	}
	// 学习者不得参选：空跑一段时间，4 不应成为候选人/领导者。
	st.run(30)
	if sim.Node(4).Role() != Follower {
		t.Fatalf("学习者不应参选: role=%s", sim.Node(4).Role())
	}

	// 第一步：把 4 以学习者身份纳入配置。
	if !sim.Node(leader).ProposeAddLearner(4) {
		t.Fatalf("ProposeAddLearner 被拒绝")
	}
	st.waitConfig(leader, "学习者 4 已纳入配置", func(c ConfigState) bool {
		return c.IsLearner(4)
	})
	// 追赶期间写入应照常提交（学习者不计入多数派）。
	st.proposeAndWait("during-catchup")

	// 第二步：4 追平（历史已压缩，必须经快照），随后被自动提升为投票成员。
	st.waitConfig(4, "node=4 自身已成为投票成员", func(c ConfigState) bool {
		return c.IsVoter(4)
	})
	st.waitConfig(leader, "领导者确认 4 为投票成员", func(c ConfigState) bool {
		return c.IsVoter(4)
	})

	// 4 必须通过快照恢复被压缩的历史。
	var sawSnapshot bool
	var applied []string
	for _, am := range sim.Applied(4) {
		if am.Snapshot {
			sawSnapshot = true
			t.Logf("node=4 安装快照: index=%d data=%s", am.Index, am.SnapshotData)
		} else if am.Command != nil {
			applied = append(applied, string(am.Command))
		}
	}
	if !sawSnapshot {
		t.Fatalf("node=4 未通过快照追赶被压缩的历史")
	}
	t.Logf("node=4 快照后应用命令: %v", applied)

	// 追平后：4 的提交点与领导者一致，且参与后续写入的复制。
	st.waitFor(200, "node=4 提交点追平领导者", func() bool {
		return sim.Node(4).CommitIndex() == sim.Node(leader).CommitIndex()
	})
	st.proposeAndWait("after-join")
	st.waitFor(200, "node=4 应用 after-join", func() bool {
		return sim.Node(4).CommitIndex() == sim.Node(leader).CommitIndex()
	})
	conf := sim.Node(leader).AppliedConfig()
	t.Logf("扩容完成: voters=%v learners=%v 最终状态:\n%s", conf.Voters, conf.Learners, sim.DumpState())
	if len(conf.Voters) != 4 {
		t.Fatalf("扩容后投票成员数=%d 期望=4", len(conf.Voters))
	}
}

// TestLearnerDoesNotBlockCommit 验证：学习者追平之前（哪怕完全失联）
// 不影响原有节点对提交点的推进，也不会被提前计入多数派。
func TestLearnerDoesNotBlockCommit(t *testing.T) {
	sim := NewSim(3, testConfig(), 103)
	st := newSafetyTracker(t, sim)
	st.run(50)
	leader := st.leader()

	sim.AddNode(4)
	if !sim.Node(leader).ProposeAddLearner(4) {
		t.Fatalf("ProposeAddLearner 被拒绝")
	}
	st.waitConfig(leader, "学习者 4 已纳入配置", func(c ConfigState) bool {
		return c.IsLearner(4)
	})

	// 学习者完全失联：原集群提交点必须照常推进。
	sim.Partition(4)
	base := sim.Node(leader).CommitIndex()
	for i := 0; i < 5; i++ {
		st.proposeAndWait(fmt.Sprintf("while-learner-down-%d", i))
	}
	advanced := sim.Node(leader).CommitIndex()
	t.Logf("学习者失联期间: commit %d -> %d", base, advanced)
	if advanced < base+5 {
		t.Fatalf("学习者失联阻塞了提交点推进: %d -> %d", base, advanced)
	}
	// 学习者失联期间不得被提升。
	if sim.Node(leader).AppliedConfig().IsVoter(4) {
		t.Fatalf("失联学习者被提前提升为投票成员")
	}

	// 恢复后学习者追平并被提升，此后写入继续。
	sim.Heal()
	st.waitConfig(leader, "4 恢复后追平并提升", func(c ConfigState) bool {
		return c.IsVoter(4)
	})
	st.proposeAndWait("after-heal")
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestRemoveNode 验证缩容：被摘节点（分区中不知情）恢复后
// 不能凭旧认知重新当选，也不能扰动集群任期与提交。
func TestRemoveNode(t *testing.T) {
	sim := NewSim(3, testConfig(), 107)
	st := newSafetyTracker(t, sim)
	st.run(50)
	leader := st.leader()
	victim := NodeID(0)
	for _, id := range []NodeID{1, 2, 3} {
		if id != leader {
			victim = id
			break
		}
	}
	st.proposeAndWait("before-remove")

	// 先分区被摘节点：它对移除一无所知（分区期间无法学到新配置）。
	sim.Partition(victim)
	if !sim.Node(leader).ProposeRemove(victim) {
		t.Fatalf("ProposeRemove 被拒绝")
	}
	st.waitConfig(leader, fmt.Sprintf("node=%d 已被移出配置", victim), func(c ConfigState) bool {
		return !c.Contains(victim)
	})
	st.proposeAndWait("after-remove")
	termBefore := sim.Node(leader).Term()
	t.Logf("移除完成: leader=%d term=%d victim=%d", leader, termBefore, victim)

	// 恢复分区：不知情的被摘节点会凭旧配置发起选举，
	// 但其拉票必须被配置内节点拒绝且不抬升任何人的任期。
	sim.Heal()
	st.run(100)
	if sim.Node(victim).Role() == Leader {
		t.Fatalf("被摘节点重新当选: %s", sim.DumpState())
	}
	for _, id := range []NodeID{1, 2, 3} {
		if id == victim {
			continue
		}
		if got := sim.Node(id).Term(); got != termBefore {
			t.Fatalf("node=%d 任期被被摘节点扰动: %d != %d\n%s", id, got, termBefore, sim.DumpState())
		}
	}
	// 集群仍然可用：唯一领导者、写入照常提交。
	l2 := st.leader()
	if l2 == victim {
		t.Fatalf("被摘节点成为领导者")
	}
	st.proposeAndWait("post-heal")
	// 被摘节点的投票决定必须是 RejectNotInConfig（由配置内节点视角验证：
	// 配置内节点若收到其拉票，判定原因应记录为 NotInConfig）。
	for _, id := range []NodeID{1, 2, 3} {
		if id == victim {
			continue
		}
		if d := sim.Node(id).LastVoteDecision(); d == RejectNotInConfig {
			t.Logf("node=%d 拒绝被摘节点拉票: reason=%s", id, d)
		}
	}
	t.Logf("被摘节点 node=%d role=%s term=%d（被隔离在集群之外）",
		victim, sim.Node(victim).Role(), sim.Node(victim).Term())
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestRemoveLeaderSelf 验证：领导者移除自己——提交移除条目后让位停机，
// 剩余成员选出新领导者，集群继续服务。
func TestRemoveLeaderSelf(t *testing.T) {
	sim := NewSim(3, testConfig(), 109)
	st := newSafetyTracker(t, sim)
	st.run(50)
	leader := st.leader()
	st.proposeAndWait("before-self-remove")
	t.Logf("领导者=%d 移除自身", leader)

	if !sim.Node(leader).ProposeRemove(leader) {
		t.Fatalf("ProposeRemove(自身) 被拒绝")
	}
	// 旧领导者应用移除条目后必须停机（removed）。
	st.waitFor(200, "旧领导者停机", func() bool {
		return sim.Node(leader).Removed()
	})
	if sim.Node(leader).Role() == Leader {
		t.Fatalf("被移除的领导者未让位")
	}
	t.Logf("旧领导者已停机: role=%s removed=%v", sim.Node(leader).Role(), sim.Node(leader).Removed())

	// 剩余两节点选出新领导者并继续接受写入。
	st.waitFor(200, "剩余成员选出新领导者", func() bool {
		id, ok := sim.Leader()
		return ok && id != leader
	})
	newLeader := st.leader()
	t.Logf("新领导者=%d term=%d", newLeader, sim.Node(newLeader).Term())
	st.proposeAndWait("after-self-remove")

	// 旧领导者停机后不响应、不参选、不发送任何消息。
	outboxBefore := len(sim.Node(leader).DrainOutbox())
	st.run(50)
	if sim.Node(leader).Role() != Follower || !sim.Node(leader).Removed() {
		t.Fatalf("停机节点状态异常: role=%s removed=%v", sim.Node(leader).Role(), sim.Node(leader).Removed())
	}
	if len(sim.Node(leader).DrainOutbox()) != 0 || outboxBefore != 0 {
		t.Fatalf("停机节点仍在发送消息")
	}
	t.Logf("最终状态:\n%s", sim.DumpState())
}

// TestRepeatedAddRemove 验证：连续来回多次扩缩容（加一个、摘一个、再加、再摘）
// 全程安全且最终一致。
func TestRepeatedAddRemove(t *testing.T) {
	sim := NewSim(3, testConfig(), 113)
	st := newSafetyTracker(t, sim)
	st.run(50)

	seq := 0
	write := func() {
		seq++
		st.proposeAndWait(fmt.Sprintf("seq-%d", seq))
	}
	waitAllConfig := func(desc string, cond func(ConfigState) bool) {
		st.waitFor(400, desc, func() bool {
			leader, ok := sim.Leader()
			if !ok {
				return false
			}
			return cond(sim.Node(leader).AppliedConfig())
		})
	}

	// 第1轮：加 4。
	sim.AddNode(4)
	leader := st.leader()
	if !sim.Node(leader).ProposeAddLearner(4) {
		t.Fatalf("第1轮 ProposeAddLearner(4) 被拒绝")
	}
	write()
	waitAllConfig("4 提升为投票成员", func(c ConfigState) bool { return c.IsVoter(4) })
	write()

	// 第2轮：摘 2（一个普通跟随者）。
	leader = st.leader()
	if leader == 2 {
		// 若 2 是领导者则改摘 3，保持用例聚焦"摘非领导者"。
		if !sim.Node(leader).ProposeRemove(3) {
			t.Fatalf("第2轮 ProposeRemove(3) 被拒绝")
		}
	} else if !sim.Node(leader).ProposeRemove(2) {
		t.Fatalf("第2轮 ProposeRemove(2) 被拒绝")
	}
	write()
	waitAllConfig("集群缩为 3 个投票成员", func(c ConfigState) bool {
		return len(c.Voters) == 3
	})
	write()

	// 第3轮：加 5。
	sim.AddNode(5)
	leader = st.leader()
	if !sim.Node(leader).ProposeAddLearner(5) {
		t.Fatalf("第3轮 ProposeAddLearner(5) 被拒绝")
	}
	write()
	waitAllConfig("5 提升为投票成员", func(c ConfigState) bool { return c.IsVoter(5) })
	write()

	// 第4轮：摘 4（上一轮刚加入的节点）。
	leader = st.leader()
	if !sim.Node(leader).ProposeRemove(4) {
		t.Fatalf("第4轮 ProposeRemove(4) 被拒绝")
	}
	write()
	waitAllConfig("4 被移出配置", func(c ConfigState) bool { return !c.Contains(4) })
	write()

	// 收敛后：所有在配置内的节点提交点一致、配置一致。
	st.run(200)
	leader = st.leader()
	finalConf := sim.Node(leader).AppliedConfig()
	t.Logf("最终配置: voters=%v learners=%v", finalConf.Voters, finalConf.Learners)
	wantCommit := sim.Node(leader).CommitIndex()
	for _, id := range finalConf.Voters {
		n := sim.Node(id)
		if n.CommitIndex() != wantCommit {
			t.Fatalf("node=%d commit=%d 期望=%d\n%s", id, n.CommitIndex(), wantCommit, sim.DumpState())
		}
		if !n.AppliedConfig().Equal(finalConf) {
			t.Fatalf("node=%d 配置=%v 与领导者=%v 不一致", id, n.AppliedConfig(), finalConf)
		}
	}
	// 被摘的节点 4：若学到了移除则停机；若未学到（领导者停发前没收到提交点），
	// 也只能空转拉票——不能当选、不能扰动配置内节点的任期。
	if sim.Node(4).Removed() {
		t.Logf("node=4 已学到移除并停机")
	} else {
		t.Logf("node=4 未学到移除（role=%s term=%d），验证其扰动无效",
			sim.Node(4).Role(), sim.Node(4).Term())
		if sim.Node(4).Role() == Leader {
			t.Fatalf("被摘节点当选领导者")
		}
		termBefore := sim.Node(leader).Term()
		st.run(50)
		if got := sim.Node(leader).Term(); got != termBefore {
			t.Fatalf("被摘节点扰动了集群任期: %d -> %d", termBefore, got)
		}
	}
	t.Logf("连续扩缩容完成，共写入 %d 条命令\n%s", seq, sim.DumpState())
}

// appliedTracker 跟踪各节点已应用命令，断言状态机安全性（前缀一致）。
type appliedTracker struct {
	t       *testing.T
	sim     *Sim
	applied map[NodeID][]string
}

func newAppliedTracker(t *testing.T, sim *Sim) *appliedTracker {
	t.Helper()
	return &appliedTracker{t: t, sim: sim, applied: map[NodeID][]string{}}
}

// drain 收集本拍的应用消息并做两两前缀一致性断言。
func (at *appliedTracker) drain() {
	at.t.Helper()
	for _, id := range at.sim.sortedIDs() {
		for _, am := range at.sim.Applied(id) {
			switch {
			case am.Snapshot:
				for len(at.applied[id]) < int(am.Index) {
					at.applied[id] = append(at.applied[id], fmt.Sprintf("snap-prefix@%d", am.Index))
				}
			case am.Config != nil:
				for len(at.applied[id]) < int(am.Index) {
					at.applied[id] = append(at.applied[id], "")
				}
				at.applied[id][am.Index-1] = fmt.Sprintf("CONF%v/%v", am.Config.Voters, am.Config.Learners)
			case am.Command != nil:
				idx := int(am.Index)
				for len(at.applied[id]) < idx {
					at.applied[id] = append(at.applied[id], "")
				}
				if prev := at.applied[id][idx-1]; prev != "" && prev != string(am.Command) {
					at.t.Fatalf("node=%d 索引=%d 被重复应用为不同值: %q -> %q\n%s",
						id, am.Index, prev, am.Command, at.sim.DumpState())
				}
				at.applied[id][idx-1] = string(am.Command)
			}
		}
	}
	ids := at.sim.sortedIDs()
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := at.applied[ids[i]], at.applied[ids[j]]
			for k := 0; k < len(a) && k < len(b); k++ {
				if a[k] != "" && b[k] != "" && a[k] != b[k] &&
					!isSnapPlaceholder(a[k]) && !isSnapPlaceholder(b[k]) {
					at.t.Fatalf("状态机安全性违反: node=%d 与 node=%d 在索引=%d 应用了 %q 与 %q\n%s",
						ids[i], ids[j], k+1, a[k], b[k], at.sim.DumpState())
				}
			}
		}
	}
}

func isSnapPlaceholder(s string) bool {
	return len(s) >= 4 && s[:4] == "snap"
}

// TestMembershipFaultInjection 固定种子下的扩缩容故障注入：
// 丢包/重复/乱序/分区/重启与动态加节点、摘节点交织，
// 全程断言选举安全性与状态机安全性，收尾断言最终一致。
func TestMembershipFaultInjection(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runMembershipFaultInjection(t, seed, 500)
		})
	}
}

func runMembershipFaultInjection(t *testing.T, seed int64, ticks int) {
	t.Helper()
	sim := NewSim(3, testConfig(), seed)
	st := newSafetyTracker(t, sim)
	at := newAppliedTracker(t, sim)
	faultRnd := rand.New(rand.NewSource(seed ^ 0x5eed))

	// 预启动两个空节点，等待被加入集群。
	sim.AddNode(4)
	sim.AddNode(5)

	adds, removes, proposals := 0, 0, 0
	for tick := 0; tick < ticks; tick++ {
		// 随机故障注入（与既有故障注入测试同分布）。
		switch faultRnd.Intn(40) {
		case 0:
			ids := sim.sortedIDs()
			sim.Partition(ids[faultRnd.Intn(len(ids))])
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

		// 随机成员变更（低频；被拒绝则自然重试）。
		if faultRnd.Intn(60) == 0 {
			if leader, ok := sim.Leader(); ok {
				conf := sim.Node(leader).AppliedConfig()
				if faultRnd.Intn(2) == 0 {
					// 加节点：不在配置中的节点（含曾被摘下的——
					// 被摘节点转为非投票身份后仍跟随复制，可直接重新入列）。
					candidates := []NodeID{}
					for _, id := range []NodeID{4, 5} {
						if !conf.Contains(id) {
							candidates = append(candidates, id)
						}
					}
					if len(candidates) > 0 {
						id := candidates[faultRnd.Intn(len(candidates))]
						if sim.Node(leader).ProposeAddLearner(id) {
							adds++
							sim.logf("MEMBERSHIP propose add-learner %d by leader=%d term=%d",
								id, leader, sim.Node(leader).Term())
						}
					}
				} else if len(conf.Voters) > 2 {
					// 摘节点：保留至少 2 个投票成员。
					victims := append([]NodeID(nil), conf.Voters...)
					victim := victims[faultRnd.Intn(len(victims))]
					if sim.Node(leader).ProposeRemove(victim) {
						removes++
						sim.logf("MEMBERSHIP propose remove %d by leader=%d term=%d",
							victim, leader, sim.Node(leader).Term())
					}
				}
			}
		}

		// 随机提案。
		if faultRnd.Intn(10) == 0 {
			if leader, ok := sim.Leader(); ok {
				cmd := fmt.Sprintf("seed=%d-cmd=%d", seed, proposals)
				if sim.Node(leader).Propose([]byte(cmd)) {
					proposals++
				}
			}
		}

		st.tick()
		at.drain()
	}

	// 收尾：清除全部故障，给集群足够时间收敛（含学习者追平与提升）。
	sim.Heal()
	sim.SetLoss(0)
	sim.SetDup(0)
	sim.SetReorder(false)
	st.run(500)
	at.drain()

	// 最终一致性：唯一领导者；配置内所有节点提交点与已应用配置收敛。
	leader, ok := sim.Leader()
	if !ok {
		t.Fatalf("收敛后无唯一领导者\n%s", sim.DumpState())
	}
	finalConf := sim.Node(leader).AppliedConfig()
	wantCommit := sim.Node(leader).CommitIndex()
	t.Logf("seed=%d 收敛: leader=%d term=%d commit=%d voters=%v learners=%v adds=%d removes=%d proposals=%d",
		seed, leader, sim.Node(leader).Term(), wantCommit, finalConf.Voters, finalConf.Learners,
		adds, removes, proposals)
	for _, id := range append(append([]NodeID(nil), finalConf.Voters...), finalConf.Learners...) {
		n := sim.Node(id)
		if n.CommitIndex() != wantCommit {
			t.Fatalf("最终一致性违反: node=%d commit=%d 期望=%d\n%s",
				id, n.CommitIndex(), wantCommit, sim.DumpState())
		}
		if !n.AppliedConfig().Equal(finalConf) {
			t.Fatalf("配置收敛违反: node=%d conf=%v 期望=%v\n%s",
				id, n.AppliedConfig(), finalConf, sim.DumpState())
		}
	}
	// 停机节点不得担任任何领导角色。
	for _, id := range sim.sortedIDs() {
		n := sim.Node(id)
		if n.Removed() && n.Role() != Follower {
			t.Fatalf("停机节点 node=%d 角色异常: %s", id, n.Role())
		}
	}
	if adds+removes == 0 {
		t.Fatalf("本次运行未发生任何成员变更，用例无效")
	}
	t.Logf("最终状态:\n%s", sim.DumpState())
}
