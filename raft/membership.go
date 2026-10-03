package raft

// 动态成员变更：单节点变更（single-server membership change，Raft 论文 §6 风格）。
//
// 规则：
//   - 每次变更只增/删一个节点；配置以 EntryConfig 日志条目传播，
//     提交并应用后（apply-on-commit）才生效。
//   - 同一时刻至多一个未提交的配置变更（hasPendingConfigChange 守卫），
//     因此任意新旧两个配置的多数派必然相交，集群不会被劈成两半。
//   - 扩容两段式：先以学习者身份加入（不计入多数派，追平前不影响提交点推进），
//     追平后由领导者自动追加"提升为投票成员"的配置条目。
//   - 缩容：被摘节点应用移除条目后转为非投票身份（仍跟随复制，可被重新加入）；
//     未学到移除的节点恢复后，其拉票被"候选人必须在当前投票成员内"规则拒绝且不抬任期。

// hasPendingConfigChange 报告日志中是否存在已追加但未提交的配置条目。
func (n *Node) hasPendingConfigChange() bool {
	for i := n.commitIndex + 1; i <= n.log.lastIndex(); i++ {
		if e, ok := n.log.entryAt(i); ok && e.Kind == EntryConfig {
			return true
		}
	}
	return false
}

// proposeConfig 由领导者追加一条配置条目并触发复制。
// 同一时刻只允许一个未提交的配置变更；非领导者或有待定变更时返回 false。
func (n *Node) proposeConfig(conf ConfigState) bool {
	if n.role != Leader || n.removed {
		return false
	}
	if n.hasPendingConfigChange() {
		return false
	}
	n.log.append(Entry{Term: n.term, Kind: EntryConfig, Config: conf.Clone()})
	n.persist()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.broadcastAppend()
	n.maybeCommit() // 单节点集群立即提交
	return true
}

// ProposeAddLearner 以学习者身份把 id 加入集群（扩容第一步）。
// 学习者只接收复制，不计入多数派；追平后由领导者自动提升。
func (n *Node) ProposeAddLearner(id NodeID) bool {
	conf := n.appliedConfig()
	if conf.Contains(id) {
		return false // 已在配置中
	}
	conf.Learners = append(conf.Learners, id)
	return n.proposeConfig(conf)
}

// ProposeRemove 把 id（投票成员或学习者）移出集群。
// 不允许移除不存在的节点，也不允许把投票成员清空。
func (n *Node) ProposeRemove(id NodeID) bool {
	conf := n.appliedConfig()
	if !conf.Contains(id) {
		return false
	}
	voters := make([]NodeID, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		if v != id {
			voters = append(voters, v)
		}
	}
	if len(voters) == 0 {
		return false // 集群至少要保留一个投票成员
	}
	learners := make([]NodeID, 0, len(conf.Learners))
	for _, l := range conf.Learners {
		if l != id {
			learners = append(learners, l)
		}
	}
	return n.proposeConfig(ConfigState{Voters: voters, Learners: learners})
}

// maybePromoteLearners 在学习者追平领导者日志时，自动追加提升配置条目。
// 追平判定：matchIndex 覆盖到领导者当前最后索引。
func (n *Node) maybePromoteLearners() {
	if n.role != Leader || n.removed || n.hasPendingConfigChange() {
		return
	}
	last := n.log.lastIndex()
	for _, l := range n.appliedConfig().Learners {
		if n.matchIndex[l] >= last {
			conf := n.appliedConfig()
			voters := append(append([]NodeID(nil), conf.Voters...), l)
			learners := make([]NodeID, 0, len(conf.Learners))
			for _, o := range conf.Learners {
				if o != l {
					learners = append(learners, o)
				}
			}
			// 保持确定性排序。
			newConf := ConfigState{Voters: voters, Learners: learners}
			sorted := newConf.Clone()
			sortNodeIDs(sorted.Voters)
			sortNodeIDs(sorted.Learners)
			n.proposeConfig(sorted)
			return // 一次只提升一个（单节点变更约束）
		}
	}
}

func sortNodeIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

// applyConfigEntry 在配置条目提交后应用之（apply-on-commit）。
// 应用后：更新投票/学习者集合；若本节点被移出配置则停机；
// 领导者为新加入的复制对象初始化进度。
func (n *Node) applyConfigEntry(e Entry) {
	conf := e.Config
	// 是否被移除：先前已在配置中、且新提交的配置不再包含本节点。
	// （全新学习者追快照历史时会应用到"尚不含自己"的旧配置条目，
	//   那不属于被移除，不能停机。）
	wasMember := n.isVoter(n.id) || n.isLearner(n.id)
	n.voters = make(map[NodeID]bool, len(conf.Voters))
	for _, v := range conf.Voters {
		n.voters[v] = true
	}
	n.learners = make(map[NodeID]bool, len(conf.Learners))
	for _, l := range conf.Learners {
		n.learners[l] = true
	}
	if wasMember && !conf.Contains(n.id) {
		// 本节点被已提交的配置移出集群：让位并转为非投票身份。
		// 该移除条目已被多数派提交，新配置无需本节点即可选主。
		// 注意不是"停机"：节点仍跟随复制，将来被重新加入时可追平回归。
		if n.role != Follower {
			n.becomeFollower(n.term, StepDownNone)
		}
		n.removed = true
	}
	if conf.Contains(n.id) {
		n.removed = false // 被重新加入（或本来就在配置中）
	}
	n.persist()

	if n.role == Leader {
		// 新出现的复制对象（如刚加入的学习者）初始化复制进度。
		for _, p := range n.replicationTargets() {
			if _, ok := n.nextIndex[p]; !ok {
				n.nextIndex[p] = n.log.lastIndex() + 1
				n.matchIndex[p] = 0
			}
		}
		n.broadcastAppend()
	}
}
