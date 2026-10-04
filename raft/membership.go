package raft

import "sort"

// 本文件实现运行期成员变更（Raft 论文 §4.1 单节点变更 + §4.2.1 学习者追赶）：
//
//   - 配置即日志：成员变更以 EntryConfig 条目进入日志，追加即生效；
//     当前配置始终可由"快照点成员 + 快照之后的变更条目"重新推导，
//     因此未提交却被截断的变更不会残留，重启后也能一致恢复。
//   - 单节点变更：每次只增/删一个节点，且同一时间至多一个未提交变更，
//     新旧配置的多数派必然相交，从根本上排除"两侧各自凑出多数"的裂脑。
//   - 学习者：新增节点先以学习者身份追赶（不计票、不竞选），
//     追平后由领导者提升为投票成员，追赶期间不拖慢原集群的提交推进。

// quorum 返回当前投票成员配置的多数派阈值。
func (n *Node) quorum() int { return len(n.voters)/2 + 1 }

func (n *Node) isVoter(id NodeID) bool   { return n.voters[id] }
func (n *Node) isLearner(id NodeID) bool { return n.learners[id] }
func (n *Node) isMember(id NodeID) bool  { return n.voters[id] || n.learners[id] }

// applyConfChangeTo 将一次单节点变更应用到给定成员集合。
func applyConfChangeTo(voters, learners map[NodeID]bool, c ConfChange) {
	switch c.Type {
	case AddLearner:
		if !voters[c.NodeID] {
			learners[c.NodeID] = true
		}
	case PromoteVoter:
		delete(learners, c.NodeID)
		voters[c.NodeID] = true
	case RemoveMember:
		delete(voters, c.NodeID)
		delete(learners, c.NodeID)
	}
}

// recomputeMembership 从快照成员与日志条目重新推导当前生效配置。
// 在启动、截断后缀、安装快照后调用，保证配置始终与日志一致。
func (n *Node) recomputeMembership() {
	voters := map[NodeID]bool{}
	learners := map[NodeID]bool{}
	for _, id := range n.log.snapshotVoters {
		voters[id] = true
	}
	for _, id := range n.log.snapshotLearners {
		learners[id] = true
	}
	lastConf := Index(0)
	for i, e := range n.log.entries {
		if e.Type == EntryConfig {
			applyConfChangeTo(voters, learners, e.Change)
			lastConf = n.log.snapshotLastIndex + Index(i) + 1
		}
	}
	// 维护 alsoReplicate：被本次推导摘除的节点（旧配置有、新配置无）
	// 在变更提交前仍继续复制；被重新加入的节点从集合中清除。
	if n.alsoReplicate == nil {
		n.alsoReplicate = map[NodeID]bool{}
	}
	for id := range n.voters {
		if !voters[id] && !learners[id] {
			n.alsoReplicate[id] = true
		}
	}
	for id := range n.learners {
		if !voters[id] && !learners[id] {
			n.alsoReplicate[id] = true
		}
	}
	for id := range voters {
		delete(n.alsoReplicate, id)
	}
	for id := range learners {
		delete(n.alsoReplicate, id)
	}
	n.voters, n.learners = voters, learners
	n.lastConfIdx = lastConf
}

// replicationTargets 返回当前需要复制的全部节点（不含自身）：
// 投票成员 + 学习者；若存在未提交的摘除变更，再加上被摘除者
// （让在线的被摘除者及时收到摘除条目；提交后即停止）。
func (n *Node) replicationTargets() []NodeID {
	set := map[NodeID]bool{}
	for id := range n.voters {
		set[id] = true
	}
	for id := range n.learners {
		set[id] = true
	}
	if n.lastConfIdx > n.commitIndex {
		for id := range n.alsoReplicate {
			set[id] = true
		}
	}
	delete(set, n.id)
	return sortedIDSet(set)
}

// membershipAt 推导 upto（含）这一点处的成员配置，用于压缩时写入快照元数据。
func (n *Node) membershipAt(upto Index) (voters, learners []NodeID) {
	v := map[NodeID]bool{}
	l := map[NodeID]bool{}
	for _, id := range n.log.snapshotVoters {
		v[id] = true
	}
	for _, id := range n.log.snapshotLearners {
		l[id] = true
	}
	for i := n.log.snapshotLastIndex + 1; i <= upto; i++ {
		if e, ok := n.log.entryAt(i); ok && e.Type == EntryConfig {
			applyConfChangeTo(v, l, e.Change)
		}
	}
	return sortedIDSet(v), sortedIDSet(l)
}

func sortedIDSet(m map[NodeID]bool) []NodeID {
	out := make([]NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// --- 变更提议（仅领导者） ---

// canProposeConfChange 判定当前是否允许发起新的成员变更：
//   - 必须是领导者；
//   - 前一次变更必须已提交（同一时间至多一个未提交变更，
//     这是单节点变更安全性的前提）；
//   - 领导者必须已提交过本任期的条目（当选时追加的空条目提交后），
//     保证其提交点是最新的，变更不会建立在未确认的日志上。
func (n *Node) canProposeConfChange() bool {
	if n.role != Leader {
		return false
	}
	if n.lastConfIdx > n.commitIndex {
		return false
	}
	t, _ := n.log.termAt(n.commitIndex)
	return t == n.term
}

// proposeConfChange 追加一条成员变更条目（追加即生效）并触发复制。
func (n *Node) proposeConfChange(c ConfChange) bool {
	if !n.canProposeConfChange() {
		return false
	}
	n.log.append(Entry{Term: n.term, Type: EntryConfig, Change: c})
	n.recomputeMembership()
	n.ensureTrackers()
	n.persist()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.broadcastAppend()
	n.maybeCommit()
	return true
}

// AddNode 请求将 id 以学习者身份加入集群。
// 学习者先追赶历史（必要时通过快照），追平后由领导者自动提升为投票成员。
// 返回 false 表示当前不可发起（非领导者、已有未提交变更、或 id 已在配置中）。
func (n *Node) AddNode(id NodeID) bool {
	if n.isMember(id) {
		return false
	}
	return n.proposeConfChange(ConfChange{Type: AddLearner, NodeID: id})
}

// RemoveNode 请求将 id 从集群摘除（投票成员或学习者均可）。
// 被摘除的节点不再计入多数派；领导者摘除自己时，在变更提交后让位。
// 不允许摘除最后一个投票成员。
func (n *Node) RemoveNode(id NodeID) bool {
	if !n.isMember(id) {
		return false
	}
	if n.isVoter(id) && len(n.voters) <= 1 {
		return false
	}
	return n.proposeConfChange(ConfChange{Type: RemoveMember, NodeID: id})
}

// maybePromoteLearners 把已追平提交点的学习者提升为投票成员。
// 追平判定：学习者的 matchIndex 已覆盖领导者当前提交点——
// 即它持有全部已提交内容，提升不会拖慢或危及提交推进。
// 提升本身也是一次成员变更，因此同样遵守"至多一个未提交变更"。
func (n *Node) maybePromoteLearners() {
	if n.role != Leader || !n.canProposeConfChange() {
		return
	}
	for _, id := range n.Learners() {
		if n.matchIndex[id] >= n.commitIndex {
			n.proposeConfChange(ConfChange{Type: PromoteVoter, NodeID: id})
			return // 一次只提升一个，其余等下一轮
		}
	}
}
