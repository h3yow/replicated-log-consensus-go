package raft

// becomeFollower 切换到 Follower；reason 记录让位原因（可解释性要求）。
func (n *Node) becomeFollower(term Term, reason StepDownReason) {
	roleChanged := n.role != Follower
	if term > n.term {
		n.term = term
		n.votedFor = -1
		n.hasVoted = false
		n.persist()
	}
	n.role = Follower
	if roleChanged || reason != StepDownNone {
		n.lastStepDown = reason
	}
	n.resetElectionTimeout()
}

// startElection 发起新一轮选举。只有当前配置中的投票成员才能竞选。
func (n *Node) startElection() {
	if !n.isVoter(n.id) {
		return
	}
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.hasVoted = true
	n.persist()
	n.votesReceived = map[NodeID]bool{n.id: true}
	n.resetElectionTimeout()
	// 单节点集群立即当选。
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.Voters() {
		if p == n.id {
			continue
		}
		n.outbox = append(n.outbox, Message{
			Type:         MsgRequestVote,
			From:         n.id,
			To:           p,
			Term:         n.term,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

// becomeLeader 当选领导者，初始化复制状态并立即广播心跳确立权威。
// 复制对象 = 当前投票成员 + 学习者 + 未提交变更的被摘除者。
func (n *Node) becomeLeader() {
	n.role = Leader
	n.nextIndex = make(map[NodeID]Index)
	n.matchIndex = make(map[NodeID]Index)
	for _, p := range n.replicationTargets() {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = n.log.lastIndex()
	// 追加一条本任期的空条目，使"只提交当前任期条目"的规则能尽快推进提交点。
	n.log.append(Entry{Term: n.term, Command: nil})
	n.persist()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.broadcastAppend()
}

// isUpToDate 判定候选人的日志是否至少与本节点一样新（Raft 论文 §5.4.1）。
func (n *Node) isUpToDate(idx Index, term Term) bool {
	lt := n.log.lastTerm()
	if term != lt {
		return term > lt
	}
	return idx >= n.log.lastIndex()
}

// handleRequestVote 处理投票请求，给出可区分的拒绝原因。
// 关键成员规则：候选人必须是本节点当前配置中的投票成员，
// 否则既不投票也不采纳其任期——被摘除的节点即使带着更高任期回来，
// 也不能迫使集群让位或重新计票。
func (n *Node) handleRequestVote(m Message) {
	if !n.isVoter(m.From) {
		n.lastVoteDecision = RejectNotMember
		n.outbox = append(n.outbox, Message{
			Type:         MsgRequestVoteResp,
			From:         n.id,
			To:           m.From,
			Term:         n.term,
			VoteGranted:  false,
			RejectReason: RejectNotMember,
		})
		return
	}
	if m.Term > n.term {
		n.becomeFollower(m.Term, StepDownHigherTerm)
	}
	grant := false
	reason := VoteGranted
	switch {
	case m.Term < n.term:
		reason = RejectStaleTerm
	case n.hasVoted && n.votedFor != m.From:
		reason = RejectAlreadyVoted
	case !n.isUpToDate(m.LastLogIndex, m.LastLogTerm):
		reason = RejectLogNotUpToDate
	default:
		grant = true
	}
	n.lastVoteDecision = reason
	if grant {
		n.votedFor = m.From
		n.hasVoted = true
		n.persist()
		// 投票给候选人后重置自己的选举计时，避免无谓竞争。
		n.resetElectionTimeout()
	}
	n.outbox = append(n.outbox, Message{
		Type:         MsgRequestVoteResp,
		From:         n.id,
		To:           m.From,
		Term:         n.term,
		VoteGranted:  grant,
		RejectReason: reason,
	})
}

// handleRequestVoteResp 处理投票响应；候选人获得多数票即当选。
// 只统计当前配置中投票成员的票（迟到的、非成员的响应不计入）。
func (n *Node) handleRequestVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	if m.VoteGranted && n.isVoter(m.From) {
		n.votesReceived[m.From] = true
		if len(n.votesReceived) >= n.quorum() {
			n.becomeLeader()
		}
	}
	// 被拒绝的响应无需动作：选举超时会以可判定的方式重试。
}
