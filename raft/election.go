package raft

// election.go：选举与任期推进。
// 安全性规则：
//  - 每任期每节点至多投出一票（持久化 votedFor）；
//  - 候选人日志必须不落后于投票人（比较 lastTerm, lastIndex）；
//  - 拒绝必须带可区分原因，便于测试断言与事后解释。

func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.electionElapsed = 0
	n.votesReceived = map[NodeID]bool{n.cfg.ID: true}
	n.persist()
	if len(n.cfg.Peers) == 1 { // 单节点集群直接当选
		n.becomeLeader()
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.send(Message{
			Type:         MsgRequestVote,
			From:         n.cfg.ID,
			To:           p,
			Term:         n.currentTerm,
			LastLogIndex: n.lastIndex(),
			LastLogTerm:  n.lastTerm(),
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.heartbeatElapsed = 0
	last := n.lastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	// 上任即广播心跳，尽快让旧任期领导者让位、提交本任期条目。
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) handleRequestVote(m Message) {
	resp := Message{
		Type: MsgRequestVoteResp,
		From: n.cfg.ID,
		To:   m.From,
		Term: n.currentTerm,
	}
	// 规则 1：本任期已投给他人（含自己竞选时）。
	if n.votedFor != "" && n.votedFor != m.From {
		resp.VoteGranted = false
		resp.VoteRejectReason = VoteRejectAlreadyVoted
		n.send(resp)
		return
	}
	// 规则 2：候选人日志至少与本地一样新。
	upToDate := m.LastLogTerm > n.lastTerm() ||
		(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.lastIndex())
	if !upToDate {
		resp.VoteGranted = false
		resp.VoteRejectReason = VoteRejectLogNotUpToDate
		n.send(resp)
		return
	}
	n.votedFor = m.From
	n.electionElapsed = 0
	n.persist()
	resp.VoteGranted = true
	resp.VoteRejectReason = VoteRejectNone
	n.send(resp)
}

func (n *Node) handleRequestVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.currentTerm {
		return
	}
	if m.VoteGranted {
		n.votesReceived[m.From] = true
		if len(n.votesReceived) >= n.majority() {
			n.becomeLeader()
		}
	}
	// 拒绝票不立即终止选举：等待选举超时后重新发起，
	// 拒绝原因已通过 VoteRejectReason 暴露给测试与日志。
}
