package raft

// compact 将 upto（含）之前的日志压缩为快照前缀并持久化。
// 快照元数据记录 upto 这一点处的成员配置，使快照之后的配置仍可推导。
func (n *Node) compact(upto Index, data []byte) {
	if upto > n.commitIndex {
		upto = n.commitIndex // 只允许压缩已提交的前缀
	}
	if upto <= n.log.snapshotLastIndex {
		return
	}
	voters, learners := n.membershipAt(upto)
	n.log.compactTo(upto, append([]byte(nil), data...), voters, learners)
	n.persist()
}

// sendSnapshot 向落后节点发送快照（含快照点处的成员配置）。
func (n *Node) sendSnapshot(to NodeID) {
	n.outbox = append(n.outbox, Message{
		Type:              MsgInstallSnapshot,
		From:              n.id,
		To:                to,
		Term:              n.term,
		SnapshotLastIndex: n.log.snapshotLastIndex,
		SnapshotLastTerm:  n.log.snapshotLastTerm,
		SnapshotData:      append([]byte(nil), n.log.snapshotData...),
		SnapshotVoters:    append([]NodeID(nil), n.log.snapshotVoters...),
		SnapshotLearners:  append([]NodeID(nil), n.log.snapshotLearners...),
	})
}

// handleInstallSnapshot 处理快照安装。
// 关键安全规则：快照不得覆盖更长的已提交后缀——
// 若本地在快照末点处存在匹配条目，保留其后的后缀。
func (n *Node) handleInstallSnapshot(m Message) {
	resp := Message{Type: MsgInstallSnapshotResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		n.outbox = append(n.outbox, resp)
		return
	}
	n.becomeFollower(m.Term, StepDownLeaderDiscovered)
	resp.Term = n.term

	if m.SnapshotLastIndex <= n.log.snapshotLastIndex {
		// 不更新的快照：直接确认，避免回退。
		resp.MatchIndex = n.log.snapshotLastIndex
		n.outbox = append(n.outbox, resp)
		return
	}

	kept := n.log.installSnapshot(m.SnapshotLastIndex, m.SnapshotLastTerm, m.SnapshotData,
		m.SnapshotVoters, m.SnapshotLearners)
	_ = kept
	// 采纳快照携带的成员配置；若保留了更长后缀，后缀中的变更条目继续生效。
	n.recomputeMembership()
	// 提交点/应用点至少推进到快照末点；快照内容作为一条应用消息交给状态机。
	if n.commitIndex < m.SnapshotLastIndex {
		n.commitIndex = m.SnapshotLastIndex
	}
	if n.lastApplied < m.SnapshotLastIndex {
		n.lastApplied = m.SnapshotLastIndex
		n.applyCh = append(n.applyCh, ApplyMsg{
			Index:        m.SnapshotLastIndex,
			Snapshot:     true,
			SnapshotData: append([]byte(nil), m.SnapshotData...),
		})
	}
	n.persist()
	resp.MatchIndex = m.SnapshotLastIndex
	n.outbox = append(n.outbox, resp)
}

// handleInstallSnapshotResp 快照确认后推进该节点的复制进度。
func (n *Node) handleInstallSnapshotResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.sendAppend(m.From)
	n.maybePromoteLearners() // 学习者可能借快照追平
}
