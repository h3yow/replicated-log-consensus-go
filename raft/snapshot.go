package raft

// compact 将 upto（含）之前的日志压缩为快照前缀并持久化。
// 快照记录压缩点处的成员配置，随 InstallSnapshot 传播给落后节点。
func (n *Node) compact(upto Index, data []byte) {
	if upto > n.commitIndex {
		upto = n.commitIndex // 只允许压缩已提交的前缀
	}
	if upto <= n.log.snapshotLastIndex {
		return
	}
	n.log.compactTo(upto, append([]byte(nil), data...), n.appliedConfig())
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
		SnapshotVoters:    append([]NodeID(nil), n.log.snapVoters...),
		SnapshotLearners:  append([]NodeID(nil), n.log.snapLearners...),
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

	snapConf := ConfigState{Voters: m.SnapshotVoters, Learners: m.SnapshotLearners}
	kept := n.log.installSnapshot(m.SnapshotLastIndex, m.SnapshotLastTerm, m.SnapshotData, snapConf)
	_ = kept
	// 采用快照携带的配置，但仅当快照不落后于本地已应用点
	// （否则本地配置更新，不能回退）。旧格式快照不含配置则保持现状。
	// 注意：此处不做"被移除"判定——快照接收方可能尚在配置之外追赶
	// （全新学习者），移除判定只发生在应用已提交配置条目时。
	if !snapConf.Empty() && m.SnapshotLastIndex >= n.lastApplied {
		n.voters = make(map[NodeID]bool, len(snapConf.Voters))
		for _, v := range snapConf.Voters {
			n.voters[v] = true
		}
		n.learners = make(map[NodeID]bool, len(snapConf.Learners))
		for _, l := range snapConf.Learners {
			n.learners[l] = true
		}
	}
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
	if !n.isVoter(m.From) && !n.isLearner(m.From) {
		return // 已移出配置的节点：忽略
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.sendAppend(m.From)
}
