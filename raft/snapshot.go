package raft

// snapshot.go：日志压缩与落后节点追赶。
// 安全性规则：
//  - 只能压缩已应用（lastApplied）的前缀；
//  - 快照安装不得覆盖更长的已提交后缀：本地已提交条目优先保留；
//  - 安装快照后提交点/应用点至少推进到快照点，保证追赶中的节点
//    对外仍提供一致可读的视图。

// Compact 将 (snapshotIndex, index] 压缩为快照；index 会被钳制到 lastApplied。
func (n *Node) Compact(index Index, data []byte) {
	if index > n.lastApplied {
		index = n.lastApplied
	}
	if index <= n.snapshotIndex() {
		return
	}
	term, ok := n.termAt(index)
	if !ok {
		return
	}
	n.entries = n.entries[index-n.snapshotIndex():]
	n.snapshot = Snapshot{Meta: SnapshotMeta{Index: index, Term: term}, Data: cloneBytes(data)}
	n.persist()
}

func (n *Node) sendSnapshot(to NodeID) {
	n.send(Message{
		Type:     MsgInstallSnapshot,
		From:     n.cfg.ID,
		To:       to,
		Term:     n.currentTerm,
		Snapshot: n.snapshot,
	})
}

func (n *Node) handleInstallSnapshot(m Message) {
	n.leaderID = m.From
	n.electionElapsed = 0
	if n.role != Follower {
		n.becomeFollower(m.Term, StepDownHigherTermSeen)
	}
	resp := Message{
		Type: MsgInstallSnapshotResp,
		From: n.cfg.ID,
		To:   m.From,
		Term: n.currentTerm,
	}
	snap := m.Snapshot
	// 不覆盖更长的已提交/已压缩前缀：快照点不超过本地快照点则视为过期。
	if snap.Meta.Index <= n.snapshotIndex() {
		resp.MatchIndex = n.snapshotIndex()
		n.send(resp)
		return
	}
	// 决定保留多少本地后缀。
	keepFrom := Index(0)
	if t, ok := n.termAt(snap.Meta.Index); ok && t == snap.Meta.Term {
		keepFrom = snap.Meta.Index + 1 // 快照点与本地日志衔接，保留其后缀
	} else if snap.Meta.Index < n.commitIndex {
		keepFrom = snap.Meta.Index + 1 // 防御：已提交后缀不可被快照覆盖
	}
	if keepFrom > 0 && keepFrom <= n.lastIndex() {
		n.entries = n.sliceFrom(keepFrom)
	} else {
		n.entries = nil
	}
	n.snapshot = Snapshot{Meta: snap.Meta, Data: cloneBytes(snap.Data)}
	if n.commitIndex < snap.Meta.Index {
		n.commitIndex = snap.Meta.Index
	}
	if n.lastApplied < snap.Meta.Index {
		n.lastApplied = snap.Meta.Index
	}
	n.persist()
	resp.MatchIndex = n.snapshotIndex()
	n.send(resp)
}

func (n *Node) handleInstallSnapshotResp(m Message) {
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.maybeCommit()
	n.sendAppend(m.From) // 继续用 AppendEntries 追赶快照之后的部分
}
