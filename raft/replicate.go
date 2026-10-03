package raft

// sendAppend 向指定节点发送 AppendEntries（nextIndex 之前的条目缺失时改发快照）。
func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	if next <= n.log.snapshotLastIndex {
		n.sendSnapshot(to)
		return
	}
	prevIdx := next - 1
	prevTerm, ok := n.log.termAt(prevIdx)
	if !ok {
		n.sendSnapshot(to)
		return
	}
	last := n.log.lastIndex()
	max := last
	if n.cfg.MaxEntriesPerMsg > 0 && last-prevIdx > Index(n.cfg.MaxEntriesPerMsg) {
		max = prevIdx + Index(n.cfg.MaxEntriesPerMsg)
	}
	n.outbox = append(n.outbox, Message{
		Type:         MsgAppendEntries,
		From:         n.id,
		To:           to,
		Term:         n.term,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(prevIdx+1, max),
		LeaderCommit: n.commitIndex,
	})
}

// broadcastAppend 向所有复制对象（投票成员 + 学习者）发送复制消息。
func (n *Node) broadcastAppend() {
	for _, p := range n.replicationTargets() {
		n.sendAppend(p)
	}
}

// handleAppendEntries 处理领导者的复制/心跳消息。
func (n *Node) handleAppendEntries(m Message) {
	resp := Message{Type: MsgAppendEntriesResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		resp.AppendReject = RejectAppendStaleTerm
		resp.ConflictIndex = n.log.lastIndex() + 1
		n.outbox = append(n.outbox, resp)
		return
	}
	// 合法的同任期/更高任期领导者：让位（分区恢复后旧领导者在此让位）。
	n.becomeFollower(m.Term, StepDownLeaderDiscovered)
	resp.Term = n.term

	prevTerm, ok := n.log.termAt(m.PrevLogIndex)
	if !ok || prevTerm != m.PrevLogTerm {
		resp.AppendReject = RejectLogInconsistent
		// 冲突提示：若 prevLogIndex 超出本地日志，建议从本地末尾+1 开始；
		// 否则建议跳到冲突任期之前。
		if m.PrevLogIndex > n.log.lastIndex() {
			resp.ConflictIndex = n.log.lastIndex() + 1
		} else {
			resp.HasConflictTerm = true
			resp.ConflictTerm = prevTerm
			first := m.PrevLogIndex
			for first > n.log.snapshotLastIndex+1 {
				t, _ := n.log.termAt(first - 1)
				if t != prevTerm {
					break
				}
				first--
			}
			resp.ConflictIndex = first
		}
		n.outbox = append(n.outbox, resp)
		return
	}

	// 逐条对齐：发现任期冲突即截断后缀，再追加缺失条目。
	changed := false
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + 1 + Index(i)
		if idx > n.log.lastIndex() {
			n.log.append(m.Entries[i:]...)
			changed = true
			break
		}
		t, _ := n.log.termAt(idx)
		if t != e.Term {
			n.log.truncateSuffix(idx - 1)
			n.log.append(m.Entries[i:]...)
			changed = true
			break
		}
		// 已存在且一致：重复投递/乱序到达时安全跳过（幂等）。
	}
	if changed {
		n.persist()
	}

	// 推进提交点：min(leaderCommit, 本地最后索引)。
	if m.LeaderCommit > n.commitIndex {
		newCommit := m.LeaderCommit
		if last := n.log.lastIndex(); newCommit > last {
			newCommit = last
		}
		n.commitIndex = newCommit
		n.persist()
		n.maybeApply()
	}

	resp.Success = true
	resp.AppendReject = AppendAccepted
	resp.MatchIndex = m.PrevLogIndex + Index(len(m.Entries))
	n.outbox = append(n.outbox, resp)
}

// handleAppendEntriesResp 处理复制响应，推进 matchIndex 并尝试提交。
func (n *Node) handleAppendEntriesResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if !n.isVoter(m.From) && !n.isLearner(m.From) {
		return // 已移出配置的节点的迟到响应：忽略，不再向其复制
	}
	if m.Success {
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
		}
		n.maybeCommit()
		n.maybePromoteLearners() // 学习者追平后自动提升为投票成员
		return
	}
	// 失败：按冲突提示回退 nextIndex 并重试。
	next := m.ConflictIndex
	if m.HasConflictTerm {
		// 若领导者本地含有该冲突任期的条目，则跳到该任期最后一条之后。
		lastOfTerm := Index(0)
		for i := n.log.lastIndex(); i > n.log.snapshotLastIndex; i-- {
			t, _ := n.log.termAt(i)
			if t == m.ConflictTerm {
				lastOfTerm = i
				break
			}
			if t < m.ConflictTerm {
				break
			}
		}
		if lastOfTerm > 0 {
			next = lastOfTerm + 1
		}
	}
	if next < 1 {
		next = 1
	}
	if next < n.nextIndex[m.From] {
		n.nextIndex[m.From] = next
	}
	n.sendAppend(m.From)
}

// maybeCommit 只提交"被多数派复制且不早于当前任期"的条目（Raft §5.4.2）。
// 多数派按当前已应用的投票成员配置计数；学习者不计入。
func (n *Node) maybeCommit() {
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		t, _ := n.log.termAt(idx)
		if t < n.term {
			break // 更早任期的条目只能随本任期条目间接收尾
		}
		count := 0
		for v := range n.voters {
			if n.matchIndex[v] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.persist()
			n.maybeApply()
			break
		}
	}
}

// maybeApply 将已提交未应用的条目送入应用队列（保证按序、不重复）。
// 配置条目在应用时生效（apply-on-commit）。
func (n *Node) maybeApply() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.log.entryAt(n.lastApplied)
		if !ok {
			continue // 快照点等位置无实体条目
		}
		if e.Kind == EntryConfig {
			n.applyConfigEntry(e)
			conf := e.Config.Clone()
			n.applyCh = append(n.applyCh, ApplyMsg{
				Index:  n.lastApplied,
				Config: &conf,
			})
			continue
		}
		n.applyCh = append(n.applyCh, ApplyMsg{
			Index:   n.lastApplied,
			Command: append([]byte(nil), e.Command...),
		})
	}
}

// Propose 由领导者追加一条本任期条目并触发复制。
func (n *Node) propose(cmd []byte) bool {
	if n.role != Leader {
		return false
	}
	n.log.append(Entry{Term: n.term, Command: append([]byte(nil), cmd...)})
	n.persist()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.broadcastAppend()
	n.maybeCommit() // 单节点集群立即提交
	return true
}
