package raft

// replication.go：日志复制与提交裁决。
// 安全性规则：
//  - 仅当条目被多数派复制、且任期不早于领导者当前任期时才可提交（Raft §5.4.2），
//    旧任期条目只能随本任期条目间接提交；
//  - 追随者只在 prevLog 精确匹配时追加，冲突处截断，但绝不截断已提交前缀；
//  - 提交点与应用点持久化，重启后不丢失、不重复应用。

func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	if next <= n.snapshotIndex() {
		n.sendSnapshot(to)
		return
	}
	prevIndex := next - 1
	prevTerm, ok := n.termAt(prevIndex)
	if !ok {
		n.sendSnapshot(to)
		return
	}
	entries := n.sliceFrom(next)
	if len(entries) > n.cfg.MaxInflight {
		entries = entries[:n.cfg.MaxInflight]
	}
	n.send(Message{
		Type:         MsgAppendEntries,
		From:         n.cfg.ID,
		To:           to,
		Term:         n.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) handleAppendEntries(m Message) {
	// 同任期（或更高任期已在 Step 中处理）的合法领导者。
	n.leaderID = m.From
	n.electionElapsed = 0
	if n.role != Follower {
		n.becomeFollower(m.Term, StepDownHigherTermSeen)
	}

	resp := Message{
		Type: MsgAppendEntriesResp,
		From: n.cfg.ID,
		To:   m.From,
		Term: n.currentTerm,
	}
	reject := func(reason AppendRejectReason, conflictIndex Index, conflictTerm Term) {
		resp.AppendRejectReason = reason
		resp.ConflictIndex = conflictIndex
		resp.ConflictTerm = conflictTerm
		n.send(resp)
	}

	// prevLog 落在本地快照之内：无法验证，提示领导者从快照之后开始发。
	if m.PrevLogIndex < n.snapshotIndex() {
		reject(AppendRejectCompacted, n.snapshotIndex()+1, 0)
		return
	}
	// prevLog 超出本地日志末尾。
	if m.PrevLogIndex > n.lastIndex() {
		reject(AppendRejectLogMismatch, n.lastIndex()+1, 0)
		return
	}
	// prevLog 任期不匹配：给出冲突任期首索引，加速回退。
	if t, _ := n.termAt(m.PrevLogIndex); t != m.PrevLogTerm {
		first := m.PrevLogIndex
		for first-1 > n.snapshotIndex() {
			ft, _ := n.termAt(first - 1)
			if ft != t {
				break
			}
			first--
		}
		reject(AppendRejectLogMismatch, first, t)
		return
	}

	// 匹配成功：截断冲突后缀并追加新条目，但绝不截断已提交前缀。
	changed := false
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + 1 + Index(i)
		if idx <= n.lastIndex() {
			if t, _ := n.termAt(idx); t == e.Term {
				continue // 已有一致条目（重复投递幂等）
			}
			if idx <= n.commitIndex {
				continue // 已提交前缀不可变（安全性防线，正常路径不会触发）
			}
			n.entries = n.entries[:idx-n.snapshotIndex()-1]
		}
		n.entries = append(n.entries, LogEntry{Term: e.Term, Command: cloneBytes(e.Command)})
		changed = true
	}

	// 推进提交点：不超过本次追加后的最新索引。
	newCommit := m.LeaderCommit
	if last := m.PrevLogIndex + Index(len(m.Entries)); last < newCommit {
		newCommit = last
	}
	if newCommit > n.commitIndex {
		n.commitIndex = newCommit
		changed = true
	}
	if changed {
		n.persist()
	}

	resp.AppendRejectReason = AppendRejectNone
	resp.MatchIndex = m.PrevLogIndex + Index(len(m.Entries))
	n.send(resp)
}

func (n *Node) handleAppendEntriesResp(m Message) {
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	switch m.AppendRejectReason {
	case AppendRejectNone:
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
		}
		n.maybeCommit()
	default:
		// 按冲突提示回退 nextIndex 并立即重试。
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			// 若本地也持有该冲突任期的条目，跳到该任期之后。
			for idx := n.lastIndex(); idx > n.snapshotIndex(); idx-- {
				t, _ := n.termAt(idx)
				if t == m.ConflictTerm {
					next = idx + 1
					break
				}
				if t < m.ConflictTerm {
					break
				}
			}
		}
		if next < 1 {
			next = 1
		}
		n.nextIndex[m.From] = next
		n.sendAppend(m.From)
	}
}

// maybeCommit 仅提交“多数派已复制且任期等于当前任期”的条目。
func (n *Node) maybeCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		t, _ := n.termAt(idx)
		if t != n.currentTerm {
			continue // 旧任期条目不可直接提交
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.persist()
			return
		}
	}
}
