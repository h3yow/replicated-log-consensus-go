package raft

// raftLog 维护带快照前缀的复制日志。
// 布局：snapshotLastIndex/snapshotLastTerm + entries[0..]，
// entries[0] 的逻辑索引为 snapshotLastIndex+1。
type raftLog struct {
	snapshotLastIndex Index
	snapshotLastTerm  Term
	snapshotData      []byte
	// 快照点处的成员配置（随 InstallSnapshot 传播）。
	snapVoters   []NodeID
	snapLearners []NodeID
	entries      []Entry
}

func newLog() *raftLog { return &raftLog{} }

// lastIndex 返回日志最后一条的逻辑索引（无条目时为快照点）。
func (l *raftLog) lastIndex() Index { return l.snapshotLastIndex + Index(len(l.entries)) }

// lastTerm 返回最后一条的任期（无条目时为快照任期）。
func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return l.snapshotLastTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// termAt 返回指定逻辑索引的任期；ok=false 表示该索引超出范围。
func (l *raftLog) termAt(idx Index) (Term, bool) {
	if idx == l.snapshotLastIndex {
		return l.snapshotLastTerm, true
	}
	if idx < l.snapshotLastIndex || idx > l.lastIndex() {
		return 0, false
	}
	return l.entries[idx-l.snapshotLastIndex-1].Term, true
}

// entryAt 返回指定逻辑索引的条目。
func (l *raftLog) entryAt(idx Index) (Entry, bool) {
	if idx <= l.snapshotLastIndex || idx > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[idx-l.snapshotLastIndex-1], true
}

// slice 返回 [from, to] 闭区间内的条目副本。
func (l *raftLog) slice(from, to Index) []Entry {
	if from > to || from <= l.snapshotLastIndex || to > l.lastIndex() {
		return nil
	}
	out := make([]Entry, to-from+1)
	copy(out, l.entries[from-l.snapshotLastIndex-1:to-l.snapshotLastIndex])
	return out
}

// truncateSuffix 截掉 idx 之后（不含 idx）的所有条目。
func (l *raftLog) truncateSuffix(idx Index) {
	if idx < l.snapshotLastIndex {
		idx = l.snapshotLastIndex
	}
	if idx < l.lastIndex() {
		l.entries = l.entries[:idx-l.snapshotLastIndex]
	}
}

// append 追加条目。
func (l *raftLog) append(entries ...Entry) {
	l.entries = append(l.entries, entries...)
}

// compactTo 将 upto（含）之前的日志压缩为快照前缀。
// upto 必须 <= lastIndex。data 为快照内容，conf 为快照点处的成员配置。
func (l *raftLog) compactTo(upto Index, data []byte, conf ConfigState) {
	if upto <= l.snapshotLastIndex {
		return
	}
	term, ok := l.termAt(upto)
	if !ok {
		return
	}
	l.entries = l.entries[upto-l.snapshotLastIndex:]
	l.snapshotLastIndex = upto
	l.snapshotLastTerm = term
	l.snapshotData = data
	l.snapVoters = append([]NodeID(nil), conf.Voters...)
	l.snapLearners = append([]NodeID(nil), conf.Learners...)
}

// installSnapshot 用快照替换前缀；若本地存在与快照末点匹配的条目，
// 则保留其后的更长后缀，否则丢弃全部本地条目。
// conf 为快照携带的成员配置（为空表示快照不含配置，保持现状——兼容旧格式）。
// 返回是否保留了后缀。
func (l *raftLog) installSnapshot(lastIndex Index, lastTerm Term, data []byte, conf ConfigState) bool {
	keepSuffix := false
	if t, ok := l.termAt(lastIndex); ok && t == lastTerm && lastIndex > l.snapshotLastIndex {
		keepSuffix = true
		l.entries = l.entries[lastIndex-l.snapshotLastIndex:]
	} else {
		l.entries = nil
	}
	l.snapshotLastIndex = lastIndex
	l.snapshotLastTerm = lastTerm
	l.snapshotData = data
	l.snapVoters = append([]NodeID(nil), conf.Voters...)
	l.snapLearners = append([]NodeID(nil), conf.Learners...)
	return keepSuffix
}
