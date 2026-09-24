package raft

// Storage 持久化接口：重启后已提交内容不得丢失、不得重复应用。
type Storage interface {
	// Persist 原子保存全部持久状态（实现需做深拷贝，避免与调用方共享底层数组）。
	Persist(s PersistentState)
	// Load 读取持久状态（返回深拷贝）。
	Load() PersistentState
}

// PersistentState 需要跨重启保存的状态。
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Entries     []LogEntry // 快照之后的日志后缀，逻辑索引从 Snapshot.Meta.Index+1 开始
	Snapshot    Snapshot   // 已压缩的前缀
	CommitIndex Index      // 持久化提交点：重启后已提交内容不丢失
	LastApplied Index      // 持久化应用点：重启后不重复应用
}

// MemoryStorage 进程内持久化实现（模拟磁盘：节点重启时保留）。
type MemoryStorage struct {
	state PersistentState
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (m *MemoryStorage) Persist(s PersistentState) {
	m.state = clonePersistent(s)
}

func (m *MemoryStorage) Load() PersistentState {
	return clonePersistent(m.state)
}

func clonePersistent(s PersistentState) PersistentState {
	out := s
	if s.Entries != nil {
		out.Entries = make([]LogEntry, len(s.Entries))
		for i, e := range s.Entries {
			out.Entries[i] = LogEntry{Term: e.Term, Command: cloneBytes(e.Command)}
		}
	}
	out.Snapshot.Data = cloneBytes(s.Snapshot.Data)
	return out
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
