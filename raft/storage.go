package raft

import "fmt"

// PersistentState 是需要跨重启保留的状态。
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID // -1 表示未投票
	HasVoted    bool
	CommitIndex Index // 已提交点持久化：重启后不重复应用、不丢失
	Log         []Entry
	SnapIndex   Index
	SnapTerm    Term
	SnapData    []byte
	// 快照点处的成员配置：重启后以此为基底、回放快照之后的日志条目，
	// 重新推导当前生效的成员配置（投票成员 + 学习者）。
	SnapVoters   []NodeID
	SnapLearners []NodeID
}

// hasNoMembershipInfo 判定持久化状态中是否完全不含成员配置信息。
// 上一版本的 PersistentState 没有成员字段、日志中也没有成员变更条目，
// 带这种历史状态升级的节点命中此判定，启动时回退到 Config.Peers；
// 而任何经动态变更（含摘除）过的状态都带有成员信息，不会误判。
func (s PersistentState) hasNoMembershipInfo() bool {
	if len(s.SnapVoters) > 0 || len(s.SnapLearners) > 0 {
		return false
	}
	for _, e := range s.Log {
		if e.Type == EntryConfig {
			return false
		}
	}
	return true
}

// Storage 抽象持久化存储。MemoryStorage 支持"崩溃-重启"语义：
// 崩溃后可通过 Snapshot() 恢复出重启前最后一次持久化的状态。
type Storage interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}

// MemoryStorage 是确定性的内存存储实现。
// 通过 Failpoint 可在 Save 时注入故障（模拟写入丢失）。
type MemoryStorage struct {
	state PersistentState
	// LoseWrites 为 true 时 Save 静默丢弃（模拟崩溃前未落盘）。
	LoseWrites bool
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{state: PersistentState{VotedFor: -1}}
}

func (m *MemoryStorage) Save(state PersistentState) error {
	if m.LoseWrites {
		return fmt.Errorf("injected storage fault: write lost")
	}
	m.state = cloneState(state)
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	return cloneState(m.state), nil
}

func cloneState(s PersistentState) PersistentState {
	out := s
	out.Log = append([]Entry(nil), s.Log...)
	out.SnapData = append([]byte(nil), s.SnapData...)
	out.SnapVoters = append([]NodeID(nil), s.SnapVoters...)
	out.SnapLearners = append([]NodeID(nil), s.SnapLearners...)
	return out
}
