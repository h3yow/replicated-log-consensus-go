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

// isEmpty 判定存储是否从未写入过（用于区分"首次引导"与"重启恢复"）。
func (s PersistentState) isEmpty() bool {
	return s.CurrentTerm == 0 && !s.HasVoted && s.CommitIndex == 0 &&
		len(s.Log) == 0 && s.SnapIndex == 0
}

// lacksMembershipInfo 判定持久化状态是否完全不含成员信息：
// 快照未携带成员、且日志中没有任何成员变更条目。
// 状态非空却满足该条件，说明它来自不支持动态成员的旧版本
// （旧版本只有静态成员，既不写快照成员也不产生变更条目）；
// 反之，只要出现过任何动态成员行为（含"被摘除"），状态里必然
// 留有快照成员或变更条目，不会误判。因此该判定可把
// "旧版本升级上来的节点"与"被动态摘除的节点"严格区分开。
func (s PersistentState) lacksMembershipInfo() bool {
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
