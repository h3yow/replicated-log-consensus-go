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
	// 当前已应用的成员配置（提交点处生效的配置）。
	Voters   []NodeID
	Learners []NodeID
	// Removed 为 true 表示本节点曾被已提交的配置移出集群（转为非投票身份）。
	// 显式持久化，避免把"尚未加入配置的全新学习者"误判为已移除。
	Removed bool
	// 快照点处的成员配置（随快照一起传播给落后节点/新节点）。
	SnapVoters   []NodeID
	SnapLearners []NodeID
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
	out.Log = make([]Entry, len(s.Log))
	for i, e := range s.Log {
		e.Config = e.Config.Clone()
		e.Command = append([]byte(nil), e.Command...)
		out.Log[i] = e
	}
	out.SnapData = append([]byte(nil), s.SnapData...)
	out.Voters = append([]NodeID(nil), s.Voters...)
	out.Learners = append([]NodeID(nil), s.Learners...)
	out.SnapVoters = append([]NodeID(nil), s.SnapVoters...)
	out.SnapLearners = append([]NodeID(nil), s.SnapLearners...)
	return out
}
