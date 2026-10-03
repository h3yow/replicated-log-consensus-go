// Package raft 实现一个可确定性重放的复制日志与领导者选举组件。
// 所有随机性由外部注入的 *rand.Rand 提供，时间由 Tick() 驱动，
// 因此在固定随机种子下整个集群行为可完全重放。
package raft

// NodeID 标识集群中的一个节点。
type NodeID int

// Term 表示任期号，单调递增。
type Term uint64

// Index 表示日志索引，从 1 开始；0 表示"哨兵"（快照点或空日志）。
type Index uint64

// Role 表示节点角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// EntryKind 区分日志条目的种类。
type EntryKind int

const (
	EntryNormal EntryKind = iota // 普通命令
	EntryConfig                  // 集群配置变更（提交并应用后生效）
)

// ConfigState 是集群成员配置：投票成员 + 追赶中的学习者。
// 学习者接收复制但不参选、不投票、不计入多数派。
type ConfigState struct {
	Voters   []NodeID
	Learners []NodeID
}

// quorum 返回投票成员的多数派阈值。
func (c ConfigState) quorum() int { return len(c.Voters)/2 + 1 }

// IsVoter 判定 id 是否为投票成员。
func (c ConfigState) IsVoter(id NodeID) bool {
	for _, v := range c.Voters {
		if v == id {
			return true
		}
	}
	return false
}

// IsLearner 判定 id 是否为学习者。
func (c ConfigState) IsLearner(id NodeID) bool {
	for _, l := range c.Learners {
		if l == id {
			return true
		}
	}
	return false
}

// Contains 判定 id 是否在配置中（投票成员或学习者）。
func (c ConfigState) Contains(id NodeID) bool { return c.IsVoter(id) || c.IsLearner(id) }

// Empty 判定配置是否为空（全新学习者尚未收到任何配置）。
func (c ConfigState) Empty() bool { return len(c.Voters) == 0 && len(c.Learners) == 0 }

// Clone 深拷贝配置。
func (c ConfigState) Clone() ConfigState {
	return ConfigState{
		Voters:   append([]NodeID(nil), c.Voters...),
		Learners: append([]NodeID(nil), c.Learners...),
	}
}

// Equal 判定两个配置是否相同。
func (c ConfigState) Equal(o ConfigState) bool {
	if len(c.Voters) != len(o.Voters) || len(c.Learners) != len(o.Learners) {
		return false
	}
	for i := range c.Voters {
		if c.Voters[i] != o.Voters[i] {
			return false
		}
	}
	for i := range c.Learners {
		if c.Learners[i] != o.Learners[i] {
			return false
		}
	}
	return true
}

// Entry 是一条日志条目。Term 为创建该条目的任期。
// Kind 为 EntryConfig 时，Config 携带提交后生效的新配置。
type Entry struct {
	Term    Term
	Kind    EntryKind
	Command []byte
	Config  ConfigState
}

// VoteRejectReason 区分投票被拒绝的原因，避免用随机行为掩盖分歧。
type VoteRejectReason int

const (
	VoteGranted          VoteRejectReason = iota
	RejectStaleTerm                       // 请求任期小于本节点当前任期
	RejectAlreadyVoted                    // 本任期内已把票投给了其他候选人
	RejectLogNotUpToDate                  // 候选人日志不如本节点新
	RejectNotInConfig                     // 候选人不在当前投票成员内（或本节点不是投票成员）
)

func (r VoteRejectReason) String() string {
	switch r {
	case VoteGranted:
		return "Granted"
	case RejectStaleTerm:
		return "StaleTerm"
	case RejectAlreadyVoted:
		return "AlreadyVoted"
	case RejectLogNotUpToDate:
		return "LogNotUpToDate"
	case RejectNotInConfig:
		return "NotInConfig"
	}
	return "Unknown"
}

// AppendRejectReason 区分 AppendEntries 被拒绝的原因。
type AppendRejectReason int

const (
	AppendAccepted        AppendRejectReason = iota
	RejectAppendStaleTerm                    // 领导者任期落后
	RejectLogInconsistent                    // prevLogIndex/prevLogTerm 不匹配
)

func (r AppendRejectReason) String() string {
	switch r {
	case AppendAccepted:
		return "Accepted"
	case RejectAppendStaleTerm:
		return "StaleTerm"
	case RejectLogInconsistent:
		return "LogInconsistent"
	}
	return "Unknown"
}

// StepDownReason 记录领导者/候选人让位的原因，便于测试与解释。
type StepDownReason int

const (
	StepDownNone             StepDownReason = iota
	StepDownHigherTerm                      // 发现更高任期（消息或投票响应）
	StepDownElectionLost                    // 选举超时，候选人放弃
	StepDownLeaderDiscovered                // 收到同任期或更高任期领导者的合法心跳
)

func (r StepDownReason) String() string {
	switch r {
	case StepDownNone:
		return "None"
	case StepDownHigherTerm:
		return "HigherTerm"
	case StepDownElectionLost:
		return "ElectionLost"
	case StepDownLeaderDiscovered:
		return "LeaderDiscovered"
	}
	return "Unknown"
}
