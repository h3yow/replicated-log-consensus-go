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

// EntryType 区分普通命令条目与成员变更条目。
type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfig           // 成员变更：单节点增删（Raft 论文 §4.1）+ 学习者提升
)

// ConfChangeType 区分成员变更动作。
type ConfChangeType int

const (
	AddLearner   ConfChangeType = iota // 以学习者身份加入（不计入多数派、不参与选举）
	PromoteVoter                       // 学习者追平后提升为投票成员
	RemoveMember                       // 摘除节点（投票成员或学习者）
)

func (t ConfChangeType) String() string {
	switch t {
	case AddLearner:
		return "AddLearner"
	case PromoteVoter:
		return "PromoteVoter"
	case RemoveMember:
		return "RemoveMember"
	}
	return "Unknown"
}

// ConfChange 描述一次单节点成员变更。
type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
}

// Entry 是一条日志条目。Term 为创建该条目的任期。
// Type=EntryConfig 时 Change 有效，Command 为空。
type Entry struct {
	Term    Term
	Type    EntryType
	Command []byte
	Change  ConfChange
}

// VoteRejectReason 区分投票被拒绝的原因，避免用随机行为掩盖分歧。
type VoteRejectReason int

const (
	VoteGranted          VoteRejectReason = iota
	RejectStaleTerm                       // 请求任期小于本节点当前任期
	RejectAlreadyVoted                    // 本任期内已把票投给了其他候选人
	RejectLogNotUpToDate                  // 候选人日志不如本节点新
	RejectNotMember                       // 候选人不在本节点当前投票成员配置中（被摘除节点不得扰乱集群）
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
	case RejectNotMember:
		return "NotMember"
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
	StepDownRemoved                         // 领导者发现自己已被提交的配置摘除
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
	case StepDownRemoved:
		return "Removed"
	}
	return "Unknown"
}
