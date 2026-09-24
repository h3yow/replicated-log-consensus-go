package raft

// NodeID 唯一标识一个节点。
type NodeID string

// Term 任期号，单调递增。
type Term uint64

// Index 日志索引，从 1 开始；0 表示“快照之前/无日志”。
type Index uint64

// Role 节点角色。
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

// LogEntry 单条日志。
type LogEntry struct {
	Term    Term
	Command []byte
}

// MsgType 消息类型。
type MsgType int

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
)

// VoteRejectReason 投票拒绝原因（可区分，不用随机行为掩盖分歧）。
type VoteRejectReason int

const (
	VoteRejectNone           VoteRejectReason = iota // 未拒绝
	VoteRejectTermStale                              // 候选人任期落后
	VoteRejectAlreadyVoted                           // 本任期已投给他人
	VoteRejectLogNotUpToDate                         // 候选人日志不够新
)

func (r VoteRejectReason) String() string {
	switch r {
	case VoteRejectNone:
		return "None"
	case VoteRejectTermStale:
		return "TermStale"
	case VoteRejectAlreadyVoted:
		return "AlreadyVoted"
	case VoteRejectLogNotUpToDate:
		return "LogNotUpToDate"
	}
	return "Unknown"
}

// AppendRejectReason 追加日志被拒绝的原因。
type AppendRejectReason int

const (
	AppendRejectNone        AppendRejectReason = iota
	AppendRejectTermStale                      // 领导者任期落后
	AppendRejectLogMismatch                    // prevLog 处日志不匹配
	AppendRejectCompacted                      // prevLog 已被压缩，需要快照
)

func (r AppendRejectReason) String() string {
	switch r {
	case AppendRejectNone:
		return "None"
	case AppendRejectTermStale:
		return "TermStale"
	case AppendRejectLogMismatch:
		return "LogMismatch"
	case AppendRejectCompacted:
		return "Compacted"
	}
	return "Unknown"
}

// StepDownReason 节点让位（退回 Follower）的原因。
type StepDownReason int

const (
	StepDownNone           StepDownReason = iota
	StepDownHigherTermSeen                // 看到更高任期的消息
	StepDownElectionLost                  // 选举失败/超时
)

// Message 节点间传递的消息（所有 RPC 合一，便于确定性模拟）。
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term Term

	// RequestVote 请求
	LastLogIndex Index
	LastLogTerm  Term

	// RequestVote 响应
	VoteGranted      bool
	VoteRejectReason VoteRejectReason

	// AppendEntries 请求
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index

	// AppendEntries 响应
	AppendRejectReason AppendRejectReason
	MatchIndex         Index // 成功时：已匹配的最大索引
	ConflictIndex      Index // 失败时：冲突定位提示
	ConflictTerm       Term

	// InstallSnapshot 请求/响应
	Snapshot Snapshot
}

// Snapshot 快照：压缩点之前的全部已应用状态。
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// SnapshotMeta 快照元数据。
type SnapshotMeta struct {
	Index Index // 快照覆盖的最后一条日志索引
	Term  Term  // 该索引处日志的任期
}
