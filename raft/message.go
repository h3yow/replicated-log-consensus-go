package raft

// MsgType 区分节点间传递的消息类型。
type MsgType int

const (
	MsgRequestVote MsgType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
)

func (t MsgType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResp:
		return "RequestVoteResp"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResp:
		return "AppendEntriesResp"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResp:
		return "InstallSnapshotResp"
	}
	return "Unknown"
}

// Message 是节点间传递的统一消息信封。
// 所有字段均为值类型，便于网络层复制（模拟重复投递）。
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term Term

	// RequestVote 字段
	LastLogIndex Index
	LastLogTerm  Term

	// RequestVoteResp 字段
	VoteGranted  bool
	RejectReason VoteRejectReason

	// AppendEntries 字段
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// AppendEntriesResp 字段
	Success         bool
	AppendReject    AppendRejectReason
	MatchIndex      Index // 成功时：已匹配的最大索引
	ConflictIndex   Index // 失败时：建议的回退位置
	ConflictTerm    Term
	HasConflictTerm bool

	// InstallSnapshot 字段
	SnapshotLastIndex Index
	SnapshotLastTerm  Term
	SnapshotData      []byte
	// 快照点处的成员配置（空表示快照不含配置——兼容旧格式，接收方保持现状）。
	SnapshotVoters   []NodeID
	SnapshotLearners []NodeID
}
