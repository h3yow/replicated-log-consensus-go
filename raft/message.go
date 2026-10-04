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
	// PrevVoters/PrevLearners 仅当 PrevLogIndex 恰好是发送者的快照点时携带：
	// 接收者若也以该点为快照点（典型场景：全新空节点从索引 1 开始追赶），
	// 则采纳该基准成员配置——初始配置只存在于索引 0 的快照元数据中，
	// 不随普通条目传播，必须由此显式传递。
	PrevVoters   []NodeID
	PrevLearners []NodeID

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
	SnapshotVoters    []NodeID // 快照点处的成员配置（随快照一起传播）
	SnapshotLearners  []NodeID
}
