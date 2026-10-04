package raft

import (
	"fmt"
	"math/rand"
)

// Config 是节点的可调参数。所有时间以逻辑 tick 为单位。
type Config struct {
	ID                NodeID
	Peers             []NodeID // 初始引导成员（含自身）；仅在空存储首次启动时生效，此后成员由日志中的变更条目决定
	ElectionTimeout   int      // 选举超时基准（tick 数）
	HeartbeatTick     int      // 心跳间隔（tick 数）
	MaxEntriesPerMsg  int      // 单条 AppendEntries 最大条目数
	SnapshotThreshold int      // 日志条目数超过该值时触发压缩；0 表示不自动压缩
}

// ApplyMsg 是已提交、可对外的应用消息。
type ApplyMsg struct {
	Index        Index
	Command      []byte
	Snapshot     bool // true 表示应改用 SnapshotData 恢复状态机
	SnapshotData []byte
}

// Node 是一个 Raft 节点。所有方法都由外部单线程驱动（Tick/Step），
// 因此无需锁，行为完全确定。
type Node struct {
	id  NodeID
	cfg Config
	rnd *rand.Rand
	st  Storage

	role     Role
	term     Term
	votedFor NodeID
	hasVoted bool
	log      *raftLog

	commitIndex Index
	lastApplied Index

	// 选举状态
	electionElapsed  int
	electionTimeout  int // 本次随机化后的超时
	votesReceived    map[NodeID]bool
	lastStepDown     StepDownReason
	lastVoteDecision VoteRejectReason // 最近一次投票判定及原因（供测试断言）

	// 领导者状态
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// 成员配置（易失，由"快照成员 + 日志中的变更条目"推导，见 membership.go）。
	// 配置在变更条目追加进日志时即生效（Raft §4.1 单节点变更），
	// 未提交的变更若被截断则通过 recomputeMembership 回滚。
	voters   map[NodeID]bool // 投票成员：参与多数派判定与选举
	learners map[NodeID]bool // 学习者：只接收复制，不计票、不竞选
	// lastConfIdx 是日志中最后一条成员变更条目的索引（0 表示快照之后无变更），
	// 用于"同一时间至多一个未提交变更"的判定与领导者自我摘除后的让位。
	lastConfIdx Index
	// alsoReplicate 是被未提交变更摘除、但仍需继续复制的节点：
	// 让在线的被摘除者能在变更提交前收到摘除条目、主动退出；
	// 变更一旦提交即停止向其复制（由 replicationTargets 按提交点过滤）。
	alsoReplicate map[NodeID]bool

	// 输出
	outbox  []Message
	applyCh []ApplyMsg
}

// NewNode 从存储恢复（或初始化）一个节点。
func NewNode(cfg Config, st Storage, rnd *rand.Rand) (*Node, error) {
	ps, err := st.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		id:            cfg.ID,
		cfg:           cfg,
		rnd:           rnd,
		st:            st,
		role:          Follower,
		term:          ps.CurrentTerm,
		votedFor:      ps.VotedFor,
		hasVoted:      ps.HasVoted,
		log:           newLog(),
		votesReceived: make(map[NodeID]bool),
	}
	n.log.entries = append(n.log.entries, ps.Log...)
	n.log.snapshotLastIndex = ps.SnapIndex
	n.log.snapshotLastTerm = ps.SnapTerm
	n.log.snapshotData = ps.SnapData
	n.log.snapshotVoters = append([]NodeID(nil), ps.SnapVoters...)
	n.log.snapshotLearners = append([]NodeID(nil), ps.SnapLearners...)
	if ps.isEmpty() {
		// 首次引导：以 Config.Peers 为初始投票成员（可能为空，
		// 空配置表示该节点等待以学习者身份被加入集群）。
		n.log.snapshotVoters = append([]NodeID(nil), cfg.Peers...)
	}
	// 重启后：恢复持久化的提交点（至少覆盖快照点），
	// lastApplied 从提交点开始，保证已应用内容不丢失、不重复。
	n.commitIndex = ps.CommitIndex
	if n.commitIndex < ps.SnapIndex {
		n.commitIndex = ps.SnapIndex
	}
	n.lastApplied = n.commitIndex
	// 成员配置一律从"快照成员 + 日志条目"重新推导，
	// 保证与持久化日志一致（未提交却被截断的变更不会残留）。
	n.recomputeMembership()
	n.resetElectionTimeout()
	return n, nil
}

// resetElectionTimeout 在 [ElectionTimeout, 2*ElectionTimeout) 内随机化超时。
// 随机源由外部注入，固定种子下可重放。
func (n *Node) resetElectionTimeout() {
	n.electionTimeout = n.cfg.ElectionTimeout
	if n.cfg.ElectionTimeout > 1 {
		n.electionTimeout += n.rnd.Intn(n.cfg.ElectionTimeout)
	}
	n.electionElapsed = 0
}

// persist 将易失前必须落盘的状态写入存储。
func (n *Node) persist() {
	_ = n.st.Save(PersistentState{
		CurrentTerm:  n.term,
		VotedFor:     n.votedFor,
		HasVoted:     n.hasVoted,
		CommitIndex:  n.commitIndex,
		Log:          append([]Entry(nil), n.log.entries...),
		SnapIndex:    n.log.snapshotLastIndex,
		SnapTerm:     n.log.snapshotLastTerm,
		SnapData:     append([]byte(nil), n.log.snapshotData...),
		SnapVoters:   append([]NodeID(nil), n.log.snapshotVoters...),
		SnapLearners: append([]NodeID(nil), n.log.snapshotLearners...),
	})
}

// --- 对外查询（供测试与上层应用） ---

func (n *Node) ID() NodeID                         { return n.id }
func (n *Node) Role() Role                         { return n.role }
func (n *Node) Term() Term                         { return n.term }
func (n *Node) CommitIndex() Index                 { return n.commitIndex }
func (n *Node) LastLogIndex() Index                { return n.log.lastIndex() }
func (n *Node) LastStepDown() StepDownReason       { return n.lastStepDown }
func (n *Node) LastVoteDecision() VoteRejectReason { return n.lastVoteDecision }

// Voters 返回当前生效的投票成员（有序）；Learners 返回当前学习者。
func (n *Node) Voters() []NodeID   { return sortedIDSet(n.voters) }
func (n *Node) Learners() []NodeID { return sortedIDSet(n.learners) }

// IsVoter/IsLearner 查询指定节点在当前配置中的身份。
func (n *Node) IsVoter(id NodeID) bool   { return n.isVoter(id) }
func (n *Node) IsLearner(id NodeID) bool { return n.isLearner(id) }

// EntryAt 返回指定索引的条目（供一致性断言）。
func (n *Node) EntryAt(idx Index) (Entry, bool) { return n.log.entryAt(idx) }

// LogString 返回日志的可读表示，用于测试日志打印判定依据。
func (n *Node) LogString() string {
	s := fmt.Sprintf("snap@%d(t%d)", n.log.snapshotLastIndex, n.log.snapshotLastTerm)
	for i := n.log.snapshotLastIndex + 1; i <= n.log.lastIndex(); i++ {
		e, _ := n.log.entryAt(i)
		s += fmt.Sprintf(" [%d]t%d:%s", i, e.Term, string(e.Command))
	}
	return s
}

// --- 驱动接口 ---

// Tick 推进一个逻辑时钟单位：选举超时与心跳都在此驱动。
func (n *Node) Tick() {
	if n.role == Leader {
		n.electionElapsed++
		if n.electionElapsed >= n.cfg.HeartbeatTick {
			n.electionElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	if !n.isVoter(n.id) {
		// 学习者与已摘除节点不参与选举：不竞选、不发起投票。
		n.electionElapsed = 0
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		// 选举超时是确定可判定的：超时即发起选举，不引入额外随机延迟。
		n.startElection()
	}
}

// Step 处理一条入站消息。更高任期的消息一律先触发让位；
// 但 RequestVote 例外：是否采纳更高任期由投票判定内部决定
// （非成员候选人的高任期不得迫使本节点让位，防止被摘除节点扰乱集群）。
func (n *Node) Step(m Message) {
	if m.Type == MsgRequestVote {
		n.handleRequestVote(m)
		return
	}
	if m.Term > n.term {
		n.becomeFollower(m.Term, StepDownHigherTerm)
	}
	switch m.Type {
	case MsgRequestVoteResp:
		n.handleRequestVoteResp(m)
	case MsgAppendEntries:
		n.handleAppendEntries(m)
	case MsgAppendEntriesResp:
		n.handleAppendEntriesResp(m)
	case MsgInstallSnapshot:
		n.handleInstallSnapshot(m)
	case MsgInstallSnapshotResp:
		n.handleInstallSnapshotResp(m)
	}
}

// Propose 向领导者提交一条命令；非领导者返回 false。
func (n *Node) Propose(cmd []byte) bool { return n.propose(cmd) }

// Compact 触发日志压缩：将 upto（含）前已提交的历史替换为快照。
func (n *Node) Compact(upto Index, data []byte) { n.compact(upto, data) }

// DrainOutbox 取出待发送消息。
func (n *Node) DrainOutbox() []Message {
	out := n.outbox
	n.outbox = nil
	return out
}

// DrainApply 取出待应用消息。
func (n *Node) DrainApply() []ApplyMsg {
	out := n.applyCh
	n.applyCh = nil
	return out
}
