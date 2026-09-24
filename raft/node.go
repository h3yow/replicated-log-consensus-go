package raft

// Config 可调参数。
type Config struct {
	ID                NodeID
	Peers             []NodeID
	ElectionTimeout   int // 选举超时下界（tick 数），实际超时为确定性抖动
	HeartbeatTimeout  int // 心跳间隔（tick 数）
	MaxInflight       int // 单次 AppendEntries 最大条目数
	SnapshotThreshold int // 日志超过该条数后允许压缩
}

func (c *Config) normalize() {
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 10
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 2
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 16
	}
	if c.SnapshotThreshold <= 0 {
		c.SnapshotThreshold = 64
	}
}

// Node 复制状态机节点。核心为单线程模型：Tick 推进时钟，Step 处理消息，
// 因此整个系统在固定种子下可确定性重放。
type Node struct {
	cfg Config

	role     Role
	leaderID NodeID

	// 持久状态（镜像到 Storage）
	currentTerm Term
	votedFor    NodeID
	entries     []LogEntry // 逻辑索引从 snapshot.Meta.Index+1 开始
	snapshot    Snapshot
	commitIndex Index
	lastApplied Index

	// 选举状态
	electionElapsed  int
	heartbeatElapsed int
	votesReceived    map[NodeID]bool

	// 领导者状态
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	storage Storage
	outbox  []Message
}

// NewNode 创建节点；若 storage 中已有状态（重启场景），完整恢复，
// 已提交与已应用位置不丢失、不重复。
func NewNode(cfg Config, storage Storage) *Node {
	cfg.normalize()
	n := &Node{
		cfg:           cfg,
		role:          Follower,
		storage:       storage,
		votesReceived: map[NodeID]bool{},
		nextIndex:     map[NodeID]Index{},
		matchIndex:    map[NodeID]Index{},
	}
	ps := storage.Load()
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.entries = ps.Entries
	n.snapshot = ps.Snapshot
	n.commitIndex = ps.CommitIndex
	n.lastApplied = ps.LastApplied
	if n.commitIndex > n.lastIndex() {
		n.commitIndex = n.lastIndex()
	}
	return n
}

// ---- 日志索引辅助 ----

func (n *Node) snapshotIndex() Index { return n.snapshot.Meta.Index }

func (n *Node) lastIndex() Index { return n.snapshotIndex() + Index(len(n.entries)) }

func (n *Node) lastTerm() Term {
	if len(n.entries) > 0 {
		return n.entries[len(n.entries)-1].Term
	}
	return n.snapshot.Meta.Term
}

// termAt 返回逻辑索引 idx 处的任期；ok=false 表示该处已被压缩。
func (n *Node) termAt(idx Index) (Term, bool) {
	switch {
	case idx == n.snapshotIndex():
		return n.snapshot.Meta.Term, true
	case idx < n.snapshotIndex():
		return 0, false
	case idx <= n.lastIndex():
		return n.entries[idx-n.snapshotIndex()-1].Term, true
	}
	return 0, false
}

// entryAt 返回逻辑索引 idx 处的条目。
func (n *Node) entryAt(idx Index) LogEntry {
	return n.entries[idx-n.snapshotIndex()-1]
}

// sliceFrom 返回逻辑索引 [from, lastIndex] 的条目副本。
func (n *Node) sliceFrom(from Index) []LogEntry {
	if from > n.lastIndex() {
		return nil
	}
	src := n.entries[from-n.snapshotIndex()-1:]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// ---- 持久化 ----

func (n *Node) persist() {
	n.storage.Persist(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Entries:     n.entries,
		Snapshot:    n.snapshot,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	})
}

// ---- 角色切换 ----

func (n *Node) becomeFollower(term Term, reason StepDownReason) {
	changed := term != n.currentTerm
	n.role = Follower
	n.currentTerm = term
	if changed {
		n.votedFor = ""
	}
	n.leaderID = ""
	n.electionElapsed = 0
	n.votesReceived = map[NodeID]bool{}
	if changed {
		n.persist()
	}
	_ = reason // 供测试/日志观察让位原因
}

// ---- 时钟与消息分发 ----

// electionTimeout 为确定性抖动：由节点 ID 导出，避免随机源，
// 固定拓扑下每次运行结果完全一致。
func (n *Node) electionTimeout() int {
	base := n.cfg.ElectionTimeout
	h := 0
	for _, c := range string(n.cfg.ID) {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return base + h%base
}

// Tick 推进一个逻辑时钟。
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTimeout {
			n.heartbeatElapsed = 0
			for _, p := range n.cfg.Peers {
				if p != n.cfg.ID {
					n.sendAppend(p)
				}
			}
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout() {
		n.startElection()
	}
}

// Step 处理一条入站消息。
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.currentTerm:
		n.becomeFollower(m.Term, StepDownHigherTermSeen)
	case m.Term < n.currentTerm:
		n.replyStale(m)
		return
	}
	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
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

// replyStale 对任期落后的消息给出可区分的拒绝原因。
func (n *Node) replyStale(m Message) {
	resp := Message{From: n.cfg.ID, To: m.From, Term: n.currentTerm}
	switch m.Type {
	case MsgRequestVote:
		resp.Type = MsgRequestVoteResp
		resp.VoteGranted = false
		resp.VoteRejectReason = VoteRejectTermStale
	case MsgAppendEntries:
		resp.Type = MsgAppendEntriesResp
		resp.AppendRejectReason = AppendRejectTermStale
	case MsgInstallSnapshot:
		resp.Type = MsgInstallSnapshotResp
	default:
		return
	}
	n.send(resp)
}

// Propose 向领导者提交一条命令；非领导者返回 false。
func (n *Node) Propose(cmd []byte) bool {
	if n.role != Leader {
		return false
	}
	n.entries = append(n.entries, LogEntry{Term: n.currentTerm, Command: cloneBytes(cmd)})
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.persist()
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
	n.maybeCommit() // 单节点集群可立即提交
	return true
}

// ---- 输出 ----

func (n *Node) send(m Message) { n.outbox = append(n.outbox, m) }

// TakeMessages 取出并清空待发送消息。
func (n *Node) TakeMessages() []Message {
	msgs := n.outbox
	n.outbox = nil
	return msgs
}

// TakeCommitted 取出新提交、待应用的条目（含起始逻辑索引），
// 并推进 lastApplied 且持久化——重启后不会重复应用。
func (n *Node) TakeCommitted() (Index, []LogEntry) {
	if n.commitIndex <= n.lastApplied {
		return 0, nil
	}
	from := n.lastApplied + 1
	out := n.sliceFrom(from)
	n.lastApplied = n.commitIndex
	n.persist()
	return from, out
}

// ---- 状态观察 ----

// Status 返回只读状态快照，供测试断言与日志打印。
func (n *Node) Status() Status {
	return Status{
		ID:          n.cfg.ID,
		Role:        n.role,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   n.lastIndex(),
		LastTerm:    n.lastTerm(),
		Leader:      n.leaderID,
	}
}

// Status 节点可观测状态。
type Status struct {
	ID          NodeID
	Role        Role
	Term        Term
	CommitIndex Index
	LastApplied Index
	LastIndex   Index
	LastTerm    Term
	Leader      NodeID
}

// majority 多数派数量。
func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

// CommittedView 返回已提交前缀（快照元数据 + 快照之后的已提交条目），
// 供测试做状态机安全性断言，不影响节点状态。
func (n *Node) CommittedView() (SnapshotMeta, []LogEntry) {
	if n.commitIndex <= n.snapshotIndex() {
		return n.snapshot.Meta, nil
	}
	return n.snapshot.Meta, n.sliceFrom(n.snapshotIndex() + 1)[:n.commitIndex-n.snapshotIndex()]
}
