package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// Sim 是确定性集群仿真器：
//   - 时间由 Tick() 逐拍推进；
//   - 消息进入有序队列，按 (sendTick, seq) 顺序投递，乱序由显式故障注入产生；
//   - 所有随机性来自固定种子的 rnd，整个运行可逐字节重放。
type Sim struct {
	nodes   map[NodeID]*Node
	storage map[NodeID]*MemoryStorage
	cfg     Config
	rnd     *rand.Rand
	seed    int64

	// bootPeers 记录每个节点首次引导时的初始成员（空存储时生效）。
	// 动态加入的节点引导配置为空——它们以空配置启动，
	// 等待领导者通过快照/日志把当前配置送过来。
	bootPeers map[NodeID][]NodeID

	tick int
	seq  int

	queue   []queuedMsg
	dropped []Message // 记录被丢弃的消息（供调试打印）

	// 故障注入开关
	partition map[NodeID]bool // 被隔离的节点集合
	lossRate  float64         // 每条消息独立丢包概率
	dupRate   float64         // 每条消息独立重复投递概率
	reorder   bool            // 投递前打乱当拍就绪队列

	// 事件日志（判定依据）
	events []string
}

type queuedMsg struct {
	m         Message
	deliverAt int
	seq       int
}

// NewSim 创建 n 节点集群。seed 固定时行为完全确定。
func NewSim(n int, cfg Config, seed int64) *Sim {
	peers := make([]NodeID, n)
	for i := range peers {
		peers[i] = NodeID(i + 1)
	}
	cfg.Peers = peers
	s := &Sim{
		nodes:     make(map[NodeID]*Node),
		storage:   make(map[NodeID]*MemoryStorage),
		bootPeers: make(map[NodeID][]NodeID),
		cfg:       cfg,
		rnd:       rand.New(rand.NewSource(seed)),
		seed:      seed,
		partition: make(map[NodeID]bool),
	}
	for _, id := range peers {
		s.bootPeers[id] = peers
		s.spawn(id)
	}
	s.logf("sim init: nodes=%v seed=%d", peers, seed)
	return s
}

// spawn 以指定引导配置创建一个节点（含独立存储）。
func (s *Sim) spawn(id NodeID) {
	c := s.cfg
	c.ID = id
	c.Peers = s.bootPeers[id]
	st := NewMemoryStorage()
	node, err := NewNode(c, st, rand.New(rand.NewSource(s.seed+int64(id)*7919)))
	if err != nil {
		panic(err)
	}
	s.nodes[id] = node
	s.storage[id] = st
}

func (s *Sim) logf(format string, args ...interface{}) {
	s.events = append(s.events, fmt.Sprintf("tick=%d "+format, append([]interface{}{s.tick}, args...)...))
}

// Events 返回全部判定日志（供测试打印）。
func (s *Sim) Events() []string { return s.events }

// Node 取节点。
func (s *Sim) Node(id NodeID) *Node { return s.nodes[id] }

// Partition 将节点集合与其余节点隔离（双向）。
func (s *Sim) Partition(ids ...NodeID) {
	for _, id := range ids {
		s.partition[id] = true
	}
	s.logf("FAULT partition: isolate %v", ids)
}

// Heal 解除全部分区。
func (s *Sim) Heal() {
	s.partition = make(map[NodeID]bool)
	s.logf("FAULT heal: all partitions removed")
}

// SetLoss 设置丢包率；SetDup 设置重复投递率；SetReorder 开关乱序。
func (s *Sim) SetLoss(r float64)  { s.lossRate = r; s.logf("FAULT lossRate=%.2f", r) }
func (s *Sim) SetDup(r float64)   { s.dupRate = r; s.logf("FAULT dupRate=%.2f", r) }
func (s *Sim) SetReorder(on bool) { s.reorder = on; s.logf("FAULT reorder=%v", on) }

// Crash 模拟节点崩溃（易失状态丢失，持久化状态保留）。
func (s *Sim) Crash(id NodeID) {
	delete(s.nodes, id)
	s.logf("FAULT crash: node %d", id)
}

// Restart 从持久化存储重启节点（成员配置由持久化日志推导，不依赖引导配置）。
func (s *Sim) Restart(id NodeID) {
	c := s.cfg
	c.ID = id
	c.Peers = s.bootPeers[id]
	node, err := NewNode(c, s.storage[id], rand.New(rand.NewSource(s.seed+int64(id)*7919+int64(s.tick))))
	if err != nil {
		panic(err)
	}
	s.nodes[id] = node
	s.logf("FAULT restart: node %d term=%d commit=%d voters=%v learners=%v",
		id, node.Term(), node.CommitIndex(), node.Voters(), node.Learners())
}

// --- 动态成员变更 ---

// AddNode 运行期扩容：若节点不存在则以空配置生成，
// 再请当前领导者发起"加入学习者"变更。返回 false 表示本次未发起
// （无领导者或领导者正忙），调用方可于后续 tick 重试。
func (s *Sim) AddNode(id NodeID) bool {
	if _, ok := s.nodes[id]; !ok {
		s.bootPeers[id] = nil // 空配置启动：等待领导者送来当前配置
		s.spawn(id)
		s.logf("CONF spawn: node %d 以空配置启动，等待追赶", id)
	}
	leader, ok := s.Leader()
	if !ok {
		return false
	}
	if !s.nodes[leader].AddNode(id) {
		return false
	}
	s.logf("CONF add: leader=%d 提议加入 node %d (learner) term=%d", leader, id, s.nodes[leader].Term())
	return true
}

// RemoveNode 运行期缩容：请当前领导者发起摘除变更。
// 只向"未被隔离"的领导者提议（被隔离领导者的变更无法提交，
// 且会让被摘除者意外学到变更，失去分区场景的测试意义）。
func (s *Sim) RemoveNode(id NodeID) bool {
	for _, nid := range s.sortedIDs() {
		if s.partition[nid] {
			continue
		}
		n := s.nodes[nid]
		if n.Role() == Leader && n.RemoveNode(id) {
			s.logf("CONF remove: leader=%d 提议摘除 node %d term=%d", nid, id, n.Term())
			return true
		}
	}
	return false
}

// disconnected 判定一条消息是否被分区阻断。
func (s *Sim) disconnected(m Message) bool {
	return s.partition[m.From] != s.partition[m.To]
}

// Tick 推进一拍：驱动所有节点时钟，收集并投递消息。
func (s *Sim) Tick() {
	s.tick++
	ids := s.sortedIDs()
	for _, id := range ids {
		s.nodes[id].Tick()
	}
	for _, id := range ids {
		for _, m := range s.nodes[id].DrainOutbox() {
			s.enqueue(m)
		}
	}
	s.deliver()
}

// Run 推进 n 拍。
func (s *Sim) Run(ticks int) {
	for i := 0; i < ticks; i++ {
		s.Tick()
	}
}

func (s *Sim) sortedIDs() []NodeID {
	ids := make([]NodeID, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *Sim) enqueue(m Message) {
	if s.disconnected(m) {
		s.logf("drop(partition): %s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
		return
	}
	if s.lossRate > 0 && s.rnd.Float64() < s.lossRate {
		s.dropped = append(s.dropped, m)
		s.logf("drop(loss): %s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
		return
	}
	s.seq++
	s.queue = append(s.queue, queuedMsg{m: m, deliverAt: s.tick, seq: s.seq})
	if s.dupRate > 0 && s.rnd.Float64() < s.dupRate {
		s.seq++
		s.queue = append(s.queue, queuedMsg{m: m, deliverAt: s.tick, seq: s.seq})
		s.logf("dup: %s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
	}
}

func (s *Sim) deliver() {
	ready := make([]queuedMsg, 0, len(s.queue))
	pending := s.queue[:0]
	for _, q := range s.queue {
		if q.deliverAt <= s.tick {
			ready = append(ready, q)
		} else {
			pending = append(pending, q)
		}
	}
	s.queue = pending
	if s.reorder && len(ready) > 1 {
		s.rnd.Shuffle(len(ready), func(i, j int) { ready[i], ready[j] = ready[j], ready[i] })
	}
	for _, q := range ready {
		dst, ok := s.nodes[q.m.To]
		if !ok {
			continue // 目标已崩溃
		}
		dst.Step(q.m)
		// 处理过程中产生的新消息入队（同拍稍后投递）。
		for _, m := range dst.DrainOutbox() {
			s.enqueue(m)
		}
	}
}

// --- 测试辅助：断言与日志 ---

// Leader 返回当前唯一领导者；无或多个时返回 false。
func (s *Sim) Leader() (NodeID, bool) {
	var leader NodeID
	count := 0
	for _, id := range s.sortedIDs() {
		if s.nodes[id].Role() == Leader {
			leader = id
			count++
		}
	}
	return leader, count == 1
}

// Applied 收集节点已应用的全部命令（含快照标记）。
func (s *Sim) Applied(id NodeID) []ApplyMsg { return s.nodes[id].DrainApply() }

// DumpState 打印所有节点状态，用于测试日志中的判定依据。
func (s *Sim) DumpState() string {
	var b strings.Builder
	for _, id := range s.sortedIDs() {
		n := s.nodes[id]
		fmt.Fprintf(&b, "node=%d role=%s term=%d commit=%d voters=%v learners=%v log=%s\n",
			id, n.Role(), n.Term(), n.CommitIndex(), n.Voters(), n.Learners(), n.LogString())
	}
	return b.String()
}
