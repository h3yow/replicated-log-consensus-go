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
	boot    map[NodeID][]NodeID // 各节点的引导配置（重启时保持一致的引导语义）
	cfg     Config
	rnd     *rand.Rand
	seed    int64

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
		boot:      make(map[NodeID][]NodeID),
		cfg:       cfg,
		rnd:       rand.New(rand.NewSource(seed)),
		seed:      seed,
		partition: make(map[NodeID]bool),
	}
	for _, id := range peers {
		s.boot[id] = peers
		s.spawn(id)
	}
	s.logf("sim init: nodes=%v seed=%d", peers, seed)
	return s
}

// spawn 按引导配置创建（或重建）一个节点。
func (s *Sim) spawn(id NodeID) {
	c := s.cfg
	c.ID = id
	c.Peers = s.boot[id]
	st := s.storage[id]
	if st == nil {
		st = NewMemoryStorage()
		s.storage[id] = st
	}
	node, err := NewNode(c, st, rand.New(rand.NewSource(s.seed+int64(id)*7919+int64(s.tick))))
	if err != nil {
		panic(err)
	}
	s.nodes[id] = node
}

// AddNode 把一个全新的空节点以学习者身份接入仿真（尚不在集群配置内）。
// 之后由领导者通过 ProposeAddLearner 把它纳入配置并复制追赶。
func (s *Sim) AddNode(id NodeID) {
	if _, ok := s.nodes[id]; ok {
		panic(fmt.Sprintf("node %d already exists", id))
	}
	s.boot[id] = nil // 空配置引导：全新学习者，等待领导者推送配置
	s.spawn(id)
	s.logf("MEMBERSHIP add-node: node %d started as pristine learner", id)
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

// Restart 从持久化存储重启节点。
func (s *Sim) Restart(id NodeID) {
	s.spawn(id)
	n := s.nodes[id]
	s.logf("FAULT restart: node %d term=%d commit=%d conf=%v", id, n.Term(), n.CommitIndex(), n.AppliedConfig())
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
		conf := n.AppliedConfig()
		fmt.Fprintf(&b, "node=%d role=%s term=%d commit=%d removed=%v learner=%v voters=%v learners=%v log=%s\n",
			id, n.Role(), n.Term(), n.CommitIndex(), n.Removed(), n.IsLearner(),
			conf.Voters, conf.Learners, n.LogString())
	}
	return b.String()
}
