package raft

import "math/rand"

// Harness 确定性仿真环境：固定随机种子下复现分区、丢包、重复投递与乱序。
// 所有随机性都来自构造时注入的种子，相同种子的运行逐比特一致。

// Faults 故障注入开关与概率（0..1）。
type Faults struct {
	DropProb      float64 // 丢包概率
	DuplicateProb float64 // 重复投递概率
	ReorderWindow int     // 乱序窗口（>1 时允许在窗口内重排投递顺序）
}

// Harness 驱动一组节点与模拟网络。
type Harness struct {
	rng     *rand.Rand
	cfgs    map[NodeID]Config
	nodes   map[NodeID]*Node
	stores  map[NodeID]*MemoryStorage
	faults  Faults
	blocked map[NodeID]bool // 被分区隔离的节点
	queue   []Message
	tick    int
}

func NewHarness(seed int64, faults Faults) *Harness {
	return &Harness{
		rng:     rand.New(rand.NewSource(seed)),
		faults:  faults,
		cfgs:    map[NodeID]Config{},
		nodes:   map[NodeID]*Node{},
		stores:  map[NodeID]*MemoryStorage{},
		blocked: map[NodeID]bool{},
	}
}

func (h *Harness) AddNode(cfg Config) {
	h.cfgs[cfg.ID] = cfg
	st := NewMemoryStorage()
	h.stores[cfg.ID] = st
	h.nodes[cfg.ID] = NewNode(cfg, st)
}

// Restart 用同一存储重启节点（模拟崩溃恢复）：持久状态保留，易失状态清零。
func (h *Harness) Restart(id NodeID) {
	h.nodes[id] = NewNode(h.cfgs[id], h.stores[id])
}

// Partition 隔离一组节点（与其余节点双向断连）。
func (h *Harness) Partition(ids ...NodeID) {
	for _, id := range ids {
		h.blocked[id] = true
	}
}

// Heal 恢复所有分区。
func (h *Harness) Heal() {
	h.blocked = map[NodeID]bool{}
}

// Node 返回节点句柄。
func (h *Harness) Node(id NodeID) *Node { return h.nodes[id] }

// Propose 便捷方法：向指定节点提交命令。
func (h *Harness) Propose(id NodeID, cmd []byte) bool {
	return h.nodes[id].Propose(cmd)
}

// Run 推进 n 个 tick：每 tick 所有节点 Tick 一次并收集出站消息，
// 然后按故障规则投递网络消息。
func (h *Harness) Run(ticks int) {
	for i := 0; i < ticks; i++ {
		h.tick++
		for _, id := range h.sortedIDs() {
			n := h.nodes[id]
			n.Tick()
			h.enqueue(n.TakeMessages())
		}
		h.deliver()
	}
}

// Flush 只投递网络消息（不推进时钟），直到队列清空。
func (h *Harness) Flush() {
	for len(h.queue) > 0 {
		h.deliver()
	}
}

func (h *Harness) enqueue(msgs []Message) {
	for _, m := range msgs {
		if h.faults.DropProb > 0 && h.rng.Float64() < h.faults.DropProb {
			continue // 丢包
		}
		h.queue = append(h.queue, m)
		if h.faults.DuplicateProb > 0 && h.rng.Float64() < h.faults.DuplicateProb {
			h.queue = append(h.queue, m) // 重复投递
		}
	}
	// 乱序：在窗口内做确定性洗牌。
	w := h.faults.ReorderWindow
	if w > 1 && len(h.queue) > 1 {
		start := len(h.queue) - w
		if start < 0 {
			start = 0
		}
		window := h.queue[start:]
		h.rng.Shuffle(len(window), func(i, j int) {
			window[i], window[j] = window[j], window[i]
		})
	}
}

func (h *Harness) deliver() {
	queue := h.queue
	h.queue = nil
	for _, m := range queue {
		// 分区：一端被隔离即不可达。
		if h.blocked[m.From] != h.blocked[m.To] {
			continue
		}
		n := h.nodes[m.To]
		n.Step(m)
		h.enqueue(n.TakeMessages())
	}
}

// sortedIDs 返回排序后的节点 ID，保证遍历顺序确定。
func (h *Harness) sortedIDs() []NodeID {
	ids := make([]NodeID, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return ids
}

// Committed 收集所有节点新提交的条目（推进各自 lastApplied）。
// 返回 map[节点] -> (起始索引, 条目)。
func (h *Harness) Committed() map[NodeID]struct {
	From    Index
	Entries []LogEntry
} {
	out := map[NodeID]struct {
		From    Index
		Entries []LogEntry
	}{}
	for _, id := range h.sortedIDs() {
		from, entries := h.nodes[id].TakeCommitted()
		if len(entries) > 0 {
			out[id] = struct {
				From    Index
				Entries []LogEntry
			}{from, entries}
		}
	}
	return out
}
