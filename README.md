# replicated-log-consensus-go

本地可运行、可确定性重放的复制日志与领导者选举组件（Raft 风格）。
所有时间由逻辑 `Tick()` 驱动、所有随机性由外部注入的固定种子 `rand.Rand` 提供，
因此在相同种子下整个集群的行为（含故障注入）可逐字节重放。

## 协议规则

### 领导者选举与任期推进
- 节点角色：Follower / Candidate / Leader。选举超时（在
  `[ElectionTimeout, 2*ElectionTimeout)` 内随机化）后发起选举，任期 +1 并自投。
- 获得多数派（`n/2+1`）选票的候选人当选；同一任期内多数派交集保证
  不会出现两个被多数派承认的领导者（由 `TestSingleLeaderPerTerm`、
  故障注入测试全程断言）。
- 投票拒绝给出**可区分原因**（`VoteRejectReason`）：
  `StaleTerm`（请求任期落后）、`AlreadyVoted`（本任期已投他人）、
  `LogNotUpToDate`（候选人日志落后，按 (lastTerm, lastIndex) 字典序比较）。
- 让位原因同样可解释（`StepDownReason`）：`HigherTerm`（发现更高任期）、
  `LeaderDiscovered`（收到合法领导者心跳）、`ElectionLost`。
  分区恢复后，旧领导者收到更高任期心跳即让位（`TestOldLeaderStepsDownAfterPartition`）。
- 投票、任期变更均先持久化再生效，超时判定是确定的，不用随机行为掩盖分歧。

### 日志复制与提交裁决
- 领导者通过 `AppendEntries`（含 prevLogIndex/prevLogTerm 一致性检查）复制条目；
  跟随者发现冲突即截断后缀再追加，重复投递/乱序到达时幂等跳过。
- 拒绝原因可区分（`AppendRejectReason`）：`StaleTerm` / `LogInconsistent`
  （后者附带冲突任期与建议回退索引，加速收敛）。
- **提交规则**：只有被多数派复制且任期不早于领导者当前任期的条目才推进提交点
  （Raft §5.4.2）；旧任期条目只能随本任期条目间接收尾。
- 提交点（commitIndex）持久化；节点重启后 `lastApplied` 从提交点恢复，
  已提交内容不丢失、不重复应用（`TestRestartNoLossNoDuplicate`）。
- 未提交条目不会进入应用队列，对外不可见。

### 日志压缩与追赶
- `Compact(upto, data)` 将已提交前缀压缩为快照（不允许压缩未提交部分）。
- 落后节点 `nextIndex` 低于领导者快照点时，领导者改发 `InstallSnapshot`。
- **快照安全规则**：若本地在快照末点处存在匹配条目，保留其后的更长已提交后缀，
  快照不得覆盖之（`TestSnapshotDoesNotOverwriteLongerSuffix`）。
- 追赶中的节点已应用序列始终是领导者已提交序列的前缀，
  对外提供一致可读视图（`TestLaggingNodeConsistentView`）。
- 快照携带**快照点处的成员配置**，接收方仅在快照不落后于本地已应用点时采用之。

### 动态成员变更（扩缩容）

采用单节点变更（single-server membership change，Raft 论文 §6 风格）：

- **配置即日志**：每次变更只增/删一个节点，以 `EntryConfig` 日志条目传播，
  **提交并应用后生效**（apply-on-commit）。同一时刻至多一个未提交的配置变更
  （`hasPendingConfigChange` 守卫），因此任意新旧两个配置的多数派必然相交，
  集群不会被劈成两半；连续多次扩缩容只需等上一次变更提交即可安全发起下一次。
- **扩容两段式**：
  1. `ProposeAddLearner(id)` 把空节点以**学习者**身份纳入配置——学习者只接收复制，
     不参选、不投票、**不计入多数派**，因此追平之前（哪怕完全失联）不影响
     原有节点对提交点的推进（`TestLearnerDoesNotBlockCommit`）；
  2. 学习者追平（历史已压缩时经 `InstallSnapshot`，快照自带配置）后，
     领导者**自动**追加"提升为投票成员"的配置条目（`maybePromoteLearners`）。
  提升条目按旧配置多数派提交，新节点随后应用并成为投票成员
  （`TestAddNodeCatchUpAndJoin`）。
- **缩容**：`ProposeRemove(id)` 提交后，被摘节点应用移除条目时让位并转为
  **非投票身份**（不参选、不投票、不计入多数），但仍跟随复制——
  因此之后可以被重新加入（重放自身移除条目不会卡死，会继续应用到重新加入的条目）。
  - 分区/失联中未学到移除的节点恢复后可能凭旧配置发起选举，但配置内节点
    执行**拉票成员资格前置检查**：候选人不在当前投票成员内时直接拒绝
    （`RejectNotInConfig`）**且不抬升本地任期**，因此被摘节点既不能重新当选，
    也不能通过任期膨胀扰动集群（`TestRemoveNode`）；其过期心跳因任期落后被拒。
  - 领导者可以移除自身：移除条目提交后让位转为非投票，剩余成员选出新领导者
    （`TestRemoveLeaderSelf`）。
- **选举与提交的多数派**都按"当前已应用的投票成员"计算；响应消息（投票/复制/快照确认）
  只被当前配置内的发送方推进，被摘节点的迟到响应被忽略。
- **工程取舍说明**：
  - 选择 apply-on-commit 而非"追加即生效"：未提交的配置变更被截断时不留痕迹，
    重启恢复语义简单（持久化的就是提交点处的配置）；代价是配置生效比追加晚一个
    提交往返，对运维场景可接受。
  - 被摘节点选择"非投票化"而非"彻底停机"：停机语义下被摘节点重新加入时
    会重放自身的移除条目并再次停机，必须清空数据目录才能回归；非投票化
    让重新加入成为普通的"加学习者→追平→提升"流程，运维更简单，且安全性
    不受影响（非投票节点无法当选、无法计入多数、其响应被领导者忽略）。
  - 学习者追平判定为 `matchIndex ≥ 领导者最后索引`；追平前不影响提交点推进，
    这是用"新节点不计入多数派"换来的可用性保证。

### 兼容范围

- **持久化状态**：`PersistentState` 新增 `Voters/Learners/Removed/SnapVoters/SnapLearners`
  字段。加载旧格式（无配置字段）状态时回退到 `Config.Peers` 作为初始投票成员，
  行为与旧版本一致；扩容前的持久化状态与快照可直接继续使用。
- **快照**：新格式快照携带快照点处的成员配置；接收方遇到**不含配置的旧格式快照**
  时保持本地配置不变（见 `handleInstallSnapshot`），新旧节点可混跑。
- **重新加入**：被摘节点无需清空数据即可被重新 `ProposeAddLearner` 加入；
  若运维上选择清空数据目录（`Sim.Wipe` 模拟），则以全新学习者身份重新入列，同样安全。
- **不变更线协议以外的兼容性**：消息信封新增字段仅用于 `InstallSnapshot`，
  旧消息流不受影响。

## 可调参数（`raft.Config`）

| 参数 | 含义 | 说明 |
|---|---|---|
| `ElectionTimeout` | 选举超时基准（tick） | 实际超时随机化为 `[T, 2T)` |
| `HeartbeatTick` | 心跳间隔（tick） | 应显著小于 ElectionTimeout |
| `MaxEntriesPerMsg` | 单条 AppendEntries 最大条目数 | 0 表示不限 |
| `SnapshotThreshold` | 自动压缩阈值 | 预留；当前由上层显式调用 `Compact` |

仿真器故障注入开关：`Partition/Heal`（分区与恢复）、`SetLoss`（丢包率）、
`SetDup`（重复投递率）、`SetReorder`（乱序）、`Crash/Restart`（节点重启）、
`AddNode`（空节点接入）、`Wipe`（清空数据目录后重新上线）。

## 验证方法

```sh
go test ./... -v
```

- `election_test.go`：同任期唯一领导者、分区恢复后旧领导者让位、
  三类投票拒绝原因可区分。
- `replication_test.go`：多数派提交与按序应用、旧任期条目提交裁决、
  重启+重复投递下不丢失不重复。
- `snapshot_test.go`：快照追赶、快照不覆盖更长已提交后缀、追赶中一致视图。
- `fault_injection_test.go`：固定种子下 5 节点、400 tick 内随机注入
  分区/丢包/重复/乱序/崩溃重启，全程断言：
  - **选举安全性**：同一任期至多一个领导者；
  - **状态机安全性**：任意两节点已应用序列前缀一致、同一索引不被应用为不同值；
  - **最终一致性**：故障清除后所有节点提交点收敛。
  `TestDeterministicReplay` 验证相同种子两次运行产生完全相同的事件序列。
- `membership_test.go`：动态扩缩容。
  - `TestAddNodeCatchUpAndJoin`：空节点经快照追平被压缩历史后自动提升为投票成员；
  - `TestLearnerDoesNotBlockCommit`：学习者失联不阻塞提交点推进；
  - `TestRemoveNode`：被摘节点分区恢复后不能重新当选、不能扰动集群任期；
  - `TestRemoveLeaderSelf`：领导者移除自身后安全交权；
  - `TestRepeatedAddRemove`：连续来回多次扩缩容全程安全、最终一致；
  - `TestMembershipFaultInjection`：固定种子（1/2/3）下扩缩容与
    丢包/重复/乱序/分区/重启交织 500 tick，全程断言选举安全性与状态机安全性，
    收尾断言配置内所有节点提交点与已应用配置收敛；
  - `TestMembershipDeterministicReplay`：含扩缩容的运行可逐字节重放。
- 测试日志打印任期、日志索引与判定依据（`Sim.Events()` 记录每次故障注入、
  丢包、重复投递；`Sim.DumpState()` 打印各节点 role/term/commit/配置/log）。

## 代码结构

```
raft/
  types.go      基础类型、配置状态（ConfigState）与可区分的判定原因枚举
  message.go    统一消息信封（InstallSnapshot 携带快照点配置）
  log.go        带快照前缀的复制日志（快照记录成员配置）
  storage.go    持久化抽象与内存实现（支持写入丢失注入；持久化成员配置）
  node.go       节点状态、驱动接口（Tick/Step/Propose/Compact）、成员资格前置检查
  election.go   选举与任期推进（仅投票成员参选/计票）
  replicate.go  日志复制与提交裁决（多数派按已应用配置计算）
  snapshot.go   日志压缩与快照安装（配置随快照传播）
  membership.go 动态成员变更（学习者加入/提升、节点移除、配置条目应用）
  sim.go        确定性仿真器（故障注入 + 事件日志 + 节点动态接入）
```
