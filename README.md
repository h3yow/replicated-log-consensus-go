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
  `LogNotUpToDate`（候选人日志落后，按 (lastTerm, lastIndex) 字典序比较）、
  `NotMember`（候选人不在当前投票成员配置中——被摘除节点的高任期请求
  既不被投票也不被采纳，见"动态成员变更"）。
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

### 动态成员变更（不停机扩缩容）

采用 Raft 论文 §4.1 的**单节点变更** + §4.2.1 的**学习者追赶**，规则如下：

- **配置即日志**：成员变更以 `EntryConfig` 日志条目复制，**追加进日志即生效**
  （多数派判定、投票、复制目标都按最新配置计算）。当前配置始终可由
  "快照点成员 + 快照之后的变更条目"重新推导（`recomputeMembership`），因此：
  - 未提交却被更高任期日志截断的变更会随截断自动回滚，不会残留；
  - 重启后从持久化的快照元数据与日志重新推导，与集群一致
    （`TestMembershipSurvivesRestart`）。
- **单节点变更**：每次只增/删一个节点，且领导者在前一次变更提交前拒绝新变更
  （`canProposeConfChange`）。新旧配置的多数派必然相交，任何时刻都不会出现
  两侧各自凑出多数的裂脑；同一任期至多一个领导者的不变量在变更期间照常成立。
- **扩容两阶段**：`AddNode(id)` 先把新节点作为**学习者**加入——学习者只接收复制，
  不计入多数派、不参与选举；历史已压缩时领导者直接发快照。当学习者的复制进度
  追平领导者提交点（`matchIndex >= commitIndex`）后，领导者自动追加提升条目把它
  转为投票成员。**取舍**：追平判定以"覆盖当前提交点"为准而非"日志末尾"——
  追赶期间新写入仍在不断到来，以提交点为准既能保证提升后立即可参与多数派，
  又不会因为持续写入而永远追不上；追赶期间学习者完全不影响原集群的提交推进。
- **缩容**：`RemoveNode(id)` 追加摘除条目。被摘除者立即退出多数派判定与复制目标；
  变更提交前领导者仍向其复制（让它能学到自己被摘除并主动退出），提交后停止。
  领导者摘除自身时，在变更提交后让位（`StepDownRemoved`），由剩余成员选出新领导者。
- **被摘除节点的隔离**：恢复连接的被摘除者可能仍持旧配置认知。两道防线：
  1. 它若学到摘除条目（在线时），自己就不再竞选（非投票成员不发起选举）；
  2. 它若没学到（分区/失联），其竞选会被所有当前成员以 `RejectNotMember` 拒绝，
     **且成员不采纳它携带的更高任期**——它无法重新当选、无法迫使现任领导者让位、
     也无法把过期日志推给任何人（只有领导者能复制日志，而它永远选不上）。
     即使它凭旧配置发起选举，也因"日志至少一样新"的投票检查拿不到
     持有摘除条目的多数派的票。
- **初始配置的传播**：初始成员是索引 0 处的快照元数据。全新空节点从索引 1 开始
  追赶时，领导者在 `PrevLogIndex` 等于自身快照点的 `AppendEntries` 中携带
  `PrevVoters/PrevLearners`，接收者据此采纳基准配置；经快照追赶时则由
  `InstallSnapshot` 直接携带。

### 兼容范围

- **持久化状态**：`PersistentState` 新增 `SnapVoters/SnapLearners`（快照点成员）。
  旧格式状态（无这两个字段、日志中也没有任何变更条目）在重启时走**升级回退**：
  判定为"旧版本静态成员集群"，按启动时给定的初始成员配置（`Config.Peers`）
  恢复投票成员身份，并**立即把恢复结果落盘**——因此重启前后成员视图一致，
  之后反复重启直接读持久化成员，不再依赖引导配置是否再次给对。
  该回退只在状态里完全没有任何成员信息时触发；只要发生过动态成员行为
  （含"被摘除"），状态里必然留有变更条目或快照成员，回退不会误判
  （`TestUpgradeDoesNotResurrectRemovedNode`）。
  升级恢复后，老节点正常参与选举与投票，并随复制流收敛到集群当前配置；
  新旧状态混跑的集群也能自动收敛（`TestUpgradeMixedClusterConverges`）。
  注意：升级重启时必须传入与旧集群一致的 `Config.Peers`。
- **快照**：扩容前生成的快照（含当时的成员配置）在扩容后依然有效——
  安装快照的节点以快照成员为基底、继续应用其后的变更条目，最终推导出的
  配置与集群一致。反之亦然。
- **消息**：`AppendEntries` 的 `PrevVoters/PrevLearners` 仅在条目挂在快照点时
  携带，为空时接收方忽略，不影响既有复制路径。
- **滚动升级**：持久化状态支持从上一版本直接升级（见上"持久化状态"条的
  升级回退）；线上消息协议无兼容承诺，同一集群内所有节点应运行同一版本。

## 可调参数（`raft.Config`）

| 参数 | 含义 | 说明 |
|---|---|---|
| `Peers` | 初始引导成员（含自身） | 仅在空存储首次启动时生效；此后成员由日志中的变更条目决定。动态加入的节点传空 |
| `ElectionTimeout` | 选举超时基准（tick） | 实际超时随机化为 `[T, 2T)` |
| `HeartbeatTick` | 心跳间隔（tick） | 应显著小于 ElectionTimeout |
| `MaxEntriesPerMsg` | 单条 AppendEntries 最大条目数 | 0 表示不限 |
| `SnapshotThreshold` | 自动压缩阈值 | 预留；当前由上层显式调用 `Compact` |

运行期扩缩容 API：`Node.AddNode(id)`（加入学习者，追平后自动提升）、
`Node.RemoveNode(id)`（摘除成员）；仿真器对应 `Sim.AddNode/RemoveNode`。
两者返回 `false` 表示当前不可发起（非领导者、已有未提交变更、或目标状态已达成），
调用方稍后重试即可。

仿真器故障注入开关：`Partition/Heal`（分区与恢复）、`SetLoss`（丢包率）、
`SetDup`（重复投递率）、`SetReorder`（乱序）、`Crash/Restart`（节点重启）。

## 验证方法

```sh
go test ./... -v
```

- `election_test.go`：同任期唯一领导者、分区恢复后旧领导者让位、
  四类投票拒绝原因可区分（含非成员候选人 `RejectNotMember` 且其任期不被采纳）。
- `replication_test.go`：多数派提交与按序应用、旧任期条目提交裁决、
  重启+重复投递下不丢失不重复。
- `snapshot_test.go`：快照追赶、快照不覆盖更长已提交后缀、追赶中一致视图。
- `membership_test.go`：动态扩缩容——
  - `TestAddNodeCatchUpAndPromote`：空节点经快照+日志追平后自动提升为投票成员，
    扩容全程集群持续可写；
  - `TestRemoveNode` / `TestRemoveLeaderSelf`：摘除普通成员/领导者自身，
    被摘除者退出多数派，集群继续服务；
  - `TestRemovedNodeCannotDisruptAfterReconnect`：摘除分区中的旧领导者，
    恢复后它无法重新当选、无法扰乱集群（其任期膨胀到 22 时集群仍稳定在 term 2）；
  - `TestConsecutiveMembershipChanges`：连续六次来回扩缩容，全程断言安全不变量；
  - `TestMembershipFaultInjection`：固定种子下 500 tick 内随机注入
    丢包/重复/乱序/分区/崩溃重启/压缩，同时随机扩容/缩容/写入，
    全程断言选举安全性与状态机安全性，收尾断言成员视图、提交点、应用序列最终一致；
  - `TestMembershipSurvivesRestart`：变更进行中重启，配置从持久化状态正确恢复；
  - `TestMembershipDeterministicReplay`：含扩缩容的运行在相同种子下逐字节重放。
- `upgrade_test.go`：旧版本持久化状态重启（升级路径）——
  - `TestUpgradeLegacyRestartWithLog`：日志非空 + 已有提交点的旧状态节点重启，
    按初始成员配置恢复身份、重新选出领导者、提交点继续推进，
    恢复结果落盘、二次重启视图稳定；
  - `TestUpgradeLegacyRestartWithSnapshot`：已产生快照（快照不含成员元数据）的
    旧状态节点重启，同样恢复；
  - `TestUpgradeMixedClusterConverges`：新旧状态混跑，重启后收敛到同一配置，
    且升级节点能跟随后续动态扩缩容；
  - `TestUpgradeDoesNotResurrectRemovedNode`：被动态摘除的节点重启后仍是非成员，
    不被升级回退误判、不扰乱集群选举。
- `fault_injection_test.go`：固定种子下 5 节点、400 tick 内随机注入
  分区/丢包/重复/乱序/崩溃重启，全程断言：
  - **选举安全性**：同一任期至多一个领导者；
  - **状态机安全性**：任意两节点已应用序列前缀一致、同一索引不被应用为不同值；
  - **最终一致性**：故障清除后所有节点提交点收敛。
  `TestDeterministicReplay` 验证相同种子两次运行产生完全相同的事件序列。
- 测试日志打印任期、日志索引与判定依据（`Sim.Events()` 记录每次故障注入、
  成员变更提议、丢包、重复投递；`Sim.DumpState()` 打印各节点
  role/term/commit/voters/learners/log）。

## 代码结构

```
raft/
  types.go      基础类型、成员变更类型与可区分的判定原因枚举
  message.go    统一消息信封（含成员配置传播字段）
  log.go        带快照前缀的复制日志（快照元数据含成员配置）
  storage.go    持久化抽象与内存实现（支持写入丢失注入）
  node.go       节点状态、驱动接口（Tick/Step/Propose/Compact）
  election.go   选举与任期推进（仅投票成员可竞选/计票）
  replicate.go  日志复制与提交裁决（按当前配置计多数派）
  snapshot.go   日志压缩与快照安装（快照携带成员配置）
  membership.go 动态成员变更：学习者追赶、自动提升、摘除与配置推导
  sim.go        确定性仿真器（故障注入 + 动态节点 + 事件日志）
```
