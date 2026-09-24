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

## 可调参数（`raft.Config`）

| 参数 | 含义 | 说明 |
|---|---|---|
| `ElectionTimeout` | 选举超时基准（tick） | 实际超时随机化为 `[T, 2T)` |
| `HeartbeatTick` | 心跳间隔（tick） | 应显著小于 ElectionTimeout |
| `MaxEntriesPerMsg` | 单条 AppendEntries 最大条目数 | 0 表示不限 |
| `SnapshotThreshold` | 自动压缩阈值 | 预留；当前由上层显式调用 `Compact` |

仿真器故障注入开关：`Partition/Heal`（分区与恢复）、`SetLoss`（丢包率）、
`SetDup`（重复投递率）、`SetReorder`（乱序）、`Crash/Restart`（节点重启）。

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
- 测试日志打印任期、日志索引与判定依据（`Sim.Events()` 记录每次故障注入、
  丢包、重复投递；`Sim.DumpState()` 打印各节点 role/term/commit/log）。

## 代码结构

```
raft/
  types.go      基础类型与可区分的判定原因枚举
  message.go    统一消息信封
  log.go        带快照前缀的复制日志
  storage.go    持久化抽象与内存实现（支持写入丢失注入）
  node.go       节点状态、驱动接口（Tick/Step/Propose/Compact）
  election.go   选举与任期推进
  replicate.go  日志复制与提交裁决
  snapshot.go   日志压缩与快照安装
  sim.go        确定性仿真器（故障注入 + 事件日志）
```
