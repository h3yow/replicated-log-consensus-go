# replicated-log-consensus-go

本地可运行、可确定性重放的复制日志与领导者选举组件（Raft 风格）。
多节点在消息丢失、乱序、重复投递和网络分区下，仍能收敛到一致且可解释的结论。

## 结构

```
raft/
  types.go        消息、日志条目、拒绝/让位原因等可观测类型
  node.go         节点核心：状态恢复、时钟、消息分发、持久化
  election.go     领导者选举与任期推进
  replication.go  日志复制与提交裁决
  snapshot.go     日志压缩与快照安装（落后节点追赶）
  storage.go      持久化接口与内存实现（模拟磁盘）
  harness.go      确定性仿真环境：故障注入与消息调度
```

核心模型：节点是**单线程状态机**，`Tick()` 推进逻辑时钟、`Step(msg)` 处理消息、
`TakeMessages()` 取出出站消息。所有随机性集中在 `Harness` 中，由固定种子驱动，
因此相同种子的运行逐比特一致、可完整重放。

## 协议规则

### 选举与任期
- 每任期每节点至多投出一票（`votedFor` 持久化，重启不翻供）。
- 候选人日志必须不落后于投票人（先比 `lastTerm`，再比 `lastIndex`）。
- 选举超时为**确定性抖动**（由节点 ID 导出），不引入随机源，便于重放。
- 投票拒绝原因可区分：`TermStale`（候选人任期落后）、`AlreadyVoted`
  （本任期已投出）、`LogNotUpToDate`（候选人日志落后）。
- 看到更高任期的消息立即让位（`StepDownHigherTermSeen`）；分区中的旧领导者
  在恢复后收到新任期心跳即退回 Follower。

### 复制与提交
- 追随者仅在 `prevLog` 精确匹配时追加；冲突处截断，但**绝不截断已提交前缀**。
- 提交裁决（Raft §5.4.2）：只有被多数派复制、**且任期等于领导者当前任期**的
  条目才可提交；旧任期条目只能随本任期条目间接提交。
- `commitIndex` 与 `lastApplied` 均持久化：重启后已提交内容不丢失，
  `TakeCommitted()` 不会重复交付已应用条目；重复投递幂等（同索引同任期视为已有）。

### 压缩与追赶
- 只能压缩已应用（`lastApplied`）的前缀；越界请求被钳制。
- 落后太多（`nextIndex ≤ snapshotIndex`）的追随者改用 `InstallSnapshot` 追赶。
- 快照安装**不得覆盖更长的已提交后缀**：快照点与本地日志衔接时保留后缀；
  快照点落后于本地已提交前缀时，已提交后缀优先。
- 追赶中的节点对外仍暴露一致可读的已提交前缀（`CommittedView`）。

## 可调参数

| 参数 | 含义 | 默认值 |
|---|---|---|
| `ElectionTimeout` | 选举超时下界（tick） | 10 |
| `HeartbeatTimeout` | 心跳间隔（tick） | 2 |
| `MaxInflight` | 单次 AppendEntries 最大条目数 | 16 |
| `SnapshotThreshold` | 允许压缩的日志长度阈值 | 64 |

故障注入（`Faults`）：`DropProb`（丢包概率）、`DuplicateProb`（重复投递概率）、
`ReorderWindow`（乱序窗口）；另有 `Partition`/`Heal`（网络分区）与
`Restart`（节点重启，保留持久存储）。

## 验证方法

```sh
go test ./... -count=1        # 全量测试
go test ./raft/ -v -run TestChaosSafetyAndConvergence  # 查看判定依据日志
```

测试以故障注入为主，全部在固定随机种子下运行：

- `election_test.go`：同任期唯一领导者、三类投票拒绝原因、分区恢复后旧领导者
  让位、全丢包下选举超时路径与拒绝路径可区分。
- `replication_test.go`：多数派提交、少数派不可提交、旧任期条目不可直接提交、
  重启不丢失不重复应用、全量重复投递幂等。
- `snapshot_test.go`：快照追赶、快照不覆盖更长已提交后缀、压缩点钳制、
  追赶中已提交前缀始终与领导者相容。
- `chaos_test.go`：丢包+重复+乱序+分区+重启组合下的状态机安全性与最终一致性、
  相同种子两次运行逐比特一致（确定性重放）、故障下反复重启后收敛。

单测日志（`go test -v`）会打印任期、日志索引与判定依据，例如：

```
场景3: 本地(lastTerm=2,lastIndex=1) 候选人(lastTerm=0,lastIndex=0) -> 拒绝原因=LogNotUpToDate
落后节点=N2 任期=4 快照点=8 commitIndex=10 已提交条目数=2
```
