# 韧性验证编排器（Go）

一套**可完全本地运行**的韧性验证编排能力：用受控的故障注入驱动稳态判定与自愈动作，
让整个验证过程在**并发执行、部分失败、重复触发与中断恢复**下仍然**可复现、可解释**。

- 不依赖任何外部系统：执行器（`SimExecutor`）以脚本化方式确定性地模拟故障注入、
  恢复、自愈与指标探测；状态以单个 JSON 文件持久化。
- 所有结论只取决于**计划配置 + 脚本输入**，无随机、无时间相关的默认放行。

## 快速开始

```bash
# 全部单元测试（包含异常链路与并发检测）
go test ./... -race -count=1 -v

# 本地演示：稳态保持 / 稳态违例后自愈收敛 / 目标不可达
go run ./cmd/demo -state ./demo-state.json

# 保留状态文件再次运行 => 触发“续跑”，已终态目标不会被重复执行
go run ./cmd/demo -state ./demo-state.json
rm -f ./demo-state.json   # 删除状态文件 => 全新运行
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `orchestrator/types.go` | 领域类型：计划、阶段、失败原因、判定结论、清理结果、报告 |
| `orchestrator/plan.go` | 计划静态校验（含判定规则自相矛盾的前置拒绝） |
| `orchestrator/executor.go` | 执行器接口与确定性模拟执行器，区分超时/取消/不可达 |
| `orchestrator/steady.go` | 稳态判定：采证 → 逐条规则比对 → 结论与可解释依据 |
| `orchestrator/healing.go` | 自愈联动：有限轮次循环与结局分类 |
| `orchestrator/heal_sink.go` | 引擎侧回调：自愈动作去重触发、确定性复核 |
| `orchestrator/store.go` | 状态持久化（JSON 原子写）与状态机守卫 |
| `orchestrator/engine.go` | 编排引擎：并发上限、超时、取消、续跑、清理保证 |
| `orchestrator/progress.go` | 运行期进度查询：不可变快照发布与一致视图 |
| `orchestrator/supervisor.go` | 多计划并行：进程内隔离编排与重复发起规则 |
| `cmd/demo/main.go` | 完全本地的演示程序 |

## 一、实验编排规则

### 1.1 执行流水线（单目标）

```
可达性检查 → 注入故障 → 稳态判定 →（违例时）自愈联动 → 清理故障 → 终态
```

对应严格单调的阶段：`injecting → injected → probing → healing → cleaning → 终态`。
终态有三种且语义可区分：

- `completed`：实验正常结束；
- `aborted`：实验因明确原因中止（不可达、超时、取消、判定异常、自愈未收敛等）；
- `cleanup_failed`：实验结束但**故障清理失败**，可能残留半生效状态，需要人工介入。

### 1.2 并发与超时

- `Plan.Concurrency` 是**目标级并发上限**（必须 ≥ 1），用信号量强制；单目标内的步骤串行。
- `Plan.RunTimeout` 是整体截止时间；每个动作另有自己的 `ActionSpec.Timeout`
  （未配置时使用 `DefaultActionTimeout = 5s`）。
- 每个动作都带独立 `context`，执行器必须把结束原因映射为可区分错误：

| 现场 | 失败原因（`FailureReason`） |
| --- | --- |
| 注入前健康检查不通 | `target_unreachable` |
| 注入动作返回普通失败 | `inject_failed` |
| 动作超过自身超时 | `action_timeout` |
| 父级 ctx 被取消 | `experiment_canceled` |
| 稳态探测执行异常 | `probe_error`（判定层归为依据不足） |
| 证据样本不够 | `evidence_insufficient` |
| 规则之间结论互相矛盾 | `evidence_contradictory` |
| 自愈动作失败 | `heal_action_failed` |
| 自愈达轮次上限仍违例 | `heal_not_converged` |
| 中断重启后只清理 | `interrupted_by_restart` |
| 清理重试耗尽 | `recover_failed` |
| 计划配置非法 | `plan_invalid` |

### 1.3 清理保证：不留半生效状态

只要故障被**确认注入成功**（或注入超时/取消导致状态未知），后续无论实验成功、
失败、超时还是被取消，都保证进入有界清理：

1. 清理使用 `context.WithoutCancel(parent)`：**实验被取消/超时不会带走清理**；
2. 清理动作自身仍有超时约束；
3. 最多尝试 `MaxCleanupAttempts = 3` 次，不会无限重试；
4. 清理结果 `not_needed / succeeded / failed` 与实验结论**分别记录**，
   清理失败时目标进入独立终态 `cleanup_failed`。

未成功注入故障时（不可达、注入前取消、注入确定性失败）标记 `not_needed`，不做无意义清理。

## 二、稳态判定依据

判定**完全确定**：结论只由采集到的样本与规则参数计算得出，可复现、可审计。

### 2.1 规则语义

```go
RuleSpec{Probe, Metric, Operator, Threshold, MaxBreaches}
```

`Operator + Threshold` 表达的是「**稳态保持必须满足的条件**」：

- `operator="<=" threshold=0.05` 表示指标必须 `<= 0.05`，样本不满足即记一次 **breach（越限）**；
- 同理支持 `>=`、`>`、`<`、`==`（均按“条件不成立即越限”解释）。

每个探测按 `ProbeSpec.MinSamples` 采 N 个样本（默认 1）；越限样本数
**超过** `MaxBreaches` 时该条规则不通过。

### 2.2 结论优先级（任何情况都不默认通过）

1. **探测异常 / 样本不足 / 指标缺失 → `inconclusive`（依据不足）**
   - 例如 `MinSamples=3` 只采到 2 个有效样本，即使 2 个都健康也不通过；
2. **多条规则既有通过又有违例 → `contradictory`（依据相互矛盾）**；
3. 全部违例 → `violated`；
4. 全部通过 → `steady`。

### 2.3 配置自相矛盾在运行前拒绝

`ValidateRules` / `ValidatePlan` 在实验开始前拒绝：空规则集、未知操作符、
规则引用未声明探测、同一 `probe+metric` 配置多条规则（即配置层面自相矛盾），
返回 `plan_invalid`，绝不带歧义启动实验。

每次判定的依据都会输出：每条规则的越限数、样本数、通过与否、原因，以及
`BasisSummary` 汇总；测试日志通过 `t.Logf` 打印关键输入与判定依据。

## 三、自愈联动

仅当初判结论为 **`violated`**（有确定证据证明稳态被破坏）且配置了 `HealPolicy`
时才触发；`inconclusive/contradictory` 不会贸然自愈。

循环为严格有限的「**触发自愈动作 → 复核判定**」：

- `MaxRounds` 是**硬上限**：到点仍违例即以 `heal_not_converged` 明确中止，绝不无限重试；
- 每轮动作以持久化步骤键 `heal:<round>:action` 去重，**同一动作一轮最多触发一次**；
  重复触发请求会被跳过并记录 `heal_action_skipped_duplicate`；
- 复核结论 `steady` → `converged`（收敛，实验完成）；
- 复核仍 `violated` → 进入下一轮；
- 复核 `inconclusive/contradictory` → 立即中止（依据不足不得默认算收敛）；
- 动作失败 / 被取消 → 分别以 `heal_action_failed`、`canceled` 中止。

所有结局最终都回到统一清理流程，故障一定被清理（或明确报告清理失败）。

## 四、中断与恢复

状态通过 `FileStore` 以「临时文件 + 原子 rename」写入单个 JSON，
每次状态推进与每个审计事件都立即落盘，崩溃边界清晰。

重新 `Run` 同一计划时按**显式规则**识别未完成实验：

1. **已终态目标**：已确认完成的步骤绝不重复执行，直接复用持久化结果；
2. **非终态且故障已确认注入**（注入后、探测中、自愈中、清理中崩溃）：
   崩溃点无法安全复现，**不重放实验**，只执行有界幂等清理，目标结论为
   `aborted / interrupted_by_restart`，清理结果单独给出；
3. **非终态且故障未注入**：从头执行，已 `MarkDone` 的步骤不重跑；
4. 持久化状态属于其他 `plan_id` 时直接拒绝运行。

状态守卫（`store.go`）把重复/乱序上报变成**显式错误而不是静默接受**：

- 阶段回退 → `ErrPhaseRollback`；
- 同一步骤重复完成上报 → `ErrStaleReport`；
- 终态后再次推进 → `ErrAlreadyTerminal`。

## 五、可解释性

- 每个目标都带有序号单调递增的审计事件流（`TargetResult.Events`）：
  可达性、注入尝试、探测样本、判定结论与依据、每轮自愈触发/跳过/复核、
  每次清理尝试、终态与原因；
- 注入 `WithLogger` 可捕获关键输入与判定依据（测试中即打印到单测日志）；
- 最终 `Report` 对每个目标分别给出：阶段、判定结论、失败原因、失败详情、
  清理结果、自愈轮次，互不混淆。

## 六、运行期进度查询

计划在按并发上限执行时，外部可以随时查询进度，不必等整轮跑完：

```go
eng := orchestrator.NewEngine(exec, store)
go eng.Run(ctx, plan)

p := eng.Progress()               // 整个计划的一致快照
tp, err := eng.TargetProgress("t01") // 单个目标
```

- 视图内容：每个目标当前的**阶段、稳态结论、自愈轮次、清理状态、
  故障是否生效、最近事件**（尾部最多 `ProgressEventLimit` 条，序号连续），
  以及计划级状态（`not_started / running / finished`）。
- **一致性保证**：快照在每次状态提交（持久化成功）后以深拷贝整体发布，
  查询读到的是同一时刻的完整视图——不会出现阶段与结论对不上、
  事件缺一段、或读到写了一半的内容；同一协程连续查询，阶段单调不回退。
- **零干扰**：查询只做原子加载，不抢执行路径的状态锁，
  任意频率的查询都不会拖慢、阻塞或改变实验结果（有并发查询对拍测试兜底）。

## 七、多计划并行运行

`Supervisor` 支持在同一进程内同时跑多个计划，各跑各的、互不干扰：

```go
sup := orchestrator.NewSupervisor(stateRootDir, func(plan *orchestrator.Plan) orchestrator.Executor {
    return newExecutorFor(plan) // 每个计划独立的执行器
})
_ = sup.Start(ctx, planA) // 异步启动；stateRootDir/<plan-id>/state.json 各自落盘
_ = sup.Start(ctx, planB)
p, _ := sup.Progress("plan-a") // 运行中随时查某个计划
repA, _ := sup.Wait("plan-a")  // 阻塞等某个计划结束
```

隔离与兼容范围：

- 每个计划有独立的 `Engine`、状态机、事件流与落盘位置
  （`dir/<plan-id>/state.json`）；并发上限只约束本计划目标，
  一个计划超时/取消/失败不影响其他计划；
- **同一计划重复 `Start`**：进行中 → 明确拒绝 `ErrPlanAlreadyRunning`
  （绝不出现两份运行同时写同一份状态）；上一轮已结束 → 允许再次发起，
  按既有续跑规则从持久化状态继续（终态目标不重放）；
- 计划 ID 必须能安全映射为独立目录（拒绝 `..`、路径分隔符等），
  防止两个计划因路径解析共享同一份状态文件；
- 兼容范围限定为**同一进程内**由 `Supervisor` 管理的并行；
  两个进程同时写同一个状态文件不在保障范围内。

## 八、本地验证指引

```bash
# 1) 确定性全量测试（52 个用例，覆盖下列异常链路）
go test ./... -race -count=1 -v

# 2) 演示程序
go run ./cmd/demo -state ./demo-state.json
```

测试覆盖的关键异常链路（均断言“清理结果 / 实验结论 / 失败原因”三者可区分）：

- 目标不可达、注入确定性失败、注入超时后仍清理；
- 探测异常 → 依据不足、规则矛盾、样本不足、越限容忍度；
- 自愈收敛、达上限未收敛、自愈动作失败、取消、误配置拒绝；
- 清理始终失败 → `cleanup_failed`（含 3 次尝试审计）；
- 整体超时 / 父 ctx 取消后清理仍执行，且原因可区分；
- 并发上限实测（barrier 执行器）、内存/文件两种存储的中断恢复、
  终态不重放、自愈动作恢复时不重复触发、外来计划状态拒绝；
- 运行期进度查询：运行前/运行中/高频并发查询下的快照一致性
  （事件序号连续、阶段与结论对得上、阶段不回退、查询不改变结果）；
- 多计划并行：三计划并行结论隔离、进行中重复发起明确拒绝、
  结束后按续跑规则继续、非法计划 ID 拒绝、单计划超时不外溢；
- CI 规模场景：3 计划并行 × 16 目标满并发 + 全程持续查询，
  结果与同计划顺序跑逐目标一致（真实文件落盘，race 检测通过）。

也可以通过修改 `cmd/demo/main.go` 中 `Script` 的字段自行构造场景：
`Unreachable`、`DoFail`、`DoDelay`（配合短超时复现超时）、`ProbeFail`、
`ProbeDelay`、`Values`（确定性指标）。
