# 实验编排规则

## 执行流水线（每个目标）

```
reachability(可选) -> inject -> [ observe -> heal ]* -> cleanup
```

1. **可达性检查**（可选）：任何失败都归类为 `target_unreachable`，该目标后续步骤全部跳过。
2. **故障注入**：带独立超时；区分超时、不可达、普通失败。
3. **稳态观测 + 自愈闭环**：见 [steady-state.md](steady-state.md)。
4. **清理**：只要注入**确认成功**，无论后续判定结果、失败或实验被取消，都必须执行清理。

目标之间按 `concurrency` 信号量并发执行，单个目标失败不影响其他目标的注入与清理；
实验级取消（SIGINT / `interrupt`）通过共享 `context` 传播到所有在执行的动作与探针。

## 可区分的失败原因

失败原因是**穷举枚举**（`model.FailureReason`），实验级与目标级都会记录，绝不混淆：

| 原因 | 触发条件 |
| --- | --- |
| `target_unreachable` | 可达性检查失败 / 动作明确返回不可达 |
| `inject_timeout` | 注入动作超过其 `timeout` |
| `inject_failed` | 注入动作普通失败（参数错误等） |
| `probe_timeout` | 探针超过其 `timeout` |
| `probe_error` | 探针返回普通错误（如下游不可达） |
| `probe_insufficient` | 规则没有任何观测证据 |
| `probe_contradictory` | 同一探针不同轮次观测值相互矛盾 |
| `heal_timeout` / `heal_failed` | 自愈动作超时 / 普通失败 |
| `heal_exhausted` | 自愈调用达到 `max_attempts` 仍未达稳态 |
| `cleanup_timeout` / `cleanup_failed` | 清理超时 / 普通失败（含有限重试后仍失败） |
| `canceled` | 实验被优雅中断 |

实验级失败原因聚合优先级：**清理失败 > 目标不可达 > 注入失败 > 判定异常 > 自愈中止**。
`canceled` 仅在本次运行被取消时采用，续跑成功后不会残留陈旧原因。

## 实验结论与状态（两套正交概念）

- **实验状态** `pending / running / paused / succeeded / failed / aborted`：执行过程是否正常完成。
- **业务结论** `steady / not_steady / inconclusive`：目标是否被验证为稳态。

| 情形 | 状态 | 结论 |
| --- | --- | --- |
| 所有目标稳态、全部清理成功 | `succeeded` | `steady` |
| 无执行异常、全部清理成功，但存在未达稳态目标 | `succeeded` | `not_steady` |
| 存在任何执行异常（不可达/注入/探针/自愈/取消） | `failed`/`paused` | `inconclusive` |
| 清理失败（可能残留半生效故障） | `failed` | `inconclusive` + `cleanup_*` |
| `resume_policy=abort` 恢复并清理完毕 | `aborted` | `inconclusive` |

> 注意：**清理失败**会被明确标记为实验失败且结论不可信，与"业务未达稳态但实验流程成功"区分开。

## 超时与取消

- 每个动作、每个探针都有独立 `timeout`，派生自实验上下文：动作超时得到 `*_timeout`，
  实验取消得到 `canceled`，二者可区分。
- 实验被取消时，**进行中的动作步骤保持 `running`**（结果未知，不能当作已确认失败），
  下游步骤保持 `pending`；取消后清理使用独立的截止时间上下文，保证清理可执行且有界。

## 清理保证（不留半生效状态）

- 注入未确认成功（不可达、注入失败/超时、取消）：不执行清理（没有确认生效的故障），
  cleanup 步骤显式 `skipped`（取消时保持 pending 待恢复决策）。
- 注入已确认成功：清理最多重试 `2` 次（独立的确定性步骤编号），
  - 成功 => 未使用的重试步骤 `skipped`；
  - 全部失败 => 实验 `failed/inconclusive/cleanup_failed`，明确提示可能残留故障。

## 并发正确性

- 所有目标共享一份实验文档；状态提交由**进程内互斥锁 + 文件锁 + 版本号 CAS** 三重保护。
- 提交必须基于最新版本：过期快照提交、重复上报、版本号回退一律返回
  `ErrVersionConflict` / `ErrDuplicateReport` / `ErrStateRollback`，绝不静默接受。
