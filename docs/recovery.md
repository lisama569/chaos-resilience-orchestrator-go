# 中断与恢复规则

## 两种停止方式

| 方式 | 文档状态 | steps 状态 | 恢复行为 |
| --- | --- | --- | --- |
| **优雅中断**（SIGINT / `interrupt`） | 落盘为 `paused` | 在执行的动作保持 `running`；下游保持 `pending` | 按 `resume_policy` 决策 |
| **进程崩溃/被杀**（kill -9、断电） | 文档停留在 `running`（写操作前状态） | 同上 | 按 `resume_policy` 决策，但必须先重置 `running` 进程内的步骤 |

关键约束：`running` 只表示"动作执行过、但结果没有落盘"。因此恢复时**绝不能把 running 步骤
当作已完成**——要么重置为 `pending` 重新执行（只读的观测步骤可幂等重采或复用已落盘证据），
要么在 abort 路径中不依赖它。

## 决策表（`recovery.Decide`）

| 当前状态 | resume_policy | 决策 |
| --- | --- | --- |
| `succeeded / failed / aborted`（终态） | 任意 | **none**：不写盘、不重复执行任何已完成步骤 |
| `paused`（优雅中断） | `resume` | **continue**：保留所有终态步骤，从第一个非终态步骤继续 |
| `paused` | `abort` | **abort**：不再继续实验，但清理已确认注入的故障后标记 `aborted` |
| `running`（崩溃遗留） | `continue(resume)` | **continue** + 重置所有 `running` 步骤为 `pending` |
| `running`（崩溃遗留） | `abort` | **abort**：重置 running，清理已确认注入的故障后标记 `aborted` |
| `pending`（创建后未启动） | `resume`/`abort` | 启动 / 直接终止 |

决策结果 `Plan{Decision, Reason, ResetSteps}` 会打印日志，保证处置依据可解释。

## 续跑时如何"不重复执行已确认完成的步骤"

- **步骤去重键**：`(target, phase, attempt, round)`，步骤 ID 由该四元组确定性生成
  （`MakeStepID`），重启后 ID 稳定可识别。
- 启动步骤执行前先查状态：
  - `succeeded/failed/skipped`（终态）=> **跳过执行**（`startStep` 返回 terminal 标志）；
  - `running` => 这只可能出现在崩溃遗留，恢复入口已统一重置为 pending；
  - `pending` => 正常执行。
- **重复上报**：对已终态步骤再次提交终态报告，返回 `ErrDuplicateReport` 并拒绝写盘。
- **状态回退**：终态步骤/实验的任何回退迁移返回 `ErrStateRollback`；越级迁移
  返回 `ErrInvalidTransition`。
- 观测证据按 `(probe, attempt, round)` 去重合并；已 succeeded 的观测轮次复用已落盘证据，
  不重新采集。

## abort 恢复路径

- 注入步骤 `succeeded`（故障确认生效）=> 用独立上下文执行清理（同样有界、有记录）；
- 注入步骤非 succeeded（结果未知或未生效）=> 不执行清理，只把下游标 `skipped`；
- 清理失败 => 实验 `failed/inconclusive/cleanup_failed`，明确提示可能残留故障；
- 全部完成 => 实验 `aborted/inconclusive/canceled`。

## 持久化与防错

- 每个状态推进都是独立事务：**读最新版本 → 校验状态机 → 单调递增版本号 → 原子重命名落盘**。
- 文件锁串行化同目录并发进程；版本 CAS 拒绝过期提交（含两个引擎实例并发跑同一实验）。
- 证据与自愈调用次数随采集/执行**立即落盘**，使崩溃窗口最小化；
  即使恰好在动作执行中崩溃，也只会重放一个结果未知的动作，不会重放已确认完成的步骤。
