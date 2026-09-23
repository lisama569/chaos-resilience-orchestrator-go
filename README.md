# Chaos Resilience Orchestrator (Go)

一套**完全本地运行**的韧性验证编排能力：用受控的故障注入驱动**稳态判定**与**自愈动作**，
并保证整个验证过程在**并发执行、部分失败与重复触发**下仍然可复现、可解释。

无外部依赖（仅 Go 标准库），所有"目标、故障、探针、自愈、清理"都是进程内的确定性模拟，
状态持久化到本地 JSON 文件，因此可以随时中断进程并重新启动后续跑或终止。

## 快速开始

```bash
# 运行内置的三目标演示（稳态 / 自愈后收敛 / 目标不可达）
go run ./cmd/orchestrator demo

# 用 JSON 计划文件运行
go run ./cmd/orchestrator run plan.json

# 中断正在运行的实验（等价于发送 SIGINT / Ctrl+C）
go run ./cmd/orchestrator interrupt <experiment-id>

# 重启后：按计划中的 resume_policy 决定续跑或终止
go run ./cmd/orchestrator resume <experiment-id>

# 列出所有实验及其状态/结论/失败原因
go run ./cmd/orchestrator list
```

状态目录由环境变量 `CHAOS_STATE_DIR` 控制（默认 `.chaos-state`）。

端到端手动验证中断恢复：

```bash
go build -o /tmp/orch ./cmd/orchestrator
CHAOS_STATE_DIR=/tmp/state /tmp/orch run docs/examples/interrupt-plan.json &
sleep 1 && kill -INT $!           # 实验落盘为 paused
CHAOS_STATE_DIR=/tmp/state /tmp/orch resume interrupt-demo
```

## 测试

```bash
go test ./...          # 全量单测（日志打印关键输入与判定依据）
go test -race ./...    # 含竞态检测
go vet ./...
```

测试覆盖的异常链路：目标不可达、注入超时/失败、探针错误与超时、稳态依据不足/相互矛盾、
自愈失败/超时/耗尽、清理失败与有界重试、优雅中断续跑、进程崩溃恢复（running 遗留）、
终态拒绝重复执行、重复上报与版本冲突拒绝。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 实验/步骤/结论/失败原因等领域类型与状态机校验 |
| `internal/action` | 动作接口、失败分类、本地确定性受控环境与内置动作 |
| `internal/probe` | 稳态证据采集探针（flag / metric-threshold / error） |
| `internal/policy` | 确定性稳态判定（依据不足/矛盾不默认通过） |
| `internal/store` | JSON 文件持久化，版本号 CAS + 原子写 + 文件锁 |
| `internal/engine` | 并发编排、超时/取消、清理保证、稳态-自愈联动 |
| `internal/recovery` | 中断识别与续跑/终止的明确规则 |
| `cmd/orchestrator` | 本地 CLI（demo/run/resume/interrupt/list） |

## 规则摘要

- **实验编排规则、失败原因枚举、并发/超时/清理保证**：见 [docs/orchestration.md](docs/orchestration.md)
- **稳态判定依据、自愈联动与收敛/中止规则**：见 [docs/steady-state.md](docs/steady-state.md)
- **中断恢复规则、状态机与重复上报防护**：见 [docs/recovery.md](docs/recovery.md)
- **计划文件格式（JSON 字段）**：见 [docs/plan-format.md](docs/plan-format.md)
