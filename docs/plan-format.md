# 计划文件格式（JSON）

`orchestrator run <plan.json>` 读取如下结构。所有 `timeout`/间隔字段均为 Go duration
数值（纳秒），如 `500000000` 表示 500ms（`1s = 1000000000`）。

```json
{
  "id": "my-experiment",
  "concurrency": 2,
  "observe_every": 100000000,
  "observe_rounds": 2,
  "resume_policy": "resume",
  "targets": [
    {
      "id": "svc-a",
      "reach":   { "kind": "unreachable", "timeout": 500000000 },
      "inject":  { "kind": "set-flag", "timeout": 500000000,
                   "params": { "key": "svc-a/fault", "value": "true" } },
      "cleanup": { "kind": "set-flag", "timeout": 500000000,
                   "params": { "key": "svc-a/fault", "value": "false" } },
      "heal": {
        "max_attempts": 2,
        "action": { "kind": "set-flag", "timeout": 500000000,
                    "params": { "key": "svc-a/svc", "value": "true" } }
      },
      "probes": [
        { "name": "svc-healthy", "kind": "flag", "key": "svc-a/svc",
          "timeout": 500000000, "want": true, "min_hits": 2 }
      ]
    }
  ]
}
```

## 字段说明

| 字段 | 含义 |
| --- | --- |
| `id` | 实验唯一 ID；同名实验不能重复创建 |
| `concurrency` | 目标级并发上限，<=0 表示不限 |
| `observe_every` | 多轮观测之间的间隔 |
| `observe_rounds` | 每次稳态观测的采集轮数（每轮采集全部探针） |
| `resume_policy` | `resume`：中断后续跑；`abort`：中断后仅清理并终止 |
| `targets[].reach` | 可选可达性动作；任何失败 => `target_unreachable` |
| `targets[].inject` / `cleanup` | 注入 / 清理动作（kind + timeout + params） |
| `targets[].heal.max_attempts` | 自愈动作调用总次数上限；0/缺省表示不自愈 |
| `targets[].heal.action` | 自愈动作 |
| `targets[].probes[]` | 稳态规则：探针 + `want` + `min_hits` |

## 内置动作（`kind`）

| kind | 参数 | 行为 |
| --- | --- | --- |
| `set-flag` | `key`, `value` | 设置本地布尔开关（注入/清理/自愈通用） |
| `inc-metric` | `key`, `delta` | 增减本地整数计数器 |
| `flaky` | `key`, `fail_until`, `flag`, `value` | 前 N 次调用失败，之后成功并设置开关 |
| `sleep` | `duration`, `flag?` | 睡眠（用于超时/取消演练），可在成功后置开关 |
| `unreachable` | — | 恒返回不可达错误 |

## 内置探针（`probes[].kind`）

| kind | 参数 | 观测值 |
| --- | --- | --- |
| `flag` | `key` | 开关布尔值 |
| `metric-threshold` | `key`, `params.threshold` | 计数器是否达到阈值 |
| `error` | `key` | 恒报错（演练判定异常链路） |

更多示例：
- [examples/steady-plan.json](examples/steady-plan.json)：稳态 + 自愈收敛 + 不可达
- [examples/interrupt-plan.json](examples/interrupt-plan.json)：长注入动作，用于演练 Ctrl+C 恢复
