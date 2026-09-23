# 稳态判定依据与自愈规则

## 稳态判定的确定性依据

判定输入**只有**计划中显式配置的规则与实际采集到的布尔观测证据：

- 每个探针规则：`want`（期望值）+ `min_hits`（观测通过所需的最少采集次数）。
- 每条证据：`(probe, attempt, round, value, at, latency)`，证据随采集立即持久化。
- 规则按探针名**字典序**评估，依据文本固定格式输出，保证多次运行结果与日志可复现。

单规则的判定是确定的：

| 情形 | 结论 | 原因 |
| --- | --- | --- |
| 无任何证据 | `inconclusive` | `probe_insufficient`（**绝不默认通过**） |
| 同一探针在不同轮次出现不同值 | `inconclusive` | `probe_contradictory`（**绝不默认通过**） |
| 值 == want 且 证据数 ≥ min_hits | 规则满足 | — |
| 否则 | 规则不满足 | — |

全部规则满足 => `steady`；任一规则不满足（证据充分但值不对/次数不够）=> `not_steady`；
出现依据不足或相互矛盾 => `inconclusive`，原因可区分。

> 探针采集错误（`probe_error`/`probe_timeout`）优先于规则判定：采集层出错意味着判定依据
> 本身不可信，即使其余探针正常，也直接给出 `inconclusive`，不会拿残缺证据"凑出"稳态。

## 自愈联动

```
observe(attempt=0)
  └─ not_steady ─> heal call #1（唯一编号，成功执行一次）-> observe(attempt=1)
                       └─ not_steady ─> heal call #2 -> observe ...
                                            └─ 达到 max_attempts => heal_exhausted，明确中止
```

规则：

1. **有限次数**：自愈动作调用总次数不超过 `max_attempts`；达到上限仍未稳态则明确中止为
   `heal_exhausted`，不无限重试。
2. **不重复触发同一动作**：每次自愈调用使用全局唯一的 `(target, heal, attempt)` 步骤；
   已终态（succeeded/failed/skipped）的步骤不会被再次执行。收敛后未使用的自愈与
   重新观测步骤全部标记 `skipped`。
3. **普通失败可在额度内继续**：`heal_failed` 记录到对应步骤后继续下一次调用；
   **超时与目标不可达立即中止**（动作是否安全生效未知，不能假设安全后继续）。
4. **inconclusive 不触发自愈**：依据不足、证据矛盾、探针异常、取消时不得盲目执行自愈。
5. **收敛只看重新观测的结果**：第 n 次自愈后的判定仅使用 attempt=n 的证据，
   但同一 attempt 内多轮观测（`observe_rounds`）必须全部满足；不同 attempt 的证据不混用。

## 可解释性

每次判定都会输出结构化依据（单测日志同样打印），例如：

```
rule=svc want=true min_hits=2 observations=[true true](2) -> rule_satisfied=true
rule=svc observations=[true false] -> CONTRADICTION => inconclusive(contradictory)
rule=svc want=true min_hits=2 -> NO EVIDENCE => inconclusive(insufficient)
probe collection error => inconclusive: probe_timeout
heal attempts exhausted without steady state
```

每轮探针采集也打印关键输入：`target / probe / round / key / value / err / latency`。

## 内置探针

| kind | 观测值 |
| --- | --- |
| `flag` | 本地环境中 `key` 开关的布尔值 |
| `metric-threshold` | `metrics[key] >= params.threshold`（参数 `threshold`） |
| `error` | 恒返回错误，用于演练稳态判定异常链路 |
