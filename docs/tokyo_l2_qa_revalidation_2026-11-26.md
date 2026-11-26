# Tokyo L2 生产质检修复、历史重验与无人值守验收报告（T3）

- **任务线 / 分支**：T3（质检入口 `main` / 采集与质检实现 `feat/tokyo-l2-reliability-fixes` / 状态分支 `tokyo-l2-qa-state`；跨线总控见 [Issue #42](https://github.com/Huhabubu/mm/issues/42)，追踪见 [Issue #39](https://github.com/Huhabubu/mm/issues/39)）
- **核查基线时间**：`2026-11-26T08:25:00Z`（CST `16:25`）
- **生产代码与状态基线 SHA**：
  - `main`：`165542d6e7d154bdcb94d43b435e7735c95fa688`（含 PR #30、#32、#34、#35、#36、#37、#38、#41）
  - `feat/tokyo-l2-reliability-fixes`：`2e97490cf62fa80029a19c5eb0e38fc0e14f9d56`（含 PR #31、#33 及 `AGENTS.md`）
  - `tokyo-l2-qa-state`：`6b23001c89e716422fff7de173ef383a11fa2959`（`2026-11-26T08:20:24Z`）
- **约束遵守**：未克隆仓库至本地；未中断或重启 EC2 采集器；未修改原始历史行情与质检状态历史；未降低质检门禁或修改 T1/T2/T4 代码及 GLFT 参数。

---

## 1. 已实现功能、合并 PR 与测试/性能证据

| PR | 目标分支 | 合并 SHA | 核心改动 | 测试 / 实测证据 |
|---|---|---|---|---|
| #30 | `main` | `f622441b3496679a93e3b957a42c74ce0858c1cb` | 主备切换重复成交最早保留去重；统一规范币种键（修复 `BTCUSDT`/`BTC` 不一致）；审计 T2 `exp/tokyo-l2-glft` 转换器（不受影响） | 单元测试通过并合并 |
| #31 | `feat/tokyo-l2-reliability-fixes` | `02058d432e21252c248cb2bfd61e36c34533b996` | 分离任务执行态与数据质量态；`anchor_unverified`/`replay_init_unverified` 终结任务但不标通过；`max_in_flight=1` 限流；避免无条件重派 | 已部署至 EC2 `/opt/mm` |
| #32 | `main` | `1f4b3c4f98d49ae5882e9f9c1088510bad99818e` | `PROTOCOL_QA_VERSION=2`；24 市场完整盘口+序列号 `.checkpoint.json` 跨 15m 持久化继承；修复 `seen_snapshot` 与 `probe_orders` 时钟偏移 | 单元测试通过 |
| #33 | `feat/tokyo-l2-reliability-fixes` | `31590e1ab0f58436b0967a969384994aabfc6b82` | `qa_dispatch.py` 改走 `raw.githubusercontent.com` 拉取 `tokyo-l2-qa-state`，消除 `/contents/` REST 限流耗尽 | 部署后 `auth_errors=0`、`rejected_errors=0` |
| #34 | `main` | `3fe03e94e3869e0b482e889bb14b36910257ed07` | 中断屏障回溯剪枝（`MAX_ANCHOR_LOOKBACK_WINDOWS=24`）、下载缓存、`--catchup-first` | 单元测试通过 |
| #35 | `main` | `1cebfcdc822974a4b26702018454b90eaae43c0d` | P0 软时间预算 `--max-runtime-seconds`（补验/定时 `1500s`，实时 `900s`）、逐窗口原子落盘报告+检查点、`rclone` 下载/回溯超时 | CI [#45585826045](https://github.com/Huhabubu/mm/actions/runs/45585826045) 30/30 通过 |
| #36 | `main` | `dd0577c12f4e60f71d87cbcd0f35d2b3d30695a7` | 只读性能剖析器 `scripts/profile_tokyo_l2_qa.py` | 冷启动剖析 [#45586537643](https://github.com/Huhabubu/mm/actions/runs/45586537643)：`225.7s/208.0s`（回溯占 `122–128s`，峰值 RSS `1086 MB`） |
| #37 | `main` | `32d5b719042faf3d0663977ce3b8413b442192cb` | 超时导致证据不足严格保持 `retryable`（禁止误判 `data_failed`/`anchor_unverified`/`passed`，禁止落盘）；`Persist` 改为 `always() && !cancelled() ...` 确保前置已完成窗口必推送 | CI [#45598011306](https://github.com/Huhabubu/mm/actions/runs/45598011306) 31/31 通过 |
| #38 | `main` | `0bd984438abc8f024eb6bc4b4cba2a984122fa9a` | 复用经哈希校验的逐文件 `.qa.json`、内存缓存 `check_partition` 与跨小时 `meta/specs` | CI [#45599541599](https://github.com/Huhabubu/mm/actions/runs/45599541599) 32/32；基准 [#45599847190](https://github.com/Huhabubu/mm/actions/runs/45599847190)：冷启动降至 `160.6s`（RSS `262 MB`），检查点继承相邻窗口降至 **`95.8s`**（RSS `298 MB`） |
| #41 | `main` | `43c84fb8ae7e298bdb7c14d55749f21dca3eb304` | `--lookback-timeout-seconds` 上限由 `300s` 提至 `720s`，摘要记录 `retryable_details` | CI [#45609827146](https://github.com/Huhabubu/mm/actions/runs/45609827146) 通过；线上首跑 [#45619910481](https://github.com/Huhabubu/mm/actions/runs/45619910481) 即刻恢复绿灯，此后连续 **96/96** 非取消运行 0 失败 |

---

## 2. 自动化闭环与无人值守运行核查

逐项对照生产代码（`.github/workflows/tokyo-l2-incremental-qa.yml`、`scripts/tokyo_l2_recurring_qa.py`、`collector/python/qa_dispatch.py`、`collector/python/hourly_incremental_qa.py`）与实际运行结果：

| 核查项 | 结论 | 代码机制与实测证据 |
|---|---|---|
| 新采集数据自动进入质检 | **通过** | EC2 `qa_dispatch.py` 从 `archive_ledger.sqlite` + `archive_manifest_index.json` 自动发现封口窗口并以 `(run_id, target_window_utc, archive_revision)` 触发 `workflow_dispatch`（`max_in_flight=1`）。近 24h 新增归档 **95** 个 15m 窗口全部进入 `window.qa.json`（`remaining_backlog=0`、`historical_overdue_windows=0`、`archived_pending_qa_windows=0`）。 |
| 被并发队列替换的任务自动恢复 | **通过（双层兜底）** | ① EC2 `reconcile_accepted_dispatches()` 对超过 `accepted_stale_timeout_s=1800s`（30m）仍无终态报告的 `ACCEPTED` 记录标 `accepted_result_missing_or_replaced` 并指数退避（`30s–900s`）重入 `PENDING`；② GitHub `schedule`（`cron: "17 */3 * * *"`，`QA_LIMIT=48`、`eff_max_catchup=4`、`max_runtime_seconds=1500`）+ 每次实时 `archive_dispatch` 自带 `eff_max_catchup=1`（`[current, backlog[0]]`）。近 24h 共 9 次并发队列 `cancelled`，均已自动恢复或进入自动补验队列。 |
| 旧版 v1 / 缺报告 / `pending` / `retryable` 自动发现处理 | **通过** | `tokyo_l2_recurring_qa.py`：L419 过滤 `protocol_qa_version != 2`（全库旧版 v1 已清零）；缺失报告与 `status=="pending"` 均不在 `recorded`/`deferred`，自动进入 `backlog`；L598–611 对 `retryable` 不落盘终态且将磁盘报告撤销为 `pending` 以便下次重试；`anchor_unverified` 在上游检查点未变时留在 `deferred` 防止饿死后续窗口，一旦上游检查点更新，`_reports_with_current_checkpoints`（L157–163）自动作废其 `input_checkpoint_digest` 并撤销为 `pending` 触发重算。 |
| 历史积压无需人工/AI 派发即可持续减少 | **通过（线上实证）** | 除每 3 小时定时任务每次最多补验 4 窗口（日上限 32 窗口）外，**每次 15m 实时派发也顺带补验 1 个最老积压窗口**（日上限 96 窗口）。实测 `2026-11-26T07:56Z → 08:20Z` 期间无任何人工批次完成，仅靠两次纯 EC2 实时任务（`#45775714597`、`#45779590645`）便使终态 v2 报告从 `344 → 347`（自动推进 `2026-11-25T05:30Z`、`05:45Z` 并将 `06:00Z` 撤销为 `pending`）。 |
| 中断/失败后从检查点继续运行 | **通过** | 每完成单窗口即调用 `persist_consistent_state(start)` 原子写入协议报告和 `.checkpoint.json`；工作流 `Persist` 步骤条件为 `always() && !cancelled() && (inspect==success \|\| protocols==success \|\| protocols==failure)`；到达软时间预算时干净退出并推送已完成窗口，后续运行自动从检查点链断点续跑。 |
| 已正确验收的历史窗口不重复检查 | **通过** | `tokyo_l2_recurring_qa.py:440–444` 使 v2 `passed`/`data_failed` 窗口在非规则变更的 `checker` 提交下保持 `recorded`；EC2 `qa_dispatch.py:375–379` 同步支持 `v2_terminal_survival`，不触发历史重派风暴。 |

### 代码审计发现的两个非阻塞边界风险（仅记录根因与最小修复建议，本轮未改动生产代码）

1. **边界风险 A — 实时任务顺带补验命中超时时连带标红作业**（`scripts/tokyo_l2_recurring_qa.py:650–658`）：
   - **根因**：`clean_exec` 判断 `not any_checked_retryable`。当实时 `archive_dispatch`（软预算 `900s`）中 `idx=0` 当前窗口已 `passed` 并落盘，但顺带补验的 `idx=1`（`backlog[0]`）因剩余预算不足触发回溯超时返回 `retryable` 时，整次运行会返回 `job_execution_status="failed"`（虽仍会推送当前窗口，但 Actions 与 Healthchecks 报红）。PR #41 合并后 93 次实时运行均未触发此情况，但存在理论边界。
   - **最小修复建议（待授权后实施）**：仅在 `current` 自身 `retryable` 或本次无任何已完成窗口时将 `clean_exec` 置为 `False`；或在剩余预算 `< 300s` 时跳过顺带补验窗口。
2. **边界风险 B — `feat/tokyo-l2-reliability-fixes` 上纯文档提交触发基础检查 SHA 失配**（`.github/workflows/tokyo-l2-incremental-qa.yml:157`、`collector/python/hourly_incremental_qa.py:1326`、`scripts/tokyo_l2_recurring_qa.py:261,373`）：
   - **根因**：工作流将 `TOKYO_L2_QA_CHECKER_COMMIT` 设为 `git -C source rev-parse HEAD`。今日仅新增 `AGENTS.md` 的提交 `2e97490cf62f` 变动了分支 HEAD，导致后续首次运行（`#45750062631`）在 Step 10 花费 `15m24s` 重验历史 27 个 `failed` 清单并重写 385 份 `window.qa.json`，同时使 PR #38 的逐文件 `.qa.json` 复用缓存短暂失配。
   - **最小修复建议（待授权后实施）**：将 `CHECKER_COMMIT` 改为 `git -C source log -n 1 --format=%H -- collector/python`，避免非代码目录的文档提交令质检缓存失效。

---

## 3. 近 24 小时处理能力与时延统计（`2026-11-25T08:25Z` – `2026-11-26T08:25Z`）

### 3.1 吞吐量与任务结果分布

- **近 24h 新增归档窗口数**：**95** 个 15m 窗口（理论满产 `96` 窗口/24h，最新 `08:00Z` 窗口处于归档封口期）。
- **近 24h 完成终态协议质检窗口数**：**334** 个（`281 passed` / `35 data_failed` / `18 anchor_unverified`），实际日吞吐为数据产生速率的 **3.52×**（含 12 个 `1500s` 人工加速批次 + 88 次实时派发 + 2 次 3h 定时任务）。
- **近 24h Workflow 运行总计（120 次）**：`105 success`、`4 failure`、`9 cancelled`、`0 timed_out`、`1 in_progress`、`1 pending`。
  - **超时（`timed_out`）**：**0** 次（PR #35 软预算完全消除 45m 硬超时白跑）。
  - **失败（`failure`）**：**4** 次（`#45600355500`、`#45605234243`、`#45610000023`、`#45614899126`），全部发生于 PR #41 合并前（`2026-11-25T11:00Z–12:33Z`，`300s` 回溯上限不足）。**PR #41 合并后（`>= 12:40Z`）非取消运行 96/96（100.0%）全部 `success`，0 失败、0 超时。**
  - **并发队列替换取消（`cancelled`）**：**9** 次（8 次 `workflow_dispatch` + 1 次 `schedule`），主因为多轮 28 分钟人工加速批次与实时派发竞争单一 `pending` 槽位；所有被替换实时窗口均已自动恢复或进入自动补验队列。

### 3.2 真实计算耗时 vs 任务排队耗时（PR #41 稳态：`2026-11-25T12:40Z` – `2026-11-26T08:25Z`）

| 作业类型 | 样本数 | 真实计算耗时（`started→completed`） | 排队等待耗时（`created→started`） | Step 10 基础检查 | Step 11 协议+回放 |
|---|---:|---|---|---|---|
| **稳态无竞争实时派发**（`queue < 120s`，反映无人值守常态） | 63 | **P50 = 403.0s (6.7m)**<br>**P95 = 772.2s (12.9m)**<br>区间 `265–937s` | **P50 = 4.0s**<br>**P95 = 5.9s**<br>区间 `3–35s` | P50 `73.0s` / P95 `110.8s` | P50 `242.0s` / P95 `678.9s` |
| **PR #41 后全部实时派发**（含与人工补验批次竞争排队） | 83 | **P50 = 487.0s (8.1m)**<br>**P95 = 822.3s (13.7m)**<br>区间 `265–937s` | **P50 = 4.0s**<br>**P95 = 1660.7s (27.7m)**<br>最高 `1741s` | P50 `81.0s` / P95 `170.6s` | P50 `278.0s` / P95 `678.9s` |
| **每 3 小时定时补验（`schedule`）** | 2 | P50 `1463.0s` / P95 `1963.4s` | P50 `481.0s` / P95 `665.5s` | P50 `467.5s` / P95 `878.3s` | P50 `912.0s` / P95 `1001.1s` |
| **人工加速批次（`1500s` 软预算）** | 12 | P50 `1711.5s` / P95 `1788.9s` | P50 `255.5s` / P95 `586.1s` | P50 `73.0s` / P95 `134.1s` | P50 `1574.0s` / P95 `1601.7s` |

- **归档结束至质检结果落盘端到端时延**（`window_planned_end → protocol.completed_at_utc`）：
  - 稳态（`>= 2026-11-25T14:45Z`，`n=50`）：**P50 = 1050.5s（17.5m）**、**P95 = 2978.4s（49.6m）**、最短 `518.0s（8.6m）`（含约 3–5 分钟 EC2 封口上传与索引同步 + 约 6.7 分钟计算；P95 尾部主要受人工加速批次排队拖累）。
  - 全量近 24h 已终态新窗口（`n=61/95`）：P50 `1137.0s（18.9m）`、P95 `10838.0s（180.6m）`（受 PR #41 合并前失败重试影响）。
- **长期处理能力结论**：**系统长期处理能力显著高于数据产生速度。**
  - 新窗口到达周期为 **900s（15m）**；无人值守无竞争状态下，实时任务**同时包含当前窗口 + 1 个历史补验窗口 + Runner 环境初始化**的总计算耗时仅 **P50 = 403s（占 900s 周期的 44.8%）**、**P95 = 772s（85.8%）**；停止人工长批次抢占队列后，排队等待回归 `4–6s`，系统拥有 >50% 的持续余量自动消化剩余历史积压。

---

## 4. 历史重验进度与异常四分类

### 4.1 按日期重验进度（`tokyo-l2-qa-state` `6b23001c`，共 385 个已归档窗口）

| UTC 日期 | 已归档窗口 | v2 终态协议报告 | `passed` | `data_failed` | `anchor_unverified` | v2 `pending` | 缺协议报告 | 旧版 v1 | 说明 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `2026-11-22`（07:45Z 起） | 65 | **65 (100%)** | 60 | 5 | 0 | 0 | 0 | 0 | 全部完成 |
| `2026-11-23` | 96 | **96 (100%)** | 79 | 17 | 0 | 0 | 0 | 0 | 全部完成（已通知 Issue #40） |
| `2026-11-24` | 96 | **96 (100%)** | 87 | 9 | 0 | 0 | 0 | 0 | 全部完成 |
| `2026-11-25` | 96 | 71 (74.0%) | 54 | 7 | 10 | 16 | 9 | 0 | 交由无人值守自动补验消化 |
| `2026-11-26`（截至 07:45Z） | 32 | 19 (59.4%) | 7 | 1 | 11 | 4 | 9 | 0 | 交由无人值守自动补验消化 |
| **合计** | **385** | **347 (90.1%)** | **287** | **39** | **21** | **20** | **18** | **0** | **检查点 347 个（不等于通过数 287）** |

> 注：历史积压已从重验启动前的 `300+`（含 20 个 `replay_init_unverified` 与全量旧版 v1）收敛至 **38 个待补验窗口**（`18` 缺报告 + `20` v2 `pending`，全在 `2026-11-25/26`）及 **21 个 `anchor_unverified`**（随上游检查点补齐将自动触发重算）；`replay_init_unverified` 与旧版 v1 均已 **0 残留**。

### 4.2 历史异常四分类（严格区分基础质检告警、真实缺失、切换重叠、旧版误判与证据不足）

1. **真实行情/采集缺失与协议违例（已确认）**：
   - **协议层确认缺失（1 窗口）**：`2026-11-25T11:00Z` `binance_futures:BTCUSDT` `missing_same_connection_anchor`（`count=8823`，紧随 `10:45Z` `gap_start=1`，导致当日 Binance `depth@100ms` 出现 `902.5s` 已知缺口）。
   - **基础质检确认 `gap_start > 0`（5 窗口）**：
     - 已完成 v2（3 窗口，均为 `bitget_v3`）：`2026-11-22T09:45Z`（`gap_start=3`）、`2026-11-23T13:45Z`（`gap_start=3`）、`2026-11-25T01:30Z`（`gap_start=3`），单次缺失约 5–15 秒。
     - 待 v2 协议自动补验（2 窗口，均为 `binance_futures`）：`2026-11-25T10:45Z`（`gap_start=1, depth_unavailable=5`）、`2026-11-26T03:00Z`（`gap_start=1, depth_unavailable=5`）。
   - **基础质检仅记录切换期短时 `depth_unavailable`（23 窗口，不可等同于真实行情大缺口）**：`loss=0`、`gap_start=0`，其中 20 个已完成 v2 协议校验且 **`protocol_status=passed`**（16 个 `bitget_v3`、1 个 `binance_futures`、2 个 `binance_futures+bitget_v3`、1 个 `bitget_v3+bybit_linear`），仅在主备 WS 切换瞬间产生毫秒至数秒级 `depth_unavailable`；另 3 个（`2026-11-25T13:30Z/15:30Z` `bitget_v3`、`2026-11-26T07:45Z` `gate_futures`）等待自动补验。
2. **主备连接切换成交重叠（6 窗口）**：检测到 `connection_overlap_trades`，已由 PR #30 按最早到达去重并保留完整成交。
3. **旧版质检误判（已在新版 v2 全部恢复）**：
   - 原 20 个历史 `replay_init_unverified` 窗口：**20/20 全部消除（0 残留）**；
   - `2026-11-22T23:00Z` Bitget `probe_orders` 时钟误判：PR #32 修复后恢复 `passed`；
   - `2026-11-22/23/24` 原因回溯窗口不足导致的大量旧版 `anchor_unverified`：经跨分区检查点继承后全部清零（0 残留）。
4. **证据不足 / 尚待验证与代表性回放受阻**：
   - **`anchor_unverified`（21 窗口，全在 `2026-11-25/26`）**：`2026-11-25` 10 个（`06:30Z–11:30Z`，主要为 `bybit_linear` 及 `09:30Z–10:00Z` 18 市场）与 `2026-11-26` 11 个（`03:30Z–07:30Z`，仅 `binance_futures` 1 个市场，源起 `03:00Z` 跳档及人工批次期间被替换的 9 个中间窗口）。随无人值守自动补验依次填补前置缺失窗口，将通过 `_reports_with_current_checkpoints` 自动触发重算。
   - **12 小时定时连接轮换导致的整点 HBT 回放 `blocked`（6 窗口）**：`2026-11-22T12:00Z`、`23T12:00Z`、`24T00:00Z`、`24T12:00Z`、`25T00:00Z`、`26T00:00Z`（三家从所 `required_stream_interval_unverified`，属计划轮换边界），外加 4 个单所 `bitget_v3` 整点 `blocked`（`23T02:00Z/16:00Z/19:00Z`、`25T14:00Z`）及 1 个 `25T07:00Z`（`bybit_linear`+`bitget_v3`）。
   - **Bitget 整点 `order_probe` 失败（4 窗口，仅见于 `2026-11-23`）**：`2026-11-23T01:00Z/18:00Z/20:00Z`（`probe_fill_unconfirmed`）与 `21:00Z`（`probe_order_unconfirmed`）。此 4 窗口 `protocol_status=passed`、四所文件检查通过且 `gate_futures`/`bybit_linear` 回放均 `passed`，但严格保持 Bitget 失败结论不放宽探针判据（`2026-11-24` Bitget 整点回放 `21/24 passed`、`3 blocked`、**0 探针失败**）。

---

## 5. 各日期、交易所、币种及行情频道数据有效时长与回测准入

覆盖 4 家交易所（`binance_futures`、`gate_futures`、`bybit_linear`、`bitget_v3`）、6 个规范币种（`BTC`、`ETH`、`SOL`、`DOGE`、`XRP`、`SUI`）及全部 12 条频道（深度、BBO、逐笔成交）。下表给出各日期各频道 6 币种有效时长区间（`min..max`）：

### 5.1 已 100% 完成 v2 重验的日期（`2026-11-22`、`2026-11-23`、`2026-11-24`）

| 日期（满产时长） | 交易所 | 深度频道（可用时长 / 占比 / 最大缺口） | BBO 频道（可用时长 / 占比） | 成交频道（可用时长 / 占比） |
|---|---|---|---|---|
| **2026-11-22** (16.25h) | `binance_futures` | `depth@100ms`: 16.249h (99.99%, gap≤4.4s) | `bookTicker`: 16.250h (100.00%) | `aggTrade`: 16.250h (100.00%) |
| **2026-11-22** (16.25h) | `gate_futures` | `futures.obu`: 16.250h (100.00%, gap≤0.1s) | `futures.book_ticker`: 16.250h (100.00%) | `futures.trades`: 16.249–16.250h (99.99–100.00%) |
| **2026-11-22** (16.25h) | `bybit_linear` | `orderbook.50`: 16.250h (100.00%, gap=0s) | `orderbook.1`: 16.250h (100.00%) | `publicTrade`: 16.250h (100.00%) |
| **2026-11-22** (16.25h) | `bitget_v3` | `books50`: 16.247–16.248h (99.98–99.99%, gap≤5.2s) | `books1`: 16.249h (100.00%) | `publicTrade`: 16.249h (99.99–100.00%) |
| **2026-11-23** (24.00h) | `binance_futures` | `depth@100ms`: 24.000h (100.00%, gap≤1.8s) | `bookTicker`: 24.000h (100.00%) | `aggTrade`: 24.000h (100.00%) |
| **2026-11-23** (24.00h) | `gate_futures` | `futures.obu`: 24.000h (100.00%, gap≤0.1s) | `futures.book_ticker`: 24.000h (100.00%) | `futures.trades`: 23.999–24.000h (100.00%) |
| **2026-11-23** (24.00h) | `bybit_linear` | `orderbook.50`: 24.000h (100.00%, gap=0s) | `orderbook.1`: 24.000h (100.00%) | `publicTrade`: 24.000h (100.00%) |
| **2026-11-23** (24.00h) | `bitget_v3` | `books50`: 23.995h (99.98%, gap≤15.0s) | `books1`: 23.998–23.999h (99.99%) | `publicTrade`: 23.992–23.998h (99.97–99.99%) |
| **2026-11-24** (24.00h) | `binance_futures` | `depth@100ms`: 24.000h (100.00%, gap≤1.6s) | `bookTicker`: 24.000h (100.00%) | `aggTrade`: 24.000h (100.00%) |
| **2026-11-24** (24.00h) | `gate_futures` | `futures.obu`: 24.000h (100.00%, gap≤0.2s) | `futures.book_ticker`: 24.000h (100.00%) | `futures.trades`: 23.999–24.000h (100.00%) |
| **2026-11-24** (24.00h) | `bybit_linear` | `orderbook.50`: 24.000h (100.00%, gap=0s) | `orderbook.1`: 24.000h (100.00%) | `publicTrade`: 23.999–24.000h (100.00%) |
| **2026-11-24** (24.00h) | `bitget_v3` | `books50`: 23.998–23.999h (99.99%, gap≤4.0s) | `books1`: 24.000h (100.00%) | `publicTrade`: 23.992–24.000h (99.97–100.00%) |

### 5.2 跨所 GLFT 回测可用交集（`follower depth ∩ follower trade ∩ binance bookTicker`）与准入结论

| 日期 | 从所 | 6 币种交集可用时长（占比） | 连续段数 / 最长连续段 | 整点 BTC HBT 回放 | 正式回测准入结论 |
|---|---|---|---|---|---|
| `2026-11-22` (16.25h) | `gate_futures` | 16.2488–16.2497h (99.99–100.00%) | 3 段 / 12.00h | 15/16 passed（1 blocked） | **可用**（07:45Z 起非完整自然日） |
| `2026-11-22` (16.25h) | `bybit_linear` | 16.2498–16.2500h (100.00%) | 3 段 / 12.00h | 15/16 passed（1 blocked） | **可用**（07:45Z 起非完整自然日） |
| `2026-11-22` (16.25h) | `bitget_v3` | 16.2470–16.2484h (99.98–99.99%) | 74 段 / 0.25h | 15/16 passed（1 blocked） | 仅建议分段研究（15m 切换切分） |
| **`2026-11-23`** (24.00h) | **`gate_futures`** | **23.9993–23.9999h (100.00%)** | 3–4 段 / **12.00h** | **22/24 passed**（2 blocked） | **正式全天回测可用**（已交付 T2 #40） |
| **`2026-11-23`** (24.00h) | **`bybit_linear`** | **24.0000h (100.00%)** | 4 段 / **12.00h** | **22/24 passed**（2 blocked） | **正式全天回测可用** |
| `2026-11-23` (24.00h) | `bitget_v3` | 23.9893–23.9954h (99.96–99.98%) | 114–115 段 / 0.25h | 16/24 passed（4 blocked, 4 probe 失败） | 暂不建议全天正式回测（需避开 4 个 probe 失败小时与 15m 切换段） |
| **`2026-11-24`** (24.00h) | **`gate_futures`** | **23.9994–24.0000h (100.00%)** | 2–3 段 / **12.00h** | **22/24 passed**（2 blocked） | **正式全天回测可用** |
| **`2026-11-24`** (24.00h) | **`bybit_linear`** | **23.9994–24.0000h (100.00%)** | 4 段 / **12.00h** | **22/24 passed**（2 blocked） | **正式全天回测可用** |
| `2026-11-24` (24.00h) | `bitget_v3` | 23.9908–23.9985h (99.96–99.99%) | 105 段 / 0.25h | **21/24 passed**（3 blocked, 0 失败） | 协议与回放洁净，仍受 15m 切换分段限制 |
| `2026-11-25`（已验 71/96） | `gate` / `bybit` / `bitget` | 17.00h / 14.50h / 17.74–17.75h（已验区间） | 最长 6.00h / 6.00h / 0.25h | 自动补验进行中 | **待无人值守自动补验清零 25 个剩余窗口后再定级**（注意 `10:45Z/11:00Z` Binance BTC 缺失） |
| `2026-11-26`（已验 19/32） | `gate` / `bybit` / `bitget` | 4.75h（已验区间；Binance `depth@100ms` 2.00–4.75h） | 最长 1.75h | 进行中 | **当日持续采集中，待自动补验收敛** |

---

## 6. 未完成验收项与明确限制

1. **EC2 服务器侧直接终验受限于 SSH 超时（未重复尝试）**：
   - 本机至 `43.206.220.182:22` 于 `2026-11-26T01:37Z` 与 `03:50Z` 两次连接超时，按指令不再重试。
   - 因此，`/data/tokyo_l2/qa_dispatch_outbox.sqlite3` 当前表状态、`journalctl -u tokyo-l2-qa-dispatch.service` 最新日志、以及 `tokyo-l2-collector.service`（`MainPID=63194`）当前进程态**未在最终验收时点完成服务器侧直连复核**（上一次直连核验为 `2026-11-25` PR #31/#33 部署后）。
   - GitHub 侧观测到的持续 15m `workflow_dispatch` 触发与 `remaining_backlog=0` 仅构成**间接证据**，不能替代完整服务器侧验收；待网络/安全组恢复后可执行一次只读 SSH 抽检补齐闭环。
2. **`2026-11-25/26` 剩余历史积压交由生产自动化收尾**：
   - 当前剩余 38 个待补验窗口（18 缺报告 + 20 `v2 pending`）与 21 个 `anchor_unverified` 链窗口不再由 AI 手动接力，直接交由现有 `archive_dispatch`（`eff_max_catchup=1`）+ 每 3 小时 `schedule`（`eff_max_catchup=4`）+ 队列中正在运行的既有 Actions 任务自动消化。
3. **两项非阻塞代码优化建议未擅自修改**：上述风险 A（顺带补验超时隔离）与风险 B（`CHECKER_COMMIT` 仅绑定 `collector/python`）已定位具体代码行并保留为最小修复建议，按 T3 变更授权原则暂不修改生产代码。
4. **跨线依赖边界**：T2 `exp/tokyo-l2-glft` 工作流仍固定 `COLLECTOR_SHA=78f7a69ac0...`（经审计不受 PR #30 成交符号问题影响，未越界修改 T2 分支）。
