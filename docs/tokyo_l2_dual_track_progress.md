# 东京 L2 行情采集项目｜我们 × Gemini 双线任务进度

> **更新日期：** 2026-11-21  
> **仓库：** `Huhabubu/mm` · 当前监控开发分支 `feat/tokyo-l2-p3-monitoring`（P1 主线：`feat/tokyo-l2-collector`）  
> **本轮代码审查基线：** `587ac3b9fdc45cbde0aeb56eecef4a547ce869d3`（基于 P1 修复 `cee6a6d`）  
> **目标：** 持续采集四家交易所的真实高频行情，安全归档至 Google Drive，为跨交易所信号指导做市的论文提供可验证的回测数据。

**状态说明：** `[x]` 已完成且有对应代码/CI/实机记录；`[ ]` 尚待完成，包括已安排但未验证的工作。每项括号说明“进行中 / 待执行 / 待联调”。本清单的完成比例仅统计下列任务条目，不代表论文整体完成率。

**阶段名称按仓库 `collector/SPEC_v1.4.md`：** P0 为协议与接口规范；P1 为离线构建、归档及确定性测试；**P2 为东京 24 小时技术试采**；**P3 为 21–30 个完整 UTC 日的正式盲采**；P4 为解盲和冻结评估。**Healthchecks / Telegram 是 P2 试采配套的监控专项；Lightsail 是独立的网络出口专项。**

## 当前总览

| 工作包 | 我们（部署、账户、真实验收） | Gemini（实现、测试、部署模板） | 目前结论 |
|---|---|---|---|
| P1 归档与恢复 | 真实上传/回读/恢复通过；真实清理待满 24 小时 | 清理安全补丁、测试与 CI 已完成 | **接近完成：差真实 `PRUNED → 恢复`** |
| P2 东京 24h 试采 | 四所采集已启动；完整 24h 门禁待验收 | Rust 采集与离线预检已实现；故障恢复专项待审 | **进行中** |
| Healthchecks / Telegram | 检查、机器人和群凭据已准备；实际心跳未联调 | 监控与通知实现，CI 37/37；审查发现 3 处上线前边界需修 | **代码实现完成、待修复与实机联调** |
| Lightsail 代理 | 已有两台 Lightsail；与本 EC2 的具体链路待核实 | 代理部署配置与故障回退代码待开发 | **未接通** |
| P3 正式盲采 / P4 论文评估 | 等 P2 验收与研究冻结 | 冻结与验收自动化待后续阶段 | **未开始** |

**清单进度：** 以各项 `[x]` / `[ ]` 为准；下方新增了监控模块代码审查项目。已勾选表示代码/离线测试或已有实机证据通过，**不等于生产服务已经接入或监控已经恢复**。

---

## A 线：我们负责——AWS 环境、账号配置、实机接入与验收

### A1. 东京 EC2 与 P1 真实归档

- [x] 东京 EC2 已部署，采集器运行在 `apne1-az4`，四家交易所的实时行情持续写盘。
- [x] Google Cloud OAuth / Drive API 与 `rclone` 远端 `gdrive_main:tokyo_l2_data` 已完成授权和真实读写。
- [x] 归档实际测试：四家交易所数据远程回读 `4/4 PASS`；云端 SQLite 快照通过 SHA-256 与 `integrity_check=ok`。
- [x] EC2 的 `qa-convert --remote` 实机恢复通过：`status=ok`，退出码 `0`，6 个币种均为 `eligible`，共 825,910 行；与原本地 QA 报告一致（除恢复记录字段）。**这是“模拟本地缺失后的远程恢复”，不等于正式账本已发生 `PRUNED`。**
- [x] EC2 代码更新到 `cee6a6d`，工作区干净；未改动或重启 Rust 采集器。
- [x] 最近一次候选检查：SQLite 中 20 个分区均为 `VERIFIED`，且符合除保留时间外的清理条件；当时最老文件只有 4.78 小时，故超过 24 小时的候选为 **0 个 / 0 GiB**。
- [ ] **【待执行】P1 实际清理前检查：** 待文件按真实修改时间满 24 小时后，复核候选个数与总字节数、剩余磁盘容量及云端备份；确认本次允许删除的范围。
- [ ] **【待执行】手动执行一次 P1 清理：** 仅在明确接受候选范围后执行 `archive --verify-remote --prune-hours 24`；核实日志、`PRUNE_INTENT`、`PRUNED_OK`、SQLite `PRUNED` 状态、已删本地文件和保留的 Manifest。
- [ ] **【待执行】真实清理后恢复验收：** 从 Google Drive 恢复实际 `PRUNED` 分区，核对 SHA-256、压缩文件完整性、HBT 产物与清理前基准一致。
- [ ] **【待授权】启用生产定时清理：** 完成上述验收后，再单独决定是否在 systemd 归档服务中启用 `--prune-hours 24`。**目前继续关闭自动清理。**

### A2. Healthchecks 与 Telegram 真实账户准备

- [x] 已创建两个 Healthchecks 监控检查：`Tokyo-L2-EC2-Archive`（1 小时周期 + 15 分钟宽限）和 `Tokyo-L2-GitHub-QA`（1 小时周期 + 35 分钟宽限）。
- [x] 两个 Healthchecks 检查已配置 Telegram 通知集成；目前已主动暂停，避免程序未接入时误报。
- [x] 已创建/准备 Telegram Bot、群组、Bot Token 与 Chat ID；密钥在私有配置中使用，**不写入公开仓库或此文档**。
- [ ] **【待配置】采集器独立健康监控：** 现有两个检查分别用于 Archive 和 GitHub-QA；采集器进程存活/数据新鲜度需新增独立检查，或由用户明确选择重用哪个检查，避免名称与监控对象不一致。
- [ ] **【待联调】按 Gemini 的环境变量与 systemd 模板将凭据部署至 EC2**；完成 Healthchecks 正常、失败、超时、恢复四类通知实测。
- [ ] **【待联调】Telegram 机器人实测：** 收到测试消息、异常告警、恢复提示和小时战报，确认群组权限、去重和频率控制。
- [ ] **【待执行】联调成功后恢复 Healthchecks 检查**，记录准确周期、宽限和服务映射，避免无意义报警。

### A3. Lightsail 网络出口专项

- [x] 已开通两台 AWS Lightsail 实例（既有准备工作）；具体将哪台用于本项目、区域和网络连通状态尚需核实。
- [ ] **【待核实】** Lightsail 实例的区域、静态公网 IP、私网 IP、VPC Peering（私有网络对等互联）条件，以及 EC2 → Lightsail 网络路由。
- [ ] **【待配置】** 验证 SSH 密钥、安全组/防火墙、EC2 到 Lightsail 的稳定连接；不把密钥放进 GitHub。
- [ ] **【待联调】** 建立受限 SSH SOCKS5 隧道（通过 SSH 将上传请求转发到 Lightsail），验证真实出口 IP。
- [ ] **【待联调】** 仅让 `rclone` 的 Google Drive 归档流量走代理；Rust 到交易所的 WebSocket 行情链路保持直连。
- [ ] **【待实测】** 用相同归档文件比较直连与代理吞吐、耗时、失败率；模拟隧道失联，确认保留本地源文件与明确记录回退路径。
- [ ] **【待授权】** 网络测试与故障回退通过后，再决定是否将代理设为生产默认路径。

### A4. P2 真实 24 小时技术试采门禁

- [ ] **【进行中】** 取得连续 24 小时、覆盖 00:00 / 12:00 UTC 双活切换的四所 × 六币种（24 市场）实测报告，并核查缺口与接收序列。
- [ ] **【待验收】** 验证 CPU / 内存 / 磁盘余量、`LOCAL_LOSS=0`、Chrony 时钟质量、四所 RTT、50 档盘口可见覆盖和归档跨日完整性。
- [ ] **【待验收】** 完成四所 BTC 全天回测数据转换、后续六币种数据可用性与冻结策略回放检查，形成 P2 放行/阻塞结论。

---

## B 线：Gemini 负责——代码开发、回归测试、CI 与部署模板

### B1. 已完成：P0/P1 基础能力与安全修复

- [x] Rust 四所双活采集、按小时封口、Manifest（文件清单）、离线协议及回放测试的基础实现。
- [x] SQLite WAL（可恢复的数据库写入日志）归档账本、双端云盘校验、增量跳过、并发锁、崩溃恢复与 `PRUNED` 状态机。
- [x] QA 事件排序问题修复，恢复数据可通过 hftbacktest 转换。
- [x] 提交 `3211e89`：删除前本地 SHA-256 复查；部分成功仍上传一致 SQLite 快照；原始 JSONL 历史保留与独立兼容投影。
- [x] 提交 `cee6a6d`：清理前重新核对云端数据与 Manifest 的 ID、大小和哈希；远端故障时保留本地文件，补充两组故障注入测试。
- [x] GitHub Actions Run `44765096063`：Python **31/31**、Rust **14/14**、两套离线 preflight 均通过；Rust 发布二进制 SHA-256 未变。
- [x] 已有 `meta_daemon.py` 的合约规格、RTT、Chrony 与磁盘采样，以及通用 Webhook 告警入口；systemd 归档服务默认不带清理参数。
- [ ] **【待同步】更新 `collector/README.md` 中旧的 JSONL/QA 命令示例：** 新版优先使用 `archive_ledger.sqlite` 或 `verified_archive_ledger.compat.jsonl`；原 `verified_archive_ledger.jsonl` 保持冻结，不应继续作为最新状态入口。

### B2. 监控专项（Gemini 内部称 P3；不是规格书的正式盲采 P3）

- [ ] **【部分完成，待修】Healthchecks 接入：** 采集器每 5 分钟与归档每小时的成功/失败心跳代码已提交并通过离线测试；真实 EC2 未联调。**systemd 状态探针不可用时目前有误发健康心跳的风险**，需改为失效即不报绿；已有 GitHub-QA 检查仍需独立接入。
- [x] **【代码与离线测试通过】Telegram Bot API 通知：** 支持 Bot Token、Chat ID 和 `sendMessage`，从私有环境变量读取；尚未执行真实群聊联调。
- [ ] **【代码基本完成，待修】风险告警：** 已从 systemd、journal、文件元数据和只读 SQLite 识别采集停止、双活中断、LOCAL_LOSS、磁盘、时钟和归档问题；**A/B 中断的恢复目前会按 60 秒窗口推断，需要真实 GAP_END/恢复证据，避免误报已恢复**。
- [ ] **【代码实现，统计口径待修】每小时战报：** 已输出四所活跃度、归档状态、磁盘、时钟、异常；**DATA、封口、SWITCH/GAP/LOSS 当前汇总了全部历史 Manifest，必须改为注明“累计”或筛选上一 UTC 小时，再声称是小时统计**。
- [x] **【代码与离线测试通过】告警发送机制：** 去重、冷却期、失败后限次重试、消息脱敏，通知异常不改变归档退出码；真实故障恢复语义需按上述问题加强。
- [x] **【模板与 CI 已完成】部署准备：** `notify.env.example`、归档与元数据守护 systemd 环境加载、示例配置及 6 组监控离线测试均已提交；还须补充 EC2 实际接入与测试文档。

### B3. 后续：Lightsail 代理的部署自动化

- [ ] **【待开发】** 提供 EC2 → Lightsail 的 SSH SOCKS5 隧道部署/守护模板，支持最小权限与进程异常自动恢复。
- [ ] **【待开发】** 提供仅作用于 `rclone` 的代理环境或 systemd drop-in；禁止全局代理影响 Rust 采集链路。
- [ ] **【待开发】** 提供代理连通性、实际出口确认、直连回退和失败保留本地原件的验证脚本。
- [ ] **【待开发】** 提供直连/代理对照报告模板，完成离线测试；实际服务器修改留给我们执行。

### B4. 后续阶段与风险收口

- [ ] **【待审/待补】** 整理 Rust 写盘异常、进程重启、断网、小时封口恢复的 P2 故障注入覆盖表；只补实际缺口，不顺手重构热路径。
- [ ] **【待 P2 放行】** 为正式 P3 盲采准备冻结配置、21–30 个完整 UTC 日期的有效性检查；正式盲采开始前需确认 Bitget `2026-12-01` 协议变更后的实帧行为。
- [ ] **【待后续】** P4 解盲与冻结研究评估自动化。不得将采集可用或预测误差直接声称为论文做市盈利。

---


## 2026-11-21 Gemini 监控专项代码审查与验收门禁

- **提交**：[`587ac3b`](https://github.com/Huhabubu/mm/commit/587ac3b9fdc45cbde0aeb56eecef4a547ce869d3)，独立分支 `feat/tokyo-l2-p3-monitoring`，修改 Python、测试、示例配置、systemd 通知环境模板、CI；未修改 Rust 热路径。
- **CI**：[`44771756847`](https://github.com/Huhabubu/mm/actions/runs/44771756847) 成功；Python **37/37**、Rust **14/14**，两套离线预检通过；发布二进制 SHA-256 与前次一致。
- **评定**：功能覆盖基本达标，**代码审查暂不批准上线**；真实 Telegram、Healthchecks 的通知及失联链路未在 EC2 验收。

### 上线前修复项目（Gemini）

- [ ] **【高优先级】健康检查失效即拒绝报绿：** `meta_daemon.py` 的 `run_sweep` 仅在 `collector_status.available` 为真时检测停机；探针失败/服务不存在且文件仍新鲜时可误发 collector Healthchecks 成功心跳。须将未知状态视作不健康或至少停止成功心跳，并增加确定性测试。
- [ ] **【高优先级】A/B 双线中断恢复的证据：** 现在 `dual_ab_outage_gt_5s` 在上次告警后超过默认 60 秒、同所其他流仍写盘时可能自动清除；须依赖明确 `GAP_END`、端点实时健康证据或等价的可验证恢复信号，而不能只按计时推断“已恢复”。
- [ ] **【中优先级】小时战报数据统计窗口：** `inspect_data_tree` 的 `total_data_lines`、`manifest_count`、`SW/GAP/LOSS` 是历次全部清单的累计值；需按上一完整 UTC 小时筛选，或清楚标注为累计统计，并新增跨两小时/跨日对照测试。
- [ ] **【联调】沿用已有两个 Healthchecks 检查：** `Tokyo-L2-EC2-Archive`（1h+15m）和 `Tokyo-L2-GitHub-QA`（1h+35m）先保持暂停；新增单独 collector 检查（Gemini 建议 5m+10m）。**不重建已有机器人、Token 或 Chat ID**；上线前核对实际心跳频率与检查的 Period/Grace。

### 不变红线

- 自动清理 `--prune-hours 24` 仍未开放；P1 真实 PRUNED → 恢复验收独立进行。
- `tokyo-l2-collector.service`、Rust 采集进程和交易所 WS 网络不得因为监控联调被重启或修改。
- 所有真实 Telegram Token、Chat ID、Healthchecks URL、Lightsail SSH 密钥只写入 EC2 私有配置，禁止进入仓库与战报。

## 交接顺序与共同约束

1. **当前可并行：** 我们准备 Lightsail 网络连通与 Healthchecks/Telegram 私有环境配置；Gemini 修复监控专项代码审查指出的 3 处边界；P1 真实清理验收等文件自然满 24 小时。
2. **P1 收口：** 手动清理并从真实 `PRUNED` 分区恢复成功后，记录 SHA-256、数据库状态及 HBT 产物校验；**生产自动清理需要单独授权**。
3. **监控上线：** 先修完健康误报、双线恢复和小时战报口径并通过 CI，再在 EC2 测正常/失败/超时/恢复通知，最后恢复 Healthchecks 检查。
4. **代理上线：** 先网络连通和真实出口核验，再让单个 `rclone` 任务使用代理，最后讨论生产默认路径。
5. **安全边界：** 不改运行中的 Rust 采集器、不上传密钥、不删除未双重远程验证的数据、不在 P2 通过前启动正式 P3 盲采。

## 证据链接

- [P1 清理安全最新提交 cee6a6d](https://github.com/Huhabubu/mm/commit/cee6a6d0ad79411acb794221e6efa3e20354d8db)
- [对应 GitHub Actions Run 44765096063](https://github.com/Huhabubu/mm/actions/runs/44765096063)
- [采集项目正式规格 SPEC_v1.4](https://github.com/Huhabubu/mm/blob/cee6a6d0ad79411acb794221e6efa3e20354d8db/collector/SPEC_v1.4.md)
- [现有监控基础 meta_daemon.py](https://github.com/Huhabubu/mm/blob/cee6a6d0ad79411acb794221e6efa3e20354d8db/collector/python/meta_daemon.py)

**证据边界：** 本文的 P3 代码与 CI 审查基于真实 GitHub 提交与日志；EC2、Healthchecks、Telegram 和 Lightsail 的实时状态依据项目中的实机操作反馈，未在本轮重新查询外部控制面。本文件存于 P3 开发分支，后续合并/同步到主线时需保留同一份文档。
- [监控专项提交 587ac3b](https://github.com/Huhabubu/mm/commit/587ac3b9fdc45cbde0aeb56eecef4a547ce869d3)
- [监控专项 CI Run 44771756847](https://github.com/Huhabubu/mm/actions/runs/44771756847)
