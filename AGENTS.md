# AGENTS.md — 项目协作规范

适用于本仓库各活跃任务线。总览以 [main/README.md](https://github.com/Huhabubu/mm/blob/main/README.md) 为准，跨任务协调见 [Issue #42](https://github.com/Huhabubu/mm/issues/42)。本文件不授权新的实验、合并或部署。

## 1. 任务线与工作分支

| 编号 | 职责 | 目标分支 | 进度入口 |
|---|---|---|---|
| T1 | 论文策略研究：GLFT、跨所价差信号、五阶段机制与第5章实证 | `exp/chapter5-cross-exchange` | [progress.md](https://github.com/Huhabubu/mm/blob/exp/chapter5-cross-exchange/progress.md) |
| T2 | 自采行情回测基础设施：下载、转换、Kaggle 与 hftbacktest 兼容性及验收 | `exp/tokyo-l2-glft` | [progress.md](https://github.com/Huhabubu/mm/blob/exp/tokyo-l2-glft/progress.md) |
| T3 | 生产采集、归档、质检及监控 | `main`（质检入口）、`feat/tokyo-l2-reliability-fixes`（采集及质检实现） | [collector/README.md](https://github.com/Huhabubu/mm/blob/feat/tokyo-l2-reliability-fixes/collector/README.md)、关联 Issue/PR、[质检状态](https://github.com/Huhabubu/mm/tree/tokyo-l2-qa-state) |
| T4 | USDC 永续独立探索实验 | `exp/usdc-perp-glft-min` | [research/usdc_perp_glft_min/PROGRESS.md](https://github.com/Huhabubu/mm/blob/exp/usdc-perp-glft-min/research/usdc_perp_glft_min/PROGRESS.md) |

每次工作前核实任务所属 T1–T4、实际目标分支、远端 HEAD（提交 SHA）、已有进度和验收标准。历史分支只作为研究证据或回归基线；临时修复分支须从对应任务线创建，禁止擅自切换研究主线、删除历史分支、强制推送或跨线合并。若分支 HEAD 在工作期间变化，先核对变化，避免覆盖并行工作。

## 2. 职责边界与交付

- T3 → T2：提供经验证的归档、窗口可用性、规则版本和证据。真实缺失、协议异常、初始化未验证分别记录，不得降低门禁标准；不得手工将未验收数据标记为通过。
- T2 → T1：验收数据转换、时间可见性、连续性、断点续跑和官方 hftbacktest 回放。T2 继承 T1 代码仅为复用；不得擅自改变既有 GLFT 逻辑、价差信号、延迟、队列或参数。验收通过后报告证据，等待用户批准接入 T1。
- T1：保留 [Gate–Binance 五阶段历史节点](https://github.com/Huhabubu/mm/tree/fix/gate-stage2-stage5b-total-markout-20261118) 与统一策略引擎，延续已约定的模型、基差及实验口径。方法变更先提出独立方案，获得批准后做聚焦回归与实证；旧报告不得覆盖。
- T3：生产采集器、EC2 部署、监控配置、质检规则、生产状态分支写入方式的变更须单独授权。自动生成的质检状态分支由既有工作流维护；不得将实验结果或人工放行写入。
- T4：与 T1 独立管理；独立实验参数和结果不得直接作为论文正式证据。变更计算环境不应顺手改变策略。跨线引用/复用须明确数据、代码版本和授权。
- 跨任务线合并、改变冻结研究口径、降低质检标准、生产部署均须用户明确批准。可提出建议，但不能将建议当作已获授权。

## 3. 执行与复现

- 只改完成当前任务必要的文件，保留既有实现、命名和历史证据。先确认失败机制或成功标准，再实施、运行聚焦测试，报告实际结果；不可用时说明原因。
- 研究结论必须区分真实回测、合成测试、探索性观察与推测。核对前视偏差、时间戳、订单簿连续性、队列、延迟、费用/资金费率及样本独立性。默认报告零手续费收益，并明确含费情景；禁止伪造数据或显著性。
- 公共仓库及公开 Actions 日志/附件仅允许安全的汇总结果和代码；不得发布原始行情、逐笔受限数据、私有 Dataset、Token 或其他凭据。遵守数据授权约束。
- Kaggle 用于实际计算，GitHub 用于版本管理、必要测试与经授权的任务调度。避免无必要的长时间 Runner 等待及重复全天实验；调整工作流需另行批准。
- 完成后更新所属分支进度入口：记录基线/结果 SHA、改动、实际测试或 Actions 运行链接、阻塞及下一步。T3 使用对应 Issue/PR 和既有自动质检报告，不伪造生产记录；T4 使用自己的 PROGRESS.md，不写入历史论文 progress.md。
- 具体任务 Issue 链接回 #42，并注明任务线及分支；总控 Issue 只维护协调、交付门禁与重要状态，不重复完整技术报告。

未获授权的工作停在方案与证据阶段；已有研究或生产流程不得因目录整理、基础设施验证而自动升级或重新设计。
