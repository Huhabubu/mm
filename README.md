# 跨交易所信号指导做市（mm）

研究目标：检验 Binance USDT 永续合约行情能否在 OKX 本地信息之外，为 OKX 永续合约做市提供额外预测价值。当前先做数据完整性、时间戳和跨所行情对齐检查，再开展预测与 GLFT 回测。

## 第一阶段：Tardis 数据 QA

- 选定 UTC 2026-11-01：Binance `BTCUSDT` 和 OKX `BTC-USDT-SWAP`
- 使用 Tardis 每月一日的免费 `quotes`（最佳买卖报价）及 `trades`（逐笔成交） CSV.gz
- 以 Tardis `local_timestamp`（采集端接收时间，微秒）建立 **仅用于质量检查** 的双所 as-of 对齐；同时统计交易所时间戳与采集时间的差值
- 数据文件只保留在 GitHub Actions 临时运行器中。公开仓库只提交统计报告；不公开转载原始 Tardis 数据
- 不将跨所采集端 `local_timestamp` 直接视为同一交易程序的真实可见时间，两所采集地点可能不同，预测实验必须做时间偏移与延迟敏感性分析

手动运行：GitHub → Actions → **Inspect Tardis BTC quotes/trades** → Run workflow。

本地执行（Python 3.11+，无第三方依赖）：

```bash
python scripts/inspect_tardis.py --day 2026-11-01
```

质量检查产出：`reports/data_qa_2026-11-01.json`，含下载字节数、完整性与数据异常、报价有效性、时间戳延迟和双所 as-of 对齐覆盖率。当前阶段不训练预测模型，也不进行交易回测。

数据来源：https://docs.tardis.dev/downloadable-csv-files
授权约束：https://docs.tardis.dev/legal/terms-of-service

## 第二阶段：跨所价格预测（实验性先导验证）

工作流：[`.github/workflows/prediction-pilot.yml`](.github/workflows/prediction-pilot.yml)。

- **P1 本地模型**：OKX 最近 100/500ms 中间价收益（bps）、买一卖一挂单量不平衡、买卖价差（bps）。
- **P2 联合模型**：与 P1 完全相同，额外加入 Binance 同样 4 个特征。
- **预测目标**：OKX 在决策时刻之后 20、50、100、200、500、1000、2000、5000ms 的中间价累计收益（bps）。
- **决策时钟**：支持 `--clock okx`、`--clock binance`、`--clock grid` 三种实验。前两种从每个 20ms 桶选择指定交易所的第一条真实报价事件；第三种使用预先确定的 100ms 等间隔网格（37ms 相位）。Binance 事件时钟的模拟额外延迟会推迟决策时刻；固定网格则保持决策时间不变。两所特征通过 **仅过去可见** 的 as-of 查找构造，目标通过未来时刻的 OKX as-of 价格计算，不跨日。
- **共同样本**：同一延迟假设下，P1/P2 使用完全相同的时间点和目标；Binance 报价最长允许 2s 未更新（保留报价年龄限制）。
- **算法**：固定正则化强度 0.01 的岭回归（Ridge Regression），仅在早期训练日估计参数；验证/测试日期完全独立于训练。
- **时间分组**：2026 年 6–8 月的 1 日训练；9 月 1 日验证；10 月、11 月的 1 日独立测试。测试日仅 2 天，**不能据此宣称稳健的统计显著性**。
- **时间偏移敏感性**：Binance 相对决策时间额外增加 0/25/50/100ms 的可见性延迟。注意它不能识别 Tardis 两个采集地点间未知的绝对时钟/网络偏差。
- **统计结果**：逐日、逐预测期分别比较 P1/P2 的均方预测误差（bps²），记录方向命中率、零预测基线及测试日等权汇总；不进行逐笔显著性伪检验。
- **限制**：本轮只用最优报价（quotes），不使用逐笔交易（trades）；目前未研究成交或 GLFT 策略收益。

**真实运行和结果**：2026-11-09 在 [GitHub Actions #42562392532](https://github.com/Huhabubu/mm/actions/runs/42562392532) 完成 Binance 事件触发实验，在 [#42562872097](https://github.com/Huhabubu/mm/actions/runs/42562872097) 完成独立固定时间网格检验。两者短期预测结果存在明显差异；详见 [研究记录](docs/prediction_pilot_2026-06_to_11.md)。首次运行由提交工作流自动触发，之后可在仓库 Actions 页手动重跑。只保存汇总 JSON，原始 Tardis 文件始终在运行器中临时下载，不提交到仓库。

特别注意：Binance 事件触发时 P1 的评估时点也由 Binance 行情活动选择，尚需固定评价时钟的稳健性对照；两所 Tardis 采集时间不等于同一实盘主机的同步接收时间。

## 原生 BBO 复核（2026-11-09 已完成）

- 使用 Tardis 原生 `book_ticker`（Binance `bookTicker` / OKX `bbo-tbt`），与由 L2 重建的 `quotes` 作同口径比较。
- [真实 QA #42581171378](https://github.com/Huhabubu/mm/actions/runs/42581171378)：2026-11-01 Binance 原生 13,834,893 条、OKX 原生 1,468,859 条，均无异常报价或采集时间倒序。详见 [数据质量报告](docs/native_bbo_qa_2026-11-01.md)。
- [原生预测 #42582157158](https://github.com/Huhabubu/mm/actions/runs/42582157158)：六个月实际数据、11 项合成测试通过。原先 `quotes` 下约 **+18.56%** 的 100ms MSE 改善在原生 BBO 下变为 **−0.97%**（Binance 事件时钟），固定网格为 **−2.62%**。详见 [预测复核报告](docs/native_bbo_prediction_2026-06_to_11.md)。
- 研究状态：旧的 100ms 强预测改善**未复现**，不能将其作为交易所领先的证明。尚未进入 GLFT 回测；下一步先检查真实 Binance 价格变化事件与 OKX 的短期响应，评估 OFI、加权中间价等微观结构特征。

## Binance 价格变动事件研究（六个日期，已完成）

- [9—11 月真实运行 #42590594754](https://github.com/Huhabubu/mm/actions/runs/42590594754)；[6—8 月真实运行 #42591370486](https://github.com/Huhabubu/mm/actions/runs/42591370486)。
- 对原生 Binance BBO **中间价发生变化**的事件（每 20ms 桶最多取第一条）分别记录上涨、下跌及 OKX 未来 20—1000ms 的同向价格响应，采用相同微秒内的原始文件顺序。
- 六天均观察到正向的 100ms 事件条件平均响应（约 +0.35 至 +0.54bps），匹配的单纯数量变化事件后仍有正向差值；额外 25—100ms 等待会缩小后续响应。
- **严格边界**：这只是按事件条件统计 OKX 价格变化；全量原生 BBO 上旧 P2 并未胜过 P1，尚未证明样本外增量或做市净利润，且 Tardis 采集时间和 OKX 服务器迁移仍需做稳健性检验。
- [六日期完整事件研究报告](docs/binance_price_event_study_2026-06_to_11.md)；源代码 `scripts/binance_price_events.py`，合成测试 `scripts/test_binance_price_events.py`。

## 最新：Binance 价格事件条件下的真正增量预测（已完成）

- [成功运行 #42599688211](https://github.com/Huhabubu/mm/actions/runs/42599688211)；[完整结果记录](docs/price_event_increment_results_2026-06_to_11.md)；[代码](scripts/predict_price_event_increment.py)。
- 对同一批 Binance 原生 BBO **中间价实际变化事件**，P1 仅使用 OKX 本地状态，P2 额外使用 Binance 此次价格变动方向/幅度；共用目标、样本、模型形式；20 项合成测试通过。
- **探索性结果（未来 100ms）**：零额外延迟下 P2 相对 P1 在 10/11 月的每日 MSE 改善为 **+4.02% / +7.89%**；额外延迟 25ms 后两天等权平均为 **−18.02%**，P1/P2 甚至均不如零预测基准。50ms 也明显失败，不能将前述响应当作真实可交易做市收益。
- 9—11 月属于此前已查看的日期，未提供新的独立盲测。GLFT 报价中心平移 A 继续暂缓，待非零延迟下的跨时期预测增量验证后再推进。

## 9 月校准：预测幅度失配诊断（最新）

- [成功运行 #42608558041](https://github.com/Huhabubu/mm/actions/runs/42608558041)（22 项测试通过）；[逐日校准与基准对比](docs/price_event_amplitude_calibration_2026-06_to_11.md)。
- 6—8 月训练的同一 Ridge 模型不变，9 月为 P1/P2 每个预测期估计限制在 0～1 的预测幅度乘数，10/11 月仅评价，测试日不参与校准。
- 未来 100ms：额外 25ms 延迟时，P2 相对 P1 MSE 改善从原始 **−18.02%** 修正为 **−1.62%**；50ms 从 **−9.17%** 修正为 **−1.26%**。校准后两个模型都比零预测更准，但 **P2 仍不及 P1**。这确认预测幅度高估是非零延迟失效的重要原因。
- 当前仍需未被研发过程观察的新日期/币种与采集时钟稳健性；GLFT A 方案及收益回测继续暂缓。

## 最新跨品种检验：ENA/USDT（2026-11-09 已完成）

- [预设方法](docs/ena_cross_symbol_protocol_2026-11-09.md) → [GitHub Actions #42617964298 成功运行](https://github.com/Huhabubu/mm/actions/runs/42617964298) → [完整结果报告](docs/ena_cross_symbol_results_2026-06_to_11.md)。
- 两所 2026-11-01 的原生 ENA BBO 质量检查通过；六个月同日数据使用与 BTC 相同 P1/P2、模型和 9 月幅度校准方式。
- **主要条件：25ms 额外延迟、未来 100ms**，ENA P2 相对 P1 的 10 月改善 **+0.409%**、11 月 **−0.480%**，两日等权 **−0.036%**，未通过预设的两日一致性标准。50ms 延迟也为负的两日等权平均。
- 0ms 额外延迟的改善达到 **+18.191%**，但与 BTC 一样，非零延迟下尚无稳定额外预测收益的证据。当前继续以 G1/P1 为必要对照，G2/GLFT 尚未进入完整交易回测。

## 逐档延迟重训核查与 BTC 反向事件检验（已完成）

- [源代码核查和事前方案](docs/symmetric_leadlag_review_protocol_2026-11-09.md) → [真实成功运行 #42637036552](https://github.com/Huhabubu/mm/actions/runs/42637036552)（25 项测试通过） → [六日期双向结果](docs/symmetric_leadlag_results_2026-06_to_11.md)。
- 已核实 0/25/50/100ms 延迟档均使用延迟后的目标重新拟合 P1/P2，并独立进行 9 月幅度校准；不存在零延迟模型直接套用 25ms 的错误。
- **新发现**：OKX 价格变化后 Binance 同样有正向响应；9—11 月未来 100ms、额外 25ms 时 B→O 每日等权 +0.1330bps、O→B +0.1426bps，二者接近。方向性信息可能快速消失，且仍有采集路径、事件大小及共同市场冲击等识别问题。
- 暂不把单向正响应认定为 Binance 独有价格发现或可实现的做市收益；接下来先做统一时间平移安慰剂和价差/tick/成本量级检验，再推进 G0 基准。

## BTC 时间平移安慰剂与 OKX 报价/费用尺度（2026-11-10）

- [实际六日期成功运行 #42734739751](https://github.com/Huhabubu/mm/actions/runs/42734739751)，28 项测试通过；[固定方案](docs/clock_shift_and_cost_protocol_2026-11-10.md)与[结果报告](docs/clock_shift_and_cost_results_2026-06_to_11.md)。
- Binance 固定价格事件相对 OKX 平移 +60s 后，未来 100ms 同向均值在六天几乎归零；9—11 月原始时间约 +0.351bps、额外等待 25ms 约 +0.133bps，但将事件**非因果地提前 50ms**后约 +0.483bps，提示跨所时间先后顺序仍存在识别风险。
- OKX BTC 原生最优报价的经验最小价格步长六天均为 0.1USDT，中位全价差约一档；OKX 官网 Lv1 示例 maker 费 2bps/次，实际历史账户费率仍待核对。**不将事件后的中间价收益或最优盘口机械价差解释成做市净利润**。
- 研究下一步按照已审定主线准备原始 GLFT 的 G0 单所回测，需先验证 OKX L2 增量数据/成交数据、合约规格、hftbacktest 队列与延迟模型；此阶段保持 P1/P2/B/C 参数范围不变。
