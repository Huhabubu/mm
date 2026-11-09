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
- **决策时钟**：每个 20ms 时间桶取第一条实际出现的 OKX 报价事件；两所特征通过 **仅过去可见** 的 as-of 查找构造，目标通过未来时刻的 OKX as-of 价格计算，不跨日。
- **共同样本**：同一延迟假设下，P1/P2 使用完全相同的时间点和目标；Binance 报价最长允许 2s 未更新（保留报价年龄限制）。
- **算法**：固定正则化强度 0.01 的岭回归（Ridge Regression），仅在早期训练日估计参数；验证/测试日期完全独立于训练。
- **时间分组**：2026 年 6–8 月的 1 日训练；9 月 1 日验证；10 月、11 月的 1 日独立测试。测试日仅 2 天，**不能据此宣称稳健的统计显著性**。
- **时间偏移敏感性**：Binance 相对决策时间额外增加 0/25/50/100ms 的可见性延迟。注意它不能识别 Tardis 两个采集地点间未知的绝对时钟/网络偏差。
- **统计结果**：逐日、逐预测期分别比较 P1/P2 的均方预测误差（bps²），记录方向命中率、零预测基线及测试日等权汇总；不进行逐笔显著性伪检验。
- **限制**：本轮只用最优报价（quotes），不使用逐笔交易（trades）；目前未研究成交或 GLFT 策略收益。

首次运行由提交工作流自动触发。之后可以进入仓库 Actions 页手动重跑。只保存公开可见的汇总 JSON，原始 Tardis 文件始终在运行器中临时下载，不提交到仓库。
