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
