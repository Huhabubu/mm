# CryptoHFTData 2026 年 10 月 BTC 四所数据质量审计（阶段性）

日期：2026-11-15（UTC）。候选窗口：2026-10-05 至 2026-10-29。交易所：Binance、OKX、Bybit、Bitget 的 BTCUSDT USDT 永续合约。

## 三小时抽样实测

2026-10-05 02 时、2026-10-15 12 时、2026-10-25 23 时，四所的订单簿与逐笔成交共 24/24 份 Parquet 可正常下载读取；扫描约 4,865 万行。这仅证明抽样文件可读，尚不能证明连续 25 天的数据完整。

证据：[四所抽样运行 #43656251546](https://github.com/Huhabubu/mm/actions/runs/43656251546)。

## Binance 2026-10-15 12 时原始数据核查

- 249,387 条成交，其中 440 条零价格、零数量记录全部属于 order_type=NA。Binance 的这种特殊成交事件有官方说明，做成交量统计时剔除，检查 ID 连续时保留。
- 盘口 135,500 次增量链条检查，考虑合法快照接续后，未发现序列不合法接续。
- UTC 12:30:18.972 至 12:30:32.489 存在 13.516534 秒没有连续盘口增量的间隔，随后以 REST 快照重建；快照后的首个增量接收时间比快照早约 90 毫秒，属于需要特殊处理的重放顺序问题。
- 官方恢复条件要求快照 ID 落入首个增量的 first_update_id 至 final_update_id 范围内；这次实测符合该条件。不能因此断言中断期间每个历史盘口状态可恢复。

证据：[首次深度审计 #43660914250](https://github.com/Huhabubu/mm/actions/runs/43660914250)；[校正快照桥接后运行 #43661167697](https://github.com/Huhabubu/mm/actions/runs/43661167697)；[官方 Binance Futures wire format](https://www.cryptohftdata.com/docs/wire-format-binance-futures)。

## Binance 官方 raw trades 对照：真实成交缺失

[官方档案交叉核验 #43661267167](https://github.com/Huhabubu/mm/actions/runs/43661267167) 成功从 Binance data.binance.vision 下载并读取 2026-10-15 BTCUSDT USD-M 永续 daily raw trades ZIP，共扫描 4,567,635 条官方市场成交；官方 aggTrades ZIP 也独立下载扫描。

| CryptoHFTData 缺口区间（UTC） | 跳过的 ID | Binance raw trades 中实际存在 |
|---|---:|---:|
| 12:30:02.993—05.192 | 9,556 | 9,554 |
| 12:30:05.625—07.878 | 5,044 | 5,043 |
| 12:31:55.326—57.480 | 2,353 | 2,353 |
| **合计** | **16,953** | **16,950** |

结论：至少有 16,950 笔真实市场成交存在于 Binance 官方档案，却不在 CryptoHFTData 同小时文件中。剩余 3 个 ID 在 Binance 官方 raw trades 中找不到。对于依赖 Binance 逐笔成交的跨所信号回测，应剔除或标记 UTC 12:30–12:32 的失真区间，并重新初始化订单簿状态。

## 仍须执行的质量检查

- [25 个日期×24 小时×4 所×2 数据类型=4,800 文件存在性 #43661071683](https://github.com/Huhabubu/mm/actions/runs/43661071683)。采用只取 1 字节的 HTTP Range 请求核查对象存在性与大小；存在性不能证明消息完整。
- [25 日预先轮换小时的内容审计 #43661485872](https://github.com/Huhabubu/mm/actions/runs/43661485872)，另抽查 Bybit 每日 UTC 02:00 交易流。
- 完整时段的 L2 序列、成交连续性和跨所时间戳仍有待更大规模核验。样本选择依据数据质量进行，不根据策略盈利结果挑选。
- 原始行情仅在 GitHub 临时执行器中处理，仓库保留汇总报告和可复现脚本。
