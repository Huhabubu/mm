# 第5.3节阶段四4A：跨交易所真实Tardis前置数据门禁

已产生审计文件 1/9。仅审计真实CSV，未执行HBT、成交或策略收益回测。

Tardis采集延迟 = local_timestamp − timestamp（毫秒），其包含网络、交易所发布节拍与时钟同步误差；不可直接解释为纯网络单程延迟。

| 交易所 | UTC日期 | 四份CSV | 归一化盘口/时钟门禁 | L2快照组/额外重建组 | L2最大间隔ms | 本所 quotes P50/P95/P99 ms | Binance book_ticker P50/P95/P99 ms | 本所−Binance P50/P95/P99 ms |
|---|---|---|---|---|---:|---|---|---|
| bitget-futures | 2026-06-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| bitget-futures | 2026-07-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| bitget-futures | 2026-08-01 | 完整 | 阻断 | 8/7 | 731.807 | 6.15/11.65/19.05 | 2.75/7.55/87.45 | 3.4/4.1/-68.4 |
| bitget-futures | 2026-09-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| bitget-futures | 2026-10-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| bitget-futures | 2026-11-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| okex-swap | 2026-09-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| okex-swap | 2026-10-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |
| okex-swap | 2026-11-01 | 缺失 | 阻断 | None/None | None | NA/NA/NA | NA/NA/NA | NA/NA/NA |

## 重建与断线风险

额外快照组是重建/重订阅的代理，精确WebSocket断线次数不能从免费归一化CSV观察。

| 交易所 | 日期 | 初始Bid/Ask档数 | 原生归一化增量行 | 额外快照组 | L2超过3秒间隔数 | L2超过10秒间隔数 | 阻断理由 |
|---|---|---|---:|---:|---:|---:|---|
| bitget-futures | 2026-06-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| bitget-futures | 2026-07-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| bitget-futures | 2026-08-01 | 1000/1000 | 38463135 | 7 | 0 | 0 | crossed_book_in_sample; market_data_gap_gt_10s=2 |
| bitget-futures | 2026-09-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| bitget-futures | 2026-10-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| bitget-futures | 2026-11-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| okex-swap | 2026-09-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| okex-swap | 2026-10-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |
| okex-swap | 2026-11-01 | None/None | None | None | None | None | No actual CSV audit artifact for expected date |

## 合约规格、单位与标准费率（分离历史认证状态）

| 交易所 | 候选Tick(USDT) | 最小下单量(单位) | 数量步长(单位) | 合约乘数(BTC/单位) | 标准Maker/Taker bps | 统一Maker bps | 六日历史逐日规格认证 |
|---|---:|---|---|---:|---|---:|---|
| bitget-futures | 0.1 | 0.0001 BTC | 0.0001 BTC | 1 | 2.0 / 6.0 | 2.0 | 未逐日认证 |

### Tardis实际成交数量与候选规格步长的交叉检查

| 交易所 | 日期 | 实测最小成交数量 | 不满足候选数量步长的成交行 | 交易价偏离候选Tick行 |
|---|---|---:|---:|---:|
| bitget-futures | 2026-06-01 | None | None | None |
| bitget-futures | 2026-07-01 | None | None | None |
| bitget-futures | 2026-08-01 | 0.0001 | 0 | 0 |
| bitget-futures | 2026-09-01 | None | None | None |
| bitget-futures | 2026-10-01 | None | None | None |
| bitget-futures | 2026-11-01 | None | None | None |
| okex-swap | 2026-09-01 | None | None | None |
| okex-swap | 2026-10-01 | None | None | None |
| okex-swap | 2026-11-01 | None | None | None |

## 归一化数据通过率（与4B许可不同）

| 交易所 | 预定日 | 四类CSV齐全 | 归一化门禁通过 | 能否自动进入4B |
|---|---:|---:|---:|---|
| bitget-futures | 6 | 1 | 0 | 暂不许可：历史规格、原生频道和策略HBT适配未全部独立认证 |
| okex-swap | 3 | 0 | 0 | 暂不许可：历史规格、原生频道和策略HBT适配未全部独立认证 |

## 研究约束

- 本报告中的L2格式一致性仅代表归一化CSV的初始快照+增量可重建表现；并非“绝无网络断层”。
- 默认Maker统一2bps；Bitget官方标准Maker 2bps/Taker 6bps，OKX官方例示Maker 2bps/Taker 5bps。
- Bitget BTCUSDT数量通常按BTC数量计量；OKX BTC-USDT-SWAP为合约张数，约0.01 BTC/张。历史规格需要交易所时点证据。
- 如果本所与Binance采集延迟分布显著不同，需分析采集频率/时间戳定义/交易所时钟，不可直接归因跨海网络。
- 4B必须单独实现并验证Bitget与OKX的HBT合约单位、L2适配及成交假设，用户要求4A通过前禁止自动启动。
