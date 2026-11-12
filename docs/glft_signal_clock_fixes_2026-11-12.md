# 2026-11-12 GLFT 跨所信号报价、tick 统计和时间轴审计修复

基线：原受影响提交 [`86bdec2`](https://github.com/Huhabubu/mm/commit/86bdec2ca4e8af143ce84c4e01058dbb9887d28c)；本轮在主线 `b93a49c` 之后修复。仅修复报价参照、统计尺度、时间一致性防护及对应测试，不重设 GLFT 风险参数、`A/k`、成交假设、2.5ms Binance 延迟、20ms 撤改单限制或手续费。

## 问题 1：目标交易所报价中间价使用两个版本（影响交易行为，P0）

- **原实现**：`causal_price_shift_ticks` 按 `searchsorted(..., side="left") - 1` 读取严格早于决策时刻的**原生目标交易所 BBO** 中间价；`run_event_glft` 同时用当前 `engine.depth(0)` 中间价建立 GLFT 保留价格。因此目标市场在当前时刻上涨或下跌后，跨所偏离可能再次包含目标市场刚刚完成的价格变化。
- **修复**：每次报价从同一个、已被 HBT 引擎消费的目标盘口取得 `mid`（tick 单位），同时用于 `reservation` 和 `causal_price_shift_ticks` 中计算跨所偏离（换算到 USDT/标的币）。原生目标 BBO 时间戳保留为 **250ms 新鲜度门槛**，不再提供跨所价差中的目标价格；历史 60s 已完成秒级基差仍严格因果读取。盘口保护和 GLFT 库存项保持不变。
- **合成回归**：Binance=110、当前引擎目标中间价=110、此前原生目标中间价=100、基差=0、β=1，报价平移必须为0；另外覆盖原生与融合盘口不一致、目标尚未追上的情况。此例仅检验代码，不代表实证数据。

## 问题 2：SOL 的“一 tick”比例误用 BTC 价格档（报告口径，P1）

- **原实现**：`quote_gap_coverage` 无条件按常量 `TICK=0.1` 检查 `|G| >= 1 tick`。OKX SOL应为0.01，Bitget SOL应为0.001（USDT/SOL）。
- **修复**：统计函数接受 `tick_size`，四市场运行器显式传 `spec.tick`；原单市场BTC调用保留 `TICK=0.1` 默认，避免破坏旧接口。回归测试覆盖四市场下0.02 USDT价差的判定。
- **影响**：只修复`fraction_abs_gap_at_least_one_tick`统计；原跨所交易报价本已传入对应市场tick。

## 问题 3：HBT 目标时钟可能单独平移（条件性风险，P1）

- **机制**：官方 `hftbacktest.data.validation.correct_local_timestamp` 在数据存在负 `local_ts-exch_ts` 时，可能整体平移转换后的目标市场本地时间。信号数组由原生CSV的本地时间产生，若只平移HBT目标数据，两条时间轴失配。
- **修复**：`extract_window` 在**实际写给转换器的行**上统计 `min_feed_latency_us` 和 `negative_feed_latency_rows`；`require_unshifted_target_clock` 在调用 `convert_fuse` / 分段转换**之前**检查全部三类目标行情（trades、incremental_book_L2、book_ticker）。若有负延迟则明确抛错并停止本轮配对回测；已在四市场与BTC独立信号运行器均接入。保持数据原有时间戳，不擅自给Binance信号追加同等偏移。
- **范围**：此前[24任务回测](https://github.com/Huhabubu/mm/actions/runs/43198245510)日志未显示官方整体平移输出，因此不声称旧结果已经被此项污染。若将来检验其他转换器或不同的数据输入途径，需要扩展对原始时间戳的监测。
- **合成回归**：接收时间比交易所时间早5微秒则中止、晚5微秒允许通过。

## 验证与论文结果口径

- 单元测试：`test_beta_one_direct_gap.py`、`test_gap_filter_placebo.py`、`test_multivenue_glft.py`、`test_check_okx_hft_data.py`；这些是**合成样本逻辑回归**，检验修复后行为，不构成真实回测成绩。
- 完整回归还应运行事件驱动策略原有测试、四市场官方HBT回放，并比对β0/β1以及带局部极大值的四模型，关注扣手续费净盯市收益、成交/撤单、100/500ms成交后有向回报、数据时间对齐。
- 原[2.5ms四市场历史收益表](multivenue_glft_results_2p5ms_2026-11-12.md)和[成交归因分析](glft_recorder_pnl_markout_attribution_2p5ms_2026-11-12.md)为**修复前实现**；保留溯源，不把旧结果当作修复后Binance领先性证据。修复后历史回测尚需单独执行、记录新运行ID，才可更新论文结论。
- 当前β固定1、尚未完成回归估计；资金费率、平仓滑点和真实跨所通信延迟仍是研究限制。
