# Bitget 第5.3节 4B 先行验证：合约等值、双层网格及工程限制

2026-11-20；研究分支：exp/chapter5-cross-exchange。本文记录先行 BTC/DOGE 2026-08-01 各50分钟的设计与可检验标准。**OKX冻结；不启动24资产日正式实验。**

## Gate合同数量转Bitget基础币

Gate冻结合同来自 configs/contract_specs.json。每天 Gate 的数量取自原第5章 scenario()：将约80 USDT目标按前10分钟 Gate 原生BBO的中位数换算并在 Gate 合约数量步长上取整；持仓上限是单笔数量20倍。采用同一日、同一币种的原Gate合同及中位价，而非独立的 Bitget 80 USDT取整。

公式（符号单位定义）：
\[
q_{\rm base}=q_{\rm Gate}\times c_{\rm Gate},\qquad
q_{\rm Bitget}=q_{\rm base},\quad
Q_{\rm Bitget}=20q_{\rm Bitget}.
\]
其中 q_Gate 是 Gate 冻结单笔合约数量（Gate数量单位），c_Gate 是每Gate单位的基础币数量（基础币/Gate单位），q_base 和 q_Bitget 是单笔实际基础币数量，Q_Bitget 是允许最大持仓的基础币数量。

| 资产 | Gate最小委托增量 | Gate乘数（基础币/Gate单位） | Gate基本币增量 | Bitget候选委托数量步长（基础币） | 是否可无损等值 |
|---|---:|---:|---:|---:|---|
| BTC | 1张 | 0.0001 BTC | 0.0001 BTC | 0.0001 BTC | 两所网格兼容 |
| ETH | 0.1 Gate单位 | 0.01 ETH | 0.001 ETH | 0.01 ETH | **并非全部日期可精确等值** |
| SOL | 0.1 Gate单位 | 1 SOL | 0.1 SOL | 0.1 SOL | 两所网格兼容 |
| DOGE | 1 Gate单位 | 10 DOGE | 10 DOGE | 1 DOGE | 两所网格兼容 |

在ETH约3000 USDT的简化数量示例中，Gate目标可能为0.027 ETH，而Bitget候选下单步长为0.01 ETH；最邻近可执行数量0.03 ETH相差约11%。该示例是**简化示例，不是论文任何实证日期结果**。适配器在发现该差异时拒绝执行，绝不擅自放大/缩小仓位。历史Bitget下单数量步长需要时点合约数据再次确认。

统一研究手续费：正Maker 0.0002（2 bps，万分之二）；Bitget公开标准Taker参考0.0006（6bps）。HBT资产数量在Bitget使用基础币单位，即 linear_asset(1)，库存风险系数按每基础币单位和合法报价Tick换算，使风险表达与Gate的基础币等值库存一致。真实交易账户手续费或当日完整合约规则没有独立历史认证。

### 两个真实8月1日样本的Gate对照数量

| BTC | Gate单笔13张 × 0.0001 BTC/张 = **0.0013 BTC** | Bitget单笔0.0013 BTC | Gate上限260张 = 0.026 BTC；Bitget上限0.026 BTC |
| DOGE | Gate单笔115单位 × 10 DOGE/单位 = **1150 DOGE** | Bitget单笔1150 DOGE | Gate上限2300单位 = 23000 DOGE；Bitget上限23000 DOGE |

以上是官方HBT真实测试前从冻结Gate合同及相应日期前10分钟BBO计算出的数量，不应把此处的同数量误解为两个交易所相同成交额、相同实际成交频率或相同手续费总额。

## DOGE底层与策略网格

- 无损原始L2及官方HBT：tick_size=0.000001 USDT，六位价格保留独立价位与原本数量；不得四舍五入合并。
- 策略物理报价Tick：quote_tick_mult=10，即合法报价刻度=0.00001 USDT。100ms时间窗中，穿透深度分桶、价格波动sigma、半价差与库存偏移均先换算成合法报价Tick；Binance引导偏移也除以合法报价Tick。
- GTX Post-Only：取整只作用于实际订单候选，买单报价严格小于底层HBT对手卖价，卖单严格高于对手买价；即使对手最优价为六位小数，最终提交订单必须仍是0.00001的整数倍。
- Gate旧路径：quote_tick_mult默认1，选原先 quoted_ticks 函数及一致的其他价格计算；用阶段一源提交与当前引擎的实际HBT Golden动作/净收益回归检查。

测试还必须在**完整相同local_timestamp的WebSocket更新全部价位行应用后**检查非交叉盘口，避免把同消息内部临时中间状态误判为成交可见的交叉盘口。

注意：成功以六位原始Tick完成HBT回放，不能证明历史遗留价格都是合法的新订单。对4A中DOGE的深档价格的合法性仍只属于未证实的历史遗留委托假说。

## 参数与风控

统一新配置：min_requote_ns=0、fit_mode=physical_1tick_poisson、wait_book_after_fill=True、post_only_clamp_mode=unified、cap_depth_at_visible_levels=False。保持Binance BBO额外可见延迟假设2.5ms，HBT提交与回应各1ms，power_prob_queue_model(2.0)及no_partial_fill_exchange。各模型独立使用一个真实HBT引擎；同日首个有效校准时刻从不下单的探针确定并强制一致。

**全日后续门禁**：先验收两场50min真实样本、Gate黄金回归，再处理ETH数量步长的潜在冲突，之后才允许设计24资产日批量任务。届时敏感性统计须包括断线/额外快照、L2大于1秒间隔前后±5秒成交窗口，以及剔除10月异常两小时、DOGE严格三日与六日双层网格对照。删除事件窗口内的成交只是事后成交质量与收益归因切片，不能直接当作一个重新回测的自洽可交易组合净收益。
