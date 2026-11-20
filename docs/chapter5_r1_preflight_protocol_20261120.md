# 跨交易所第5章 R1 前置修复与补审（2026-11-20）

执行分支：`exp/chapter5-cross-exchange`。本阶段仅允许 Bitget BTC/DOGE 既有50分钟工程冒烟、Gate 50分钟真实重放及 Gate 4币×6日数据延迟统计。**Bitget 24资产日全量HBT任务继续禁止创建和启动。** 不修改旧BTC论文主文与第5章已归档报告。

## 规则0：Gate目标量与Bitget最近合法量
对每个交易日的Gate前10分钟BBO参照中位价，按冻结Gate合约规格计算 Gate 当日委托目标基础币数量 `q_gate`。Bitget按候选合约最小数量增量及最小委托量，选择离 `q_gate` 最近的合法数量；中点取整固定为向上。该操作可能带来正负实际名义额偏差，必须记录 `gate_target_qty_base`、`bitget_actual_qty_base`、`quantity_signed_difference_base`、`nominal_difference_at_gate_reference_usdt` 及仓位倍数。库存上下限按Gate倍数（目前20）乘Bitget实际单笔委托量。同币种的Bitget历史数量步长仍为候选合约规格，未获得独立历史时点认证。

## C1：同微秒跨所事件冲突
原始 `visible_timestamps()` 在额外可见延迟后加1纳秒，保持严格事件排序。在新模式 `cross_timestamp_tie_mode=microsecond` 下，跟踪最近一次**HBT已实际消费的目标市场feed**时间与此次Binance变价的可见时间；以两者的 `//1000` 判断是否属于同一微秒。原 `legacy_ns` 模式保留原逻辑，用于冻结回归。冲突被拦截仅在跨所价差超触发阈值、Binance方向与相对变化量通过并且此前未激活时增加 `same_timestamp_ties_blocked`。另记录所有变价同微秒碰撞次数供诊断。要求真实50分钟回放输出两个数字；**单元测试中的人工构造冲突无法替代真实回放**。

## C2：粗Tick防交叉
新 `unified` 报价中买卖理论价落在同一合法Tick时：正库存保护卖价、退买价；负库存保护买价、退卖价；零库存用未取整的理论报价中心在合法Tick网格上对称包络（中心正好在一档时双侧各退一档）。DOGE 的 `dual_grid_quoted_ticks` 同步使用该方向规则，仍保留1e-6原始L2盘口与1e-5合法委托网格。老 `legacy` 规则不变。

## C3：有效交易段库存
100毫秒Recorder采样、平均绝对持仓、持仓RMS和`pos_std`只采用共享`forced_start_ns`起的实际交易段。训练和成功校准前的零库存等待不再稀释标准差；旧模式`forced_start_ns=0`保持原采样区间。Gate旧源码Golden回归必须验证买卖操作、成交及净损益完全一致。

## Gate六日采集延迟质量审计
读取真实Tardis normalized CSV：Gate四币 `quotes`（L2重建BBO）和同日Binance `book_ticker`（原生最优报价）。每个交易日全UTC区间，以 `local_timestamp-timestamp` 的非负样本计算与4A一致、精度0.1ms的P50/P95/P99；负值另记。称为**采集时间差**，不能解释为单程网络延迟。前5日数据按4A分类为100ms/20档快照，2026-11-01按原生增量分类；规范化CSV不能提供独立原始频道认证。只有24个真实完整结果齐全才形成汇总表。

## 统一新汇总报表
收益从官方HBT期末盯市Net和手续费重建Gross；`gross_bps = 10000*Gross / 成交额`。500ms/1000ms的`Total Markout`基于真实成交价格与之后可观察中间价，记录样本数；库存标准差用基础币并同时报告 `pos_std/order_qty`。同一个成交记录静态映射0bps、-0.5bps、+2bps三档Maker费率（含返佣的负费率）。

相对同日`glft`的含费净收益差应严格等于 `fee_saved+vol_shrink_gross+quality_gross`。三个分项分别为基准减新模型手续费、因缩量减少基准每单位成交毛亏损、信号模型单位成交毛收益变化。逐日验证残差数值上约零，再按**独立交易日等权**汇总；不得把池化成交数量取代等权天数。

## 验收门禁
1. 离线单测：微秒冲突、库存方向、防止等待期采样、最近合法量及三项会计恒等式。
2. 真实Gate 50分钟`same_timestamp_ties_blocked`输出，确认确实发生；Bitget原有BTC/DOGE 50分钟冒烟和Gate旧Golden持续成功。
3. Gate 24资产日采集延迟P50/P95/P99与Binance逐行并列，禁止虚构未完成的分位数。
4. 审计结果未到齐前，**R1不判定通过、Bitget 4B全日仍冻结**。
