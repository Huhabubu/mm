# V4：紧急撤单、因果波动率与两级毒流过滤的单日审计
2026-11-14｜独立分支 research/sparse-source-defense-20261114

**基线约束**：main 仍是 619294c3d34a2b7ed7387f472ac0192cabaa807e；原正式脚本、旧研究报告和 2.5ms 归档保持不变。2026-06-01 属于反复研究过的探索日。**本轮没有运行六日期防御收益回测。**

## 1. 任务1：立即撤单，常规改价继续节流

研究执行文件：[engine](../scripts/experiment_source_defense_engine.py)。

danger-side 进入或维持防御时，实际已有订单若可撤（HBT order.cancellable）且尚未离开危险位置，立即调用官方 HBT engine.cancel。此时绕过 MIN_REQUOTE_NS=20ms 的普通改价节流。保护后的重新挂单、非危险侧普通改价和解除防御后的改价仍受20ms限制；保护期间安全侧已经排队的挂单不因Binance防御而被撤单/重报。已在更外侧的订单不被调回内侧。

[聚焦撤单测试](../scripts/test_experiment_source_defense_emergency_cancel.py)：模拟 t=0ms 提交Ask，t=2ms可撤且Binance向上脉冲，直接在t=2ms调用引擎撤单指令；普通改价要等t=20ms，重复在途取消被抑制。单元测试调用的是引擎同一撤单决策函数，市场成交/撤单生效则仍须等待官方HBT延迟。真实三币回放也记录了非零紧急撤单数，例如SOL z1_cancel_shield为Bid59、Ask53次，BTC z2_cancel_both为Bid216、Ask226次。

[本轮正式聚焦测试33例全部通过JSON](../reports/source_defense/v4_tests/unit_tests.json) · [测试MD](../reports/source_defense/v4_tests/unit_tests.md) · [CI #43552864235](https://github.com/Huhabubu/mm/actions/runs/43552864235)。

## 2. 任务2：1000PEPE G0条件Markout与自适应相对尺度

[PEPE只读分桶代码](../scripts/experiment_source_defense_pepe_g0_buckets.py) · [结果JSON](../reports/source_defense/v4_pepe_g0_buckets/pepe_g0_buckets.json) · [结果MD](../reports/source_defense/v4_pepe_g0_buckets/pepe_g0_buckets.md)。

Binance 1000PEPEUSDT中间价先除1000，换成USDT/PEPE后与OKX原生PEPE-USDT-SWAP同步比较。基于真实官方HBT原G0的192笔成交，在成交交易所时间减1ms的严格历史可见BBO与1s EMA条件下：

| 触发绝对价差 | 成交数 | 100ms平均Markout | 500ms平均Markout |
|---|---:|---:|---:|
|未触发|31|−2.4394bps|−2.8148bps|
|0.5–1.0bps|26|−2.9092bps|−2.3508bps|
|1–2bps|60|−2.8628bps|−2.4747bps|
|>2bps|20|−3.0548bps|−3.0547bps|

另有50笔（26.0%）触发标签不可评估，0.2–0.5bps仅5笔，<0.2bps没有有效触发笔数；单日样本量不能支持自动确定稳定高毒阈值。分桶是成交前BBO价格状态代理，不能冒充原始挂单决策时间或真实主动吃单因果证据。

[自适应信号代码](../scripts/experiment_source_defense_signal.py)依据目标所纯预热窗口100ms中间价收益标准差，固定z倍数乘波动率并实行max(0.3bps,1Tick的bps比例)下界。退出回差和单侧外推幅度以实时目标中间价换算bps，报价外推换为至少一个原生Tick。

|币种|预热100ms收益标准差|z1实际gap/impulse|z2实际gap/impulse|
|---|---:|---:|---:|
|BTC|0.1687bps|0.300/0.300bps|0.422/0.300bps|
|SOL（20min）|0.2657bps|1.211/1.211bps|1.211/1.211bps|
|1000PEPE|0.4725bps|0.709/0.473bps|1.181/0.709bps|

**关键限制：SOL在两组z下都由约1.2106bps的一Tick下界决定门槛，100ms波动率受粗Tick离散化压低。不能声称z2已经让SOL自然实现1.5–2.5bps高毒触发。**

## 3. 任务3：Queue Shield与Binance主动吃单的正交消融

[真实Tardis trades读取器](../scripts/experiment_source_defense_trade_flow.py)、[严格过去可见性测试](../scripts/test_experiment_source_defense_trade_flow.py)。Tardis trades的side是主动吃单方，price乘amount是Binance标的报价单位下的USDT成交名义金额。使用local_timestamp+2.5ms额外可见延迟，决策只读严格早于当前时刻的过去100ms记录，要求同方向净主动成交额占总成交额至少35%且累计成交额至少100USDT。BTC/SOL/1000PEPE在本次下载窗内分别检索到132541/77848/39305条Binance主动成交记录。

Queue Shield使用HBT重建的目标所BBO危险侧一档数量，在预热历史中得到该侧P50分位数；常规危险信号仅在本侧盘口较薄时防御，绝对缺口至少为常规gap阈值2倍时视为预先定义的极端例外。无外部额外盘口数据，严格不使用交易之后的深度信息。

[单日真实HBT入口](../scripts/experiment_source_defense_v4_pilot.py) · [汇总脚本](../scripts/experiment_source_defense_v4_aggregate.py) · [三币详细消融JSON](../reports/source_defense/v4_aggregate/v4_ablation_summary.json) · [完整表MD](../reports/source_defense/v4_aggregate/v4_ablation_summary.md)。

每市场固定 2组z参数 × 2种动作（撤单/外推）× 4类微结构过滤（无/Queue/Trades/两者）+ 同窗G0，共17种。所有比较模型的GLFT参数重新拟合时序一致；Binance增加2.5ms可见延迟，订单请求和回报各1ms，队列及成交均由官方HBT模拟。所有经济结果已扣Maker2bps手续费。

|市场与测试窗|方案|成交/保留率|撤单|扣费净PnL USDT|100ms Lift|500ms Lift|
|---|---|---:|---:|---:|---:|---:|
|BTC 00:10–01:00|G0|296/100%|3169|−7.1619|—|—|
|BTC|z2 cancel none|148/50.0%|2913|−3.8863|−0.0903|−0.0336|
|BTC|z2 cancel both|154/52.0%|2912|−4.0154|−0.1084|−0.0287|
|SOL 00:20–01:10|G0|200/100%|1999|−4.6530|—|—|
|SOL|z1 cancel none|164/82.0%|1913|−3.8215|+0.2249|+0.1590|
|SOL|z1 cancel shield|168/84.0%|1912|−3.9344|+0.2437|+0.1666|
|SOL|z1 cancel trades|164/82.0%|1913|−3.8215|+0.2249|+0.1590|
|SOL|z1 cancel both|168/84.0%|1912|−3.9344|+0.2437|+0.1666|
|PEPE 00:10–01:00|G0|192/100%|1099|−4.9072|—|—|
|PEPE|z1 cancel trades|125/65.1%|1449|−3.0573|+0.2365|+0.4686|
|PEPE|z2 cancel both|145/75.5%|1224|−3.7249|+0.1778|−0.1073|

消融结果：SOL的Queue Shield将z1 cancel成交从164→168（加4笔），100ms/500ms Lift约从+0.225/+0.159→+0.244/+0.167bps；再加Binance主动成交过滤，在这一天**没有进一步改变实际成交与最终净收益**。PEPE的z1 cancel加入Binance主动成交流使成交107→125笔，500ms Lift+0.197→+0.469bps，其单日30s区块重抽样95%区间约[+0.100,+0.898]bps，但成交保留率65.1%，未达最低75%。BTC所有预设方案均缩量严重，且100ms Lift为负。

## 4. 任务4：逐币红线及五月训练、六月验证的等量G0

最接近描述性红线的是**SOL z1 cancel shield**：成交保留84%、撤单1912（G0为1999），100/500ms选择性均值+0.2437/+0.1666bps，P10保留减被排除分别约+0.00117/+1.19647bps。**100ms尾部改善几乎为零；500ms Lift的单日30秒区块95%区间为[−0.245,+0.509]bps，跨越零**，所以不能证明显著跨所增量信息。该策略Added新成交11笔、500ms均值Markout约−1.340bps。使用HBT净收益与成交额的精确会计桥接，其成交额缩放后剩余毛差约−0.026USDT，仍混合库存、执行与队列效应，不是纯因果Selection Alpha。

本次单独尝试了SOL同为20min研究校准的严格等量G0：[训练与测试入口](../scripts/experiment_source_defense_equal_volume_sol20.py) · [May冻结训练JSON](../reports/source_defense/v4_equal_volume_SOL/sol20_equal_volume_train.json) · [June评估JSON](../reports/source_defense/v4_equal_volume_SOL/sol20_equal_volume_test.json) · [MD](../reports/source_defense/v4_equal_volume_SOL/sol20_equal_volume_test.md)。

仅在2026-05-01对原G0静态半价差额外加宽整数0/1/2/3/4个Tick，五档成交为20/8/4/2/4笔。按**事前固定80%训练成交保留目标**，最接近的是0 Tick。冻结到2026-06-01，则G0成交200笔，候选SOL z1_cancel_shield成交168笔，成交数差19.05%、手续费差19.05%，**严格2%与近似3%均未达标**。没有针对六月再调整参数，因此本轮没有合格的Equal-Volume反事实、没有可信纯Selection Alpha的单独识别。

## 5. 最终判定与停止条件

- 任务1紧急撤单20ms阻塞修复：通过，含真实HBT紧急请求和t=2ms确定性测试。
- 任务2 1000PEPE标准化价格/原G0分桶：已完成，26%标签仍因行情有效性门槛缺失；三币动态bps阈值已真实执行，SOL仍被1Tick底线主导。
- 任务3 Queue Shield与Binance taker trades：真实单日独立消融完成，市场间增量方向不一致，未发现稳定净利润证据。
- 任务4 BTC：成交保留率和Selectivity Lift未达标。SOL：成交与选择性均值描述性过线，但置信区间跨0且等量G0匹配失败。PEPE：高Lift组缩量过多，保留率合格组500ms Lift为负。
- **不运行六日期全量防御PnL回测。** 未来须先做独立日显著性、真实撤单ACK/排队、历史1000PEPE合约元数据与资金费率审计，避免依据同一个已探索日期调参制造虚假跨所优势。

底层完整成功工作流：[3币HBT #43552330145](https://github.com/Huhabubu/mm/actions/runs/43552330145)、[反事实汇总 #43552521724](https://github.com/Huhabubu/mm/actions/runs/43552521724)、[PEPE分桶 #43552461750](https://github.com/Huhabubu/mm/actions/runs/43552461750)、[May→June等量G0 #43552690118](https://github.com/Huhabubu/mm/actions/runs/43552690118)、[聚焦33单测 #43552864235](https://github.com/Huhabubu/mm/actions/runs/43552864235)。
