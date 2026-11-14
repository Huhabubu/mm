# Sparse Source-Attributed Defense V5：P80净主动成交流、Random-Pause与六日期检验

日期：2026-11-14。工作分支：research/sparse-source-defense-20261114。正式main保持619294c3d34a2b7ed7387f472ac0192cabaa807e。旧正式2.5ms基准脚本和原归档研究报告均未更改；所有V5新输出为独立研究结果。**不将这六个曾用于参数准入的日期视为完全样本外。**

## 0. 最重要的结论

- **四目标市场 × 六日期的24份官方hftbacktest全部完成。** SOL OKX与DOGE OKX使用20分钟预热及3+档研究校准，1000PEPE OKX和Bitget使用10分钟原5+档；每个50分钟交易窗与各自G0对照一致。
- SOL、DOGE及OKX PEPE的Z1单侧撤单+Queue Shield在四个期限多数平均Selectivity Lift为正，但两阶段95%置信区间均跨越0；Bitget PEPE的100/500ms Lift为负。没有形成足以证明跨交易所做市选择性优势的独立日证据。
- 高强度Binance主动成交确认使用目标日期**预热期**100ms单向净吃单名义额的P80分位数，并保持方向比门槛35%、最低总名义额100 USDT；在不同日期实际改变了订单成交，已避免旧单纯同号过滤空转。但并不始终提高选择性、也不构成因果归因。
- May事前固定的Random-Pause G0五种种子在四市场的成交/手续费误差均**未达到2%-3%**，因此不能宣称严格等成交量，更无法将收益桥接残差解释为纯Selection Alpha。
- BTC在0ms理想化额外信号可见延迟和原2.5ms下，所测100/500ms Lift依然为负，故2.5ms并非现有单日失败的充分解释。
- 所有收益展示的主口径为**含2bps Maker费用的官方HBT净收益**；零费与-0.2bps返佣仅作为明确标记的敏感性对照。各市场六日净收益总和仍为负。

## 1. 任务1：P80主动成交流与PEPE缺失标签

[高强度Tardis主动吃单读取器](../scripts/experiment_source_defense_trade_flow.py)使用标准CSV中的主动买/卖方向、每笔成交价×数量USDT名义额和local_timestamp+2.5ms假设到达时刻。每次决策仅用过去100ms严格早于当前时刻的可见成交。用纯预热期100ms固定时间网格下出现过正向净买额的窗口计算买方P80，用负向净卖额的绝对值窗口计算卖方P80；强度通过需同时满足同方向净金额≥该侧P80、净额/总成交额≥35%及该窗成交名义额≥100USDT。独立固定对照为Z1 cancel QS和同策略+P80，阈值只读预热，不以测试成交质量优化。

|市场|六日Z1+QS成交|六日Z1+QS+P80成交|P80过滤的成交增量|
|---|---:|---:|---:|
|SOL OKX|626|632|+6|
|DOGE OKX|702|710|+8|
|1000PEPE OKX|924|994|+70|
|1000PEPE Bitget|356|372|+16|

注意：P80条件阻断了部分源信号，但由于有保持窗口、已有在途订单和队列状态，过滤事件数并不等于被避免的成交数；例如SOL在六月两版仍有完全相同的最终成交。该结果证明增强过滤已发生决策效果，不证明其边际收益显著。

[1000PEPE G0分桶与原因排查](../scripts/experiment_source_defense_pepe_g0_buckets.py)使用原官方HBT的192笔G0成交，Binance 1000PEPEUSDT报价先除1000再与目标OKX PEPE-USDT-SWAP比。原250ms严格时效有142笔可评估、50笔缺失；其中39笔位于无有效EMA网格，另有11笔Binance过去100ms起点的原生BBO过旧。进一步拆分39笔：11笔的EMA最新网格缺失但250ms内曾存在有效EMA；28笔连250ms内的有效EMA也缺失。然而因其他原生报价时效约束，单独使用最近有效250ms EMA仍是**142/192**。将原生BBO最大可用年龄暂时扩大到2秒、EMA仍限250ms的只读敏感性才达到**164/192（85.4%）**，余28笔不可评估。该2秒版本**未用作防御策略的新可见性假设**，正式策略继续250ms时效。

对应结果：[PEPE条件分桶JSON](../reports/source_defense/v4_pepe_g0_buckets/pepe_g0_buckets.json)、[MD](../reports/source_defense/v4_pepe_g0_buckets/pepe_g0_buckets.md)；超过2bps组原20笔的100/500ms Markout均约−3.055bps，但不能据单日推断普适阈值。

## 2. 任务2：Source-blind Random-Pause G0

[研究HBT引擎](../scripts/experiment_source_defense_engine.py)新mode4使用固定50ms随机决策时钟（由本地时间确定），危险侧Buy/Ask方向通过确定性种子随机生成；禁止读取Binance中间价、Binance消息时钟、Binance主动成交额或当前差值来生成停报。暂停期间仅立即撤销危险侧已可撤订单，不重新提交；到期后正常20ms改价/提交节流，HBT负责1ms订单请求与回报、GTX Post-only、成交与排队。若已有订单在途，不能重复撤销。

训练步骤只用**2026-05-01**四市场历史窗口，估计原Z1 QS防御实际时间占比、平均连续保护时长及Ask/Buy时间占比；固定计算每50ms停报机会概率与按历史比例的单边方向概率；每测试日期相同五个固定种子11、29、47、73、101。五月训练不访问六月及之后任何成交或价格。其独立记录见[五月校准工作流](https://github.com/Huhabubu/mm/actions/runs/43554528277)。随机概率由事前占用时间近似而来；不保证每个日子实际有同等填单量。

|市场|Z1+QS六日成交|Random-Pause五种子六日均值±SD|成交误差|手续费误差|严格等量|
|---|---:|---:|---:|---:|---|
|SOL OKX|626|733.2 ± 4.1|17.12%|17.10%|失败|
|DOGE OKX|702|794.8 ± 7.6|13.22%|13.21%|失败|
|PEPE OKX|924|1177.0 ± 7.3|27.38%|27.17%|失败|
|PEPE Bitget|356|442.2 ± 1.5|24.21%|24.16%|失败|

**结论：所有市场2%-3%阈值均不达标**。不得标记Equal-Volume，也没有纯Selection Alpha因果识别。Random-Pause五种子均值/标准差、真实每日持仓与每期限费前Gross Edge、AS Cost等列在六日期完整JSON中。

## 3. 任务3：BTC单日高门槛与0ms/2.5ms边界

[BTC诊断脚本](../scripts/experiment_source_defense_v5_btc_boundary.py)、[JSON](../reports/source_defense/v5_btc_boundary/btc_latency_boundary.json)、[MD](../reports/source_defense/v5_btc_boundary/btc_latency_boundary.md)；[成功真实HBT回测](https://github.com/Huhabubu/mm/actions/runs/43554904489)。

|额外BN延迟|BTC模型|保留率|100ms Lift|500ms Lift|
|---|---|---:|---:|---:|
|2.5ms|Z1 cancel QS|48.7%|−0.020bps|−0.022bps|
|0ms|Z1 cancel QS|46.7%|−0.033bps|−0.048bps|
|2.5ms|0.8bps + P80 cancel|74.0%|−0.010bps|−0.083bps|
|0ms|0.8bps + P80 cancel|74.0%|−0.010bps|−0.083bps|
|2.5ms|1.0bps + P80 widen|80.7%|−0.098bps|−0.119bps|
|0ms|1.0bps + P80 widen|80.7%|−0.087bps|−0.082bps|

此处的G0不消费Binance价格或事件时钟，六日研究一致；BTC本次G0为300笔，与V4旧G0使用Binance消息时钟的296笔口径不同。0ms是假设零额外Binance接收延迟（交易订单请求与ACK仍为1ms），**不能证明现实东京—香港套利链路边界**。两条件都未出现100/500ms选择性转正，现阶段应检查源信号经济含义与队列/反事实匹配，而不只归罪于2.5ms延迟。

## 4. 任务4：六个历史日期，四市场独立日汇总

[六日期研究主脚本](../scripts/experiment_source_defense_v5_multiday.py)、[六日期合并及两阶段Bootstrap](../scripts/experiment_source_defense_v5_aggregate.py)；[成功的24份官方HBT及汇总](https://github.com/Huhabubu/mm/actions/runs/43554787965)。

固定历史日期：2026-06-01、07-01、08-01、09-01、10-01、11-01，每天50min。SOL与DOGE使用00:20–01:10 UTC研究版20min预热；PEPE两个交易所使用00:10–01:00 UTC原5档10min。3/4有效档若需要先验只取严格早于当日的之前有效日期k，逐日来源来自上一阶段六日准入JSON；正式原拟合基线不变。历史合约规格仍是用当前公开instrument元数据比对历史价阶，尚未独立存证。OKX/Bitget合同张数、基础币金额、Binance1000PEPE×1000规格均逐市场转换。

每日期统计原G0（不看BN价格或消息时钟）、事件时钟G0（控制刷新）、Z1 cancel QS、Z1+P80、BN-gap条件下OKX本地冲击安慰剂、五种Seed Random-Pause。**本地安慰剂的条件A仍使用BN快价差，仅条件B改由OKX本地价格冲击驱动；不可称完全源盲本地对照。**

### 4.1 逐币成交、撤单和含费经济结果

|市场|G0总成交/撤单|Z1+QS总成交/撤单|六日平均G0含费净PnL USDT/日|六日平均Z1+QS含费净PnL USDT/日|
|---|---|---|---:|---:|
|SOL OKX|732 / 7839|626 / 7764|−2.8686|−2.4551|
|DOGE OKX|814 / 7230|702 / 7187|−3.3317|−2.7935|
|1000PEPE OKX|1199 / 10763|924 / 12663|−5.4098|−3.9273|
|1000PEPE Bitget|446 / 66277|356 / 64074|−1.8550|−1.5565|

所有净收益依然为负，当前经济改善大部分来自减少成交的收费和交易额缩放，不能当作策略已获得正交易优势。特别是PEPE OKX总撤单从10763升至12663，没有满足“撤单接近原G0”的严格要求；Bitget提交/撤单次数很高，需审计真实限速与订单队列。

### 4.2 选择性：成交时间+方向配对，六日期聚合

选取原G0独有的Dropped成交与共有Kept成交，Selectivity Lift=Kept未来标记收益均值减Dropped收益均值，单位bps；同日原始成交时间和方向一对一配对，不保证相同挂单ID，故是观察性归因。四期限为100ms/500ms/1s/5s。区间按六个UTC日期为一层、日内30秒时间块为第二层的2000次固定种子bootstrap估计。六日期全部曾用于之前的参数准入诊断，不能当严格样本外。

|市场|Z1+QS保留率|500ms有效Dropped|100ms Lift|500ms Lift|500ms 95%Bootstrap区间|500ms P10改善|
|---|---:|---:|---:|---:|---|---:|
|SOL OKX|85.52%|138|+0.2865|+0.2012|[−0.2714,+0.5462]|+0.2285|
|DOGE OKX|86.24%|160|+0.3259|+0.3370|[−0.8919,+1.2119]|+0.7979|
|PEPE OKX|77.06%|422|+0.1576|+0.2373|[−0.5024,+0.8154]|**−1.0375**|
|PEPE Bitget|79.82%|116|−0.5846|−0.5157|[−1.9589,+0.0931]|**−1.3034**|

**没有市场的100ms和500ms联合选择性已满足六日期区间全正的证据标准**。SOL虽均值和500ms P10为正但只有138个有效Dropped。DOGE六日G0 814笔中6月单日522笔，簇分布不均衡。OKX PEPE的平均Lift正而尾部P10明显恶化；Bitget PEPE则平均Lift为负，进一步凸显跨交易所流动性/合同规格的差异。

加入P80后，SOL、DOGE、PEPE OKX、PEPE Bitget的100ms平均Lift依次+0.2774、+0.3637、+0.1345、−0.5214bps，500ms依次+0.1662、+0.3931、+0.2929、−0.4024bps；四市场的95%簇Bootstrap区间仍覆盖零。OKX PEPE的500ms P10从−1.0375升到+0.0249bps，但仍未证实显著稳定收益。

### 4.3 收益桥接和费率敏感性

总净收益改善的会计桥接是：
净改善 = 减少的模拟Maker费用 + 由G0原毛结果按成交额比例缩放的变化 + 剩余毛结果差。
剩余毛差包括期末库存、做市排队次序、报价路径、成交选择性等多种因素，**不能称为因果Selection Alpha**。

|市场|Z1+QS六日总净改善USDT|Fee Saved|Volume-Scale Gross|剩余毛差USDT|
|---|---:|---:|---:|---:|
|SOL OKX|+2.4807|+1.7003|+0.7893|−0.0089|
|DOGE OKX|+3.2291|+1.8028|+0.9467|+0.4796|
|PEPE OKX|+8.8951|+4.3844|+3.0167|+1.4940|
|PEPE Bitget|+1.7914|+1.4428|+0.8005|−0.4518|

|Z1+QS六日净权益合计USDT|Maker 2bps主结果|Maker 0bps诊断|Maker −0.2bps返佣假设|
|---|---:|---:|---:|
|SOL OKX|−14.7308|−4.6762|−3.6707|
|DOGE OKX|−16.7610|−5.4567|−4.3263|
|PEPE OKX|−23.5636|−8.7195|−7.2351|
|PEPE Bitget|−9.3388|−3.6230|−3.0514|

资金费率、真实交易所排队位置、拒单/撤单ACK、历史合约参数与成交概率的误差仍需稳健性验证。

## 5. 本轮测试与审查门槛

[聚焦单元测试源码：主动成交P80](../scripts/test_experiment_source_defense_trade_flow.py)、[EMA严格过去重读测试](../scripts/test_experiment_source_defense_v5_pepe_coverage.py)；[聚焦33项测试全部通过JSON](../reports/source_defense/v5_tests/v5_tests.json)、[MD](../reports/source_defense/v5_tests/v5_tests.md)、[工作流记录](https://github.com/Huhabubu/mm/actions/runs/43555125020)。单元测试仅验证边界逻辑，真实撮合与数值汇总由HBT及单独工作流验证。

[六日期完整金额、各四期限Markout、P10、两层Bootstrap、随机种子误差、G0/本地安慰剂/返佣敏感性JSON](../reports/source_defense/v5_sixdate/v5_sixdate_summary.json)和[研究表格MD](../reports/source_defense/v5_sixdate/v5_sixdate_summary.md)。

- 已完成六日期测试，但统计结果不支持“显著跨所净选择性Alpha”，且四个市场随机等量均未达标。
- 仅有PEPE OKX的有效500ms Dropped组超过200笔，说明扩大天数仍未必能解决所有市场的统计功效。
- 现阶段不应继续只在这些已探索日期上调z、P80或保持窗为正好过线；下一轮应优先核对原生BBO与HBT合成盘口差异、独立留出日期、各日收益稳定性、真实撤单时间和订单队列假设，以及更可识别的随机对照设计。
- 研究归档仅提交源码、测试及紧凑JSON/MD。大量历史行情及逐笔NPZ留在CI临时目录/短期Actions artifact中，未提交Git分支。
