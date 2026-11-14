# V6：Tardis双时钟、直接毒流、提前盘口特征与等曝光时间平移

2026-11-14｜研究分支：research/sparse-source-defense-20261114

**主分支 main 保持 619294c3d34a2b7ed7387f472ac0192cabaa807e，正式归档2.5ms G0基线未修改。** 本轮固定Z1、P50 Queue Shield、P80；未重新寻优这些阈值，也未重跑四市场×六日期全部V5矩阵。所有新增模拟撮合仍在官方hftbacktest 2.4.4、1ms订单请求延迟＋1ms回报延迟、Maker 2bps费用、原队列模型中执行。所有新增原始逐笔/逐事件细节只存在本轮GitHub Actions短期artifact，**Git不提交NPZ**；仅提交代码、测试和精简JSON/MD。

## 一、诊断1：Tardis双所采集时钟

[只读来源](../scripts/experiment_source_defense_v6_clock_microprice.py)；[完整P10/P50/均值/P90/负延迟比例及跳变配对JSON](../reports/source_defense/v6_clock/clock_microprice_summary.json)、[MD表](../reports/source_defense/v6_clock/clock_microprice_summary.md)、[成功CI](https://github.com/Huhabubu/mm/actions/runs/43581791506)。

样本日期：2026-06-01、2026-07-01，均UTC00:00至01:10:05，使用原生Tardis book_ticker，单位微秒。采集偏差定义为当地采集器时间 local_timestamp 减来源事件时间 timestamp，以下中位数换算为毫秒：

|标的/目标|6月BN|6月目标|7月BN|7月目标|6月BN−目标采集偏差中位数|
|---|---:|---:|---:|---:|---:|
|BTC OKX|3.098|35.343|2.794|27.508|−32.245ms|
|SOL OKX|3.074|27.973|3.448|28.860|−24.899ms|
|DOGE OKX|2.411|27.511|2.921|35.788|−25.100ms|
|1000PEPE OKX|2.619|35.298|2.415|27.967|−32.679ms|
|1000PEPE Bitget|2.619|6.610|2.415|7.425|−3.991ms|

Binance价格是BN 1000PEPEUSDT除以1000换成USDT/PEPE，目标PEPE仍为USDT/PEPE。对于同方向BBO中间价跳变，按各自时钟在±100ms内寻找目标所最近一次同方向事件，差值定义为目标时刻减Binance时刻（正表示Binance配对时间较早），非唯一配对：

|6月目标|按采集时间：有符号差中位数|按交易所事件时间：有符号差中位数|采集时间下BN先出现的匹配比例|事件时间下BN先出现的匹配比例|
|---|---:|---:|---:|---:|
|BTC OKX|+14.177ms|+7.000ms|69.6%|60.0%|
|SOL OKX|+39.593ms|+21.000ms|77.3%|68.6%|
|DOGE OKX|+33.200ms|+19.000ms|73.3%|63.9%|
|1000PEPE OKX|+43.745ms|+23.000ms|78.1%|68.6%|
|1000PEPE Bitget|+4.423ms|+1.000ms|69.5%|56.1%|

**此处不是网络物理传播延迟测量。** 交易所时间戳发布/匹配系统及Tardis采集器时钟的同步精度、行情发布时间、API通道与路由均未独立校准。非唯一“最近同向跳变”会让目标一笔行情配上多个Binance事件，并受到两边事件密度影响。即使同一币种的Binance采集记录领先，也不能直接证明Binance价格在可交易意义上物理领先，也无法将本地中位差减去事件时间差视作单程网络延迟。

### BTC 2.5ms 与0ms高门槛最终成交完全相同的真正解释

[引擎实际防御激活/撤单请求日志](../reports/source_defense/v6_mechanism_cases/BTC/2026-06-01/case.json)。

|BTC high08_cancel_p80|BN额外可见延迟0ms|BN额外可见延迟2.5ms|
|---|---:|---:|
|最终HBT成交笔数|222|222|
|Maker 2bps后净PnL|−5.239929 USDT|−5.239929 USDT|
|防御激活次数|222|217|
|紧急撤单请求次数|197|188|
|最近同危险侧对应激活事件数（±20ms）|与2.5ms匹配216个|与0ms匹配216个|
|匹配激活的2.5ms减0ms时差中位数| colspan |**+2.500ms**|

紧急撤单请求在两档的配对时差中位数也是**+2.500ms**（187个成功匹配，源0ms版197请求，2.5ms版188请求）。**逐笔成交交易所时间、方向、价格、数量的完整序列并不完全一致**：最终成交笔数和净权益恰好相同，只是模型的离散撮合结果与订单费用/规模汇总重合，不能推断“信号完全没触发变化”或“真实套利窗口必小于/大于2.5ms”。需订单ACK与原生行情的测量链路识别。

## 二、诊断2：Group A / B 严格按时间分类，避免把所有Dropped都当作直接拦截

[研究引擎可选完整防御起止与紧急撤单本地时间戳](../scripts/experiment_source_defense_engine.py)，[只读与最小官方HBT重放](../scripts/experiment_source_defense_v6_mechanism.py)、[六日期组聚合](../scripts/experiment_source_defense_v6_mechanism_aggregate.py)、[结果JSON](../reports/source_defense/v6_mechanism/mechanism_shift60_summary.json)、[MD](../reports/source_defense/v6_mechanism/mechanism_shift60_summary.md)。所有Markout按官方成交后的本地观察时刻与目标所原生BBO过去状态计算，单位bps；负值代表做市商不利的后续价格位移。

定义：

- Group A：原G0独有成交的**方向等于危险挂单侧**，且其交易所成交时间落在新策略本地防御起止窗口中，或者同方向紧急撤单请求发出后的200ms内。额外同时报告严格仅防御激活期及仅撤单余波。
- Group B：原G0独有成交中不满足以上时间、方向条件的部分。
- Kept：G0与新策略在**相同交易所时间＋方向**下配对的共有成交。
- Added：新策略新增的、不能匹配原G0的成交。

**Group B只是“窗口外Dropped”；其发生机制是否来自之前的队列重置、持仓差或订单撤销，不能仅凭时间分类确认。Group A也不是已证明交易所取消ACK先于G0反事实成交。** 本地动作时间与交易所成交时间跨两个时钟，事件存在1ms+1ms订单延迟假设，不可据此声称严格逐订单取消因果效应。

SOL OKX 六个独立日期（2026-06至11月每月1日，20min预热、50min交易）：

|原G0相对Z1+QS分组|笔数|占139笔Dropped|100ms均值|500ms均值|500ms P10|
|---|---:|---:|---:|---:|---:|
|**Group A直接期/撤单200ms余波**|**74**|**53.2%**|**−2.432bps**|**−2.561bps**|**−3.935bps**|
|严格实际防御激活期|73|52.5%|−2.449|−2.579|−3.980|
|仅撤单余波|1|0.7%|−1.206|−1.206|−1.206|
|Group B窗口外|65|46.8%|−1.563|−1.626|−2.726|
|Kept|593|—|−1.742|−1.926|−3.390|
|Added|33|—|−1.597|−2.003|−4.099|

该分解发现：严格防御窗口内对应的原G0成交更毒；窗口外Dropped平均比Kept更安全，拉低了整体Lift；Added成交的500ms P10较差，仍可能造成尾部净结果损失。另有各组1s Markout、样本计数、HBT净PnL/库存于上述JSON。6月单日BTC原G0 300笔、新策略146笔，Group A136、Group B55；DOGE G0 522 / 新策略446，A70、B44；OKX PEPE G0 192 / 新策略107，A84、B25。后三个市场是**六月诊断日**，不作六日期显著性推断。

## 三、诊断3：Binance预跳Microprice与50ms盘口量失衡变化

[原生只读BBO事件研究](../scripts/experiment_source_defense_v6_clock_microprice.py)、[全10ms时点控制](../scripts/experiment_source_defense_v6_microprice_control.py)、[两个市场完整JSON](../reports/source_defense/v6_microprice_control/microprice_unconditional_summary.json)、[MD](../reports/source_defense/v6_microprice_control/microprice_unconditional_summary.md)、[回放工作流](https://github.com/Huhabubu/mm/actions/runs/43582146621)。

Microprice用BN最优买价、卖价及买一/卖一量加权，价格相对中间价的偏离在决策前严格可见；盘口量失衡为(买一数量−卖一数量)/(买一数量＋卖一数量)，还检查**50ms严格过去值至当前值的变化**。只看2026-06-01 BTC/SOL：

|诊断，目标100ms未来中间价带符号响应|BTC OKX|SOL OKX|
|---|---:|---:|
|事后挑出BN mid跳变，提前5ms Microprice方向命中|97.2%|97.4%|
|前50ms没有其它BN价格跳变，再按未来跳变挑样本，提前5ms命中|96.0%|97.8%|
|**固定10ms全部有效时点的Microprice方向平均响应**|**+0.0225bps**|**+0.0347bps**|
|**固定10ms全部有效时点的50ms量失衡变化方向平均响应**|**+0.0113bps**|**+0.0215bps**|
|10ms有效观测时点|353484|332825|

注意跳变条件化会使用未来发生的BN跳变**挑选样本**；高命中率不代表用户当时可知下次跳变何时发生。10ms连续时点有高度重叠，收益统计相关性强。全时点平均效应很小，远小于每笔Maker 2bps；也尚未通过官方HBT模拟撤单ACK、报价冲击、队列损失及库存影响。本轮未新增Microprice真实策略。

### SOL 单日γ风险厌恶探测（非正式基准修改）

只在2026-06-01 SOL OKX，将原研究模型风险厌恶γ从50提升为75 USDT^-1（其对应GLFT代码参数还乘合约ctVal和Tick），其它模型输入、手续费及HBT执行相同。初始GLFT半价差（原生Tick单位）**2.103→2.348**；G0成交**204→100**，Z1+QS成交**168→70**，高风险厌恶下Z1相对高γ G0仅保留70%。高γ下100ms/500ms平均选择性Lift约**+0.0856/+0.1739bps**，500ms P10差约**+0.0031bps**。扣费净PnL：高γ G0 **−2.4036 USDT**，高γ Z1 **−1.6689 USDT**。因此风险厌恶/报价宽度确实极大改变基准成交规模与双方相对表现；**本单日结果不足以将高γ设为新正式基线，也不支持高γ后跨所选择性已显著提升。**

## 四、诊断4：SOL +60秒Time-Shift与严格等防御曝光修正

[最小机制执行](../scripts/experiment_source_defense_v6_mechanism.py)、[精确留存脉冲的等曝光执行](../scripts/experiment_source_defense_v6_matched_exposure.py)、[等曝光六日期聚合](../scripts/experiment_source_defense_v6_matched_exposure_aggregate.py)；[六日期等曝光JSON](../reports/source_defense/v6_matched_exposure_summary/matched_exposure_shift60.json)、[MD](../reports/source_defense/v6_matched_exposure_summary/matched_exposure_shift60.md)、[官方HBT完成任务](https://github.com/Huhabubu/mm/actions/runs/43581559830)。

第一轮直接后移全量脉冲+60s、保持原50分钟交易窗（禁止把最后一分钟信号“模50min”循环搬回开头，因为那会把当天未来信号放入过去）：原窗口防御时长共保留**95.23%**，6日Source成交626、后移733，含费净权益Source −14.7308、后移 −17.2309 USDT；这版不能声称完整保留所有防御脉冲。

**进一步做了严格等曝光配对**：每个日期先对称剔除原真实Source中最后60秒无法再后移的脉冲，对跨界脉冲两边裁成相同部分；然后将这份完全相同的保留脉冲时间表分别放在原时刻和+60秒。两组都不开BN行情时钟、不读BN当前价差、P80或方向；由官方HBT独立执行挂撤单。**六日逐日的计划防御次数与时长完全相同，实际HBT记录的防御次数与时长也逐日完全一致**；危险侧方向与每一脉冲持续时间由同一表得到。回顾性Source时间表本身来自当日第一次含Binance的研究策略输出，因此这是回顾性配对时序安慰剂，不能独立部署、更不是随机对照试验。

|SOL OKX 6日，同一保留防御曝光|原时刻Source脉冲|+60s平移脉冲|
|---|---:|---:|
|总成交|626|733|
|撤单请求|7762|8305|
|含2bps手续费净PnL USDT|−14.7853|−17.2309|
|100ms平均成交Markout|−1.7416bps|−1.7994bps|
|500ms平均成交Markout|−1.9410bps|−1.9701bps|
|500ms每笔费前Gross Edge|−0.9010bps|−0.8960bps|
|500ms Markout P10|−3.6154bps|−3.6147bps|

Source与Shift每笔本身**成交样本不同**，均值只可作为策略结果描述。按六个交易日等权得到Source−Shift的100ms Markout差为**+0.0489bps**（按日Bootstrap95%区间[+0.0039,+0.0872]）；500ms均差**+0.0294bps**（[−0.0213,+0.0756]，跨零）；1s均差约+0.0258bps（区间跨零）。500ms P10差约**−0.0007bps**，基本无尾部改善。100ms区间正但幅度很小，且6个日期已经用于早前策略准入；不能由这一项判定跨所因果Alpha。

六日含费净权益差Source优于+60s **+2.4456USDT**，严格会计分解：

|来源|USDT|
|---|---:|
|Maker手续费节省|+1.7163|
|按成交额缩放+60s对照的毛收益变化|+0.7961|
|剩余毛收益差（库存、执行、队列及选择性混合项）|**−0.0668**|
|合计净权益差|**+2.4456**|

因此**等防御次数与时长不等于等成交量/手续费**。Source的626笔与Shift的733笔仍显著不等量；Source保留脉冲恰好与不利成交密集期对齐，导致成交减少并节省费用，是当前主要经济解释。+60s失去此时序后，撤单次数甚至更多，说明队列重置/一般性改价也会变化，仍需要订单ID和exchange ACK精确追踪，才能拆出队列惩罚及可归因选择性。

## 五、可重复验证和审查结论

[V6聚焦测试脚本](../scripts/test_experiment_source_defense_v6_mechanism.py)、[测试JSON](../reports/source_defense/v6_tests/v6_unit_tests.json)、[MD](../reports/source_defense/v6_tests/v6_unit_tests.md)、[最终成功CI](https://github.com/Huhabubu/mm/actions/runs/43582024009)：32项聚焦单测通过，包括2ms紧急撤单、严格+60s有序/不回绕/等曝光日历、双时钟signed lead-lag、EMA/主动成交流因果边界。历史HBT模型仍只改研究版执行引擎，未改main或旧归档。

**四项最终判定：**

1. 双所采集记录存在系统性异质时差；尚不能确认真实交易所间物理因果领先，尤其不能拿OKX原生采集事件落后几十ms当作已经证实可交易的BN alpha。
2. SOL Group A的500ms标记损失明显差于Group B；Group B平均相对更安全，能够解释整体选择性被稀释。但按时间窗对比无法证明交易所撤单ACK确实先于成交。
3. Microprice与50ms一档量失衡变化有很小的全时点正向标记响应，远小于当前2bps手续费；下一步需要结合订单冻结、较短预测期限、信号频率及真样本外检验，暂不作为新的报价中心。
4. +60s后移保留原防御脉冲的时序形状，但对称裁剪后两个实验组虽等防御曝光，仍不等成交量。双方500ms P10几乎无差异，收益优势主要是少成交与省手续费，且宏观残差为负。当前无证据可把残差定性为独立的纯跨所Selection Alpha。

后续最小建议：优先做原生事件时间与采集时间对齐的独立证据/服务器实测，以及以订单ID和有效ACK为主键的取消-成交对照；增加未探索过的完整交易日和更长盘中时间窗，按日期汇总后才继续考虑Microprice或参数升级。任何进一步阈值调整应预先固定并留真正样本外。
