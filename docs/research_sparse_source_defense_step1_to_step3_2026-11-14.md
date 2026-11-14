# 跨所来源双门控：Step 1—3 单日审计与六日准入（2026-11-14）

**独立分支**：research/sparse-source-defense-20261114。原main正式基线619294c3d34a2b7ed7387f472ac0192cabaa807e及其归档报告没有修改。数据日期属于已探索样本。本轮未进行任何六日期新防御策略收益回测。

## Step 1 只读归因

[详细结果JSON](../reports/source_defense/step1/step1_diagnostics.json) · [分桶报告MD](../reports/source_defense/step1/step1_diagnostics.md) · [原始校验运行](https://github.com/Huhabubu/mm/actions/runs/43548040042)

原G0官方成交，使用交易所成交时间减1ms的已可见价格快照代理（不是原始挂单决策时刻），以额外2.5ms后可见的Binance原生BBO中间价过去100ms移动≥1个目标档位、且与快基差方向一致，作为价格冲击触发。此处的Binance价格变动尚未用主动成交流确认。

| BTC原G0成交条件 | 笔数 | 100ms均值Markout | 500ms均值Markout | 500ms P10 |
|---|---:|---:|---:|---:|
| 未触发 | 118 | −1.0641bps | −1.2132bps | −2.1992bps |
| 触发且价差0.2～0.5bps | 26 | −1.2007bps | −1.3371bps | −2.1632bps |
| 触发且价差0.5～1.0bps | 90 | −1.3624bps | −1.7021bps | −2.9796bps |
| 触发且价差1.0～2.0bps | 36 | −1.3151bps | −1.4486bps | −2.2577bps |

BTC另有17笔触发且价差0～0.2bps、9笔不可评估、>2bps为0笔。SOL的低bps桶有效样本很少，不能据此强行定阈值。此表说明0.5～1bps有条件不利价格变化，但未作独立日显著性推断。

上一轮2tick策略BTC Added（新策略独有成交）68笔，成交额5526.78USDT，100/500ms平均Markout分别−1.1700/−1.3642bps；所有比较策略终点库存为0。新策略相对G0的原生BBO日内库存持有MTM价格漂移代理为+0.9551USDT。Added真实有不利成交后价格变化，但单日逐笔重排与库存路径不允许将它和毛PNL剩余差直接命名为因果Selection Alpha。

## Step 2 bps计价与有限防抖

[信号](../scripts/experiment_source_defense_signal.py) · [HBT单侧执行引擎](../scripts/experiment_source_defense_engine.py) · [单日入口](../scripts/experiment_source_defense_bps_pilot.py) · [汇总程序](../scripts/experiment_source_defense_step2_aggregate.py)

新执行版本的gap、Binance脉冲、hysteresis_exit和widen均以实时目标中间价转为bps，再把widen价格映射为至少1个价格tick。保留单侧20ms操作限制，短暂保持50/75/100ms（不因行情事件无限顺延），两项退出信号同时回落时立即解除；保护一侧时在所有事件上禁止安全侧撤单重挂。所有成交由官方hftbacktest GTX限价单完成。

三组基于Step1分桶而事先固定的离散候选：
1. c1：EMA半衰期1s，gap0.5bps、impulse0.2bps、exit0.1bps、widen0.5bps、hold50ms。
2. c2：EMA半衰期1s，gap0.5bps、impulse0.5bps、exit0.15bps、widen1bps、hold75ms。
3. c3：EMA半衰期2s，gap1.0bps、impulse0.5bps、exit0.2bps、widen1.5bps、hold100ms。

每组单独比较widen、cancel和OKX本地触发安慰剂，另含G0和原双侧beta1。每市场11种模型，均10min预热+50min交易，Binance人为额外延迟2.5ms，maker模拟费率2bps。真实运行：[Actions #43548486905](https://github.com/Huhabubu/mm/actions/runs/43548486905)。

[净收益、Added/Dropped/Kept及4个期限的P5/P10/Bootstrap完整JSON](../reports/source_defense/step2_bps_summary/step2_bps_summary.json) · [精简表格](../reports/source_defense/step2_bps_summary/step2_bps_summary.md) · [归因成功任务](https://github.com/Huhabubu/mm/actions/runs/43548679989)

| 2026-06-01目标市场 | 模型 | 成交笔数 | 保留率 | 撤单请求 | 扣费净PnL USDT | Lift100ms | Lift500ms |
|---|---|---:|---:|---:|---:|---:|---:|
| OKX BTC | 原G0 | 296 | 100% | 3169 | −7.1619 | — | — |
| OKX BTC | 原beta1 | 82 | 27.7% | 13751 | −2.0118 | −0.1441 | −0.1051 |
| OKX BTC | c1 widen | 168 | 56.8% | 3522 | −4.1263 | −0.1577 | −0.0841 |
| OKX BTC | c2 widen | 176 | 59.5% | 3525 | −4.4466 | −0.0989 | +0.0161 |
| OKX BTC | c3 widen | 236 | 79.7% | 3188 | −5.7740 | −0.0762 | −0.0577 |
| OKX BTC | c3 cancel | **238** | **80.4%** | **2994** | **−5.9089** | **−0.0701** | **−0.0642** |
| OKX BTC | c3 OKX placebo | 298 | 100.7% | 3130 | −7.2361 | −0.2803 | −0.4610 |
| OKX SOL | 原G0 | 124 | 100% | 1820 | −3.0019 | — | — |
| OKX SOL | c3 widen | 58 | 46.8% | 2562 | −1.1744 | −0.2345 | +0.0668 |
| OKX SOL | c3 cancel | 54 | 43.5% | 1820 | −1.0714 | −0.0593 | +0.3799 |

**红线失败。** BTC c3 cancel满足成交保留率≥80%且撤单恢复接近原G0，但100ms/500ms平均选择性均为负；500ms的单日30秒时间块重抽样95%区间[−0.2988,+0.1960]bps包含零，P10 500ms保留组减排除组仅+0.0390bps。SOL仍过度缩量。全部官方HBT净收益依旧为负。

BTC c3 cancel新增成交32笔、名义额2600.12USDT，Added 500ms均值Markout−1.3821bps；终点库存约为0、平均绝对库存约0.06353张。相对G0净改善+1.2530USDT由费率减少+0.9431、成交额缩放的原G0毛结果变化+0.4610及剩余毛结果−0.1511USDT构成。末项包含库存、报价和队列因素，并未被识别为纯选择性Alpha。

### Equal-Volume G0公平对照

[前期训练与测试脚本](../scripts/experiment_source_defense_equal_volume_g0.py) · [五月训练参数JSON](../reports/source_defense/equal_volume_test/equal_volume_train.json) · [六月检验JSON](../reports/source_defense/equal_volume_test/equal_volume_test.json) · [MD](../reports/source_defense/equal_volume_test/equal_volume_test.md)

只用2026-05-01原G0历史回测与事前80%成交保留目标，确定静态加宽0.05bps，五月保留率80.13%。六月固定此参数后成交254笔，而c3 cancel为238笔，成交数差6.72%、手续费差6.73%。**两个误差均未达到严格<2%，不能将该比较标记为合格的Equal-Volume G0。** 不以六月已知成交数追调价差。证据：[Actions #43549107993](https://github.com/Huhabubu/mm/actions/runs/43549107993)。

## Step 3 3档GLFT的独立稳健估计与六日期准入

[稳健估计函数](../scripts/experiment_source_defense_robust_fit.py) · [官方HBT预热取样](../scripts/experiment_source_defense_robust_admission.py) · [严格日序先验聚合](../scripts/experiment_source_defense_robust_aggregate.py) · [48案例成功工作流](https://github.com/Huhabubu/mm/actions/runs/43549259951)

[六日期逐市场与日期参数可靠性JSON](../reports/source_defense/step3_fit/step3_fit_admission.json) · [简明MD](../reports/source_defense/step3_fit/step3_fit_admission.md)

正式GLFT校准器依旧原样保留≥5个正计数距离档。研究版以独立函数允许至少3档、≥10个不同100ms交易穿越时间窗、至少一半有效波动样本和合理A/k/sigma范围。对3～4个距离档的k必须引用**同币种同交易所严格更早日期且此前符合报价有效性检查**的估计，以相当于5个档位的权重进行先验收缩，再用当日预热历史估计A、sigma。20min预热只允许从UTC00:20开始交易，不能与00:10起点直接比较。

| 品种/目标所 | 原始5档10min | 研究3档10min | 研究3档20min |
|---|---:|---:|---:|
| SOL OKX | 2/6 | 5/6 | **6/6** |
| SOL Bitget | 6/6 | 6/6 | 6/6 |
| DOGE OKX | 4/6 | 5/6 | **6/6** |
| DOGE Bitget | 3/6 | 3/6 | 4/6 |
| XRP OKX | 3/6 | 5/6 | 5/6 |
| XRP Bitget | 4/6 | 5/6 | 5/6 |
| 1000PEPE OKX | **6/6** | 6/6 | 6/6 |
| 1000PEPE Bitget | **6/6** | 6/6 | 6/6 |

SOL OKX 11月在10min时只有2档、20min有4档，所以依赖10月先验通过研究估计；DOGE OKX 11月10min缺少独立穿越事件，延长预热补足；XRP OKX 11月20min仍不足独立穿越事件，因此合规地保留失败。1000PEPE两目标所直接使用原≥5档口径便达到6/6，但只是参数可估计证据，不代表做市盈利或跨所领先已证实。

### 1000PEPE千倍换算与合约规格

[量纲检查脚本](../scripts/experiment_source_defense_pepe_units.py) · [六日期合约量纲JSON](../reports/source_defense/step3_pepe_units/step3_pepe_units.json) · [汇总MD](../reports/source_defense/step3_pepe_units/step3_pepe_units.md) · [12个校验任务](https://github.com/Huhabubu/mm/actions/runs/43549733019)

Binance 1000PEPEUSDT价格按每1000 PEPE报价，因此须除以1000后才可和目标所PEPE价格比较；其基础币量等价转换为PEPE单位则乘以1000。使用当前公开规格及历史BBO验证：OKX最小价格档1e−10 USDT/PEPE、lot0.1张、ctVal=10,000,000 PEPE/张；Bitget最小价格档1e−10 USDT/PEPE、交易数量步长1000 PEPE、以PEPE基础币数量为成交单位。约80USDT订单按六月价格换算为OKX 2.3张（名义约78.6945USDT）和Bitget 23,385,000 PEPE（名义约79.9989USDT），两者原始数量不可直接比较。

六个月首日×两个目标交易所的12个短窗归一化价差检查均通过，最高偏差约0.1874%。OKX 9月按当前lot接近80USDT时名义误差约+2.134%，需保留实际名义金额差。**这些检查使用当前可查的合约规格核验历史行情价格阶梯兼容性，尚不能替代各历史时点的官方合约规格存档。**

## 停止与剩余审计

Step1只读与Step2单日官方HBT验证均完成，Step3六日期参数识别与1000PEPE逐月价量单位检查均完成。**Step2三个单日验收条件没有同时通过，因此本轮没有扩大为六日期防御收益回测。** 新币种能否进入正式四梯度交易集还取决于真实历史合约元数据、跨所100ms领先信号和成交深度等独立检查。

所有单日收益均包含模拟2bps挂单手续费；0bps及−0.2bps返佣仅作为敏感性，不将无手续费收益当作经济成果。还需真实订单拒绝/撤单ACK、资金费率、队列位置与不同独立样本日检验。所有已探索日期均不视为样本外，也没有识别到严格因果选择性Alpha。
