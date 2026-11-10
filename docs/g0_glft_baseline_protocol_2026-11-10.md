# G0 原始 GLFT：OKX 合约规格与回测预设（2026-11-10）

本阶段 G0 只使用 OKX BTC-USDT-SWAP 的 Tardis L2、Trades 与原生 BBO，官方 hftbacktest 2.4.4 做队列、延迟及撮合。10min 训练估计参数，随后 5min 在同日后续数据评估；这不等于每日独立收益。

## 单位和状态

按 OKX 官方 public/instruments 字段定义，永续合约数量单位为张；近期记录 BTC-USDT-SWAP ctVal=0.01 BTC/张、ctMult=1、lotSz=minSz=0.01 张、tickSz=0.1 USDT/BTC。Tardis amount 为交易所原始提供值，先不换算；引擎用 linear_asset(0.01)，故 0.1 张 = 0.001 BTC，例如价格 77500 时名义金额为 77.5 USDT。代码按 Decimal 检查行情的价格、数量网格。

官方实时产品接口若可访问，只提供运行当天的快照。**2026-11-01 历史正式规格未得到档案验证**，上述均为有来源、等待历史复核的情景参数。maker 费 0.0002、taker 费 0.0005 取自 OKX Lv1 示例，并非账户真实历史费率；入单和回应分别固定 1ms，撤单通过相同引擎，非实测延迟。概率队列采用 power_prob_queue_model(2.0)，成交模型 no_partial_fill_exchange，以上都需稳健性检验。

## 原始 GLFT 报价

每 100ms 用早期训练窗口主动成交相对上一时刻 mid 的最远突破深度拟合 lambda(delta)=A*exp(-k*delta)，lambda 和 A 单位每秒，delta 为价档 ticks，k 单位每价档倒数；前 70 个半 tick 价档拟合 log lambda。sigma 为每秒中间价波动率（tick/sqrt(second)），按 100ms 变化的标准差乘 sqrt(10) 计算。训练后锁定 A、k、sigma，不加漂移、跨所信号、时变参数或双侧不同 A/k。

按照 GLFT 原论文 4.6–4.7 近似，风险系数 gamma=0.05 是研究预设参数；每次订单 delta=0.1 张，库存 q 以张表示，最多绝对 2 张。最优报价距离的半价差和库存 skew 使用 hftbacktest 官方教程系数 c1/c2，报价保护在现有最优买卖价之外，使用 GTX(post-only) 挂单，每 100ms 检查撤换。

## 净权益及未来逆向选择定义

交易引擎净盯市权益（USDT）：equity = cash_balance + inventory_contracts * 0.01 * OKX_mid_USDT_per_BTC - cumulative_fees。报告模拟成交单数、库存、订单数量、撤单请求、USDT 权益。5 分钟片段只验证 G0 交易管线，不能当成全日净收益或统计证据。

在运行任何 G2 之前锁定成交后逆向选择指标：主窗口 100ms，次要 500ms、1s、2s。设每次模拟 maker 成交方向 s 为买入 +1、卖出 -1，成交量 v 为张，成交时有效 mid 为 M(t)（USDT/BTC），成交后 tau 时间的过去可见 mid 为 M(t+tau)。方向调整后的价格 markout = s*v*0.01*[M(t+tau)-M(t)] USDT。负数代表逆向选择损失；成交价相对 mid 的价差捕获和手续费单独记录。必须取得引擎真实成交生效时间，不能以 100ms 策略轮询发现成交的时刻替代。因此 G0 第一版仅预先锁定指标定义，暂不报告未经验证的四档 markout。

## 未完成的可信度要求

以后扩大到完整 UTC 日，日收益逐日独立评估；核实历史合约规格、费率、资金费率，实测下撤单延迟、队列成交假设，检查全日盘口 BBO 一致性及内存资源。未来 G2 仅加载完整 OKX 盘口，Binance 预先计算信号按时间对齐。

资料：OKX https://www.okx.com/docs-v5/en/ ；OKX 官方 https://github.com/okx/agent-skills/blob/github-main/skills/okx-cex-market/references/instrument-commands.md ；Tardis https://docs.tardis.dev/downloadable-csv-files ；hftbacktest https://hftbacktest.readthedocs.io/en/py-v2.2.0/tutorials/GLFT%20Market%20Making%20Model%20and%20Grid%20Trading.html

## 本轮运行与后续扩展记录

首次 10min 校准加 5min 评价（Actions #42801137160）无真实数据模拟成交；后按相同 A/k/sigma 和相同策略公式延长为 10min 校准加 50min 评价（Actions #42801845615）得到买卖各 1 笔成交和净盯市权益 −0.15292666 USDT。该值仅是片段回测；完整证据见 [G0 结果报告](g0_glft_baseline_results_2026-11-10.md)。后续主线仍需完整每日独立评价。
