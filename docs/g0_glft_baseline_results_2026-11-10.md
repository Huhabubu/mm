# G0 原始 GLFT 基准：OKX 合约单位核查与真实交易回测（2026-11-10）

## 状态与复现

- [主运行 #42801845615：success，2026-11-01 的 10min 校准 + 50min 真实行情交易回测](https://github.com/Huhabubu/mm/actions/runs/42801845615)；4 项参数/单位合成测试及 1 项官方 hftbacktest 合成成交资金校验均通过。
- [初始 10min 校准 + 5min 交易运行 #42801137160：success](https://github.com/Huhabubu/mm/actions/runs/42801137160)；5min 交易窗口零成交，资金及库存均零。
- [脚本](../scripts/g0_glft_baseline.py)；[事前协议](g0_glft_baseline_protocol_2026-11-10.md)；[扩展 60min 工作流](../.github/workflows/g0-okx-glft.yml)。报告 JSON 在对应 Actions 附件，仅保存聚合指标，原始 CSV/NPZ 不公开上传。

## 合约规格与数据单位

GitHub Actions 运行时 2026-11-10 10:46:11 UTC 成功查询 OKX public/instruments：BTC-USDT-SWAP 线性永续、ctVal=0.01 BTC/张、ctMult=1、ctValCcy=BTC、tickSz=0.1 USDT/BTC、lotSz=minSz=0.01 张，状态 live。此为 **2026-11-10 实时 API**，不能证明 2026-11-01 的历史规则相同。

Tardis 的真实样本 00:00–01:00 UTC，Trades 22,008 条、L2 有效更新 961,140 条、原生 BBO 58,072 条；转换后事件 1,057,077 条。逐行数量/价格网格审查分别覆盖 Trades 全部 22,008 条、L2 前 50,000 条、BBO 前 50,000 条；三路审查样本均 0 个 off-tick/off-lot，观测最小正数量 0.01 张。L2 开头 8 条快照前增量已跳过。注意：这些检查证明样本与所设规格相容，完整日的规格及数据连续性尚需检查。

每张合约 0.01 BTC；回测报价和成交数量保持张为单位，引擎资产按 linear_asset(0.01) 配置。例如 0.1 张等于 0.001 BTC。

## 原始 GLFT 训练参数（首 10min，仅 OKX 数据）

| 参数 | 实测值 | 单位 |
|---|---:|---|
| A | 0.050428829 | 次/秒 |
| k | 0.005232968 | 每 tick 的倒数 |
| sigma | 13.088815 | tick/√秒 |
| GLFT 半价差 | 174.136955 | tick = 17.4136955 USDT |
| skew | 800.194736 | tick/张 |

gamma=0.05 为预设风险参数，订单 0.1 张，最大绝对库存 2 张；前 10 分钟计算 A、k、sigma，之后保持参数不变。价格事件进入 hftbacktest，按 100ms 频率更新 maker 双侧 GTX 限价单。

## 真实 OKX 后续 50min 交易回测结果

| 指标 | hftbacktest 真实数据模拟值 |
|---|---:|
| 决策轮询 | 29,998 次 |
| 提交挂单 | 362 次 |
| 撤单请求 | 358 次 |
| 买入 / 卖出成交 | 1 / 1 笔 |
| 引擎累计成交数 | 2 笔（与成交检测一致） |
| 期末库存 | 0 张 |
| 期间最大绝对库存 | 0.1 张 |
| 累计成交名义金额 | 155.13330 USDT |
| 交易现金余额 | −0.12190 USDT |
| 已扣 maker 费用情景 | 0.03102666 USDT |
| 净盯市权益 / 本评价窗口净盈亏 | −0.15292666 USDT |

期末库存为 0，故净权益 = 现金余额 − 已支付手续费，直接得到 −0.15292666 USDT。该结果仅对应交易所 2026-11-01 00:10 至约 01:00 UTC 的行情片段、预设 1ms 进单+1ms 回应延迟、power_prob_queue_model(2.0)、no_partial_fill_exchange 和 OKX Lv1 maker 2bps/taker 5bps 费用情景。没有推定真实账户已产生此笔盈亏。

前 5min 评价窗口（同一校准参数）零成交，扩展到 50min 后出现 2 笔成交。该差异说明 G0 的成交非常稀疏；不能用这两笔成交估计可靠的日均收益或统计显著性。此为单个片段数据，不是一个完整交易日。

## 代码与学术边界

- 官方 hftbacktest 做撮合及队列成交；没有自建撮合引擎。另行构造一个合成行情的主动卖单穿价事件，用引擎验证 maker 买单的库存、现金余额与手续费计算；其测试通过属于软件单元验证，不计入真实回测成交。
- 官方实时产品规格不能回填为 2026-11-01 可靠历史规格；手续费费率、历史合约条款、资金费率与网络延迟均需进一步核验。
- 目前 GLFT 参数仅用 10min 校准，稳定性未知；尚未全日逐日独立回测、实测成交队列和不同延迟场景稳健性。
- 成交后逆向选择按事前协议固定 100ms 主窗口、500ms/1s/2s 次窗口，需真实模拟成交生效时间及同源后续中间价。当前未计算，因此不报告 markout。
- 下一关键步骤：保持 G0 参数与模型口径清晰，扩充多日、全日、延迟/费率情景并核对队列模型后，才可能正式比较 G1/G2。

来源：OKX 官方公共 API 文档 https://www.okx.com/docs-v5/en/ ；hftbacktest 官方 GLFT 教程 https://hftbacktest.readthedocs.io/en/py-v2.2.0/tutorials/GLFT%20Market%20Making%20Model%20and%20Grid%20Trading.html ；Tardis 数据说明 https://docs.tardis.dev/downloadable-csv-files
