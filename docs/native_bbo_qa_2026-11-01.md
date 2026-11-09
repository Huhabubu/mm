# 原生 BBO 与 L2 重建报价数据检查（2026-11-01 UTC）

**执行状态：已由真实 GitHub Actions 运行完成。**

- [成功运行 #42581171378](https://github.com/Huhabubu/mm/actions/runs/42581171378)
- [QA 原始聚合 JSON 附件（14 天）](https://github.com/Huhabubu/mm/actions/runs/42581171378/artifacts/13657531324)
- 代码：`scripts/compare_bbo_feeds.py`、`scripts/test_compare_bbo_feeds.py`
- Tardis 数据口径：[book_ticker（原生 BBO）与 quotes（从 L2 重建）](https://docs.tardis.dev/downloadable-csv-files/data-types#book_ticker)

## 1. 实际文件扫描

| 交易所 | 来源 | 记录数 | 压缩大小 | 中间价变化次数 | 异常报价 / 采集时间倒序 |
|---|---|---:|---:|---:|---|
| Binance | quotes / depth(L2) 重建 | 1,439,837 | 17.61 MB | 12,243 | 0 / 0 |
| Binance | **book_ticker / bookTicker** | **13,834,893** | **95.40 MB** | **74,335** | 0 / 0 |
| OKX | quotes / books(L2) 重建 | 563,434 | 7.27 MB | 8,420 | 0 / 0 |
| OKX | **book_ticker / bbo-tbt** | **1,468,859** | **16.93 MB** | **12,784** | 0 / 0 |

与 `quotes` 相比，原生 `book_ticker`：
- Binance 全部记录数约为 **9.61 倍**，中间价变化事件约为 **6.07 倍**；
- OKX 全部记录数约为 **2.61 倍**，中间价变化事件约为 **1.52 倍**。

**注意：** 记录数和中间价变化次数不同。原生 BBO 的大量更新只修改挂单数量，价格未改变。两种数据来自不同的市场数据 WebSocket 通道，不能把这些差异直接等同于“真实交易所价格领先”。

## 2. 10ms 固定网格下，同所两种行情差异

两个行情源分别采用历史 as-of 对齐，保持只选取决策时刻及之前已采集的消息。

| 指标 | Binance | OKX |
|---|---:|---:|
| 同一时点，原生与重建中间价存在差异 | 0.233% | 0.496% |
| 差异达到 0.1bps 以上的时点 | 13,792 / 8,639,998 | 34,026 / 8,639,968 |
| 原生 BBO 最近更新年龄抽样中位数 | 27.46ms | 52.97ms |
| L2 重建最近更新年龄抽样中位数 | 34.85ms | 77.07ms |

虽然大多数时刻的中间价相同，**短期价格预测主要围绕变化事件开展，不能从总体一致比例推出信号没有影响**。

时间戳：
- 两所 `book_ticker` 文件的 `local_timestamp` 均无倒序。
- Binance 原生 BBO 有 **4,979,295** 条连续消息与前一行使用相同微秒时间戳；OKX 有 **119** 条。后续事件驱动预测必须明确相同微秒内事件的处理顺序，不能仅按微秒时间戳声称内部事件存在先后关系。
- 2026-11-01 两种来源均具有全天覆盖；L2 `quotes` 的采集时间属于指定日期。

## 3. 对先前实验的影响

先前使用 `quotes` 的 Binance 事件触发实验得到 100ms 预测期平均 MSE 改善约 **18.56%**，但当时目标价格和特征均基于来源发布频率存在差异的 L2 重建行情。

因此需要直接使用两所原生 `book_ticker`，**同时替换预测特征与 OKX 未来价格标签**，沿用同一 P1/P2 模型、相同时间拆分、同一触发时钟和额外延迟机制，以检验旧结果能否复现。

请注意：原生 `book_ticker` 在短期场景仍需验证采集端传播延迟；同一东京 GCP 区域不能保证相同网络到达时间。旧 `quotes` 数据将继续保留作为行情源敏感性对照。

## 4. 结果保存

- 原始 Tardis 数据每次在 GitHub Actions 临时工作目录下载，不提交到公开仓库。
- 仓库保留 QA 代码、本汇总和可复现的工作流；聚合 JSON 暂存于 Actions Artifact。
- 原生 BBO P1/P2 预测工作流为 `.github/workflows/native-bbo-prediction.yml`。
