# Tardis 采集位置、验证集与预测特征研究备忘（2026-11-09）

## 一、官方采集位置核查

资料：
- Binance USDS-M Futures: https://docs.tardis.dev/historical-data-details/binance-futures
- OKX Swap: https://docs.tardis.dev/historical-data-details/okex-swap
- Tardis 时间戳定义: https://docs.tardis.dev/faq/data
- CSV quotes vs book_ticker: https://docs.tardis.dev/downloadable-csv-files/data-types

| 项目 | Binance USDT 永续 | OKX USDT Swap |
|---|---|---|
| Tardis 采集位置（2026 年 6–11 月） | GCP asia-northeast1，东京 | GCP asia-northeast1，东京（自 2026-05-21 起） |
| 2026 年此前采集位置 | 自 2020-05-14 起东京 | 2022-05-04 至 2026-05-21 为香港 AWS ap-east-1 |
| 交易所服务器（Tardis 官方记录） | 东京 AWS ap-northeast-1 | 自 2026-08-04 起东京 AWS ap-northeast-1；此前香港阿里云 |
| 原生 BBO | `bookTicker` | `bbo-tbt`（10ms） |
| L2 通道（本样本） | 实时动态 `depth` | 自 2026-05-21 起 `books`（100ms）；此前 `books-l2-tbt` 为 10ms |

Tardis `local_timestamp` 表示采集器接收到 WebSocket 消息的时刻；CSV 采用微秒精度。两所 2026-06 至 2026-11 采集器处于同一云区域，官方文档未说明是否同一可用区/同一主机，也不保证网络路径延迟相同。

**对之前预测实验的关键修正：** Tardis `quotes` 来源于 L2 订单簿重建，而两所 2026 年采样的 L2 发布频率可能不同。先前“Binance 事件触发时，100ms 目标误差改善 18.56%”只能当作特定数据源下的先导观察，**不得用于学术结论或声称存在可执行的跨所领先**。首先改用两所 `book_ticker`（Binance 原生 bookTicker、OKX 原生 bbo-tbt）做同口径对照，检验更高频目标行情。

## 二、当前验证集的实际功能

- 训练集：2026-06-01、07-01、08-01；
- 验证集：2026-09-01；
- 初始测试集：2026-10-01、11-01。

当前 `scripts/predict_leadlag.py` 在训练集上估计固定正则化强度（0.01）的岭回归参数；9 月只输出指标，没有执行参数搜索或模型选择，**属于中间时期的独立诊断样本**。

由于已多次观察 10–11 月在不同特征/时钟下的表现，这些日期今后应视为探索过程已使用样本；最终论文应在冻结预处理、特征、时钟、参数和模型后，采用额外的未触碰日期进行样本外确认。

## 三、参考文献：指标设计

Rajendran, Suresh & Singaravelu, Divya (2026), *Order-flow toxicity in cryptocurrency perpetual futures: Order flow signals, price-impact persistence, and the limits of volatility-based metrics*, International Review of Economics & Finance, DOI: 10.1016/j.iref.2026.105876。公开文献：
https://doi.org/10.1016/j.iref.2026.105876

原文 §3.2 和 Appendix A：先构建 21 个候选指标，去掉两个直接波动率指标后，No-vol-19 留 19 个订单簿及成交流指标；指标全部滞后到上一秒，使用 1s 数据。文章预测的是未来 5s 带订单流方向的逆向选择/价格冲击风险，并研究尾部风险的样本外排序；本项目的主因变量是 OKX 未来 20ms–5s 中间价收益，**不直接复制该论文的标签与时间聚合频率**。

可按现有数据逐步扩展：

| 优先级 | 特征（英文） | 数据需求 | 对预测的潜在意义 |
|---|---|---|---|
| 先做 | Bid/ask queue imbalance | quotes/book_ticker | 双侧可见流动性失衡，已有 1 档版本 |
| 先做 | Weighted mid-price displacement | quotes/book_ticker | 加权报价与简单中间价的偏差 |
| 先做 | Best-level Order Flow Imbalance (OFI) | 连续 quotes/book_ticker | 结合买卖档价格和挂单量变化，比较更新与撤销的净压力 |
| 先做 | Short-window quote update activity / quote age | quotes/book_ticker | 检查信息更新强度和数据源之间异步时序 |
| 后做 | Trade volume/count imbalance | trades | 根据主动买卖的成交量和笔数衡量交易方向 |
| 后做 | Signed flow autocorrelation and intensity | trades | 识别连续主动冲击 |
| 进入 L2 后 | Passive withdrawal / multi-level pressure | incremental_book_L2 | 研究不同深度订单撤销、订单簿深度压力 |

其他相关研究：
- Cont, Rama; Kukanov, Arseniy; Stoikov, Sasha (2014). *The Price Impact of Order Book Events*, Journal of Financial Econometrics, 12(1), 47–88. DOI: 10.1093/jjfinec/nbt003。
- Gould, Martin D.; Bonart, Julius (2015，arXiv 预印本). *Queue Imbalance as a One-Tick-Ahead Price Predictor in a Limit Order Book*. https://arxiv.org/abs/1512.03492
- Stoikov, Sasha (2018). *The micro-price: a high-frequency estimator of future prices*, Quantitative Finance, 18(12), 1959–1966. DOI: 10.1080/14697688.2018.1489139。

**重要**：Stoikov 定义的 micro-price 是预测未来价格的估计量；最简单的按对手档数量加权的中间价只是一个可检验代理，不能将二者无条件视为同一个指标。

## 四、下一步先后顺序

1. 先只验证同一日的 `book_ticker` 是否可从 Tardis 免费数据下载，并记录 Binance/OKX 原生 BBO 数据频率、时间戳、覆盖率。
2. 再在一致的原生 BBO 口径下重跑同样的 P1/P2 与三种决策时钟，检查旧 `quotes` 结果是否被推翻。
3. 通过后引入最少数量的 OFI、加权中间价偏移和成交量不平衡特征，逐步做消融；特征与参数只在训练/验证日筛选，冻结后用真正未触碰的测试日确认。
4. B（漂移动态控制）与 C（信号驱动成交强度）继续暂缓，以 GLFT 参考价平移 A 为回测主线。
