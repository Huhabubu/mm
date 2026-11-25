# USDC 永续 GLFT 做市最小实验（USDT 永续信号）

目标只是先跑通最小链路：Tardis 免费日数据 → hftbacktest 2.4.4 → 在 Binance USDⓈ-M **ETHUSDC** 永续上用简单 GLFT 双边挂 post-only 单，公允价可选由 **ETHUSDT** 永续 bookTicker 引导。结果只用于验证链路与第一组诊断，不构成策略结论。

## 运行方式

向分支 `exp/usdc-perp-glft-min` 推送本目录或 `.github/workflows/usdc-perp-glft-min.yml` 即触发：

1. GitHub runner 先跑 `test_glft_core.py`（只依赖 numpy + numba）；
2. 用仓库 secret `KAGGLE_API_TOKEN` 创建/更新私有 Kaggle notebook `usdc-perp-glft-min`，在 Kaggle 上下载 Tardis 数据、转换并回测；
3. 只取回 `report.json`、`run.log`、`job_status.json` 三个汇总文件；原始行情与 NPZ 只留在 Kaggle `/tmp`。

不使用 Google Drive、不挂载凭据数据集，也不改动 `exp/tokyo-l2-glft` 的 stage3 notebook。

## 数据

| 用途 | Tardis 交易所 / 类型 | 合约 |
|---|---|---|
| 撮合回放 | `binance-futures` `trades` + `incremental_book_L2` | ETHUSDC |
| 公允价信号 | `binance-futures` `book_ticker` | ETHUSDT |

默认日期 2026-11-01（Tardis 免费日为每月 1 日，当天为周日）。tick 0.01、lot 0.001 取自 2026-11-25 的 exchangeInfo，未核实 11-01 当天是否相同。

## 模型

决策步长 100ms，前 10 分钟只估计不挂单。

- 波动率：最近 10 分钟每步中间价变化 $\Delta m$（tick），$\sigma=\sqrt{\mathrm{Var}(\Delta m)/\Delta t}$。
- 成交强度：每步成交价相对上一步中间价的最大穿透深度（0.1 tick 分格），最小二乘拟合 $\lambda(\delta)=A e^{-k\delta}$。
- GLFT（Guéant 2017 闭式近似，与 hftbacktest 教程同式），库存 $q$ 以单笔下单量为单位，$\Delta=1$：

$$
c_1=\frac{1}{\gamma\Delta}\ln\left(1+\frac{\gamma\Delta}{k}\right),\qquad
c_2=\sqrt{\frac{\gamma}{2A\Delta k}\left(1+\frac{\gamma\Delta}{k}\right)^{\frac{k}{\gamma\Delta}+1}}
$$

$$
p^{bid}=f-\left(c_1+\tfrac{\Delta}{2}\sigma c_2\right)-\sigma c_2\,q,\qquad
p^{ask}=f+\left(c_1+\tfrac{\Delta}{2}\sigma c_2\right)-\sigma c_2\,q
$$

- 公允价 $f=m^{C}+\alpha\left(m^{T}+b-m^{C}\right)$，$b$ 为 $m^{C}-m^{T}$ 的 EWMA（半衰期 60s）；$\alpha=0$ 为纯 GLFT，$\alpha=1$ 为完全由 USDT 永续引导。
- 报价不越过对手最优价（GTX）；库存上限 ±5 单位；每单约 0.03 ETH；报价变化才撤改。
- hftbacktest：订单往返各 1ms 固定延迟、`power_prob(2.0)` 队列、不部分成交、maker 费 0。
- 参数组 $(\alpha,\gamma)\in\{0,1\}\times\{0.05,0.01\}$。

## 输出（`report.json`）

每组参数：零费 PnL、成交笔数与成交额、假设 maker 费 0.5/1/2bp 的 PnL、1/5/30/60s markout、逐小时 PnL、最大回撤、最大库存、拟合参数分位数、信号可用比例。markout 定义：

$$
\text{markout}_{h}=s\cdot\frac{m^{C}_{t+h}-p_{fill}}{p_{fill}}\times10^{4}\ \text{bp},\qquad s=\pm1
$$

## 已知限制

- 单币、单日（周日），参数未调，不能据此判断策略好坏。
- 100ms 决策步长无法体现亚百毫秒的信号优势；现货信号尚未接入。
- 成交价由每步仓位与余额变化反推，同一步多笔成交会合并（计数见 `multi_fill_steps`）。
- USDC 永续 maker 0 费率为使用者给定口径，未核实是否覆盖 2026-11-01。
- 队列模型与 1ms 延迟是假设，不代表东京实机。
