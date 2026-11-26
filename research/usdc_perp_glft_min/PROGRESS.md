# 进度记录（2026-11-26）

分支 `exp/usdc-perp-glft-min`（T4，独立探索，不作论文正式证据）。

## 第二版：Gate 研究的价差信号移植（进行中）

改动起点 `f1af35c`，代码提交 `b4eee4e`、`e1bebfa`、`4414cef`、`e1aefb1`、`38eb67d`，触发运行的提交 `bd0ed84`。

- `basis_moments.py`：`exact_moments_5s` 原样复制自 `exp/tokyo-l2-glft@6a6345b` 的 Gate 研究 `gate_basis_weighted_moments.py`，即 600s 连续时间加权的 $B_t$、$\sigma_t$，每 5s 发布一次，覆盖率低于 99% 时报错。
- `origin_glft.py`：`gate_origin_filter` 原样复制自同一提交的 `gate_origin_glft_strategy.py`，规则为 1σ 正常带、1.645σ 触发、主导检验、Binance 价格事件激活、平局剔除。事件循环参照 `run_gate_origin_glft`，按用户要求有以下改动：
  - 去掉 `MIN_REQUOTE_NS`（20ms）限频，目标价位一变就撤单重挂；撤单未确认前不重挂。
  - USDT 信号不加额外延迟，只加 +1ns 用于同时间戳排序。
  - 订单延迟单程 1ms。
  - maker 费为 0。
  - GLFT 的拟合沿用本目录 `glft_core`。
- `kaggle_task.py`：9 个模型，即 β=0 基准，加上两侧平移和单边防御各取 β∈{0.25,0.5,0.75,1}。目标 ETHUSDC 永续，信号 ETHUSDT 永续 bookTicker。$B_t$ 用两边的 bookTicker 中间价离线计算。
- `test_origin_glft.py`：10 个单元测试，覆盖过滤器状态转移，以及基准矩的常数、交替序列和无前视性。
- Workflow：增加新测试，预期模型数改为 9；Kaggle notebook 打包时加入两个新模块。

## 运行

- [Actions 45764146949](https://github.com/Huhabubu/mm/actions/runs/45764146949)（`bd0ed84`）：runner 上 16 个单元测试全部通过。15 分钟冒烟测试完成，只有最后 5 分钟在挂单，9 个模型结果完全相同：PnL −0.077 USDC，14 笔成交。原因是这 5 分钟内 1.645σ 触发 9340 次，主导条件一次都没满足，信号从未激活。仅凭冒烟窗口无法判断这是市场常态还是实现问题，要看全天结果。ETHUSDT 的中间价变化事件只有 599 次（15 分钟），这个数偏低，需要在全天报告里用 `signal_rows` 核对。
- Kaggle 全天（同一运行，`job_status.git_sha` = `bd0ed84` 已核对，returncode 0）：ETHUSDC 2026-11-01，maker 费 0，单程延迟 1ms，无改单限频。

  | 模型 | PnL (USDC) | 成交 | 成交额 (USDC) | 100ms / 1s / 60s markout (bp) |
  |---|---|---|---|---|
  | baseline | −25.69 | 6625 | 449,848 | −0.471 / −0.517 / −0.574 |
  | both β0.25 / 0.5 / 0.75 / 1 | −25.58 / −25.70 / −25.83 / −25.95 | 6601–6663 | 448k–452k | 约 −0.47 / −0.51 / −0.57 |
  | defensive β0.25 / 0.5 / 0.75 / 1 | −25.45 / −25.35 / −25.22 / −25.19 | 6527–6561 | 443k–446k | 约 −0.47 / −0.51 / −0.57 |

  信号诊断（各 β 基本一致）：决策约 1691 万次，触发检查 1691 万次，超过 1.645σ 264 万次（15.6%），通过主导检验 25 万次，激活 1438 次，信号生效的决策 21.2 万次，占全部决策 1.25%，平局剔除 0 次。基准差 $B_t$ 中位数 2.258 USDT，$\sigma_t$ 中位数 8.66 tick。
  结论：这只是单日探索，没有做显著性检验。单边防御随 β 增大单调改善，β=1 比基准好 0.50 USDC（约 2%）；两侧平移在 β≥0.5 时略差。markout 几乎没有变化，亏损主体仍是基准 GLFT 在 100ms 内遭到的逆向选择。
  待核对：ETHUSDT bookTicker 全天 1481 万行，但判定的中间价变化只有 81,829 次（约 0.95 次/秒），与 USDC 的 $\sigma\approx6.3$ tick/$\sqrt{s}$ 相比偏低。目前分不清是行情本身如此，还是唤醒或计数逻辑有问题，需要直接用 CSV 统计中间价变化次数来核对。

## 第一版（已作废的信号用法）

[Actions 45741078187](https://github.com/Huhabubu/mm/actions/runs/45741078187)（`b7e80f8`）：100ms 轮询，EWMA 基准，整段直接平移。4 组参数全部亏损，PnL 在 −30.5 到 −45.4 USDC 之间。这一版的信号用法与 Gate 研究不一致，不能作为对这套信号的检验。

## 下一步

先核对 ETHUSDT 中间价变化次数（CSV 直接统计 vs 回测内计数）；之后的方向等待用户决定。
