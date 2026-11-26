# Unified Backtest & Five-Stage Research Report: `tokyo_l2_20261123_binance_gate_btc_unified`

## 1. Experiment Configuration & Provenance

- **Data Source**: `google_drive_tokyo_l2`
- **Exchange Pair**: `binance_futures` (`BTCUSDT`) -> `gate_futures` (`BTC_USDT`)
- **Base Symbol**: `BTC`
- **Eval Dates**: `['2026-11-23']` | **Train Dates**: `[]` | **Holdout Dates**: `[]`
- **Latency & Order Controls**: signal extra delay `0.000 ms`, order entry `1.000 ms`, order response `1.000 ms` (Uniform 1ms one-way simulated assumption; not measured live exchange order latency), min requote cooldown `0.000 ms`, native modify `0`
- **Config SHA-256**: `d266adcd789a147146d106de78f519451f03cc9dc8e65a1dc6a1f0a51144ba38`
- **Status**: `complete` | **Started UTC**: `2026-11-26T10:15:32Z` | **Finished UTC**: `2026-11-26T11:24:35Z`

---
## 2. Trading Day: `2026-11-23`

- **Conversion Cache**: `hit=False` (`cache_miss`), key=`62943a3e271e0b9288601d49`
- **Archive & QA**: status=`ok`, failed_items=`[]`, partitions=`192`
- **Interruptions / Anomalies**: `3` episodes (diagnostic only; official HBT fills preserved)

- **Contract & Sizing**: tick=`0.1` (legal=`0.1`, mult=`1`), lot=`1.0`, ctval=`0.0001`, qty=`13.0`, cap=`260.0`, gamma_code=`0.0005`, shared_start_ns=`1795392600166000000`

### Stage 5A: Model Comparison (Zero-Fee Gross Primary + Fee Sensitivity)

| Model | Fills | Turnover (USDT) | Gross PnL (USDT) | Gross (bps) | Net +2bps (USDT) | TM 100ms | TM 500ms | TM 1000ms | Pos Std | Max Abs | Submits | Cancels | Max Ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `glft` | 20,146 | 1621045.19 | -70.3841 | -0.4342 | -394.5931 | -0.3501 | -0.4312 | -0.4104 | 4.2448 | 65.0 | 497,771 | 463,702 | 349 |
| `both_1p0` | 17,758 | 1429506.42 | -39.1148 | -0.2736 | -325.0161 | -0.3385 | -0.3793 | -0.2851 | 4.0014 | 104.0 | 526,838 | 501,394 | 491 |
| `defensive_1p0` | 16,936 | 1362773.25 | -49.4660 | -0.3630 | -322.0207 | -0.3140 | -0.3888 | -0.3510 | 3.7712 | 52.0 | 514,059 | 490,251 | 411 |
| `risk_side_pause` | 16,828 | 1354080.73 | -49.1783 | -0.3632 | -319.9945 | -0.3220 | -0.3916 | -0.3534 | 3.7678 | 52.0 | 490,081 | 466,514 | 224 |

### Stage 1: PnL Attribution & Accounting Closure (Conventions A & B)

| Model | Zero-Fee PnL | Spread Capture (A) | Inventory PnL (A) | Residual A | Spread Capture (B) | Inventory PnL (B) | Residual B | RMS Pos |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `glft` | -70.3841 | 91.1035 | -161.4876 | -0.000000 | -34.6052 | -35.7789 | -0.000000 | 4.2450 |
| `both_1p0` | -39.1148 | 78.6142 | -117.7290 | -0.000000 | -32.3628 | -6.7520 | -0.000000 | 4.0014 |
| `defensive_1p0` | -49.4660 | 78.1844 | -127.6504 | -0.000000 | -26.4348 | -23.0313 | -0.000000 | 3.7713 |
| `risk_side_pause` | -49.1783 | 75.3616 | -124.5400 | -0.000000 | -27.6894 | -21.4890 | -0.000000 | 3.7679 |

### Stage 2: Execution-Price Total Markout & Fill Quality (All / Buy / Sell / Tails)

| Model | Horizon | Fresh N | Mean (bps) | Notional-Wtd | Buy (bps) | Sell (bps) | Neg Frac | P10 (bps) | Worst10 (bps) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `glft` | 100ms | 20,143 | -0.3501 | -0.3493 | -0.3415 | -0.3587 | 0.7532 | -1.2877 | -2.4854 |
| `glft` | 500ms | 20,138 | -0.4312 | -0.4314 | -0.4101 | -0.4522 | 0.6665 | -2.1448 | -4.1763 |
| `glft` | 1000ms | 20,116 | -0.4104 | -0.4106 | -0.3733 | -0.4475 | 0.6240 | -2.6364 | -5.1179 |
| `both_1p0` | 100ms | 17,756 | -0.3385 | -0.3380 | -0.3304 | -0.3466 | 0.7417 | -1.2925 | -2.6651 |
| `both_1p0` | 500ms | 17,753 | -0.3793 | -0.3795 | -0.3338 | -0.4247 | 0.6522 | -2.1695 | -4.4516 |
| `both_1p0` | 1000ms | 17,737 | -0.2851 | -0.2845 | -0.2038 | -0.3664 | 0.6073 | -2.6385 | -5.2862 |
| `defensive_1p0` | 100ms | 16,934 | -0.3140 | -0.3132 | -0.3258 | -0.3023 | 0.7427 | -1.2488 | -2.4041 |
| `defensive_1p0` | 500ms | 16,930 | -0.3888 | -0.3889 | -0.4104 | -0.3672 | 0.6576 | -2.0679 | -3.9389 |
| `defensive_1p0` | 1000ms | 16,916 | -0.3510 | -0.3509 | -0.3706 | -0.3314 | 0.6140 | -2.5315 | -4.7802 |
| `risk_side_pause` | 100ms | 16,826 | -0.3220 | -0.3213 | -0.3311 | -0.3130 | 0.7450 | -1.2501 | -2.4018 |
| `risk_side_pause` | 500ms | 16,823 | -0.3916 | -0.3917 | -0.4135 | -0.3698 | 0.6586 | -2.0636 | -3.9397 |
| `risk_side_pause` | 1000ms | 16,808 | -0.3534 | -0.3533 | -0.3723 | -0.3345 | 0.6145 | -2.5248 | -4.7972 |

### Stage 3: Baseline GLFT Read-Only Shadow Signal Audit

- **Parity Verified**: `True` | **Total GLFT Fills**: `20146` | **Categories**: `{'inactive_or_no_effect': 17846, 'threatened': 2112, 'opposite': 188}`
- **Order Lifecycle & Price Consistency**: open_at_deadline=`0.9703`, cancellable=`0.9703`, price_exact_match_among_open=`1.0000`, threatened_theoretical_cancellable=`2028/2112` (`0.9602`)

| Category | Fills | Fraction | 100ms TM (bps) | 500ms TM (bps) | 1000ms TM (bps) | 500ms Neg Frac | 500ms Worst10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `threatened` | 2,112 | 0.1048 | -0.4288 | -0.8755 | -1.0378 | 0.6801 | -6.8320 |
| `inactive_or_no_effect` | 17,846 | 0.8858 | -0.3494 | -0.4214 | -0.3998 | 0.6685 | -3.8178 |
| `opposite` | 188 | 0.0093 | 0.4721 | 3.6276 | 5.6083 | 0.3191 | -4.4878 |

### Stage 5B: Same-Fill Execution Economics Decomposition (500ms Horizon)

| Slice | Fills | Total Markout (bps) | TM - 2bps | TM + 0.5bps | Capture (bps) | Drift (bps) | Updated-Quote TM |
|---|---:|---:|---:|---:|---:|---:|---:|
| `all_fills` | 20,146 | -0.4312 | -2.4312 | 0.0688 | 0.4666 | -0.8974 | -0.4312 |
| `threatened` | 2,112 | -0.8755 | -2.8755 | -0.3755 | 1.0103 | -1.8863 | -0.8755 |
| `inactive_or_no_effect` | 17,846 | -0.4214 | -2.4214 | 0.0786 | 0.4019 | -0.8228 | -0.4214 |
| `opposite` | 188 | 3.6276 | 1.6276 | 4.1276 | 0.5044 | 3.1228 | 3.6276 |
| `threatened_cancellable` | 2,028 | -0.8295 | -2.8295 | -0.3295 | 1.0185 | -1.8486 | -0.8295 |
| `threatened_baseline_only_vs_both_1p0` | 2,040 | -0.7862 | -2.7862 | -0.2862 | 1.0054 | -1.7922 | -0.7862 |
| `threatened_baseline_only_vs_defensive_1p0` | 2,081 | -0.8873 | -2.8873 | -0.3873 | 1.0105 | -1.8984 | -0.8873 |
| `threatened_baseline_only_vs_risk_side_pause` | 2,110 | -0.8768 | -2.8768 | -0.3768 | 1.0107 | -1.8881 | -0.8768 |

### Stage 4 & Stage 5C Governance Status

- **Stage 4 (Non-Binance Controls)**: `status=pending_independent_training_date`, `executed=False` — No independent non-overlapping training date lock provided; refusing to train Stage 4 controls on evaluation date or reuse historical Tardis-trained parameters.
- **Stage 5C (Holdout Validation)**: `status=pending_authorized_holdout_date`, `executed=False` — No authorized untouched holdout date & pre-registered mode lock provided; refusing to run Stage 5C on explored evaluation date.

