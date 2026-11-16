# Binance-origin Gate GLFT: six UTC-day fee-inclusive exploratory comparison

> ⚠️ **历史含返佣结果，仅用于审计。** 本表采用了未获用户授权的 Maker **−0.01%** 返佣，不能作为正常扣费后净收益结论。正确的普通账户 Maker **+0.02%** 扣费情景请使用 [六日无返佣、手续费后净收益复核](../gate_origin_glft_full_day_vip0_no_rebate/report.md)。

All dates are previously explored historical samples, not blind tests.
Net means official HBT end-of-day marked equity **after** fees/rebates;
negative signed fees represent maker rebates in the Gate public-fee scenario.
This does not include funding payments or final forced liquidation cost.
Each date trades 00:10–24:00 UTC; first 10min are calibration only.
Each model shares queue, delays, inventory/risk parameter and feed.

| Model | Equal-day net mean (USDT/day) | Mean net delta vs GLFT | Daily mean fills | Daily mean cancels |
|---|---:|---:|---:|---:|
| glft | +22.9505 | +0.0000 | 8898.8 | 98669.7 |
| both_0p25 | +24.4139 | +1.4635 | 7167.0 | 106809.0 |
| both_0p5 | +25.2795 | +2.3290 | 6584.0 | 106886.7 |
| both_0p75 | +25.4143 | +2.4638 | 6303.3 | 106847.8 |
| both_1p0 | +25.8043 | +2.8538 | 6232.0 | 106288.2 |
| defensive_0p25 | +22.4964 | -0.4541 | 7117.0 | 102880.7 |
| defensive_0p5 | +22.3249 | -0.6256 | 6495.0 | 103078.2 |
| defensive_0p75 | +21.4275 | -1.5230 | 6159.0 | 103159.0 |
| defensive_1p0 | +21.0364 | -1.9140 | 6028.3 | 102857.2 |

## Daily fee-inclusive net equity (USDT, 1430min/day)

| Date | glft | both_0p25 | both_0p5 | both_0p75 | both_1p0 | defensive_0p25 | defensive_0p5 | defensive_0p75 | defensive_1p0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-06-01 | +53.0493 | +69.5760 | +77.2939 | +81.4920 | +84.5841 | +58.8026 | +61.9239 | +60.5148 | +59.7577 |
| 2026-07-01 | +26.2307 | +21.1535 | +20.3267 | +19.6116 | +19.3161 | +20.9156 | +19.3136 | +18.2620 | +17.7193 |
| 2026-08-01 | +5.8846 | +5.3311 | +4.9314 | +4.9229 | +4.9131 | +5.1226 | +4.6673 | +4.6340 | +4.5793 |
| 2026-09-01 | +24.0414 | +22.9452 | +22.5163 | +21.5458 | +21.4028 | +22.7718 | +21.9258 | +20.7805 | +20.3907 |
| 2026-10-01 | +27.0469 | +25.5581 | +24.9371 | +23.2633 | +23.0878 | +25.6604 | +24.6895 | +22.9790 | +22.5099 |
| 2026-11-01 | +1.4498 | +1.9197 | +1.6715 | +1.6502 | +1.5219 | +1.7052 | +1.4290 | +1.3945 | +1.2617 |

## Caveats
- All beta and both/defensive modes were fixed before this six-day comparison.
- Fee/rebate scenario uses Gate API at run time; historical account-specific fees unknown.
- Tardis collector timestamps and assumed Binance extra 2.5ms delay do not prove production latency.
- Signals use 600s time-weighted historical spread & sigma updated every 5 seconds.
- The sign/magnitude and activation filters do not establish Binance causal leadership.
- Compare fills and fee changes before attributing higher net PnL to lower adverse selection.
- Additional 100/500ms fill markout audits and untouched test dates remain necessary.
