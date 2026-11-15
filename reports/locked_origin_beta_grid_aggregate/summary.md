# Locked-origin Binance gate: beta gradient official HBT

Tardis real BBO (Binance, OKX BTC/USDT-SWAP), native OKX L2/trades
converted with official hftbacktest 2.4.4. Historical normal gap
mean and population std use exact continuous time-weighting over
the prior 600 seconds, recomputed every 5 seconds, no quote-age cap.
Inside 1sigma refresh Binance/OKX origin prices; outside lock until
reentry. Require >1.96sigma abnormal spread, Binance same direction
and abs Binance move > abs OKX move, then shift GLFT centre by beta*gap.

Same market tape, same calibration, same fees, inventory and 1ms/1ms
order latency for all five beta values. No legacy P90/P50 gate.
Signal measured using 2.5ms hypothetical Binance added delay.
All net results are fee-inclusive, BTC target market only;
not a 24-hour result or fresh holdout. Funding and final forced
unwind/slippage are absent.

| Beta | Mean net USDT/50m | Mean change vs beta0 USDT | Mean fee USDT | Mean fills | Mean cancels |
|---:|---:|---:|---:|---:|---:|
| 0 | -4.0388 | +0.0000 | 2.7346 | 170.7 | 1588.7 |
| 0.25 | -3.7484 | +0.2904 | 2.5425 | 158.7 | 1715.0 |
| 0.5 | -3.7350 | +0.3037 | 2.5329 | 158.0 | 1718.7 |
| 0.75 | -3.7240 | +0.3148 | 2.5434 | 158.7 | 1723.7 |
| 1 | -3.7113 | +0.3274 | 2.5211 | 157.3 | 1712.3 |

## Individually paired days

- 2026-09-01: source events 676,480, >1.96sigma with anchor 58,230, passes source dominance 30,650, gate onsets 69
  - beta 0: net -6.3762 USDT, fee 4.2525, fills 270, cancels 2349
  - beta 0.25: net -6.2740 USDT, fee 4.1580, fills 264, cancels 2417
  - beta 0.5: net -6.2934 USDT, fee 4.1580, fills 264, cancels 2420
  - beta 0.75: net -6.3292 USDT, fee 4.1895, fills 266, cancels 2415
  - beta 1: net -6.3344 USDT, fee 4.1895, fills 266, cancels 2407
- 2026-10-01: source events 542,840, >1.96sigma with anchor 45,427, passes source dominance 35,159, gate onsets 120
  - beta 0: net -4.0185 USDT, fee 2.7876, fills 167, cancels 1784
  - beta 0.25: net -3.5686 USDT, fee 2.5539, fills 153, cancels 1996
  - beta 0.5: net -3.5873 USDT, fee 2.5872, fills 155, cancels 1996
  - beta 0.75: net -3.4959 USDT, fee 2.5872, fills 155, cancels 2014
  - beta 1: net -3.4614 USDT, fee 2.5204, fills 151, cancels 1985
- 2026-11-01: source events 254,826, >1.96sigma with anchor 39,874, passes source dominance 33,203, gate onsets 64
  - beta 0: net -1.7216 USDT, fee 1.1638, fills 75, cancels 633
  - beta 0.25: net -1.4024 USDT, fee 0.9156, fills 59, cancels 732
  - beta 0.5: net -1.3244 USDT, fee 0.8535, fills 55, cancels 740
  - beta 0.75: net -1.3468 USDT, fee 0.8535, fills 55, cancels 742
  - beta 1: net -1.3382 USDT, fee 0.8535, fills 55, cancels 745

### Interpretation limit
This gate is a **directional excursion classification**, not proof of
causal Binance price leadership. Past monthly free dates were already
used for exploratory decisions; use fresh market dates/symbols for
out-of-sample significance. The native target BBO source gate is
rechecked against the HBT fused current target midpoint at execution.
