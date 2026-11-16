# Gate BTC_USDT rolling historical cross-venue basis: tick movement

Selected estimator: `time_weighted`, Binance minus Gate midprice,
strictly previous 600s, recomputed every 5s, without quote-age exclusion.
Binance collector timestamps have an extra assumed +2.5ms visibility delay
in the upstream CSV. Gate BBO is Tardis L2-reconstructed quotes.
Assumed Gate order price tick = **0.1 USDT per BTC**
(Gate API contract property `order_price_round`).
Gate documentation: https://www.gate.com/docs/apiv4/index.html
The historical 2026-09 to 11 instrument tick is assumed unchanged; 
validate historical contract configuration before final paper use.

10min prehistory, then UTC 00:10:00..00:59:55, 50min at 5s cadence:
600 computed basis estimates and **599 adjacent transitions per day**.
Continuous absolute change is `abs(B_t - B_(t-5s)) / 0.1` and may
be fractional. Integer quote tick level is `floor(B_t / 0.1 + 0.5)`.
Integer transition is the absolute difference of adjacent levels.
**These are basis changes, not actual maker-order amendments or cancels.**

| Day | Median abs change (tick equiv.) | P95 abs change | Max abs change | Changes >=1 tick | Changes >=0.5 tick |
|---|---:|---:|---:|---:|---:|
| 2026-09-01 | 0.328 | 0.965 | 1.725 | 25/599 | 176/599 |
| 2026-10-01 | 0.293 | 0.821 | 1.197 | 12/599 | 143/599 |
| 2026-11-01 | 0.346 | 0.864 | 1.294 | 13/599 | 180/599 |

| Day | Rounded-level changed updates | 1-tick transitions | 2-tick transitions | >=3-tick transitions | Cumulative abs integer ticks |
|---|---:|---:|---:|---:|---:|
| 2026-09-01 | 224/599 | 219 | 5 | 0 | 229 |
| 2026-10-01 | 207/599 | 206 | 1 | 0 | 208 |
| 2026-11-01 | 223/599 | 221 | 2 | 0 | 225 |

Data sources:
 - [September through November 5-second rolling baseline CSVs](../binance_gate_basis_600s_5s_no_age/)
 - Computed on existing true Tardis data, without downloading a new source.
 - All windows have >99% time coverage; at the 00:00 UTC boundary the first one
   may miss a fraction of a second before the first quote.
 - A 5-second estimate update can cross a tick boundary even when
   the raw mean changes by less than one full tick.
 - A common Gate quote level is a separate decision from GLFT inventory,
   model spread, and actual amend/cancel execution.
