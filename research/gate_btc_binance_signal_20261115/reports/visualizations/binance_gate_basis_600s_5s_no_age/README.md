# Binance–Gate historical basis: four methods, matched scales

BTC USDT perpetual, 50min measurement following first 10min warmup. All four methods use a strictly-past 600s window and recompute each 5s. Binance BBO has an assumed +2.5ms visibility delay; Gate uses L2-reconstructed quotes without added delay. Event timeline is the union of both feeds, with latest historically visible value at each event. Rows at the same microsecond are reduced to the final state.

A: piecewise-constant time-weighted mean, holding the last known BBO until the next event. B: 600 fixed 1-second points, strict-past as-of, equal weight. C: equally weighted event samples, compute event mean and population sigma, discard points outside mean ±2.17009 sigma (nominal central Gaussian 97%), then recompute mean on retained points; actual rejection proportion is recorded. D: equally weighted event samples without trimming.

Quote-age exclusion: disabled. CSV retains availability coverage diagnostics (with no age cap, initial BBO presence only). Plots include available-time fraction in the lower panel. Only eligible windows enter the high-coverage stability comparison. Future 5s spread MAE is a descriptive diagnostic only; future samples are never used in estimator construction. No trading simulation or fee-inclusive PnL is performed.

- 2026-09-01: [rolling_basis_2026-09-01_50min.png](rolling_basis_2026-09-01_50min.png), [5s series](rolling_basis_2026-09-01_50min.csv)
- 2026-10-01: [rolling_basis_2026-10-01_50min.png](rolling_basis_2026-10-01_50min.png), [5s series](rolling_basis_2026-10-01_50min.csv)
- 2026-11-01: [rolling_basis_2026-11-01_50min.png](rolling_basis_2026-11-01_50min.png), [5s series](rolling_basis_2026-11-01_50min.csv)
