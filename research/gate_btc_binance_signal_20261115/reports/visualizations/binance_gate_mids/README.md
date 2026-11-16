# Binance / Gate event-aligned mid-price plots

BTC USDT perpetuals, Tardis 1st-of-month free data, 00:10–01:00 UTC, 50 minutes for each date.

**Blue**: Binance native `book_ticker` midpoint. **Red**: Gate Tardis `quotes` midpoint reconstructed from L2. **Yellow**: raw difference between the two mid-prices (top shaded area and bottom gap panel).

The time axis uses Tardis collector `local_timestamp` (UTC). Take the union of **all event timestamps from both exchanges**. At each timestamp take the latest available price at or before that time (backward as-of). Rows sharing a microsecond timestamp are collapsed to the final quote at that timestamp, so no inter-venue ordering is asserted inside a microsecond. No future quotes, no transport delay adjustment, and no 60-second basis subtraction. This is descriptive visualization, not a trading backtest.

- [2026-09-01 — 50 minutes](binance_gate_BTC_2026-09-01_50min.png)
- [2026-10-01 — 50 minutes](binance_gate_BTC_2026-10-01_50min.png)
- [2026-11-01 — 50 minutes](binance_gate_BTC_2026-11-01_50min.png)
