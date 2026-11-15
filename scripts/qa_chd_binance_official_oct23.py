"""Check remaining Binance Oct 23 missing-ID candidate against official trades."""
from pathlib import Path
import qa_chd_binance_official_crosscheck as x

x.DAY = "2026-10-23"
x.INTERVALS = [(8198073900, 8198073908)]
x.OUTPUT = Path("reports/chd_binance_20261023_exchange_crosscheck.json")
if __name__ == "__main__":
    x.main()
