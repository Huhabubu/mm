"""Memory-bounded Tardis CSV -> official HBT NPZ segment conversion.

Each source CSV is consumed once in original local-timestamp row order.
We split the DATA FILES only; the official HftBacktest engine is constructed
exactly once per strategy from the ordered list of NPZ files. Never reset
queue, inventory, cash, order replies, or GLFT rolling estimation at boundaries.
"""
import csv
import gc
from contextlib import ExitStack
from pathlib import Path

from check_okx_hft_data import SCHEMAS


def convert_fuse_parts(paths, counts, root, day_start_us, end_us, chunk_minutes,
                       tick_size, lot_size):
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order

    if chunk_minutes < 1 or chunk_minutes > 1440:
        raise ValueError("Chunk minutes out of range")
    if end_us <= day_start_us:
        raise ValueError("Invalid UTC interval")
    root = Path(root)
    parts = []
    details = []
    consumed = dict.fromkeys(SCHEMAS, 0)
    with ExitStack() as stack:
        readers = {}
        pending = {}
        for kind in SCHEMAS:
            handle = stack.enter_context(Path(paths[kind]).open(
                encoding="utf-8", newline=""))
            readers[kind] = csv.DictReader(handle)
            pending[kind] = next(readers[kind], None)
        start = day_start_us
        index = 0
        while start < end_us:
            end = min(start + chunk_minutes * 60 * 1_000_000, end_us)
            # Short-window runners append 0.5s of cleanup tape after 60m.
            # Merge that tiny tail into this last full segment, preserving it.
            if end_us - end <= 1_000_000:
                end = end_us
            part_csv = {}
            part_counts = {}
            for kind, fields in SCHEMAS.items():
                segment = root / f"part_{index:03d}_{kind}.csv"
                part_csv[kind] = segment
                with segment.open("w", encoding="utf-8", newline="") as out:
                    writer = csv.DictWriter(out, fieldnames=fields)
                    writer.writeheader()
                    n = 0
                    row = pending[kind]
                    while row is not None and int(row["local_timestamp"]) < end:
                        if int(row["local_timestamp"]) < start:
                            raise RuntimeError(f"{kind} row crossed segment boundary backwards")
                        writer.writerow(row)
                        n += 1
                        row = next(readers[kind], None)
                    pending[kind] = row
                    part_counts[kind] = n
                    consumed[kind] += n
            if min(part_counts.values()) <= 0:
                raise RuntimeError(f"Empty Tardis source in segment {index}: {part_counts}")
            dest = root / f"hbt_part_{index:03d}.npz"
            print("FULL_DAY_PART_BEGIN", index, "start_us", start,
                  "end_us", end, "rows", part_counts, flush=True)
            fused = tardis.convert_fuse(
                str(part_csv["trades"]), str(part_csv["incremental_book_L2"]),
                str(part_csv["book_ticker"]), tick_size=tick_size,
                lot_size=lot_size, output_filename=str(dest),
                ss_buffer_size=max(
                    1000, counts["incremental_book_L2"]["snapshot_rows"] + 100),
                snapshot_mode="process")
            validate_event_order(fused)
            events = len(fused)
            if not events:
                raise RuntimeError(f"Empty official HBT segment {index}")
            del fused
            gc.collect()
            for csv_file in part_csv.values():
                csv_file.unlink()
            parts.append(dest)
            details.append({"index": index, "start_us": start, "end_us": end,
                            "source_rows": part_counts, "hbt_events": events})
            print("FULL_DAY_PART_END", index, "events", events,
                  "file_bytes", dest.stat().st_size, flush=True)
            start = end
            index += 1
        for kind, expected in counts.items():
            if consumed[kind] != expected["rows"]:
                raise RuntimeError(
                    f"Lost/repeated {kind} rows in conversion: "
                    f"{consumed[kind]} != {expected['rows']}")
    return parts, details
