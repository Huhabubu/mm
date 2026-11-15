"""Causally normalize Gate per-message *complete* L2 snapshots to deltas.

Only a data-format adapter for official hftbacktest: no matching, queues or
fills. Snapshot groups are keyed by Tardis collector local_timestamp. They
must contain both bid and ask sides; if not, fail rather than guessing.
"""
from itertools import chain,groupby


def normalized_gate_l2_rows(reader, stats):
    group_iterator=((key,list(rows)) for key,rows in groupby(
        reader,key=lambda row:row["local_timestamp"]))
    first=next(group_iterator,None)
    second=next(group_iterator,None)
    if first is None or second is None:
        raise ValueError("Need at least two L2 update groups")
    first_flags={row["is_snapshot"].lower() for row in first[1]}
    second_flags={row["is_snapshot"].lower() for row in second[1]}
    first_is_snapshot=first_flags=={"true"}
    second_is_snapshot=second_flags=={"true"}
    if not first_is_snapshot:
        raise ValueError("Gate L2 should begin with an initial snapshot")
    if second_is_snapshot:
        stats["l2_format"]="per_message_full_snapshots_rewritten_to_deltas"
        previous={}
        for i,(stamp,group) in enumerate(chain((first,second),group_iterator)):
            if any(row["is_snapshot"].lower()!="true" for row in group):
                raise ValueError(f"Gate full-snapshot feed unexpectedly changed mode at {stamp}")
            next_book={}
            last_row_by_price={}
            sides=set()
            for row in group:
                side=row["side"];price=float(row["price"])
                if side not in ("bid","ask"):
                    raise ValueError("Invalid Gate snapshot side")
                sides.add(side)
                size=float(row["amount"])
                if size<0:
                    raise ValueError("Negative snapshot size")
                key=(side,price)
                next_book[key]=size
                last_row_by_price[key]=row
            if sides!={"bid","ask"}:
                raise ValueError(f"Gate complete snapshot missing a side at {stamp}")
            stats["source_snapshot_groups"]+=1
            stats["source_l2_rows"]+=len(group)
            if i==0:
                # Keep only the first *full* snapshot as an initial snapshot.
                for row in group:
                    yield row
                    stats["emitted_l2_rows"]+=1
            else:
                # Remove levels that disappeared in a full depth replacement.
                for (side,price) in sorted(previous.keys()-next_book.keys()):
                    deleted=dict(group[0])
                    deleted["is_snapshot"]="false"
                    deleted["side"]=side
                    deleted["price"]=str(price)
                    deleted["amount"]="0"
                    yield deleted
                    stats["emitted_l2_rows"]+=1
                    stats["removed_l2_levels"]+=1
                # Replace only price levels whose absolute quantities changed.
                # Exact same-timestamp row order is preserved for new/changed.
                for key,row in last_row_by_price.items():
                    if previous.get(key)!=next_book[key]:
                        new=dict(row);new["is_snapshot"]="false"
                        yield new
                        stats["emitted_l2_rows"]+=1
            previous=next_book
    else:
        stats["l2_format"]="initial_snapshot_plus_native_deltas"
        for _,group in chain((first,second),group_iterator):
            stats["source_l2_rows"]+=len(group)
            for row in group:
                yield row
                stats["emitted_l2_rows"]+=1
