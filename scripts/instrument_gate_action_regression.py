"""Deterministic action-request auditing of two unchanged research strategy sources.

Creates temporary *instrumented copies* solely inside the CI test.  Production
legacy functions and production shared core are never modified by this module.
"""
import importlib.util
from pathlib import Path


def instrument_action_requests(source_path, output_path, function_name):
    source = Path(source_path).read_text(encoding="utf-8")
    def replace(anchor, value):
        nonlocal source
        n = source.count(anchor)
        if n != 1:
            raise ValueError(f"Expected one instrumentation anchor, got {n}: {anchor}")
        source = source.replace(anchor, value)
    replace(
        "    bid_action = ask_action = t0 - MIN_REQUOTE_NS",
        "    bid_action = ask_action = t0 - MIN_REQUOTE_NS\n"
        "    action_log = np.empty((1_000_000, 4), np.int64)\n"
        "    n_actions = 0")
    # Both strategies use the identical old exchange request API; emit
    # [local request nanoseconds, operation (1 cancel/2 buy/3 sell),
    #  order ID, price tick] in actual code execution order.
    cancel_anchor = "                        engine.cancel(0, order.order_id, False)"
    if source.count(cancel_anchor) != 2:
        raise ValueError("Expected exactly two side-specific cancel sites")
    trace_cancel = (
        "                        if n_actions >= len(action_log):\n"
        "                            raise ValueError('Order action audit limit exceeded')\n"
        "                        action_log[n_actions,0] = now\n"
        "                        action_log[n_actions,1] = 1\n"
        "                        action_log[n_actions,2] = order.order_id\n"
        "                        action_log[n_actions,3] = order.price_tick\n"
        "                        n_actions += 1\n"
        + cancel_anchor
    )
    source = source.replace(cancel_anchor, trace_cancel)
    buy_anchor = "                engine.submit_buy_order(0, oid, want_bid*tick_size, order_qty, GTX, LIMIT, False)"
    sell_anchor = "                engine.submit_sell_order(0, oid, want_ask*tick_size, order_qty, GTX, LIMIT, False)"
    for code, anchor, quote in ((2,buy_anchor,"want_bid"),(3,sell_anchor,"want_ask")):
        replace(anchor,
                "                if n_actions >= len(action_log):\n"
                "                    raise ValueError('Order action audit limit exceeded')\n"
                "                action_log[n_actions,0] = now\n"
                f"                action_log[n_actions,1] = {code}\n"
                "                action_log[n_actions,2] = oid\n"
                f"                action_log[n_actions,3] = {quote}\n"
                "                n_actions += 1\n"
                + anchor)
    replace("activated,active_decisions,tie_excluded],dtype=np.int64))",
            "activated,active_decisions,tie_excluded],dtype=np.int64)), action_log[:n_actions]")
    Path(output_path).write_text(source,encoding="utf-8")
    spec=importlib.util.spec_from_file_location(Path(output_path).stem,output_path)
    if spec is None or spec.loader is None:
        raise ValueError("Cannot import instrumented test-only module")
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module,function_name)
