"""Stage3 original GLFT shadow-gate diagnostic; single shared event loop."""
from core_glft_engine import run_core_glft, gate_origin_filter
from g0_glft_baseline import GAMMA, TICK, CTVAL, ORDER_QTY, POSITION_CAP


def run_gate_stage3_shadow(engine, binance_visible_ns, binance_mid, trade_steps,
                           beta, basis_grid_ns, basis_values, basis_sigmas,
                           quote_mode, gamma_code=GAMMA,
                           tick_size=TICK, contract_value=CTVAL,
                           order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                           recorder=None, stop_at_ns=0, probe_ns=None, shadow_gate=False):
    return run_core_glft(
        engine,binance_visible_ns,binance_mid,trade_steps,
        beta,basis_grid_ns,basis_values,basis_sigmas,quote_mode,
        gamma_code,tick_size,contract_value,order_qty,position_cap,
        recorder,stop_at_ns,probe_ns,shadow_gate)
