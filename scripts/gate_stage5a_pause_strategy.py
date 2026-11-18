"""Stage5A risk-side pause entrypoint; execution uses one shared HBT event loop."""
from numba import njit
from core_glft_engine import run_core_glft, gate_origin_filter, legacy_public_result
from g0_glft_baseline import GAMMA, TICK, CTVAL, ORDER_QTY, POSITION_CAP


@njit
def stage5_risk_pause_sides(quote_mode, shift_ticks):
    return (quote_mode == 3 and shift_ticks < 0.0,
            quote_mode == 3 and shift_ticks > 0.0)


def run_gate_stage5a_pause(engine, binance_visible_ns, binance_mid, trade_steps,
                           beta, basis_grid_ns, basis_values, basis_sigmas,
                           quote_mode, gamma_code=GAMMA,
                           tick_size=TICK, contract_value=CTVAL,
                           order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                           recorder=None, stop_at_ns=0):
    return legacy_public_result(run_core_glft(
        engine,binance_visible_ns,binance_mid,trade_steps,
        beta,basis_grid_ns,basis_values,basis_sigmas,quote_mode,
        gamma_code,tick_size,contract_value,order_qty,position_cap,
        recorder,stop_at_ns))
