"""Stage4 reference / preselected static-widen / Gate-vol controls, shared HBT loop."""
import numpy as np
from core_glft_engine import run_core_glft, gate_origin_filter, legacy_public_result
from g0_glft_baseline import GAMMA, TICK, CTVAL, ORDER_QTY, POSITION_CAP


def run_gate_stage4_control(engine, binance_visible_ns, binance_mid, trade_steps,
                            beta, basis_grid_ns, basis_values, basis_sigmas,
                            quote_mode, gamma_code=GAMMA,
                            tick_size=TICK, contract_value=CTVAL,
                            order_qty=ORDER_QTY, position_cap=POSITION_CAP,
                            recorder=None, stop_at_ns=0, control_kind=0,
                            extra_half_ticks=0.0, local_sigma_ceiling=np.inf):
    return legacy_public_result(run_core_glft(
        engine,binance_visible_ns,binance_mid,trade_steps,
        beta,basis_grid_ns,basis_values,basis_sigmas,quote_mode,
        gamma_code,tick_size,contract_value,order_qty,position_cap,
        recorder,stop_at_ns,control_kind=control_kind,
        extra_half_ticks=extra_half_ticks,
        local_sigma_ceiling=local_sigma_ceiling))
