"""Marcação patrimonial compartilhada; não gera sinais nem executa operações."""
import math

import numpy as np
import pandas as pd


def cashflow_values(cashflows, times):
    """Quantia por fechamento de barra; não aceita alinhamento temporal implícito."""
    if cashflows is None:
        return np.zeros(len(times), dtype=float)
    message = "Fluxos externos exigem uma quantia finita por barra, na mesma ordem e moeda da conta."
    if isinstance(cashflows, pd.Series) and isinstance(cashflows.index, pd.DatetimeIndex):
        if not cashflows.index.equals(pd.DatetimeIndex(times)):
            raise ValueError(message)
    try:
        raw = np.asarray(cashflows, dtype=object)
        if raw.ndim != 1 or len(raw) != len(times) or any(isinstance(v, (bool, np.bool_)) for v in raw):
            raise ValueError(message)
        values = raw.astype(float)
        if not np.isfinite(values).all():
            raise ValueError(message)
    except (TypeError, ValueError) as error:
        raise ValueError(message) from error
    return values


def unitized_equity(equity, flows, initial_equity):
    """Cotas emitidas/resgatadas ao NAV anterior ao fluxo, após a marca do fechamento."""
    values = np.asarray(equity, dtype=float)
    if not np.any(flows):
        return values.copy()
    message = "Fluxos externos exigem patrimônio/cotas positivos e não podem resgatar todo o patrimônio."
    if initial_equity <= 0 or not math.isfinite(initial_equity):
        raise ValueError(message)
    units = float(initial_equity)
    performance = np.empty(len(values), dtype=float)
    for i, (value, flow) in enumerate(zip(values, flows)):
        if flow:
            before = value - flow
            if not math.isfinite(before) or before <= 0 or value <= 0 or units <= 0:
                raise ValueError(message)
            nav = before / units
            units += flow / nav
            if not math.isfinite(units) or units <= 0:
                raise ValueError(message)
        performance[i] = value / units * initial_equity
    return performance


def valuation_metadata(spec, cfg):
    return {
        "version": 1, "resolution": "bar_close", "timestamp_basis": "bar_start",
        "status": "limited" if not spec.known or (cfg.swap and spec.swap_mode not in (0, 1)) or
                  (spec.provenance and spec.provenance.get("specification_status") != "known") or
                  spec.provenance.get("series_role") in ("continuous_study", "mixed") else "modeled",
        "intrabar_observed": False, "price_side": "long_bid_short_ask",
        "cost_basis": "executed_commissions_and_slippage_accrued_swap",
        "hypothetical_exit_costs": False, "currency_basis": "tick_value_account_currency",
        "fx_conversion": "not_performed", "external_cashflows": "unitized",
        "cashflow_timing": "after_bar_close", "legacy_return_basis": "pnl_over_initial_capital",
        "spec_known": bool(spec.known),
        "instrument_context": dict(spec.provenance),
        "specification_historical_status": spec.provenance.get("specification_status", "unverified_manual_input"),
        "swap_status": "disabled" if not cfg.swap else
                       "points_model" if spec.swap_mode == 1 else
                       "disabled_by_symbol" if spec.swap_mode == 0 else "unsupported_mode",
        "closed_curve_basis": "closed_trades",
    }


def floating_metrics(curve, initial_equity):
    """DD da série completa; a curva legada de encerradas mantém suas métricas."""
    values = np.asarray(curve, dtype=float)
    if not math.isfinite(initial_equity) or not np.isfinite(values).all():
        raise ValueError("Patrimônio ausente ou inválido; confira preços, custos e capital inicial.")
    out = {"max_floating_drawdown_money": None, "max_floating_drawdown_pct": None,
           "floating_drawdown_status": "no_observations", "valuation_resolution": "bar_close",
           "time_weighted_return_pct": None}
    if not len(values):
        return out
    if initial_equity > 0:
        out["time_weighted_return_pct"] = float(values[-1] / initial_equity - 1)
    values = np.concatenate([[float(initial_equity)], values])
    peak = np.maximum.accumulate(values)
    loss = peak - values
    out["max_floating_drawdown_money"] = float(loss.max())
    if np.any(peak <= 0):
        out["floating_drawdown_status"] = "nonpositive_peak"
    else:
        out.update(max_floating_drawdown_pct=float((loss / peak).max()), floating_drawdown_status="defined")
    return out
