"""
Estatística do Laboratório (T16.4): as métricas do backtester (mesmas fórmulas de `compute_metrics`) e o intervalo de
confiança por bootstrap em blocos de dias.

Toda métrica vem com `n` e com a marca `small` (n < mínimo): o Laboratório não deixa concluir nada de célula pequena.
"""
from typing import Optional, Tuple
from numbers import Integral
import math

import numpy as np
import pandas as pd

from mt5_extracao import profit_factor

MIN_TRADES = 30               # [TRADE_ANALYTICS] min_trades_cell


def metrics(pnl, initial_equity: float = 100_000.0, r=None) -> dict:
    """n, acerto, fator de lucro, payoff, expectativa (por operação), resultado e queda máxima (% do pico)."""
    pnl = list(pnl)
    pf = profit_factor.fields(pnl)
    if pf["profit_factor_status"] == "invalid_pnl":
        raise ValueError("PnL ausente ou inválido; confira as operações antes de calcular métricas.")
    pnl = np.asarray(pnl, dtype=float)
    n = len(pnl)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    equity = initial_equity + np.concatenate([[0.0], np.cumsum(pnl)])
    peak = np.maximum.accumulate(equity)
    dd = np.where(peak > 0, (peak - equity) / peak, 0.0)
    out = {"n": n, "win_rate": float(len(wins) / n) if n else 0.0, **pf,
           "payoff": float(wins.mean() / -losses.mean()) if len(wins) and len(losses) else 0.0,
           "expectancy": float(pnl.mean()) if n else 0.0, "net": float(pnl.sum()),
           "max_drawdown_pct": float(dd.max()) if n else 0.0}
    if r is not None:
        r = np.asarray(pd.Series(r, dtype=float).dropna(), dtype=float)
        out["expectancy_r"] = float(r.mean()) if len(r) else 0.0
    return out


def bootstrap_ci(values: pd.Series, days: pd.Series, n_boot: int = 1000, alpha: float = 0.10,
                 seed: int = 7) -> Tuple[Optional[float], Optional[float]]:
    """
    Intervalo (1 − alpha) da média por operação, sorteando DIAS inteiros com reposição (bootstrap em blocos): operações
    do mesmo dia não são independentes. (None, None) com menos de 2 dias.
    """
    frame = pd.DataFrame({"v": pd.Series(values, dtype=float).values, "d": pd.to_datetime(days).dt.normalize().values})
    frame = frame.dropna()
    sums = frame.groupby("d")["v"].agg(["sum", "count"])
    if len(sums) < 2:
        return None, None
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(sums), size=(n_boot, len(sums)))
    total, count = sums["sum"].values[pick].sum(axis=1), sums["count"].values[pick].sum(axis=1)
    means = total / np.maximum(count, 1)
    return float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2))


def _duration_blocks(rows):
    """Dias UTC de entrada, fundindo janelas que se sobrepõem entre dias."""
    cohorts = {}
    try:
        for i, row in enumerate(rows):
            start = pd.Timestamp(row["unit"]["start_utc"])
            start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
            if pd.isna(start):
                return None
            end = start + pd.Timedelta(minutes=row["upper"])
            day = start.normalize()
            group = cohorts.setdefault(day, {"start": start, "end": end, "members": []})
            group["start"], group["end"] = min(group["start"], start), max(group["end"], end)
            group["members"].append(i)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    blocks = []
    for day, group in sorted(cohorts.items()):
        if blocks and group["start"] <= blocks[-1]["end"]:
            blocks[-1]["end"] = max(blocks[-1]["end"], group["end"])
            blocks[-1]["members"].extend(group["members"])
        else:
            blocks.append({**group, "day": day})
    labels = [None] * len(rows)
    for block in blocks:
        for i in block["members"]:
            labels[i] = block["day"]
    return pd.Series(labels)


def duration_curve(units, ages, *, minimum=MIN_TRADES, n_boot=1000, alpha=0.10, seed=7, check=None):
    """Limites empíricos de T>idade, preservando saídas intervalares e censuras.

    Não estima probabilidade de vitória. `continuation_lower/upper` são limites
    de identificação da amostra, distintos da banda amostral exploratória.
    `ages` é a grade explícita em minutos; nenhum corte vira política operacional.
    """
    try:
        alpha = float(alpha)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("Alpha inválido no protocolo de duração.") from error
    if (isinstance(minimum, bool) or not isinstance(minimum, Integral) or minimum < 1 or
            isinstance(n_boot, bool) or not isinstance(n_boot, Integral) or n_boot < 1 or
            isinstance(seed, bool) or not isinstance(seed, Integral) or seed < 0 or
            not math.isfinite(alpha) or not 0 < alpha < 1):
        raise ValueError("Protocolo inválido de duração; confira amostra mínima, bootstrap, alpha e seed.")
    minimum, n_boot, seed = int(minimum), int(n_boot), int(seed)
    grid = []
    for value in ages:
        if isinstance(value, (bool, np.bool_)):
            raise ValueError("Idades devem ser minutos finitos e não negativos.")
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError("Idades devem ser minutos finitos e não negativos.") from error
        if not math.isfinite(value) or value < 0:
            raise ValueError("Idades devem ser minutos finitos e não negativos.")
        grid.append(value)
    grid = sorted(set(grid))
    groups, seen, excluded = {}, set(), []
    for i, unit in enumerate(units):
        if check is not None and i % 256 == 0:
            check()
        ident = unit.get("identity", {})
        identity = (ident.get("run_id"), ident.get("source"), ident.get("seq"), unit.get("observation_unit", "trade"))
        if (not isinstance(identity[0], str) or not identity[0] or not isinstance(identity[1], str) or not identity[1] or
                isinstance(identity[2], bool) or not isinstance(identity[2], Integral) or identity[2] < 0):
            raise ValueError("Identidade inválida na população de duração; confira o ledger.")
        if identity in seen:
            raise ValueError("Operação duplicada na população de duração; não conte reexecuções como outra amostra.")
        seen.add(identity)
        lower = unit.get("age_lower_min", unit.get("age_min"))
        upper = unit.get("age_upper_min", unit.get("age_min"))
        censored = unit.get("right_censored")
        if lower is None or upper is None or censored is None:
            excluded.append({"identity": ident, "reason": "duration_unavailable"})
            continue
        if (isinstance(lower, (bool, np.bool_)) or isinstance(upper, (bool, np.bool_)) or
                not isinstance(censored, (bool, np.bool_))):
            raise ValueError("Duração ou censura inválida; confira o produtor.")
        lower, upper = float(lower), float(upper)
        if not math.isfinite(lower) or not math.isfinite(upper) or lower < 0 or upper < lower or (censored and lower != upper):
            raise ValueError("Intervalo de duração incoerente; confira o produtor.")
        context = {name: ident.get(name, unit.get(name)) for name in (
            "run_id", "source", "symbol", "timeframe", "robot_key", "direction", "params_hash")}
        context.update(observation_unit=identity[-1],
                       timing_basis=unit.get("sequence_basis", unit.get("timestamp_basis", "unknown")))
        context["quantity"] = ("ledger_timestamp_age" if context["timing_basis"] == "ledger_timestamp" else
                               "cycle_model_duration" if context["timing_basis"] == "modeled_execution_order" else
                               "declared_duration")
        key = tuple(context.values())
        groups.setdefault(key, {"context": context, "rows": []})["rows"].append(
            {"lower": lower, "upper": upper, "censored": bool(censored), "unit": unit})
    result = {"schema_version": 1, "method": "empirical_duration_identification_bounds", "unit": "minute",
              "ages_min": grid, "n_input": len(seen), "n_excluded": len(excluded), "excluded": excluded,
              "n_eligible": len(seen) - len(excluded), "quantile_method": "inverse_empirical_cdf_order_bounds",
              "alpha": alpha, "n_boot": int(n_boot), "seed": int(seed), "groups": [],
              "confidence_method": "percentile_outer_band_utc_day_and_overlap_blocks",
              "inference_status": "exploratory_assumes_independence_between_blocks",
              "probability_of_winning": None}
    for _, group in sorted(groups.items(), key=lambda pair: str(pair[0])):
        rows = group["rows"]
        lower = np.array([r["lower"] for r in rows])
        upper = np.array([r["upper"] for r in rows])
        censored = np.array([r["censored"] for r in rows])
        n = len(rows)
        blocks = _duration_blocks(rows)
        n_blocks = int(blocks.nunique()) if blocks is not None else 0
        small = n < minimum
        reason = "small_sample" if small else "missing_followup_clock" if blocks is None else "insufficient_blocks" if n_blocks < 2 else None
        curve = []
        for age in grid:
            if check is not None:
                check()
            known_alive = np.where(censored, lower >= age, lower > age)
            possible_alive = censored | (upper > age)
            low_ci, high_ci = None, None
            if reason is None:
                low_ci = bootstrap_ci(pd.Series(known_alive, dtype=float), blocks, n_boot, alpha, seed)[0]
                high_ci = bootstrap_ci(pd.Series(possible_alive, dtype=float), blocks, n_boot, alpha, seed)[1]
            curve.append({"age_min": age, "n": n,
                          "n_risk_lower": int((lower >= age).sum()),
                          "n_risk_upper": int(np.where(censored, lower >= age, upper >= age).sum()),
                          "n_unknown_after_censor": int((censored & (lower < age)).sum()),
                          "n_exact_events": int((~censored & (lower == upper) & (upper == age)).sum()),
                          "n_exit_windows_covering_age": int((~censored & (lower < upper) & (lower <= age) & (upper >= age)).sum()),
                          "continuation_lower": float(known_alive.mean()),
                          "continuation_upper": float(possible_alive.mean()),
                          "outer_ci_low": low_ci, "outer_ci_high": high_ci})
        quantiles = []
        for q in (0.25, 0.5, 0.75):
            rank = max(0, math.ceil(q * n) - 1)
            upper_q = float(np.sort(np.where(censored, np.inf, upper))[rank])
            quantiles.append({"q": q, "identified_lower_min": float(np.sort(lower)[rank]),
                              "identified_upper_min": upper_q if math.isfinite(upper_q) else None,
                              "upper_unbounded": not math.isfinite(upper_q)})
        result["groups"].append({"context": group["context"], "n": n, "small": small,
                                  "n_blocks": n_blocks, "ci_unavailable_reason": reason,
                                  "n_closed_exact": int((~censored & (lower == upper)).sum()),
                                  "n_closed_interval": int((~censored & (lower < upper)).sum()),
                                  "n_right_censored": int(censored.sum()), "quantile_bounds": quantiles,
                                  "curve": curve})
    return result
