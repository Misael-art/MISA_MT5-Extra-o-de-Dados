"""
Agregações do Laboratório (T16.4): as perguntas das duas lentes sobre o livro enriquecido.

    lab = frame(db, config, robot_key="python:donchian")    # lente por robô (todos os ativos)
    lab = frame(db, config, symbol="WIN$N")                  # lente por ativo (todos os robôs)
    by_dimension(lab, ["day_of_week"])                       # n, acerto, expectativa, intervalo, ...
    crosstab(lab, "time_bucket", "day_of_week")              # heatmap (valor e n por célula)
    robot_view(lab); asset_view(lab); equity_curves(lab, "symbol"); daily_correlation(lab)
"""
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from mt5_extracao.trade_analytics import enrich, ledger, stats

DAY_NAMES = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")


def _initial_equity(config) -> float:
    try:
        return float(config.get("STRATEGY", "initial_equity", fallback="100000")) if config is not None else 100_000.0
    except ValueError:
        return 100_000.0


def min_trades(config) -> int:
    try:
        return int(config.get("TRADE_ANALYTICS", "min_trades_cell", fallback=str(stats.MIN_TRADES)))
    except (AttributeError, ValueError):
        return stats.MIN_TRADES


def frame(db, config, features: Optional[pd.DataFrame] = None, **filters) -> pd.DataFrame:
    """Livro filtrado (ledger.load) com as colunas do enriquecimento. `features` pronto (cache da T16.5) evita recalcular."""
    trades = ledger.load(db, **filters)
    if trades.empty:
        return trades
    feats = enrich.enrich(db, config, trades) if features is None else features
    merged = trades.merge(feats, on=["run_id", "seq"], how="left", suffixes=("", "_f"))
    merged["day"] = pd.to_datetime(merged["entry_time"].fillna(merged["exit_time"])).dt.normalize()
    return merged


def summarize(group: pd.DataFrame, initial_equity: float = 100_000.0, minimum: int = stats.MIN_TRADES,
              ci: bool = True) -> dict:
    ordered = group.sort_values("exit_time") if "exit_time" in group else group
    m = stats.metrics(ordered["pnl"], initial_equity, ordered.get("r_multiple"))
    lo, hi = stats.bootstrap_ci(ordered["pnl"], ordered["day"]) if ci and "day" in ordered else (None, None)
    m.update(ci_low=lo, ci_high=hi, small=m["n"] < minimum)
    return m


def by_dimension(lab: pd.DataFrame, dims: Iterable[str], config=None, ci: bool = True) -> pd.DataFrame:
    """Uma linha por combinação das dimensões (ordem natural), com as métricas e a marca de amostra pequena."""
    dims = list(dims)
    if lab.empty:
        return pd.DataFrame(columns=dims + ["n"])
    rows = []
    for key, group in lab.dropna(subset=dims).groupby(dims, sort=True):
        key = key if isinstance(key, tuple) else (key,)
        rows.append(dict(zip(dims, key), **summarize(group, _initial_equity(config), min_trades(config), ci)))
    return pd.DataFrame(rows)


def crosstab(lab: pd.DataFrame, row: str, col: str, metric: str = "expectancy", config=None) -> Dict[str, pd.DataFrame]:
    """Heatmap: {"value": métrica por célula, "n": operações por célula, "small": célula pequena}."""
    table = by_dimension(lab, [row, col], config, ci=False)
    if table.empty:
        empty = pd.DataFrame()
        return {"value": empty, "n": empty, "small": empty}
    return {"value": table.pivot(index=row, columns=col, values=metric),
            "n": table.pivot(index=row, columns=col, values="n").fillna(0).astype(int),
            "small": table.pivot(index=row, columns=col, values="small").fillna(True).astype(bool)}


def robot_view(lab: pd.DataFrame, config=None) -> pd.DataFrame:
    """Lente por robô: o robô em cada ativo e timeframe (melhor expectativa primeiro)."""
    table = by_dimension(lab, ["symbol", "timeframe"], config)
    return table.sort_values("expectancy", ascending=False).reset_index(drop=True) if not table.empty else table


def asset_view(lab: pd.DataFrame, config=None) -> pd.DataFrame:
    """Lente por ativo: cada robô (e configuração) no ativo."""
    table = by_dimension(lab, ["robot_key", "timeframe"], config)
    return table.sort_values("expectancy", ascending=False).reset_index(drop=True) if not table.empty else table


def equity_curves(lab: pd.DataFrame, by: str) -> Dict[str, pd.Series]:
    """Resultado acumulado por saída, uma curva por valor de `by` (ativo, robô ou modo de execução)."""
    out = {}
    for key, group in lab.dropna(subset=["exit_time"]).groupby(by):
        ordered = group.sort_values("exit_time")
        out[str(key)] = pd.Series(ordered["pnl"].cumsum().values, index=pd.to_datetime(ordered["exit_time"]))
    return out


def daily_correlation(lab: pd.DataFrame, by: str = "robot_key", min_days: int = 20) -> pd.DataFrame:
    """Correlação dos resultados diários entre robôs (ou ativos): perdas que se somam (positiva) ou compensam."""
    if lab.empty:
        return pd.DataFrame()
    daily = lab.pivot_table(index="day", columns=by, values="pnl", aggfunc="sum")
    return daily.corr(min_periods=min_days)


def day_label(value) -> str:
    try:
        return DAY_NAMES[int(value)]
    except (TypeError, ValueError, IndexError):
        return str(value)
