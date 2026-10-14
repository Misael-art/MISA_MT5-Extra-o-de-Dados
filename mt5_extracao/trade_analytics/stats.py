"""
Estatística do Laboratório (T16.4): as métricas do backtester (mesmas fórmulas de `compute_metrics`) e o intervalo de
confiança por bootstrap em blocos de dias.

Toda métrica vem com `n` e com a marca `small` (n < mínimo): o Laboratório não deixa concluir nada de célula pequena.
"""
from typing import Optional, Tuple

import numpy as np
import pandas as pd

PF_CAP = 99.0                 # o mesmo teto do card (robots/backtest.py)
MIN_TRADES = 30               # [TRADE_ANALYTICS] min_trades_cell


def metrics(pnl, initial_equity: float = 100_000.0, r=None) -> dict:
    """n, acerto, fator de lucro, payoff, expectativa (por operação), resultado e queda máxima (% do pico)."""
    pnl = np.asarray(pd.Series(pnl, dtype=float).dropna(), dtype=float)
    n = len(pnl)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    gross_profit, gross_loss = float(wins.sum()), float(-losses.sum())
    pf = gross_profit / gross_loss if gross_loss > 0 else (PF_CAP if gross_profit > 0 else 0.0)
    equity = initial_equity + np.concatenate([[0.0], np.cumsum(pnl)])
    peak = np.maximum.accumulate(equity)
    dd = np.where(peak > 0, (peak - equity) / peak, 0.0)
    out = {"n": n, "win_rate": float(len(wins) / n) if n else 0.0, "profit_factor": float(min(pf, PF_CAP)),
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
