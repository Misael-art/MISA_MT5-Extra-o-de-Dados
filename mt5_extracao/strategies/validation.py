"""
Validação de estratégia × ativo × timeframe (Filtro C).

    veredito = validate(df, "donchian", spec, BacktestConfig(), Criteria())
    veredito["approved"], veredito["checks"]

Etapas:
1. Walk-forward: os dados são divididos em n_windows + is_parts partes iguais. Em cada janela,
   os parâmetros são escolhidos (grade da estratégia, objetivo = soma de R) nas is_parts partes
   anteriores e testados na parte seguinte, que a otimização nunca viu (fora da amostra).
2. Robustez: cada parâmetro numérico × 0,8 e × 1,2; robusto se todas as variantes têm PF > 1.
3. Monte Carlo: reamostra (com reposição) os retornos das operações fora da amostra e mede o
   drawdown máximo no percentil 95.
"""
import itertools
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from . import get_strategy
from .backtester import BacktestConfig, SymbolSpec, Trade, compute_metrics, run_backtest

log = logging.getLogger(__name__)

PF_CAP = 999.0   # profit factor "infinito" (sem perdas) é gravado/exibido como 999


@dataclass
class Criteria:
    min_profit_factor: float = 1.3
    min_trades: int = 100
    min_positive_windows: float = 0.5   # fração de janelas fora da amostra com lucro (estritamente maior)
    max_drawdown: float = 0.20          # limite do usuário para o DD p95 do Monte Carlo
    robustness_min_pf: float = 1.0
    n_windows: int = 5
    is_parts: int = 2
    mc_runs: int = 1000
    mc_percentile: float = 95.0
    seed: int = 42


def _cap(pf: float) -> float:
    return PF_CAP if math.isinf(pf) else float(pf)


def _grid(key: str) -> List[dict]:
    grid = get_strategy(key).param_grid
    if not grid:
        return [{}]
    names = list(grid)
    return [dict(zip(names, values)) for values in itertools.product(*(grid[n] for n in names))]


class _SignalCache:
    """Sinais calculados uma vez por combinação de parâmetros (eles não olham o futuro)."""

    def __init__(self, df, key, base_params):
        self.df, self.key, self.base = df, key, dict(base_params or {})
        self._cache = {}

    def get(self, params):
        full = {**self.base, **params}
        k = tuple(sorted(full.items()))
        if k not in self._cache:
            strategy = get_strategy(self.key, **full)
            self._cache[k] = (strategy, strategy.gerar_sinais(self.df))
        return self._cache[k]


def _run_slice(cache, params, spec, config, start, end, warmup):
    strategy, signals = cache.get(params)
    first = max(0, start - warmup)
    return run_backtest(cache.df.iloc[first:end], strategy, spec, config, start_index=start - first,
                        signals=signals.iloc[first:end])


def walk_forward(df: pd.DataFrame, key: str, spec: Optional[SymbolSpec] = None,
                 config: Optional[BacktestConfig] = None, criteria: Optional[Criteria] = None,
                 base_params: Optional[dict] = None) -> dict:
    criteria = criteria or Criteria()
    config = config or BacktestConfig()
    df = df.reset_index(drop=True)
    n = len(df)
    parts = criteria.n_windows + criteria.is_parts
    size = n // parts
    warmup = get_strategy(key).min_bars
    if size < 50:
        raise ValueError(f"Poucos dados para o walk-forward: {n} barras (mínimo {parts * 50}). "
                         "Extraia um período maior ou use um timeframe menor.")
    cache = _SignalCache(df, key, base_params)
    windows, oos_trades = [], []
    for w in range(criteria.n_windows):
        is_start, is_end = w * size, (w + criteria.is_parts) * size
        oos_end = n if w == criteria.n_windows - 1 else is_end + size
        best = None
        for params in _grid(key):
            r = _run_slice(cache, params, spec, config, is_start, is_end, warmup)
            score = r.metrics["total_r"]
            if best is None or score > best[0]:
                best = (score, params, r.metrics)
        oos = _run_slice(cache, best[1], spec, config, is_end, oos_end, warmup)
        oos_trades.extend(oos.trades)
        windows.append({
            "window": w + 1,
            "oos_start": str(df["time"].iloc[is_end]), "oos_end": str(df["time"].iloc[oos_end - 1]),
            "params": best[1],
            "is_profit_factor": _cap(best[2]["profit_factor"]), "is_trades": best[2]["trades"],
            "oos_profit_factor": _cap(oos.metrics["profit_factor"]), "oos_trades": oos.metrics["trades"],
            "oos_net_profit": oos.metrics["net_profit"],
        })
    positive = sum(1 for w in windows if w["oos_net_profit"] > 0)
    return {"windows": windows, "oos_trades": oos_trades,
            "oos_metrics": compute_metrics(oos_trades, config.initial_equity),
            "positive_windows": positive / len(windows)}


def robustness(df: pd.DataFrame, key: str, spec: Optional[SymbolSpec] = None,
               config: Optional[BacktestConfig] = None, params: Optional[dict] = None,
               min_pf: float = 1.0) -> dict:
    base = {**get_strategy(key).default_params, **(params or {})}
    variants = []
    for name, value in base.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value == 0:
            continue
        for factor in (0.8, 1.2):
            if isinstance(value, int):
                new = int(round(value * factor))
                if new == value:
                    new = value + (1 if factor > 1 else -1)
                new = max(1, new)
            else:
                new = value * factor
            r = run_backtest(df, get_strategy(key, **{**base, name: new}), spec, config)
            variants.append({"param": name, "value": new, "factor": factor,
                             "profit_factor": _cap(r.metrics["profit_factor"]), "trades": r.metrics["trades"]})
    ok = [v for v in variants if v["profit_factor"] > min_pf]
    return {"variants": variants, "share_ok": len(ok) / len(variants) if variants else 0.0,
            "robust": bool(variants) and len(ok) == len(variants)}


def monte_carlo(trades: List[Trade], runs: int = 1000, percentile: float = 95.0, seed: int = 42) -> dict:
    returns = np.array([t.return_pct for t in trades], dtype=float)
    if len(returns) == 0:
        return {"dd_percentile": 0.0, "dd_median": 0.0, "ruin_probability": 0.0, "runs": 0}
    rng = np.random.default_rng(seed)
    sample = rng.choice(returns, size=(runs, len(returns)), replace=True)
    equity = np.cumprod(1.0 + sample, axis=1)
    equity = np.concatenate([np.ones((runs, 1)), equity], axis=1)
    peak = np.maximum.accumulate(equity, axis=1)
    dd = ((peak - equity) / peak).max(axis=1)
    return {"dd_percentile": float(np.percentile(dd, percentile)), "dd_median": float(np.median(dd)),
            "ruin_probability": float((dd >= 0.5).mean()), "runs": int(runs)}


def _pct(x: float) -> str:
    return f"{x * 100:.1f}%".replace(".", ",")


def _num(x: float, digits: int = 2) -> str:
    return f"{x:.{digits}f}".replace(".", ",")


def validate(df: pd.DataFrame, key: str, spec: Optional[SymbolSpec] = None,
             config: Optional[BacktestConfig] = None, criteria: Optional[Criteria] = None,
             params: Optional[dict] = None) -> dict:
    """Veredito do Filtro C com cada critério explicado em português."""
    criteria = criteria or Criteria()
    config = config or BacktestConfig()
    df = df.reset_index(drop=True)
    wf = walk_forward(df, key, spec, config, criteria, params)
    full = run_backtest(df, get_strategy(key, **(params or {})), spec, config)
    rob = robustness(df, key, spec, config, params, criteria.robustness_min_pf)
    mc = monte_carlo(wf["oos_trades"], criteria.mc_runs, criteria.mc_percentile, criteria.seed)
    oos = wf["oos_metrics"]
    pf_oos = _cap(oos["profit_factor"])

    checks = [
        {"name": "Profit factor fora da amostra", "value": pf_oos, "threshold": criteria.min_profit_factor,
         "passed": pf_oos > criteria.min_profit_factor,
         "text": f"PF fora da amostra {_num(pf_oos)} (mínimo > {_num(criteria.min_profit_factor)})"},
        {"name": "Operações fora da amostra", "value": oos["trades"], "threshold": criteria.min_trades,
         "passed": oos["trades"] >= criteria.min_trades,
         "text": f"{oos['trades']} operações fora da amostra (mínimo {criteria.min_trades})"},
        {"name": "Janelas walk-forward positivas", "value": wf["positive_windows"],
         "threshold": criteria.min_positive_windows, "passed": wf["positive_windows"] > criteria.min_positive_windows,
         "text": f"{_pct(wf['positive_windows'])} das janelas com lucro (mínimo > {_pct(criteria.min_positive_windows)})"},
        {"name": "Robustez (parâmetros ±20%)", "value": rob["share_ok"], "threshold": 1.0, "passed": rob["robust"],
         "text": f"{_pct(rob['share_ok'])} das variações de parâmetro com PF > {_num(criteria.robustness_min_pf)}"},
        {"name": f"Drawdown Monte Carlo (p{criteria.mc_percentile:g})", "value": mc["dd_percentile"],
         "threshold": criteria.max_drawdown,
         "passed": mc["runs"] > 0 and mc["dd_percentile"] <= criteria.max_drawdown,
         "text": f"DD p{criteria.mc_percentile:g} de {_pct(mc['dd_percentile'])} (limite {_pct(criteria.max_drawdown)})"},
    ]
    approved = all(c["passed"] for c in checks)
    failed = [c["text"] for c in checks if not c["passed"]]
    return {
        "strategy": key, "params": {**get_strategy(key).default_params, **(params or {})},
        "approved": approved,
        "status": "✅ aprovada" if approved else "❌ reprovada: " + "; ".join(failed),
        "checks": checks,
        "oos_metrics": {k: v for k, v in oos.items() if k != "exit_reasons"},
        "full_metrics": {k: v for k, v in full.metrics.items() if k != "exit_reasons"},
        "windows": wf["windows"], "robustness": rob, "monte_carlo": mc,
        "skipped_min_lot": full.skipped_min_lot,
        "data_start": str(df["time"].iloc[0]), "data_end": str(df["time"].iloc[-1]), "bars": int(len(df)),
        "criteria": asdict(criteria),
    }


# --- persistência (_strategy_validation, tabela interna e aditiva) ---

_CREATE = (
    "CREATE TABLE IF NOT EXISTS _strategy_validation ("
    " symbol TEXT NOT NULL, timeframe TEXT NOT NULL, strategy TEXT NOT NULL,"
    " approved INTEGER NOT NULL, status TEXT, profit_factor_oos REAL, trades_oos INTEGER,"
    " positive_windows REAL, robust INTEGER, mc_dd REAL, params_json TEXT, result_json TEXT,"
    " data_start TEXT, data_end TEXT, created_at TEXT NOT NULL,"
    " PRIMARY KEY (symbol, timeframe, strategy))")


def _json_default(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    return str(obj)


def save_validation(db, symbol: str, timeframe: str, verdict: dict) -> None:
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(text(
            "INSERT INTO _strategy_validation (symbol, timeframe, strategy, approved, status, profit_factor_oos, "
            "trades_oos, positive_windows, robust, mc_dd, params_json, result_json, data_start, data_end, created_at) "
            "VALUES (:s, :tf, :k, :a, :st, :pf, :n, :pw, :rb, :dd, :p, :r, :ds, :de, :c) "
            "ON CONFLICT(symbol, timeframe, strategy) DO UPDATE SET approved = excluded.approved, "
            "status = excluded.status, profit_factor_oos = excluded.profit_factor_oos, "
            "trades_oos = excluded.trades_oos, positive_windows = excluded.positive_windows, "
            "robust = excluded.robust, mc_dd = excluded.mc_dd, params_json = excluded.params_json, "
            "result_json = excluded.result_json, data_start = excluded.data_start, data_end = excluded.data_end, "
            "created_at = excluded.created_at"),
            {"s": symbol, "tf": timeframe, "k": verdict["strategy"], "a": int(verdict["approved"]),
             "st": verdict["status"], "pf": _cap(verdict["oos_metrics"]["profit_factor"]),
             "n": int(verdict["oos_metrics"]["trades"]),
             "pw": float(next(c["value"] for c in verdict["checks"] if c["name"].startswith("Janelas"))),
             "rb": int(verdict["robustness"]["robust"]), "dd": float(verdict["monte_carlo"]["dd_percentile"]),
             "p": json.dumps(verdict["params"], default=_json_default),
             "r": json.dumps(verdict, default=_json_default, ensure_ascii=False),
             "ds": verdict["data_start"], "de": verdict["data_end"],
             "c": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})


def load_validations(db, symbols: Optional[List[str]] = None, timeframe: Optional[str] = None) -> pd.DataFrame:
    """Vereditos gravados (vazio se a tabela ainda não existir)."""
    from sqlalchemy import inspect, text
    cols = ["symbol", "timeframe", "strategy", "approved", "status", "profit_factor_oos", "trades_oos",
            "positive_windows", "robust", "mc_dd", "params_json", "data_start", "data_end", "created_at"]
    if not inspect(db.engine).has_table("_strategy_validation"):
        return pd.DataFrame(columns=cols)
    with db.engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT {', '.join(cols)} FROM _strategy_validation"), conn)
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    if timeframe:
        df = df[df["timeframe"] == timeframe]
    return df.reset_index(drop=True)
