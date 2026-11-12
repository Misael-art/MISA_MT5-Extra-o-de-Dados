"""
Agregações do Laboratório (T16.4): as perguntas das duas lentes sobre o livro enriquecido.

    lab = frame(db, config, robot_key="python:donchian")    # lente por robô (todos os ativos)
    lab = frame(db, config, symbol="WIN$N")                  # lente por ativo (todos os robôs)
    by_dimension(lab, ["day_of_week"])                       # n, acerto, expectativa, intervalo, ...
    crosstab(lab, "time_bucket", "day_of_week")              # heatmap (valor e n por célula)
    robot_view(lab); asset_view(lab); equity_curves(lab, "symbol"); daily_correlation(lab)
"""
from typing import Dict, Iterable, List, Optional
import json
import re

import numpy as np
import pandas as pd

from mt5_extracao.trade_analytics import ledger, stats

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


def _distinct_runs(db, trades, connection):
    """Agrupa réplicas comprovadas pelo contrato do produtor e por todas as operações."""
    from mt5_extracao.trade_analytics import cache

    known = trades[trades.run_id.notna()]
    records = {row["run_id"]: row for row in cache._run_part(db, known, connection=connection)} if not known.empty else {}
    representatives, repeated, unqualified, references = {}, {}, [], {}
    for run_id, part in known.groupby("run_id", sort=True):
        if not part.source.eq("python").all():
            continue
        # A janela visível pode omitir diferenças no restante da execução.
        part = ledger.load(db, run_id=run_id, connection=connection)
        record = records.get(run_id) or {}
        try:
            metrics = json.loads(record.get("metrics_json") or "{}")
            identity = metrics.get("execution_identity") or {}
            same_scope = all(part[column].nunique(dropna=False) == 1 and
                             part[column].iloc[0] == record.get(column)
                             for column in ("robot_key", "symbol", "timeframe"))
            qualified = (record.get("source") == "backtest" and part.source.eq("python").all() and same_scope and
                         part.completeness.eq(ledger.COMPLETE).all() and
                         type(identity.get("version")) is int and identity["version"] == 1 and
                         identity.get("cache_eligible") is True and identity.get("status") == "frozen_reference" and
                         identity.get("data_manifest_id") == metrics.get("data_manifest_id") and
                         all(isinstance(identity.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", identity[key])
                             for key in ("digest", "code_sha256", "data_manifest_id")))
            if not qualified:
                unqualified.append(run_id)
                continue
            metadata = {"scope": [record.get(key) for key in ("robot_key", "symbol", "timeframe")],
                        "execution": identity, "params": json.loads(record.get("params_json") or "{}"),
                        "valuation": json.loads(record.get("valuation_json") or "null"),
                        "comparison": metrics.get("comparison_context"), "currency": metrics.get("currency")}
            key = (json.dumps(metadata, sort_keys=True, ensure_ascii=False, allow_nan=False),
                   cache._frame_digest(part.drop(columns=["run_id"])))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Metadados de execução inválidos no livro. Confira o run antes de analisar a amostra.") from exc
        references[run_id] = identity["data_manifest_id"]
        if key in representatives:
            repeated.setdefault(representatives[key], []).append(run_id)
        else:
            representatives[key] = run_id
    excluded = {run_id for aliases in repeated.values() for run_id in aliases}
    return trades.loc[~trades.run_id.isin(excluded)], repeated, unqualified, references


def frame(db, config, features: Optional[pd.DataFrame] = None, *, include_repeated_runs: bool = False, **filters) -> pd.DataFrame:
    """Livro filtrado (ledger.load) com as colunas do enriquecimento. `features` pronto (cache da T16.5) evita recalcular."""
    repeated, unqualified, references = {}, [], {}
    if db is None or getattr(db, "engine", None) is None:
        return ledger.load(db, **filters)
    with db.consistent_read() as connection:
        trades = ledger.load(db, connection=connection, **filters)
        if trades.empty:
            return trades
        if not include_repeated_runs:
            trades, repeated, unqualified, references = _distinct_runs(db, trades, connection)
    # A validação dos blocos pode ser grande; encerrar a leitura do livro primeiro.
    from mt5_extracao.analytics import datasets
    for run_id, manifest_id in references.items():
        if getattr(datasets.for_run(db, run_id), "manifest_id", None) != manifest_id:
            raise ValueError("Referência de dados indisponível; recupere o snapshot antes de analisar o run.")
    if features is None:
        from mt5_extracao.trade_analytics import cache       # T16.5: calcula só os runs que mudaram
        features = cache.features(db, config, trades)
    feats = features
    merged = trades.merge(feats, on=["run_id", "seq"], how="left", suffixes=("", "_f"))
    merged["day"] = pd.to_datetime(merged["entry_time"].fillna(merged["exit_time"])).dt.normalize()
    merged.attrs["repeated_runs"] = {"groups": repeated, "unqualified": unqualified,
                                     "include_repeated_runs": bool(include_repeated_runs)}
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


def _pnl_numbers(values: pd.Series) -> np.ndarray:
    numbers = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    boolean = values.map(lambda value: isinstance(value, (bool, np.bool_))).to_numpy(dtype=bool)
    return np.where(np.isfinite(numbers) & ~boolean, numbers, np.nan)


def outcome_counts(lab: pd.DataFrame, dimension: str) -> pd.DataFrame:
    """Contagens por sinal do PnL recebido; zero e ausência nunca são perdas.

    `n` inclui todas as operações, inclusive resultados inválidos e dimensão
    ausente. A dimensão ausente permanece como NA para a apresentação explicá-la.
    Não altera as métricas financeiras nem o livro recebido.
    """
    columns = [dimension, "n", "wins", "losses", "flat", "unknown"]
    if lab.empty:
        return pd.DataFrame(columns=columns)
    rows = []
    for key, group in lab.groupby(dimension, sort=True, dropna=False, observed=True):
        values = _pnl_numbers(group["pnl"])
        valid = np.isfinite(values)
        rows.append({dimension: key, "n": len(group),
                     "wins": int(np.count_nonzero(valid & (values > 0))),
                     "losses": int(np.count_nonzero(valid & (values < 0))),
                     "flat": int(np.count_nonzero(valid & (values == 0))),
                     "unknown": int(np.count_nonzero(~valid))})
    return pd.DataFrame(rows, columns=columns)


def hourly_summary(lab: pd.DataFrame, config=None) -> dict:
    """Grade diária explícita, sem resultado inventado para faixas sem medida.

    Metadados distinguem operações sem faixa válida e sem PnL disponível.
    As métricas dos grupos observados vêm de by_dimension, sem interpolação.
    """
    from mt5_extracao.trade_analytics import enrich

    minutes = enrich.time_bucket_minutes(config)
    labels = [f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(0, 1440, minutes)]
    timed = lab.loc[lab["time_bucket"].isin(labels)] if not lab.empty else lab
    numbers = _pnl_numbers(timed["pnl"]) if not timed.empty else np.array([], dtype=float)
    finite = np.isfinite(numbers)
    measured = timed.loc[finite].copy()
    if not measured.empty:
        measured["pnl"] = numbers[finite]
    observed = by_dimension(measured, ["time_bucket"], config, ci=False)
    if observed.empty:
        table = pd.DataFrame({"time_bucket": labels, "n": 0, "expectancy": np.nan,
                              "win_rate": np.nan, "small": False})
    else:
        table = observed.set_index("time_bucket").reindex(labels)
        table.index.name = "time_bucket"
        table["n"] = table["n"].fillna(0).astype(int)
        missing = table["n"].eq(0)
        table.loc[missing, ["expectancy", "win_rate"]] = np.nan
        table["small"] = table["small"].astype("boolean").fillna(False).astype(bool) & ~missing
        table = table.reset_index()[["time_bucket", "n", "expectancy", "win_rate", "small"]]
    return {"table": table, "bucket_min": minutes, "timed_n": len(timed), "measured_n": len(measured),
            "untimed_n": len(lab) - len(timed), "missing_result_n": len(timed) - len(measured)}


def crosstab(lab: pd.DataFrame, row: str, col: str, metric: str = "expectancy", config=None) -> Dict[str, pd.DataFrame]:
    """Heatmap: {"value": métrica por célula, "n": operações por célula, "small": célula pequena}."""
    table = by_dimension(lab, [row, col], config, ci=False)
    if table.empty:
        empty = pd.DataFrame()
        return {"value": empty, "n": empty, "small": empty}
    return {"value": table.pivot(index=row, columns=col, values=metric),
            "n": table.pivot(index=row, columns=col, values="n").fillna(0).astype(int),
            "small": table.pivot(index=row, columns=col, values="small").astype("boolean").fillna(True).astype(bool)}


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
