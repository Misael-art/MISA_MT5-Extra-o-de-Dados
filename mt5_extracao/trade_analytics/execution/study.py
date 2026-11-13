"""
Estudo pareado da recuperação condicional (T16.15): as hipóteses H0–H6 sobre as MESMAS entradas, barras, custos e
sinais de um run do livro, com o resultado líquido e o risco apresentados juntos.

    report = run(db, config, run_id)                       # sinais padrão, zona 0,6 R
    report["variants"]["H2"]["verdict"]                    # "aprofundar", "reprovada" ou "amostra insuficiente"

Uma operação original é UM caso de comparação, por mais ordens que a variante gere. A frequência de recuperação nunca
aprova sozinha: o parecer exige diferença líquida com intervalo acima de zero, reserva final e estresse de custo
positivos e cauda e queda não piores que a base. Tudo aqui é estudo: nenhuma variante vira preset nem toca o executor.
Definições e limites: docs/propostas/ANALYTICS_OPERACOES.md, seções 11.10 e 11.11.
"""
from dataclasses import replace
from typing import Optional

import numpy as np
import pandas as pd

from mt5_extracao.trade_analytics import aggregate, rules, stats
from mt5_extracao.trade_analytics.execution import modes as registry, reversal, runner

# hipótese -> (modo, parâmetros fixos da hipótese)
VARIANTS = {
    "H0": ("ponto_fixo", {}),
    "H1": ("saida_antecipada", {}),
    "H2": ("recuperacao_unica", {}),
    "H3": ("recuperacao_proporcional", {}),       # fator 2 com uma adição: exposição limitada a 3× por caso
    "H4": ("recuperacao_gerida", {}),
    "H5": ("gradiente_dinamico", {"niveis": 1, "k_atr": 1.0, "fator_lote": 1.0}),
    "H6": ("reversao_independente", {}),
}
STRESS_R = 0.05         # escorregamento extra por execução a mercado, em fração do risco inicial mediano
TAIL_TOLERANCE = 1e-9


def _frame(result) -> pd.DataFrame:
    rows = []
    for trade, seq, case in zip(result.trades, result.source_seqs, result.cases):
        rows.append({"seq": seq, "pnl": trade.pnl, "entry_time": trade.entry_time, "exit_time": trade.exit_time,
                     "r_multiple": trade.r_multiple, "volume": trade.volume, **{k: v for k, v in case.items() if k != "seq"}})
    return pd.DataFrame(rows)


def _quantile(values, q):
    return float(np.quantile(values, q)) if len(values) else None


def _excursions(frame: pd.DataFrame) -> dict:
    """Frente II: quanto as vencedoras suportaram, quanto as perdedoras chegaram a ganhar e quando."""
    out = {"basis": "ohlc_bars_intrabar_order_unknown", "margin": "indisponível: a especificação não traz margem"}
    for name, part in (("winners", frame[frame.pnl > 0]), ("losers", frame[frame.pnl <= 0])):
        capture = part["capture"].dropna()
        out[name] = {
            "n": int(len(part)),
            "mae_r_p50": _quantile(part.mae_r, 0.5), "mae_r_p90": _quantile(part.mae_r, 0.9),
            "mfe_r_p50": _quantile(part.mfe_r, 0.5), "mfe_r_p90": _quantile(part.mfe_r, 0.9),
            "bars_to_mae_p50": _quantile(part.bars_to_mae.dropna(), 0.5),
            "bars_to_mfe_p50": _quantile(part.bars_to_mfe.dropna(), 0.5),
            "capture_p50": _quantile(capture, 0.5),
            "floating_min_worst": float(part.floating_min.min()) if len(part) else None,
        }
    return out


def _simulate(db, config, run_id, data, base, zone_r, cfg=None):
    """{hipótese: SimResult | ValueError} com os mesmos dados; `cfg` troca só os custos (estresse)."""
    scoped = dict(data, cfg=cfg) if cfg is not None else data
    out = {}
    for label, (key, fixed) in VARIANTS.items():
        mode = registry.MODES[key]
        params = dict(mode.params, **fixed)
        if mode.kind == registry.RECOVERY:
            params["zona_r"] = zone_r
        try:
            out[label] = runner.resimulate(db, config, run_id, mode.policy(base, params), key, data=scoped)
        except ValueError as error:
            out[label] = error
    return out


def _paired(label, key, result, base: pd.DataFrame, initial, holdout, minimum, stressed_diff) -> dict:
    mode = registry.MODES[key]
    if isinstance(result, ValueError):
        return {"mode": key, "name": mode.name, "verdict": "indisponível", "reasons": [str(result)]}
    frame = _frame(result)
    if list(frame.seq) != list(base.seq):
        return {"mode": key, "name": mode.name, "verdict": "indisponível",
                "reasons": ["Casos diferentes da base; a comparação pareada não é válida."]}
    diff = frame.pnl.to_numpy(dtype=float) - base.pnl.to_numpy(dtype=float)
    touched = frame.intervention.notna().to_numpy() | (frame.added_volume.to_numpy(dtype=float) > 0) \
        | (np.abs(diff) > 1e-9)
    low, high = stats.bootstrap_ci(pd.Series(diff), base.entry_time)
    losses = frame.pnl[frame.pnl < 0]
    metrics = {**stats.metrics(frame.sort_values("exit_time").pnl, initial, frame.r_multiple), **result.metrics}
    out = {
        "mode": key, "name": mode.name, "study_only": True, "metrics": metrics,
        "n_cases": int(len(frame)), "n_intervened": int(touched.sum()),
        "recovered": int((touched & (diff > 0)).sum()), "aggravated": int((touched & (diff < 0)).sum()),
        "diff_net": float(diff.sum()), "diff_mean": float(diff.mean()) if len(diff) else 0.0,
        "diff_ci90": [low, high], "diff_worst_case": float(diff.min()) if len(diff) else 0.0,
        "loss_mean": float(losses.mean()) if len(losses) else 0.0,
        "pnl_p01": _quantile(frame.pnl, 0.01), "pnl_p05": _quantile(frame.pnl, 0.05),
        "pnl_worst": float(frame.pnl.min()) if len(frame) else None,
        "floating_min_worst": float(frame.floating_min.min()) if len(frame) else None,
        "max_volume": float(result.max_volume), "adds": int(result.adds), "capped": int(result.capped),
        "diff_search_mean": float(diff[~holdout].mean()) if (~holdout).any() else None,
        "diff_holdout_mean": float(diff[holdout].mean()) if holdout.any() else None,
        "n_holdout_intervened": int((touched & holdout).sum()),
        "diff_net_stress": stressed_diff,
    }
    if label == "H0":
        out["verdict"], out["reasons"] = "base", []
        return out
    reasons = []
    if out["n_intervened"] < minimum:
        out["verdict"] = "amostra insuficiente"
        out["reasons"] = [f"{out['n_intervened']} caso(s) com intervenção; o mínimo é {minimum}."]
        return out
    base_metrics = stats.metrics(base.sort_values("exit_time").pnl, initial)
    if low is None or low <= 0:
        reasons.append("O intervalo de 90% da diferença média por caso não fica acima de zero.")
    if out["diff_holdout_mean"] is None or out["diff_holdout_mean"] <= 0:
        reasons.append("Na reserva final a diferença não é positiva.")
    if stressed_diff is None or stressed_diff <= 0:
        reasons.append("Com custo estressado a diferença líquida não é positiva.")
    if out["pnl_p05"] < _quantile(base.pnl, 0.05) - TAIL_TOLERANCE:
        reasons.append("A cauda de perdas (p5) piora em relação à base.")
    if metrics["max_drawdown_pct"] > base_metrics["max_drawdown_pct"] + TAIL_TOLERANCE:
        reasons.append("A queda máxima piora em relação à base.")
    out["verdict"], out["reasons"] = ("aprofundar" if not reasons else "reprovada"), reasons
    return out


def run(db, config, run_id: str, *, signals=None, min_signals: int = 1, zone_r: float = 0.6,
        stress_r: float = STRESS_R, data: Optional[dict] = None) -> dict:
    """Relatório pareado H0–H6 de um run do livro. Não grava nada."""
    from mt5_extracao import timeframes
    from mt5_extracao.analytics import data_eligibility, tick_fidelity
    if not 0 < float(zone_r) < 1:
        raise ValueError("A zona deve ficar entre 0 e 1 R (ex.: 0.6).")
    data = dict(data or runner.inputs(db, config, run_id))
    names = reversal.parse(signals)
    data["signals"] = reversal.compute(data["bars"], names, min_signals)
    base_policy = runner.base_policy(data["robot_key"])
    cfg = data["cfg"]
    results = _simulate(db, config, run_id, data, base_policy, zone_r)
    if isinstance(results["H0"], ValueError) or not results["H0"].trades:
        raise ValueError("A base (ponto fixo) não pôde ser re-simulada neste run: "
                         + (str(results["H0"]) if isinstance(results["H0"], ValueError) else "nenhuma operação válida."))
    base = _frame(results["H0"])
    trades = data["trades"]
    risk = ((trades.entry_price - trades.stop_price) * trades.direction).astype(float)
    risk = risk[np.isfinite(risk) & (risk > 0)]
    point = float(getattr(data["spec"], "point", 0) or 0)
    stressed = {}
    if stress_r and len(risk) and point > 0:
        extra = float(stress_r) * float(risk.median()) / point
        harsh = _simulate(db, config, run_id, data, base_policy, zone_r,
                          cfg=replace(cfg, slippage_points=cfg.slippage_points + extra))
        if not isinstance(harsh["H0"], ValueError):
            harsh_base = _frame(harsh["H0"])
            for label, item in harsh.items():
                if not isinstance(item, ValueError):
                    frame = _frame(item)
                    if list(frame.seq) == list(harsh_base.seq):
                        stressed[label] = float(frame.pnl.sum() - harsh_base.pnl.sum())
    holdout = rules.split(base, config).holdout.to_numpy(dtype=bool)
    minimum = aggregate.min_trades(config)
    variants = {label: _paired(label, key, results[label], base, cfg.initial_equity, holdout, minimum,
                               stressed.get(label))
                for label, (key, _fixed) in VARIANTS.items()}
    quality = data.get("data_quality")
    if quality is None:
        tf = timeframes.parse(data["timeframe"])
        quality = data_eligibility.assess(data["bars"], bar_minutes=(tf.minutes if tf else None) or 1440)
    fidelity = tick_fidelity.grade(quality)
    zone = data["signals"]
    return {
        "schema_version": 1, "run_id": run_id, "robot_key": data["robot_key"], "symbol": data["symbol"],
        "timeframe": data["timeframe"], "study_only": True,
        "protocol": {"zone_r": float(zone_r), "signals": list(names), "min_signals": int(min_signals),
                     "decision": "fechamento da barra; execução na abertura seguinte",
                     "stress_slippage_r": float(stress_r) if stressed else None,
                     "holdout_cases": int(holdout.sum()), "min_interventions": int(minimum),
                     "signal_bars": {str(d): int(zone["reversal"][d].sum()) for d in (1, -1)},
                     "data_provenance_status": data.get("data_provenance_status")},
        "fidelity": fidelity, "decision_certified": tick_fidelity.supports(fidelity, "fechamento"),
        "intrabar_certified": tick_fidelity.supports(fidelity, "intrabarra"),
        "excursions": _excursions(base), "variants": variants,
        "skipped": int(results["H0"].skipped), "approximate": int(results["H0"].approximate),
    }
