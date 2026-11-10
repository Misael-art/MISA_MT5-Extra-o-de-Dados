"""
Classe de fidelidade A–E do conjunto de dados que um estudo consumiu (T16.14) e auditoria de ticks.

    report = data_eligibility.assess(bars, bar_minutes=15, ticks=ticks, evidence=evidence)
    grade(report, audit(ticks, point=spec.point))     # {"grade": "B", "label": ..., "reasons": [...], ...}

Não mede nada por conta própria além da auditoria: a classe deriva das capacidades de `data_eligibility.assess` (T20.6)
e, quando existir, do relatório nativo do Tester (`data_eligibility.tester_report`). Nunca sobe de classe por
inferência: cobertura integral de ticks reais só vale com declaração explícita da mesma execução.
Critérios: docs/propostas/ANALYTICS_OPERACOES.md, seção 11.10.
"""
from typing import Optional

import numpy as np
import pandas as pd

VERSION = 1
LABELS = {
    "A": "ticks reais com origem e cobertura verificadas",
    "B": "ticks reais com limitações identificadas",
    "C": "sequência de ticks reconstruída, híbrida ou de origem não comprovada",
    "D": "só barras: ordem intrabarra desconhecida",
    "E": "qualidade ou procedência desconhecida",
}
# O que cada tipo de decisão exige do dado para ser certificável.
REQUIREMENTS = {"intrabarra": ("A", "B"), "fechamento": ("A", "B", "C", "D")}
FLAG_BITS = {"bid": 2, "ask": 4, "last": 8, "volume": 16, "buy": 32, "sell": 64}
GAP_SECONDS = 60
SPREAD_ANOMALY = 10.0          # vezes a mediana


def audit(ticks: Optional[pd.DataFrame], point: Optional[float] = None) -> dict:
    """Mede a sequência capturada como está gravada (ordem time_msc, seq). Não corrige nem descarta nada."""
    empty = {"version": VERSION, "rows": 0, "defects": ["no_ticks"]}
    if ticks is None or len(ticks) == 0 or "time_msc" not in ticks:
        return empty
    order = [c for c in ("time_msc", "seq") if c in ticks]
    stored = pd.to_numeric(ticks["time_msc"], errors="coerce").to_numpy(dtype=float)
    data = ticks.sort_values(order, kind="stable")
    msc = pd.to_numeric(data["time_msc"], errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(msc).any():
        return {"version": VERSION, "rows": int(len(data)), "defects": ["invalid_time"]}
    bid = pd.to_numeric(data.get("bid", pd.Series(np.nan, index=data.index)), errors="coerce").to_numpy(dtype=float)
    ask = pd.to_numeric(data.get("ask", pd.Series(np.nan, index=data.index)), errors="coerce").to_numpy(dtype=float)
    quoted = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > 0)
    crossed = quoted & (ask < bid)
    same_ms = pd.Series(msc).value_counts()
    gaps = np.diff(msc) / 1000.0 if len(msc) > 1 else np.array([])
    days = pd.Series(msc[np.isfinite(msc)] // 86_400_000).value_counts()
    out = {
        "version": VERSION, "rows": int(len(data)),
        "first_time_msc": int(np.nanmin(msc)), "last_time_msc": int(np.nanmax(msc)),
        "invalid_time": int((~np.isfinite(msc)).sum()),
        "stored_order_regressions": int((np.diff(stored) < 0).sum()) if len(stored) > 1 else 0,
        "same_ms_groups": int((same_ms > 1).sum()), "max_same_ms": int(same_ms.max()),
        "provider_sequence": "seq" in ticks,
        "quoted_rows": int(quoted.sum()), "unquoted_rows": int((~quoted).sum()), "crossed_quotes": int(crossed.sum()),
        "last_rows": int((pd.to_numeric(data["last"], errors="coerce") > 0).sum()) if "last" in data else 0,
        "real_volume_rows": int((pd.to_numeric(data["volume_real"], errors="coerce") > 0).sum())
        if "volume_real" in data else 0,
        "days": int(len(days)), "ticks_per_day_min": int(days.min()), "ticks_per_day_median": float(days.median()),
        "gaps_over_60s": int((gaps > GAP_SECONDS).sum()),
        "largest_gaps_s": [float(v) for v in np.sort(gaps)[::-1][:5]],
    }
    if "flags" in data:
        flags = pd.to_numeric(data["flags"], errors="coerce").fillna(0).astype("int64").to_numpy()
        out["flags"] = {name: int(((flags & bit) != 0).sum()) for name, bit in FLAG_BITS.items()}
    spread = (ask - bid)[quoted & ~crossed]
    if len(spread):
        scale = point if point and point > 0 else 1.0
        median = float(np.median(spread))
        out["spread"] = {"unit": "points" if point and point > 0 else "price", "median": median / scale,
                         "p99": float(np.quantile(spread, 0.99)) / scale, "max": float(spread.max()) / scale,
                         "anomalous": int((spread > SPREAD_ANOMALY * median).sum()) if median > 0 else 0}
    defects = []
    if out["invalid_time"]:
        defects.append("invalid_time")
    if out["stored_order_regressions"]:
        defects.append("stored_order_regressions")
    if out["crossed_quotes"]:
        defects.append("crossed_quotes")
    if out["unquoted_rows"]:
        defects.append("rows_without_bid_ask")
    if out["same_ms_groups"] and not out["provider_sequence"]:
        defects.append("same_timestamp_order_unknown")
    if out["gaps_over_60s"]:
        # Pode ser pregão fechado: sem calendário qualificado é limitação, não falha comprovada.
        defects.append("gaps_not_reconciled_with_sessions")
    out["defects"] = defects
    return out


def grade(report: Optional[dict], tick_audit: Optional[dict] = None, tester: Optional[dict] = None) -> dict:
    """Classe A–E a partir do relatório T20.6, da auditoria de ticks e, se houver, do relatório do Tester."""
    reasons = []
    caps = (report or {}).get("capabilities") or {}

    def state(name):
        return (caps.get(name) or {}).get("state")
    ticks_ok = state("ticks") == "available"
    origin = (caps.get("ticks") or {}).get("origin", "unknown")
    complete = state("complete_real_ticks") == "available"
    if tester is not None:
        declared = ((tester.get("capabilities") or {}).get("complete_real_ticks") or {}).get("state") == "available"
        complete = complete or declared
        reasons.append("Relatório do Tester declara 100% de ticks reais." if declared else
                       "Relatório do Tester não declara cobertura integral de ticks reais.")
    defects = list((tick_audit or {}).get("defects") or [])
    if ticks_ok and origin == "observed":
        if complete and tick_audit is not None and not defects:
            letter = "A"
        else:
            letter = "B"
            if not complete:
                reasons.append("Cobertura integral de ticks reais não comprovada.")
            if tick_audit is None:
                reasons.append("Auditoria da sequência de ticks não executada.")
            reasons.extend(f"Auditoria: {item}." for item in defects)
    elif ticks_ok:
        letter = "C"
        reasons.append(f"Ticks capturados com origem {origin}: não comprovam eventos observados.")
    elif state("ohlcv") == "available":
        letter = "D"
        reasons.append("Sem ticks qualificados nesta execução; só barras OHLC.")
    else:
        letter = "E"
        reasons.append("Sem relatório de qualidade ou sem barras válidas.")
    return {"version": VERSION, "grade": letter, "label": LABELS[letter], "reasons": reasons,
            "certifies": {kind: letter in allowed for kind, allowed in REQUIREMENTS.items()},
            "basis": "t20_6_capabilities_not_independent_venue_validation"}


def supports(result: dict, decision: str) -> bool:
    """A classe certifica esse tipo de decisão ("intrabarra" ou "fechamento")?"""
    if decision not in REQUIREMENTS:
        raise ValueError(f"Tipo de decisão desconhecido: {decision}. Use {', '.join(REQUIREMENTS)}.")
    return bool(result["certifies"][decision])
