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

VERSION = 2
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


def _integers(values: pd.Series):
    """Inteiros não negativos de 64 bits, com máscara que conserva entradas inválidas."""
    numbers = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    valid = np.isfinite(numbers) & (numbers >= 0) & (numbers < 2**63) & (numbers == np.floor(numbers))
    valid &= ~values.map(lambda value: isinstance(value, (bool, np.bool_))).to_numpy(dtype=bool)
    return np.where(valid, numbers, 0).astype("int64"), valid


def audit(ticks: Optional[pd.DataFrame], point: Optional[float] = None) -> dict:
    """Mede a sequência capturada como está gravada (ordem time_msc, seq). Não corrige nem descarta nada."""
    empty = {"version": VERSION, "rows": 0, "defects": ["no_ticks"]}
    if ticks is None or len(ticks) == 0 or "time_msc" not in ticks:
        return empty
    order = [c for c in ("time_msc", "seq") if c in ticks]
    stored = pd.to_numeric(ticks["time_msc"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    # Ordenar pelo valor numérico; textos/NaN/infinito continuam contabilizados como
    # inválidos, sem contaminar extremos ou intervalos nem modificar a captura.
    data = ticks.assign(time_msc=pd.to_numeric(ticks["time_msc"], errors="coerce"))
    if "seq" in data:
        sequence, valid_sequence = _integers(data["seq"])
        data = data.assign(seq=np.where(valid_sequence, sequence, np.nan))
    data = data.sort_values(order, kind="stable")
    msc = pd.to_numeric(data["time_msc"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    finite = np.isfinite(msc)
    if not finite.any():
        return {"version": VERSION, "rows": int(len(data)), "invalid_time": int(len(data)),
                "defects": ["invalid_time"]}
    bid = pd.to_numeric(data.get("bid", pd.Series(np.nan, index=data.index)), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    ask = pd.to_numeric(data.get("ask", pd.Series(np.nan, index=data.index)), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    quoted = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > 0)
    crossed = quoted & (ask < bid)
    same_ms = pd.Series(msc[finite]).value_counts()
    # Não inventar um intervalo entre eventos separados por timestamp inválido.
    valid_pairs = finite[:-1] & finite[1:]
    gaps = (msc[1:][valid_pairs] - msc[:-1][valid_pairs]) / 1000.0
    stored_pairs = np.isfinite(stored[:-1]) & np.isfinite(stored[1:])
    days = pd.Series(msc[np.isfinite(msc)] // 86_400_000).value_counts()
    invalid_sequence, duplicate_sequence = 0, 0
    if "seq" in data:
        _sequence, valid_sequence = _integers(data["seq"])
        invalid_sequence = int((~valid_sequence).sum())
        duplicate_sequence = int(data.loc[finite & valid_sequence].duplicated(["time_msc", "seq"], keep=False).sum())
    out = {
        "version": VERSION, "rows": int(len(data)),
        "first_time_msc": int(msc[finite].min()), "last_time_msc": int(msc[finite].max()),
        "invalid_time": int((~np.isfinite(msc)).sum()),
        "stored_order_regressions": int((stored[1:][stored_pairs] < stored[:-1][stored_pairs]).sum()),
        "same_ms_groups": int((same_ms > 1).sum()), "max_same_ms": int(same_ms.max()),
        "provider_sequence": "seq" in ticks and not invalid_sequence and not duplicate_sequence,
        "invalid_sequence": invalid_sequence, "duplicate_sequence_rows": duplicate_sequence,
        "invalid_flags": 0,
        "quoted_rows": int(quoted.sum()), "unquoted_rows": int((~quoted).sum()), "crossed_quotes": int(crossed.sum()),
        "last_rows": int((pd.to_numeric(data["last"], errors="coerce") > 0).sum()) if "last" in data else 0,
        "real_volume_rows": int((pd.to_numeric(data["volume_real"], errors="coerce") > 0).sum())
        if "volume_real" in data else 0,
        "days": int(len(days)), "ticks_per_day_min": int(days.min()), "ticks_per_day_median": float(days.median()),
        "gaps_over_60s": int((gaps > GAP_SECONDS).sum()),
        "largest_gaps_s": [float(v) for v in np.sort(gaps)[::-1][:5]],
    }
    if "flags" in data:
        flags, valid_flags = _integers(data["flags"])
        out["invalid_flags"] = int((~valid_flags).sum())
        out["flags"] = {name: int(((flags & bit) != 0).sum()) for name, bit in FLAG_BITS.items()}
    spread = (ask - bid)[quoted & ~crossed]
    try:
        scale = float(point) if point is not None and not isinstance(point, (bool, np.bool_)) else np.nan
    except (ValueError, TypeError, OverflowError):
        scale = np.nan
    valid_point = bool(np.isfinite(scale) and scale > 0)
    out["point_state"] = "available" if valid_point else "unavailable" if point is None else "invalid"
    if len(spread):
        scale = scale if valid_point else 1.0
        median = float(np.quantile(spread, 0.5))
        statistics = np.array([median, np.quantile(spread, 0.99), spread.max()])
        with np.errstate(over="ignore", invalid="ignore"):
            converted = statistics / scale
        if not np.isfinite(converted).all():
            out["point_state"] = "invalid"
            valid_point = False
            converted = statistics
        out["spread"] = {"unit": "points" if valid_point else "price", "median": float(converted[0]),
                         "p99": float(converted[1]), "max": float(converted[2]),
                         "anomalous": int((spread > SPREAD_ANOMALY * median).sum()) if median > 0 else 0}
    defects = []
    if out["invalid_time"]:
        defects.append("invalid_time")
    if out["invalid_sequence"]:
        defects.append("invalid_sequence")
    if out["duplicate_sequence_rows"]:
        defects.append("duplicate_sequence")
    if out["invalid_flags"]:
        defects.append("invalid_flags")
    if out["point_state"] == "invalid":
        defects.append("invalid_point")
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
    rows = (tick_audit or {}).get("rows")
    audited = ((tick_audit or {}).get("version") == VERSION and type(rows) is int and rows > 0)
    if ticks_ok and origin == "observed":
        if complete and audited and not defects:
            letter = "A"
        else:
            letter = "B"
            if not complete:
                reasons.append("Cobertura integral de ticks reais não comprovada.")
            if not audited:
                reasons.append("Auditoria da sequência de ticks ausente, vazia ou de versão anterior.")
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
    certifies = {kind: letter in allowed for kind, allowed in REQUIREMENTS.items()}
    blocking = set(defects) - {"gaps_not_reconciled_with_sessions"}
    certifies["intrabarra"] = certifies["intrabarra"] and audited and not blocking
    return {"version": VERSION, "grade": letter, "label": LABELS[letter], "reasons": reasons,
            "certifies": certifies,
            "basis": "t20_6_capabilities_not_independent_venue_validation"}


def supports(result: dict, decision: str) -> bool:
    """A classe certifica esse tipo de decisão ("intrabarra" ou "fechamento")?"""
    if decision not in REQUIREMENTS:
        raise ValueError(f"Tipo de decisão desconhecido: {decision}. Use {', '.join(REQUIREMENTS)}.")
    return bool(result["certifies"][decision])
