"""Adaptação visual de fatos persistidos; não calcula indicadores nem estatística."""
from datetime import datetime

REGIME_NAMES = {"TREND": "Tendência", "RANGE": "Lateral", "TRANSITION": "Transição", "UNCERTAIN": "Incerto"}


def empty(data, ident, kind, title, reason):
    return {"id": ident, "kind": kind, "title": title, "subject_ref": "subject", "source_refs": [],
            "as_of_utc": data["as_of_utc"], "status_namespace": kind, "source_status": None,
            "display_state": "UNAVAILABLE", "default_visible": False,
            "hidden_reason_code": "NO_DATA", "hidden_reason_short": reason,
            "n_observations": None, "unit": None, "quality": None, "temporal": {}, "evidence": {},
            "priority": {"policy": "pending-DASH-11", "score": None, "score_state": "UNAVAILABLE"},
            "visual": {"value": None, "series": []}, "details": {"reason": reason}}


def _clock(fact):
    return {key: fact.get(key) for key in ("observation_time", "available_at_utc", "observed_at_utc",
                                          "valid_until_utc", "source_revision", "revision", "algorithm_version")}


def regime(data):
    section = empty(data, "regime", "regime", "Regime e concordância dos indicadores",
                    "Nenhum estado de regime vigente no recorte; consulte as capacidades e os fatos arquivados.")
    history = sorted((r for r in data["facts"] if r["feature_id"] == "regime.state"), key=lambda r: r["observation_time"])
    if not history:
        return section
    last = history[-1]
    if last["display_state"] != "AVAILABLE" or last["category"] not in REGIME_NAMES:
        section["details"]["reason"] = "Último regime sem validade/contrato elegível; não substituído por um registro anterior."
        section["hidden_reason_short"] = section["details"]["reason"]
        return section
    states = [r for r in history if r["display_state"] == "AVAILABLE" and r["category"] in REGIME_NAMES]
    version = last["algorithm_version"]
    # Mesma versão e observação; não juntar confiança de outro algoritmo/recorte.
    confidence = {r["observation_time"]: r for r in data["facts"] if r["feature_id"] == "regime.confidence"
                  and r["algorithm_version"] == version and r["display_state"] == "AVAILABLE"}
    definition = next((d for d in data["contracts"] if d["feature_id"] == "regime.confidence"
                       and d["algorithm_version"] == version), {})
    method = definition.get("parameters", {}).get("confidence")
    if not isinstance(method, str) or not method.strip():
        method = None
    cells = {}
    for fact in states:
        stamp = datetime.fromisoformat(fact["observation_time"])
        bound = confidence.get(fact["observation_time"])
        value = bound["value"] if method and bound is not None else None
        if value is not None and not 0 <= value <= 1:
            value = None
        cells[stamp.weekday(), stamp.hour] = {"day": stamp.weekday(), "hour": stamp.hour,
            "state": fact["category"], "label": REGIME_NAMES.get(fact["category"], fact["category"]),
            "intensity": value, "observation_time": fact["observation_time"], "temporal": _clock(fact),
            "confidence_temporal": _clock(bound) if bound is not None else None}
    cut = datetime.fromisoformat(data["as_of_utc"])
    current = next((c for c in cells.values() if c["day"] == cut.weekday() and c["hour"] == cut.hour), None)
    section.update(source_refs=[{"feature_id": "regime.state", "algorithm_version": version},
                                {"feature_id": "regime.confidence", "algorithm_version": version}],
        source_status=last["category"], display_state="AVAILABLE", default_visible=True,
        hidden_reason_code=None, hidden_reason_short=None, n_observations=len(states), unit="estado do regime",
        quality=last["quality"], temporal=_clock(last),
        evidence={"confidence_method": method, "confidence_is_probability": False,
                  "sample_semantics": "registros de estado no recorte; não são eventos independentes"},
        visual={"value": REGIME_NAMES.get(last["category"], last["category"]), "direction": None,
                "series": [], "cells": list(cells.values()), "current_day": cut.weekday(), "current_hour": cut.hour,
                "current_cell_has_data": current is not None, "unit": "concordância0…1",
                "intensity_label": "Concordância dos indicadores ·0…1" if method else "Concordância não declarada"},
        details={"reason": "Último registro de cada dia da semana × hora UTC no recorte, sem média ou frequência. "
                 "Concordância é a medida declarada pelo produtor, não probabilidade de lucro. "
                 "O estado dominante acima é o último registro disponível; o contorno marca a hora do corte.",
                 "current_cell": current, "latest_observation_time": last["observation_time"]})
    return section
