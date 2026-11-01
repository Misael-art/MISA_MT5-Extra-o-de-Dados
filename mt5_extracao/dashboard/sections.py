"""Adaptação visual de fatos persistidos; não calcula indicadores nem estatística."""
import math
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


def volumes(data):
    """RVOL canônico: 1× é a mediana por definição, não uma mediana calculada aqui.

    Cada fonte/metodologia é uma faixa independente. Não juntar amostras entre
    versões, fontes ou minutos distintos, nem ressuscitar o penúltimo valor.
    """
    from mt5_extracao.timeframes import Timeframe
    step_seconds = Timeframe[data["subject"]["timeframe"]].minutes * 60
    sections = []
    for weekday in (False, True):
        prefix = "rvol.same_weekday" if weekday else "rvol"
        for source in ("tick", "real"):
            ident = prefix + "." + source
            history = sorted((f for f in data["facts"] if f["feature_id"] == ident),
                             key=lambda f: f["observation_time"])
            if not history:
                continue
            last = history[-1]
            version = last["algorithm_version"]
            title = "Volume relativo · " + ("ticks" if source == "tick" else "volume real")
            if weekday:
                title += " · mesmo dia da semana"
            section = empty(data, ident, "volume", title, "Último RVOL sem validade ou contrato elegível.")
            definition = next((d for d in data["contracts"] if d["feature_id"] == ident
                               and d["algorithm_version"] == version), {})
            params = definition.get("parameters", {})
            if (params.get("volume_source") != source or params.get("same_weekday") is not weekday):
                section["details"]["reason"] = "Fonte/metodologia de RVOL não declarada de forma compatível."
                section["hidden_reason_short"] = section["details"]["reason"]
                sections.append(section)
                continue
            counts = {f["observation_time"]: f for f in data["facts"]
                      if f["feature_id"] == prefix + ".reference_sessions"
                      and f["algorithm_version"] == version and f["display_state"] == "AVAILABLE"}
            def point(fact):
                valid = fact["display_state"] == "AVAILABLE" and fact["value"] is not None and fact["value"] >= 0
                bound = counts.get(fact["observation_time"])
                if bound and (bound.get("minute_of_session") != fact.get("minute_of_session")
                              or bound["source_revision"] != fact["source_revision"]):
                    bound = None
                count = bound["value"] if bound else None
                if count is not None and (count < 1 or not count.is_integer()):
                    count = None
                return {"time": fact["observation_time"], "value": fact["value"] if valid else None,
                        "reference_sessions": count, "minute_of_session": fact.get("minute_of_session"),
                        "temporal": _clock(fact), "reference_temporal": _clock(bound) if bound else None}
            series = [point(f) for f in history if f["algorithm_version"] == version]
            for index, item in enumerate(series):
                prior = series[index - 1] if index else None
                item["break_before"] = prior is not None and (
                    (datetime.fromisoformat(item["time"]) - datetime.fromisoformat(prior["time"])).total_seconds() > step_seconds
                    or (item["minute_of_session"] is not None and prior["minute_of_session"] is not None
                        and item["minute_of_session"] <= prior["minute_of_session"]))
            latest = series[-1]
            section.update(source_refs=[{"feature_id": ident, "algorithm_version": version},
                {"feature_id": prefix + ".reference_sessions", "algorithm_version": version}],
                temporal=_clock(last), quality=last["quality"], unit="× mediana", details={
                    "reason": "Acumulado dividido pela mediana do mesmo minuto das sessões anteriores. "
                    "Referência tracejada 1×; realce acima de 1× significa acima da mediana, não sinal operacional. "
                    "Linha sem suavização estatística; ausências explícitas interrompem a série. "
                    "Reconstrução conhecida no corte; n conta sessões de referência do último ponto, não barras.",
                    "volume_source": source, "same_weekday": weekday, "parameters": params,
                    "latest_observation_time": last["observation_time"]})
            if latest["value"] is not None:
                section.update(display_state="AVAILABLE", default_visible=True, hidden_reason_code=None,
                    hidden_reason_short=None, n_observations=latest["reference_sessions"],
                    evidence={"sample_semantics": "sessões anteriores comparáveis no minuto do último ponto",
                              "confidence_is_probability": False, "reference": "mediana do acumulado no mesmo minuto"},
                    visual={"value": latest["value"], "series": series, "baseline": 1,
                            "threshold": 1, "threshold_semantics": "acima da mediana; não é aprovação",
                            "unit": "× mediana", "direction": None})
            sections.append(section)
    return sections or [empty(data, "volume", "volume", "Volume relativo",
                             "Nenhum RVOL persistido no recorte; volume ausente não é zero.")]


# DASH-06 — hipótese estatística (T13.4). O produtor ainda não existe: o gerador
# só emite o estado pendente. hypothesis() adapta o DTO de pattern_evaluations
# (docs/analytics/ESPECIFICACAO.md §7.2/§8) quando ele existir; não calcula
# lift, IC, amostra mínima nem aprovação.
HYPOTHESIS_STATES = ("DRAFT", "EXPLORATORY", "VALIDATING", "APPROVED", "MONITORING", "RETIRED")
HYPOTHESIS_VISIBLE = {"APPROVED", "VALIDATING"}
HYPOTHESIS_HIDDEN = {
    "EXPLORATORY": "Exploratória: só por expansão manual; não é hipótese aprovada.",
    "DRAFT": "Rascunho: só auditoria manual, sem aparência de conclusão.",
    "MONITORING": "Em monitoramento: não herda aprovação; aguarda recibo de deriva.",
    "RETIRED": "Retirada na origem; permanece auditável.",
}
HYPOTHESIS_PENDING = ("O catálogo de hipóteses estatísticas (T13.4) ainda não existe; nenhuma hipótese foi "
                      "calculada ou inferida. Aprovação de bateria de robô não é hipótese aprovada.")


def hypothesis_pending(data):
    section = empty(data, "hypothesis", "hypothesis", "Hipóteses estatísticas", HYPOTHESIS_PENDING)
    section.update(default_visible=True, hidden_reason_code=None, hidden_reason_short=None,
                   evidence={"reason_code": "T13_4_PENDING"})
    return section


def _fraction(value, label):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
        raise ValueError(f"INVALID_SOURCE: {label} precisa ser fração 0…1.")
    return float(value)


def _confidence(raw):
    """IC só com método, nível e limites coerentes; caso contrário, anel neutro "Não informada"."""
    if not isinstance(raw, dict):
        return None
    method, level, low, high = (raw.get(k) for k in ("method", "level", "low", "high"))
    numbers = (level, low, high)
    if (not isinstance(method, str) or not method.strip()
            or any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in numbers)
            or not 0 < level < 1 or low > high):
        return None
    return {"method": method.strip(), "level": float(level), "low": float(low), "high": float(high),
            "is_probability_of_profit": False}


def hypothesis(data, record):
    """Adapta uma avaliação canônica ao cartão de anéis, recusando estados/namespaces alheios."""
    if not isinstance(record, dict) or record.get("status_namespace") != "hypothesis":
        raise ValueError("INVALID_SOURCE: só avaliações do catálogo de hipóteses; bateria de robô e relações "
                         "têm namespace próprio.")
    status = record.get("status")
    if status not in HYPOTHESIS_STATES:
        raise ValueError("INVALID_SOURCE: estado de hipótese desconhecido.")
    ident, version = record.get("hypothesis_id"), record.get("version")
    if not all(isinstance(v, str) and v.strip() for v in (ident, version)):
        raise ValueError("INVALID_SOURCE: hipótese sem ID/versão.")
    cut = datetime.fromisoformat(data["as_of_utc"])
    available = datetime.fromisoformat(record["available_at_utc"])
    if available > cut:
        raise ValueError("FUTURE_EVIDENCE: avaliação disponível após o corte.")
    n = record.get("n_events")
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        raise ValueError("INVALID_SOURCE: n_events precisa ser inteiro não negativo.")
    lift = record.get("lift")
    if lift is not None and (isinstance(lift, bool) or not isinstance(lift, (int, float)) or lift < 0
                             or not math.isfinite(lift)):
        raise ValueError("INVALID_SOURCE: lift precisa ser razão finita não negativa.")
    oos = record.get("oos") if isinstance(record.get("oos"), dict) else None
    if status == "APPROVED" and not (oos and oos.get("test_from") and oos.get("test_to")):
        raise ValueError("INVALID_SOURCE: APPROVED exige evidência fora da amostra declarada.")
    p_condition, p_base = _fraction(record.get("p_condition"), "p_condition"), _fraction(record.get("p_base"), "p_base")
    confidence = _confidence(record.get("confidence"))
    hidden = HYPOTHESIS_HIDDEN.get(status)
    title = record.get("title") if isinstance(record.get("title"), str) and record["title"].strip() else ident
    section = empty(data, "hypothesis:" + ident, "hypothesis", title, hidden or "")
    section.update(
        source_refs=[{"hypothesis_id": ident, "version": version}], status_namespace="hypothesis",
        source_status=status, display_state="AVAILABLE", default_visible=status in HYPOTHESIS_VISIBLE,
        hidden_reason_code=None if hidden is None else "STATUS_" + status, hidden_reason_short=hidden,
        n_observations=n, unit="lift (× taxa-base)", quality=None,
        temporal={"available_at_utc": record["available_at_utc"], "validated_at_utc": record.get("validated_at_utc"),
                  "train_from": record.get("train_from"), "train_to": record.get("train_to"),
                  "test_from": oos.get("test_from") if oos else None, "test_to": oos.get("test_to") if oos else None},
        evidence={"p_condition": p_condition, "p_base": p_base, "lift": lift, "confidence": confidence,
                  "expectancy_net": record.get("expectancy_net"), "approved_is_recommendation": False,
                  "lift_scale": "razão p_condicional/p_base; 1 = igual à taxa-base; sem limite superior"},
        visual={"value": lift, "direction": None, "series": [],
                "rings": [{"ring": "sample", "label": "n", "value": n},
                          {"ring": "lift", "label": "lift", "value": lift, "reference": 1.0},
                          {"ring": "confidence", "label": "IC" if confidence else "Não informada",
                           "value": confidence}]},
        details={"reason": "Avaliação persistida do catálogo; APPROVED indica evidência de utilidade no contexto "
                           "definido, não recomendação de compra/venda.",
                 "status_label": {"APPROVED": "Aprovada", "VALIDATING": "Em validação"}.get(status, status)})
    return section


# DASH-07 — validação temporal a partir de _strategy_validation (strategies.validation).
# O recibo atual guarda as janelas OOS por fold, critérios e veredito do Filtro C;
# não guarda datas de treino, custos (BacktestConfig) nem fuso de created_at.
NAIVE_CLOCK_MARGIN_HOURS = 14  # maior adiantamento de fuso civil (UTC+14)


def _naive(value):
    stamp = datetime.fromisoformat(str(value).replace("T", " ").split("+")[0].strip())
    return stamp.replace(tzinfo=None)


def validation_sections(data, rows):
    """Uma faixa por estratégia validada no ativo/TF; nada é recalculado."""
    from datetime import timedelta
    cut = datetime.fromisoformat(data["as_of_utc"])
    cut_naive = cut.replace(tzinfo=None)  # as_of_utc já está em UTC
    result, late = [], 0
    for row in rows:
        try:
            created = _naive(row["created_at"])
        except (TypeError, ValueError):
            late += 1
            continue
        # created_at é hora local sem fuso: só entra se precede o corte em qualquer fuso civil.
        if created + timedelta(hours=NAIVE_CLOCK_MARGIN_HOURS) > cut_naive:
            late += 1
            continue
        verdict = row.get("result") if isinstance(row.get("result"), dict) else {}
        windows = verdict.get("windows") if isinstance(verdict.get("windows"), list) else []
        folds = []
        for w in windows:
            if not isinstance(w, dict):
                continue
            folds.append({k: w.get(k) for k in ("window", "oos_start", "oos_end", "oos_trades", "oos_profit_factor",
                                                "oos_profit_factor_status", "oos_net_profit", "is_trades",
                                                "is_profit_factor", "params")})
        approved = bool(row["approved"])
        strategy = str(row["strategy"])
        pf = row.get("profit_factor_oos")
        pf = float(pf) if isinstance(pf, (int, float)) and not isinstance(pf, bool) and math.isfinite(pf) else None
        hidden = None if approved else "Reprovada no Filtro C; critérios e janelas permanecem auditáveis."
        section = empty(data, "validation:" + strategy, "validation", "Validação walk-forward · " + strategy, hidden or "")
        checks = [{k: c.get(k) for k in ("name", "text", "passed", "value", "threshold")}
                  for c in verdict.get("checks", []) if isinstance(c, dict)]
        section.update(
            source_refs=[{"table": "_strategy_validation", "symbol": row["symbol"], "timeframe": row["timeframe"],
                          "strategy": strategy}],
            status_namespace="validation", source_status="FILTER_C_APPROVED" if approved else "FILTER_C_REJECTED",
            display_state="AVAILABLE" if folds else "INSUFFICIENT", default_visible=approved,
            hidden_reason_code=None if approved else "FILTER_C_REJECTED", hidden_reason_short=hidden,
            n_observations=int(row["trades_oos"]) if row.get("trades_oos") is not None else None,
            unit="profit factor fora da amostra",
            temporal={"created_at_local_naive": str(row["created_at"]), "data_start": row.get("data_start"),
                      "data_end": row.get("data_end"),
                      "clock_rule": f"created_at sem fuso; incluído só se +{NAIVE_CLOCK_MARGIN_HOURS} h precede o corte"},
            evidence={"checks": checks, "criteria": verdict.get("criteria"),
                      "profit_factor_status": row.get("profit_factor_status"),
                      "positive_windows": row.get("positive_windows"), "robust": row.get("robust"),
                      "monte_carlo_dd": row.get("mc_dd"),
                      "costs": {"state": "NOT_RECORDED",
                                "reason": "O recibo não grava BacktestConfig; custos não são conferíveis aqui."},
                      "train_dates": {"state": "NOT_RECORDED",
                                      "reason": "O recibo grava só as janelas OOS; o treino antecede cada OOS."},
                      "approved_is_hypothesis": False},
            visual={"value": pf, "direction": None, "series": [], "folds": folds,
                    "data_start": row.get("data_start"), "data_end": row.get("data_end")},
            details={"reason": "Veredito do Filtro C gravado; aprovação é por critérios de robustez da estratégia, "
                               "não hipótese estatística aprovada nem recomendação.",
                     "status_label": "Filtro C aprovado" if approved else "Filtro C reprovado"})
        if pf is None:
            section["details"]["pf_reason"] = "PF fora da amostra não calculável: " + str(row.get("profit_factor_status"))
        result.append(section)
    if late:
        note = empty(data, "validation:late", "validation", "Validações após o corte",
                     f"{late} recibo(s) com relógio sem fuso possivelmente posterior ao corte; omitidos desta fotografia.")
        note.update(hidden_reason_code="CLOCK_AFTER_CUT", n_observations=late, status_namespace="dashboard")
        result.append(note)
    return result
