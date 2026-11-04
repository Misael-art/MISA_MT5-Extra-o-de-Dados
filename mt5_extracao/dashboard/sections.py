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
    if last["display_state"] == "INVALIDATED":
        # Vermelho é do alerta de invalidação retroativa; aqui só o estado, sem recorrer ao registro anterior.
        section.update(display_state="INVALIDATED", default_visible=True, hidden_reason_code=None,
                       hidden_reason_short=None, temporal=_clock(last), source_status="INVALIDATED")
        section["details"]["reason"] = ("O último regime foi invalidado na origem; nenhum registro anterior o "
                                        "substitui. Veja o alerta de invalidação no topo.")
        return section
    if last["display_state"] not in {"AVAILABLE", "STALE"} or last["category"] not in REGIME_NAMES:
        section["details"]["reason"] = "Último regime sem validade/contrato elegível; não substituído por um registro anterior."
        section["hidden_reason_short"] = section["details"]["reason"]
        return section
    states = [r for r in history if r["display_state"] in {"AVAILABLE", "STALE"} and r["category"] in REGIME_NAMES]
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
    if last["display_state"] == "STALE":
        until = datetime.fromisoformat(last["valid_until_utc"])
        section.update(display_state="STALE")
        section["temporal"]["stale"] = {"valid_until_utc": last["valid_until_utc"],
                                        "expired_minutes_before_cut": int((cut - until).total_seconds() // 60),
                                        "observation_time": last["observation_time"]}
        section["details"]["reason"] += " O último estado está com validade encerrada no corte; não é atual."
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


# DASH-06 — hipótese estatística (T13.4). P06 grava avaliações de pesquisa;
# o cartão abaixo conserva esse estado sem derivar lift, IC ou aprovação.
HYPOTHESIS_STATES = ("DRAFT", "EXPLORATORY", "VALIDATING", "APPROVED", "MONITORING", "RETIRED")
HYPOTHESIS_VISIBLE = {"APPROVED", "VALIDATING"}
HYPOTHESIS_HIDDEN = {
    "EXPLORATORY": "Exploratória: só por expansão manual; não é hipótese aprovada.",
    "DRAFT": "Rascunho: só auditoria manual, sem aparência de conclusão.",
    "MONITORING": "Em monitoramento: não herda aprovação; aguarda recibo de deriva.",
    "RETIRED": "Retirada na origem; permanece auditável.",
}
HYPOTHESIS_PENDING = ("Nenhuma avaliação T13.4 persistida para este ativo, tempo e corte. "
                      "Aprovação de bateria de robô não é hipótese aprovada.")


def hypothesis_pending(data):
    section = empty(data, "hypothesis", "hypothesis", "Hipóteses estatísticas", HYPOTHESIS_PENDING)
    section.update(default_visible=True, hidden_reason_code=None, hidden_reason_short=None,
                   evidence={"reason_code": "NO_PATTERN_EVALUATION"})
    return section


def pattern_evaluation(data, item):
    """Mostra o recibo P06 como pesquisa exploratória, sem inventar taxa ou aprovação."""
    receipt = item["receipt"]
    if (receipt.get("source_namespace") != data["subject"]["base_namespace"]
            or receipt.get("symbol") != data["subject"]["symbol"]
            or receipt.get("timeframe") != data["subject"]["timeframe"]):
        raise ValueError("INVALID_SOURCE: avaliação pertence a outro ativo, TF ou base.")
    if (receipt.get("statistical_approval") is not False
            or receipt.get("economic_approval") is not False
            or receipt.get("qualified_final_holdout") is not False):
        raise ValueError("INVALID_SOURCE: estado de aprovação exige contrato posterior ao P06.")
    if datetime.fromisoformat(item["available_at_utc"]) > datetime.fromisoformat(data["as_of_utc"]):
        raise ValueError("FUTURE_EVIDENCE: avaliação disponível após o corte.")
    ident = receipt["request_id"] + ":" + receipt["data_manifest_id"]
    section = empty(data, "hypothesis:" + ident, "hypothesis",
                    "Pesquisa · " + receipt["family"], "Avaliação exploratória; consulte o recibo.")
    section.update(status_namespace="hypothesis", source_status="EXPLORATORY",
                   display_state="AVAILABLE", default_visible=False,
                   hidden_reason_code="STATUS_EXPLORATORY",
                   hidden_reason_short=HYPOTHESIS_HIDDEN["EXPLORATORY"],
                   source_refs=[{"protocol_id": receipt["protocol_id"],
                                 "request_id": receipt["request_id"],
                                 "data_manifest_id": receipt["data_manifest_id"],
                                 "source_namespace": receipt["source_namespace"]}],
                   n_observations=receipt["event_count"], unit="eventos", quality=None,
                   temporal={"available_at_utc": item["available_at_utc"],
                             "data_as_of_utc": receipt["as_of_utc"]},
                   evidence={"negative_count": receipt["negative_count"],
                             "censored_count": receipt["censored_count"],
                             "validation": receipt["validation"],
                             "economic_status": receipt["economic"]["status"],
                             "statistical_approval": False, "economic_approval": False,
                             "qualified_final_holdout": False,
                             "approved_is_recommendation": False},
                   visual={"value": None, "series": [],
                           "rings": [{"ring": "sample", "label": "n", "value": receipt["event_count"]},
                                     {"ring": "lift", "label": "lift", "value": None, "reference": 1.0},
                                     {"ring": "confidence", "label": "Não informada", "value": None}]},
                   details={"reason": "Recibo de pesquisa persistido; resultado sem qualificação final.",
                            "definition_hash": receipt["definition_hash"],
                            "results_sha256": receipt["results_sha256"],
                            "signal_feature_id": item["signal_feature_id"],
                            "outcome_feature_id": item["outcome_feature_id"]})
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
    if status == "RETIRED":
        # §7.1.8: fora da narrativa e da lista comum de ocultas; só auditoria (exceção: alerta retroativo).
        section["details"]["audit_only"] = True
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


# DASH-10 — modo Robô: resultados persistidos em robots.store, sem iniciar nada.
def _within_cut(created, cut):
    """created_at sem fuso (hora local): só conta se precede o corte em qualquer fuso civil."""
    from datetime import timedelta
    try:
        stamp = _naive(created)
    except (TypeError, ValueError):
        return False
    return stamp + timedelta(hours=NAIVE_CLOCK_MARGIN_HOURS) <= cut.replace(tzinfo=None)


def _series(points):
    out = []
    for point in points or []:
        if isinstance(point, (list, tuple)) and len(point) == 2 and isinstance(point[1], (int, float)) \
                and not isinstance(point[1], bool) and math.isfinite(point[1]):
            out.append({"time": str(point[0]), "value": float(point[1])})
    return out


def robot_run_section(data, record, run, others):
    metrics = run.get("metrics") or {}
    number = lambda key: metrics.get(key) if isinstance(metrics.get(key), (int, float)) \
        and not isinstance(metrics.get(key), bool) and math.isfinite(metrics.get(key)) else None
    net = number("net_profit")
    section = empty(data, "robot:run", "robot", "Execução selecionada · " + (record.get("name") or record["key"]), "")
    currency = metrics.get("currency")
    section.update(
        source_refs=[{"table": "_robot_runs", "run_id": run["run_id"], "robot_key": run["robot_key"]}],
        status_namespace="robot", source_status="RUN_RECORDED", display_state="AVAILABLE", default_visible=True,
        hidden_reason_code=None, hidden_reason_short=None, n_observations=number("trades"),
        unit="resultado líquido" + (" · " + currency if currency else " · moeda não declarada"),
        temporal={"created_at_local_naive": run.get("created_at"), "data_start": run.get("data_start"),
                  "data_end": run.get("data_end"),
                  "clock_rule": f"created_at sem fuso; incluído só se +{NAIVE_CLOCK_MARGIN_HOURS} h precede o corte"},
        evidence={"profit_factor": number("profit_factor"), "profit_factor_status": metrics.get("profit_factor_status"),
                  "max_drawdown_pct": number("max_drawdown_pct"), "win_rate": number("win_rate"),
                  "return_pct": number("return_pct"), "trades": number("trades"),
                  "params": run.get("params"), "source": run.get("source"), "bars": run.get("bars"),
                  "data_provenance_status": run.get("data_provenance_status"),
                  "data_manifest_id": run.get("data_manifest_id"),
                  "execution_identity": metrics.get("execution_identity"),
                  "currency_source": metrics.get("currency_source") or ("declared" if currency else "unknown"),
                  "other_runs_aggregated": False, "is_realized_pnl": False},
        visual={"value": net, "direction": None if net is None else ("UP" if net > 0 else "DOWN" if net < 0 else "FLAT"),
                "baseline": None, "decimals": 2, "series": _series(run.get("equity")),
                "series_label": "Curva de capital do backtest (pontos reduzidos gravados)"},
        details={"reason": f"Backtest gravado {run['run_id']} de {run.get('symbol')} {run.get('timeframe')} "
                           f"({run.get('data_start')} → {run.get('data_end')}). Resultado simulado, não PnL realizado. "
                           + (f"{len(others)} outra(s) execução(ões) ficam listadas em separado, sem agregação."
                              if others else "Única execução conhecida no corte."),
                 "status_label": "Backtest gravado"})
    if net is None:
        section["details"]["reason"] += " Resultado líquido ausente no registro."
    return section


def robot_battery_section(data, check):
    approved = bool(check.get("approved"))
    section = empty(data, "robot:battery", "robot", "Bateria de segurança", "")
    section.update(
        source_refs=[{"table": "_robot_checks", "check_id": check.get("check_id"), "run_id": check.get("run_id")}],
        status_namespace="robot", source_status="BATTERY_APPROVED" if approved else "BATTERY_REJECTED",
        display_state="AVAILABLE", default_visible=True, hidden_reason_code=None, hidden_reason_short=None,
        n_observations=None, unit=None,
        temporal={"created_at_local_naive": check.get("created_at")},
        evidence={"approved": approved, "allows_real": bool(check.get("allows_real")),
                  "static_status": check.get("static_status"), "statistical_status": check.get("statistical_status"),
                  "preset": check.get("preset"), "fingerprint": check.get("fingerprint"),
                  "symbol": check.get("symbol"), "timeframe": check.get("timeframe"),
                  "items": check.get("items"), "battery_is_hypothesis": False},
        visual={"value": "Aprovada" if approved else "Reprovada", "direction": None, "series": []},
        details={"reason": "Etapa de bateria do catálogo de robôs; não é hipótese estatística aprovada nem "
                           "autorização de operação.", "status_label": "Bateria"})
    return section


def robot_other_runs_section(data, others):
    section = empty(data, "robot:other-runs", "robot", "Outras execuções do robô",
                    "Execuções diferentes da selecionada; não agregadas nem comparadas implicitamente.")
    section.update(status_namespace="robot", hidden_reason_code="OTHER_RUNS", n_observations=len(others),
                   evidence={"runs": [{k: r.get(k) for k in ("run_id", "symbol", "timeframe", "data_start", "data_end",
                                                             "created_at", "source")} for r in others]})
    return section


def robot_no_result_section(data, record, late):
    reason = "Sem resultado neste recorte. Nenhum backtest, compilação ou sessão é iniciado pela leitura."
    if late:
        reason += f" {late} execução(ões) com relógio possivelmente posterior ao corte foram omitidas."
    section = empty(data, "robot:run", "robot", "Execução · " + (record.get("name") or record["key"]), reason)
    section.update(status_namespace="robot", default_visible=True, hidden_reason_code=None, hidden_reason_short=None)
    return section
