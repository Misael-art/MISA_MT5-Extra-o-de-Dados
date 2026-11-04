"""Política de ordenação visual DASH-11 (PROMPT_DASHBOARD.md §7.1/§7.2).

Ordena apresentação; não redefine lift, IC, amostra mínima, aprovação ou validade.
Elegibilidade e estados da origem já vêm decididos nas faixas: nada aqui promove,
rebaixa ou torna visível uma faixa. Cada fator registra método e entradas.
"""
import math

POLICY = "DASH-11/1"
NOVELTY_BASAL = 0.5          # primeira revisão/sem baseline: positivo, sinalizado "Sem comparação anterior"
NOVELTY_STALE = 1.0          # snapshot vencido é mudança relevante para a leitura
LN2 = math.log(2)

# Grupos obrigatórios (§7.1). O alerta de invalidação retroativa fica em `alerts`, sempre acima de tudo.
GROUPS = (
    ("surprise", "Surpresa abre a narrativa regular"),
    ("subject", "Resultado do robô selecionado"),
    ("approved", "Hipóteses APPROVED"),
    ("validating", "Hipóteses VALIDATING, com rótulo de validação em andamento"),
    ("context", "Contexto: regime, volume, validação, vencidos e dependências"),
)
GROUP_INDEX = {name: index for index, (name, _) in enumerate(GROUPS)}
GROUP_LABEL = dict(GROUPS)

POLICY_DOC = {
    "policy": POLICY,
    "groups": [{"group": g, "meaning": m} for g, m in GROUPS],
    "formula": "score_bruto = I × E × N; score = score_bruto / soma(score_bruto válidos do grupo)",
    "families": {
        "hypothesis": {"I": "|ln(lift)| / (|ln(lift)| + ln 2); lift ausente → indisponível",
                       "E": "n/(n+100) × (IC declarado ? 1 : 0,5) × (OOS declarado ? 1 : 0,5)"},
        "validation": {"I": "|ln(PF OOS)| / (|ln(PF OOS)| + ln 2); PF ausente → indisponível",
                       "E": "n/(n+100), n = operações fora da amostra"},
        "volume": {"I": "|ln(RVOL)| / (|ln(RVOL)| + ln 2); RVOL ≤ 0 ou ausente → indisponível",
                   "E": "n/(n+20), n = sessões de referência"},
        "regime": {"I": "indisponível: estado categórico sem magnitude comparável",
                   "E": "indisponível"},
    },
    "novelty": {"basal": NOVELTY_BASAL, "stale": NOVELTY_STALE,
                "rule": "Sem baseline compatível: fator basal positivo e 'Sem comparação anterior'; "
                        "vencido no corte: 1,0. Nenhuma tabela/cache de histórico é criada."},
    "ties": "grupo → score válido → impacto → evidência → ID estável",
    "zero_sum": "Soma zero ou nenhum score válido: ordem por impacto, evidência e ID; motivo registrado.",
}


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _ratio_impact(value, label):
    if not _finite(value) or value <= 0:
        return {"value": None, "method": f"{label} ausente ou não positivo"}
    magnitude = abs(math.log(value))
    return {"value": magnitude / (magnitude + LN2), "method": f"|ln({label})|/(|ln({label})|+ln 2)",
            "input": value}


def _sample(n, half, label):
    if not _finite(n) or n < 0:
        return {"value": None, "method": f"{label} não informado"}
    return {"value": n / (n + half), "method": f"n/(n+{half})", "input": n}


def group_of(section):
    if section["kind"] == "surprise":
        return "surprise"
    if section["kind"] == "robot" and section["id"] == "robot:run":
        return "subject"
    if section["kind"] == "robot" and section["id"] == "robot:battery":
        return "subject"
    if section["kind"] == "hypothesis" and section["source_status"] == "APPROVED":
        return "approved"
    if section["kind"] == "hypothesis" and section["source_status"] == "VALIDATING":
        return "validating"
    return "context"


def factors(section):
    kind, evidence, visual = section["kind"], section.get("evidence") or {}, section.get("visual") or {}
    if kind == "hypothesis" and section["source_status"]:
        impact = _ratio_impact(evidence.get("lift"), "lift")
        sample = _sample(section.get("n_observations"), 100, "n_events")
        if sample["value"] is not None:
            ci = 1.0 if evidence.get("confidence") else 0.5
            oos = 1.0 if (section.get("temporal") or {}).get("test_from") else 0.5
            sample = {**sample, "value": sample["value"] * ci * oos,
                      "method": sample["method"] + " × IC(1|0,5) × OOS(1|0,5)", "ci": ci, "oos": oos}
        evidence_factor = sample
    elif kind == "validation" and section["status_namespace"] == "validation":
        impact = _ratio_impact(visual.get("value"), "PF")
        evidence_factor = _sample(section.get("n_observations"), 100, "operações OOS")
    elif kind == "volume" and section["display_state"] in {"AVAILABLE", "STALE"}:
        impact = _ratio_impact(visual.get("value"), "RVOL")
        evidence_factor = _sample(section.get("n_observations"), 20, "sessões")
    else:
        reason = "sem magnitude comparável nesta família"
        impact, evidence_factor = {"value": None, "method": reason}, {"value": None, "method": reason}
    if section["display_state"] == "STALE":
        novelty = {"value": NOVELTY_STALE, "method": "vencido no corte"}
    else:
        novelty = {"value": NOVELTY_BASAL, "method": "basal", "label": "Sem comparação anterior"}
    return {"I": impact, "E": evidence_factor, "N": novelty}


def prioritize(data):
    """Escreve `priority` em cada faixa e reordena as visíveis; fotografia continua determinística."""
    visible = [s for s in data["sections"] if s["default_visible"]]
    hidden = [s for s in data["sections"] if not s["default_visible"]]
    by_group = {}
    for section in visible:
        by_group.setdefault(group_of(section), []).append(section)
    for group, members in by_group.items():
        for section in members:
            f = factors(section)
            raw = None if any(f[k]["value"] is None for k in "IEN") else f["I"]["value"] * f["E"]["value"] * f["N"]["value"]
            section["priority"] = {"policy": POLICY, "group": group, "group_label": GROUP_LABEL[group],
                                   "group_rank": GROUP_INDEX[group], "factors": f,
                                   "score_raw": raw, "score": None,
                                   "score_state": "UNAVAILABLE" if raw is None else "PENDING"}
        total = sum(s["priority"]["score_raw"] for s in members if s["priority"]["score_raw"] is not None)
        for section in members:
            p = section["priority"]
            if p["score_raw"] is None:
                p["reason"] = "Fator não apurado; ordem pelo desempate determinístico do grupo."
            elif total > 0:
                p.update(score=p["score_raw"] / total, score_state="AVAILABLE")
            else:
                p.update(score_state="ZERO_SUM", reason="Soma dos scores do grupo é zero; ordem por impacto, evidência e ID.")
        members.sort(key=lambda s: (
            s["priority"]["score"] is None,
            -(s["priority"]["score"] or 0),
            -(s["priority"]["factors"]["I"]["value"] or 0),
            -(s["priority"]["factors"]["E"]["value"] or 0),
            s["id"]))
    for section in hidden:
        section["priority"] = {"policy": POLICY, "group": None, "group_rank": None, "score": None,
                               "score_state": "NOT_RANKED", "reason": "Faixa oculta: fora da narrativa padrão."}
    ordered = [s for group, _ in GROUPS for s in by_group.get(group, [])]
    for position, section in enumerate(ordered, 1):
        section["priority"]["position"] = position
    data["sections"] = ordered + sorted(hidden, key=lambda s: s["id"])
    data["provenance"]["priority_policy"] = POLICY_DOC
    return data
