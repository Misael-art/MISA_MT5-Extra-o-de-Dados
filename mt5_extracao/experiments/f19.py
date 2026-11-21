"""Pré-registro dos braços de pesquisa F19 no journal canônico de experimentos."""
import math

import pandas as pd

from ..composition import Composition
from ..trade_analytics.trajectories.clocks import bar_end_in_source
from .schema import ROLES, closed


OBJECTIVE = "f19_paired_session_net"
COST_FIELDS = ("commission_per_lot", "slippage_points")
ARMS = ("reference", "candidate")
SCENARIOS = ("base", "stress")
FOCUS = {"signal": {"strategy"}, "filter": {"filters"},
         "exit": {"protection", "profit_exit", "partial_exit_plan", "conditional_policy"}}


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def validate(definition, context):
    objective = definition["objective"]
    closed(objective, ("id", "version", "focus", "horizon_seconds", "embargo_seconds", "costs",
                       "initial_equity", "block_sessions", "bootstrap_replicates"), "Plano pareado F19")
    if objective["version"] != 1 or not isinstance(objective["focus"], str) or objective["focus"] not in FOCUS:
        raise ValueError("Plano F19 exige versão 1 e foco signal, filter ou exit.")
    if not _finite(objective["initial_equity"]) or objective["initial_equity"] <= 0:
        raise ValueError("Informe capital inicial positivo e finito, comum aos dois braços.")
    limits = {"horizon_seconds": (1, 31_536_000), "embargo_seconds": (1, 31_536_000),
              "block_sessions": (1, 250), "bootstrap_replicates": (1, 10_000)}
    for name, (minimum, maximum) in limits.items():
        if type(objective[name]) is not int or not minimum <= objective[name] <= maximum:
            raise ValueError(f"{name} precisa ser inteiro entre {minimum} e {maximum}.")
    if objective["embargo_seconds"] < objective["horizon_seconds"]:
        raise ValueError("O embargo precisa cobrir o horizonte declarado.")
    costs = objective["costs"]
    closed(costs, SCENARIOS, "Cenários de custo")
    for scenario in costs.values():
        closed(scenario, COST_FIELDS, "Custos do cenário")
        if any(not _finite(value) or value < 0 for value in scenario.values()):
            raise ValueError("Os custos do cenário precisam ser finitos e não negativos.")
    if costs["base"] == costs["stress"] or any(costs["stress"][key] < costs["base"][key] for key in COST_FIELDS):
        raise ValueError("O cenário adverso precisa aumentar ao menos um custo, preservando os demais pisos.")
    criteria = definition["criteria"]
    closed(criteria, ("id", "version", "minimum_net_cash_difference", "confidence"), "Critério pareado F19")
    if (criteria["version"] != 1 or not _finite(criteria["minimum_net_cash_difference"]) or
            not _finite(criteria["confidence"]) or not 0 < criteria["confidence"] < 1):
        raise ValueError("Declare limiar monetário finito e confiança entre zero e um.")

    grid = {}
    for arm in definition["grid"]:
        closed(arm, ("arm", "composition"), "Braço do plano F19")
        if arm["arm"] not in ARMS or arm["arm"] in grid:
            raise ValueError("Declare referência e candidato uma única vez.")
        grid[arm["arm"]] = Composition(arm["composition"]).to_dict()
    if set(grid) != set(ARMS):
        raise ValueError("O protocolo exige referência e candidato congelados.")
    identity = context["identity"]
    if any(arm["context"] != {"symbol": identity["symbol"], "timeframe": identity["timeframe"]} for arm in grid.values()):
        raise ValueError("Os braços precisam usar o ativo e timeframe dos snapshots.")
    reference, candidate = (grid[key] for key in ARMS)
    changed = {key for key in reference if reference[key] != candidate[key]}
    if not changed or not changed <= FOCUS[objective["focus"]]:
        raise ValueError("Os braços precisam isolar a mudança declarada mantendo o mesmo dimensionamento.")

    roles = context["roles"]
    if set(roles) != set(ROLES) or identity["time"].get("offset_state") != "declared":
        raise ValueError("Declare treino, validação e reserva com relógio congelado.")
    for left, right in zip(ROLES, ROLES[1:]):
        close = bar_end_in_source(roles[left]["last"], identity["timeframe"])
        if pd.Timestamp(roles[right]["first"]) - close < pd.Timedelta(seconds=objective["embargo_seconds"]):
            raise ValueError(f"A janela {right} começa antes do embargo de {left}.")


def classify_request(definition, key, payload):
    arm = next((item["arm"] for item in definition["grid"]
                if payload["composition"] == Composition(item["composition"]).to_dict()), None)
    settings = payload["settings"]
    if settings["initial_equity"] != definition["objective"]["initial_equity"]:
        raise ValueError("Capital inicial do pedido difere do protocolo congelado.")
    costs = {field: settings[field] for field in COST_FIELDS}
    scenario = next((name for name, values in definition["objective"]["costs"].items() if costs == values), None)
    if arm is None or scenario is None or key != f"{payload['role']}.{arm}.{scenario}":
        raise ValueError("Use a composição, custos e chave congelados: papel.braço.cenário.")
    if payload["role"] == "holdout" and payload["purpose"] != "evaluation":
        raise ValueError("A reserva do F19 só admite avaliação dos braços congelados.")
    return {"arm": arm, "scenario": scenario}
