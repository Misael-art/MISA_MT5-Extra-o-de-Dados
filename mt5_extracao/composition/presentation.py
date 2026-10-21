"""Descrição da seleção persistida, sem estimar lotes, risco ou preços de execução."""
from .schema import Composition


def _number(value):
    return f"{value:g}".replace(".", ",")


def recorded_summary(run):
    metrics = run.get("metrics") or {}
    if "composition" not in metrics:
        return []
    try:
        composition = Composition(metrics["composition"])
    except (TypeError, ValueError):
        return ["Composição registrada indisponível: contrato desconhecido ou inválido."]
    value = composition.to_dict()
    sizing = value["sizing"]["params"]
    capital = "saldo atualizado" if sizing["compound"] else "capital inicial"
    budget = f"risco {_number(sizing['risk_fraction'] * 100)}% sobre {capital}"
    stop = value["protection"]
    stop_text = (f"stop {_number(stop['params']['stop_points'])} pontos"
                 if stop["mode"] == "fixed_points" else "stop da estratégia")
    target = value["profit_exit"]
    if target["mode"] == "fixed_points":
        target_text = f"alvo {_number(target['params']['target_points'])} pontos"
    elif target["mode"] == "fixed_r":
        target_text = f"alvo {_number(target['params']['target_r'])} R"
    else:
        target_text = "alvo da estratégia"
    policy = value["conditional_policy"]
    if policy["mode"] != "disabled":
        labels = {"max_seconds": "prazo", "progress_seconds": "progresso", "gain_seconds": "ganho",
                  "loss_seconds": "perda", "tie_seconds": "empate"}
        deadlines = [f"{label} {policy['params'][key]} s" for key, label in labels.items()
                     if policy["params"].get(key)]
        timing = "saída temporal adicional" + (": " + ", ".join(deadlines) if deadlines else "")
    else:
        timing = "tempo da estratégia"
    filters = []
    for item in value["filters"]:
        if item["mode"] == "session_v1":
            quota = item["params"]["max_cycles"]
            noun = "entrada" if quota == 1 else "entradas"
            filters.append(f"sessão/cota de {quota} {noun}" if quota else "sessão")
        elif item["mode"] == "cooldown_v1":
            filters.append(f"espera de {item['params']['seconds']} s")
        else:
            filters.append("filtro registrado")
    return [f"Composição registrada {composition.config_hash[:8]} · {budget}.",
            "; ".join([stop_text, target_text, timing, *filters]) + "."]
