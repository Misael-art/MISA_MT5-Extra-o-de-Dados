"""
Operação por agente (sem interface): o retrato do projeto e as próximas ações sugeridas.

    estado = status(config, db)     # dicionário pronto para JSON; é o que 'mt5x status' mostra

As ações são sempre comandos do 'mt5x'. O que depende de uma pessoa vem marcado com requires_human.
Nada aqui envia ordens nem fala com o MT5: é leitura do banco e das pastas de robôs.
"""
from typing import Dict, List

from mt5_extracao import study, timeframes


def _tf_name(label: str) -> str:
    tf = timeframes.parse(label)
    return tf.name if tf else label


def base_state(db) -> dict:
    """Séries de barras na base e quais estão desatualizadas."""
    inventory = [item for item in db.data_inventory() if item["rows"]]
    series, stale = [], []
    for item in inventory:
        tf = timeframes.parse(item["timeframe"])
        fresh = study.freshness(item["end"], tf.minutes if tf else 1)
        row = {"symbol": item["symbol"], "timeframe": _tf_name(item["timeframe"]), "bars": int(item["rows"]),
               "start": item["start"], "end": item["end"], "freshness": fresh}
        series.append(row)
        if fresh != "em dia":
            stale.append(row)
    return {"symbols": len({s["symbol"] for s in series}), "series": len(series),
            "bars": sum(s["bars"] for s in series), "items": series, "stale": stale}


def status(config, db) -> dict:
    from mt5_extracao import robots as robots_core
    from mt5_extracao.strategies import REGISTRY, specs
    from mt5_extracao.strategies.validation import load_validations
    base = base_state(db)
    catalog = robots_core.load_catalog(config, db)
    by_stage: Dict[str, int] = {}
    for robot in catalog:
        by_stage[robot.stage] = by_stage.get(robot.stage, 0) + 1
    validations = load_validations(db)
    approved = validations[validations["approved"] == 1] if len(validations) else validations
    symbols = sorted({s["symbol"] for s in base["items"]})
    without_spec = [s for s in symbols if not specs.load_spec(db, s).known]
    tested = {(r.symbol, r.timeframe, r.strategy) for r in validations.itertuples()} if len(validations) else set()
    untested: Dict[str, List[str]] = {}
    for s in base["items"]:
        if any((s["symbol"], s["timeframe"], key) not in tested for key in REGISTRY):
            untested.setdefault(s["timeframe"], []).append(s["symbol"])

    actions = []

    def add(action_id, why, command, requires_human=False, needs_mt5=False):
        actions.append({"priority": len(actions) + 1, "id": action_id, "why": why, "command": command,
                        "requires_human": requires_human, "needs_mt5": needs_mt5})

    if not base["items"]:
        add("extrair", "A base está vazia: sem barras não há estudo nem teste.",
            "mt5x extract --symbols <ativos> --tf D1 --from 2018-01-01", needs_mt5=True)
    stale_by_tf: Dict[str, List[str]] = {}
    for s in base["stale"]:
        stale_by_tf.setdefault(s["timeframe"], []).append(s["symbol"])
    for tf, names in sorted(stale_by_tf.items()):
        add("atualizar", f"{len(names)} série(s) {tf} desatualizada(s): testes usariam dados velhos.",
            f"mt5x update --symbols {','.join(sorted(names))} --tf {tf}", needs_mt5=True)
    if without_spec:
        add("especificacoes", f"{len(without_spec)} ativo(s) sem especificação de contrato: lotes e valores em "
                              "dinheiro ficam aproximados.",
            f"mt5x specs --symbols {','.join(without_spec)}", needs_mt5=True)
    for tf, names in sorted(untested.items()):
        add("validar", f"{len(set(names))} ativo(s) {tf} com estratégias ainda não validadas.",
            f"mt5x validate --symbols {','.join(sorted(set(names)))} --tf {tf} --strategies all")
    uncompiled = [r for r in catalog if r.kind == robots_core.MQL5 and r.stage == "importado"]
    if uncompiled:
        add("compilar", "Robô(s) MQL5 ainda não compilado(s): " + ", ".join(r.name for r in uncompiled) + ".",
            "mt5x robots compile " + uncompiled[0].key.split(":", 1)[1], requires_human=True)
    add("auditar", "Auditoria de segurança (projeto, patrimônio e capital): rode a cada ciclo e antes de operar.",
        "mt5x audit")
    return {
        "base": base,
        "robots": {"total": len(catalog), "mql5": sum(r.kind == robots_core.MQL5 for r in catalog),
                   "python": sum(r.kind == robots_core.PYTHON for r in catalog), "by_stage": by_stage,
                   "high_risk": [r.key for r in catalog if r.risk.level == "Alto"]},
        "validations": {"total": int(len(validations)), "approved": int(len(approved)),
                        "approved_items": [{"symbol": r.symbol, "timeframe": r.timeframe, "strategy": r.strategy,
                                            "profit_factor": float(r.profit_factor_oos), "trades": int(r.trades_oos),
                                            "created_at": r.created_at} for r in approved.itertuples()]},
        "specs_missing": without_spec,
        "next_actions": actions,
    }
