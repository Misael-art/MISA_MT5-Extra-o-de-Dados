"""Campanha de treino nativa e seleção reproduzível para uma política portátil."""
from pathlib import Path

from ..experiments.schema import digest
from . import ultra_market, ultra_market_preset
from .ultra_catalog import MARKET_NATIVE, MARKET_PATTERN


_PLAN = {"candidates", "max_attempts", "max_modules", "min_cycles", "min_net_cents"}
_TRIAL = {"id", "strategy", "parameters", "pattern", "owner_rules"}


def _prepare(config, manifest, plan):
    if not isinstance(plan, dict) or set(plan) != _PLAN:
        raise ValueError("plan: declare candidates, max_attempts, max_modules, min_cycles e min_net_cents.")
    for field, lower, upper in (("max_attempts", 1, 64), ("max_modules", 1, 13),
                                ("min_cycles", 1, 2147483647), ("min_net_cents", -100000, 2147483647)):
        if type(plan[field]) is not int or not lower <= plan[field] <= upper:
            raise ValueError(f"plan.{field}: esperado inteiro entre {lower} e {upper}.")
    trials = plan["candidates"]
    if not isinstance(trials, list) or not 1 <= len(trials) <= plan["max_attempts"]:
        raise ValueError("plan.candidates: lista vazia ou acima do orçamento de tentativas.")
    seen, prepared = set(), []
    for index, trial in enumerate(trials):
        if not isinstance(trial, dict) or set(trial) - _TRIAL or not {"id", "strategy"} <= trial.keys():
            raise ValueError(f"plan.candidates[{index}]: declare id, strategy e opções suportadas.")
        ident, key = trial["id"], trial["strategy"]
        if not isinstance(ident, str) or not ident.strip() or ident in seen or len(ident) > 100:
            raise ValueError(f"plan.candidates[{index}].id: identificador vazio, longo ou repetido.")
        if not isinstance(key, str) or key not in (*MARKET_NATIVE, MARKET_PATTERN):
            raise ValueError(f"plan.candidates[{index}].strategy: módulo fora do catálogo de mercado.")
        seen.add(ident)
        params = {key: trial["parameters"]} if "parameters" in trial else {}
        rules = {key: trial["owner_rules"]} if "owner_rules" in trial else {}
        prepared.append(ultra_market_preset.portable(config, manifest, strategies=[key], pattern=trial.get("pattern"),
                                                     parameters=params, owner_rules=rules))
    if len({digest(p["profile"]["modules"][0]) for p in prepared}) != len(prepared):
        raise ValueError("plan.candidates: há alternativas idênticas no treino.")
    return prepared


def select(plan, results):
    """O plano antecede a fita de avaliação. O ranking usa apenas os ensaios do treino."""
    eligible = sorted((item for item in results if item["cycles"] >= plan["min_cycles"]
                       and item["net_cents"] >= plan["min_net_cents"]),
                      key=lambda item: (-item["net_cents"], item["index"]))
    chosen = {}
    for item in eligible:
        if len(chosen) == plan["max_modules"]:
            break
        chosen.setdefault(item["strategy"], item["id"])
    for item in results:
        item["decision"] = ("too_few_cycles" if item["cycles"] < plan["min_cycles"] else
                            "below_net_threshold" if item["net_cents"] < plan["min_net_cents"] else
                            "selected" if chosen.get(item["strategy"]) == item["id"] else
                            "another_candidate_selected" if item["strategy"] in chosen else "module_limit")
    return list(chosen.values())


def module_decisions(results):
    by_module = {key: [] for key in (*MARKET_NATIVE, MARKET_PATTERN)}
    for item in results:
        by_module[item["strategy"]].append({"candidate": item["id"], "decision": item["decision"]})
    return {key: {"decision": ("selected" if any(row["decision"] == "selected" for row in candidates)
                               else "excluded" if candidates else "not_in_plan"), "candidates": candidates}
            for key, candidates in by_module.items()}


def run(config, training, plan, output, *, timeout=300, progress=None):
    from ..robots.ultra_market_runner import run as run_tester

    manifest = ultra_market.verify(training)
    candidates = _prepare(config, manifest, plan)
    root = Path(output).resolve()
    root.mkdir(parents=True, mode=0o700)
    preflight = dict(plan=plan, criterion="net_cents_desc_then_preregistered_order_one_per_module",
                     training_manifest_sha256=manifest["manifest_hash"], training_clock=manifest["time"],
                     observed_contract=manifest["contract"], economic_approval=False)
    ultra_market_preset.write(root / "plan.json", preflight)
    results, last = [], "plan_frozen"

    def save(status, **fields):
        state = dict(status=status, last_completed=last, candidates=len(candidates), completed=len(results),
                     results=results, economic_approval=False, **fields)
        stage = root / "progress.tmp"
        ultra_market_preset.write(stage, state)
        stage.replace(root / "progress.json")
        if progress:
            progress(status, root / "progress.json")
        return state

    for i, (trial, candidate) in enumerate(zip(plan["candidates"], candidates, strict=True)):
        def update(status, receipt_path):
            save("training", candidate=trial["id"], tester_stage=status, tester_receipt=str(receipt_path))

        save("training", candidate=trial["id"])
        receipt = run_tester(config, training, root / "training", mode="portfolio", timeout=timeout,
                             preset=candidate, progress=update)
        if receipt["status"] not in ("single_owner_matches", "portfolio_without_overlap"):
            return save("training_incomplete", candidate=trial["id"], tester_receipt=receipt.get("receipt_path"),
                        reason=receipt.get("reason") or receipt["status"])
        comparison = receipt["comparison"]
        results.append(dict(id=trial["id"], strategy=trial["strategy"], index=i,
                            net_cents=comparison["final"]["cash"] - 100000,
                            cycles=len({d["position"] for d in comparison["deals"] if d["entry"] == "DEAL_ENTRY_OUT"}),
                            policy_sha256=candidate["preset_sha256"], inputs=candidate["profile"]["modules"][0]["inputs"],
                            receipt=receipt["receipt_path"], journal_sha256=receipt["journal_sha256"]))
        last = trial["id"]

    chosen = select(plan, results)
    selection = dict(**preflight, status="selected" if chosen else "no_candidate",
                     results=results, chosen=chosen)
    selection_sha = digest(selection)
    policy_path = None
    if chosen:
        selected = [trial for ident in chosen for trial in plan["candidates"] if trial["id"] == ident]
        keys = [trial["strategy"] for trial in selected]
        policy = ultra_market_preset.portable(
            config, manifest, strategies=keys, pattern=next((trial["pattern"] for trial in selected if "pattern" in trial), None),
            parameters={trial["strategy"]: trial["parameters"] for trial in selected if "parameters" in trial},
            owner_rules={trial["strategy"]: trial["owner_rules"] for trial in selected if "owner_rules" in trial},
            research=dict(selection_sha256=selection_sha, training_manifest_sha256=manifest["manifest_hash"],
                          training_end_msc=manifest["time"]["end_msc"]))
        policy_path = root / "policy.json"
        ultra_market_preset.write(policy_path, policy)
        selection["policy_sha256"] = policy["preset_sha256"]
    ultra_market_preset.write(root / "selection.json", dict(selection, selection_sha256=selection_sha,
                                                           modules=module_decisions(results)))
    last = "selection_frozen"
    return save(selection["status"], chosen=chosen, selection_path=str(root / "selection.json"),
                policy_path=str(policy_path) if policy_path else None)
