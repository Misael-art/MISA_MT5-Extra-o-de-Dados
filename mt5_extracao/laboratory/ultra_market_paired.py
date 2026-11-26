"""Treino por ablação em passes independentes do mesmo Tester e do mesmo caixa."""
from collections import Counter
from datetime import date, datetime, timezone
from html import unescape
import json
from pathlib import Path
import re
from uuid import uuid4

from .. import profit_factor
from ..analytics import pattern_families
from ..experiments.schema import digest
from ..robots import mt5_tools
from ..robots.backtest import _CELL, _ROW, _TAG
from . import ultra_market, ultra_market_preset as preset
from .ultra_catalog import MARKET_NATIVE, MARKET_PATTERN

SCHEMA = "ultra-market-paired/1"
_SUCCESS = {"portfolio_matches", "single_owner_matches", "portfolio_without_overlap"}
_FIELDS = {"schema", "symbol", "family", "parameters", "invalidation", "stop_points", "target_points",
           "companion", "max_attempts", "min_cycles", "min_net_cents", "max_equity_drawdown_pct",
           "development", "economic_windows"}


def _window(value):
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("Esperado um intervalo de datas com fim exclusivo.")
    start, end = (date.fromisoformat(v) for v in value)
    if start >= end:
        raise ValueError("Intervalo de datas vazio.")
    return start, end


def _check_training_window(manifest, plan):
    windows = plan["economic_windows"]
    if not isinstance(windows, dict) or set(windows) != {"training", "validation", "reserve"}:
        raise ValueError("Informe os três cortes econômicos.")
    train, validation, reserve = (_window(windows[k]) for k in ("training", "validation", "reserve"))
    dev = _window(plan["development"])
    if train[1] > validation[0] or validation[1] > reserve[0] or not (dev[1] <= train[0] or dev[0] >= reserve[1]):
        raise ValueError("Cortes econômicos ou de desenvolvimento sobrepostos.")
    observed = tuple(date.fromisoformat(manifest["time"][k][:10]) for k in ("start_inclusive", "end_exclusive"))
    if observed not in (dev, train):
        raise ValueError("A pesquisa só abre o treino ou a fita de desenvolvimento registrada.")


def arms(config, manifest, plan):
    if (set(plan) != _FIELDS or plan["symbol"] != manifest["symbol"] or plan["family"] != "reversal"
            or plan["invalidation"] != "close_at_low" or plan["companion"] not in MARKET_NATIVE
            or plan["schema"] != SCHEMA):
        raise ValueError("Perfil, família, controle ou esquema de ablação incompatível.")
    for key, lo, hi in (("max_attempts", 5, 64), ("min_cycles", 1, 2**31-1), ("min_net_cents", 1, 2**31-1),
                        ("max_equity_drawdown_pct", 1, 100)):
        if type(plan[key]) is not int or not lo <= plan[key] <= hi:
            raise ValueError(f"plan.{key}: limite inválido.")
    family = pattern_families.compile_family(plan["family"], plan["parameters"], symbol=manifest["symbol"],
        timeframe="M1", clock_version=manifest["time"]["basis"], calendar_version="broker_bars",
        cost_model_id="tester_observed_bid_ask", horizon_bars=1)
    body = family.body
    body["invalidation_rules"] = [dict(id="close_at_low", action="cancel_before_entry", when={
        "op": "compare", "comparison": "lte", "left": {"op": "field", "name": "close"},
        "right": {"op": "field", "name": "low"}})]
    companion = [plan["companion"]]
    policies = {"companion": preset.portable(config, manifest, strategies=companion)}
    for conditional, solo, joint in ((False, "signal", "static"), (True, "filtered_signal", "conditional")):
        pattern = dict(definition=body, apply_invalidation=conditional,
                       stop_points=plan["stop_points"], target_points=plan["target_points"])
        for name, keys in ((solo, [MARKET_PATTERN]), (joint, companion + [MARKET_PATTERN])):
            policies[name] = preset.portable(config, manifest, strategies=keys, pattern=pattern)
    return policies


def drawdown(html):
    for row in _ROW.findall(html):
        cells = [unescape(_TAG.sub("", c)).strip() for c in _CELL.findall(row)]
        for label, value in zip(cells, cells[1:]):
            if label == "Equity Drawdown Relative:":
                match = re.fullmatch(r"([\d.,]+)%\s*\(([\d .,]+)\)", value)
                if match:
                    pct = float(match[1].replace(",", "."))
                    if 0 <= pct <= 100:
                        return pct
    raise ValueError("Drawdown relativo de patrimônio ausente no relatório do Tester.")


def metrics(receipt):
    root = Path(receipt["receipt_path"]).parent
    report = root / "report.htm"
    if ultra_market._sha(report) != receipt["report_sha256"]:
        raise ValueError("Hash do relatório de patrimônio divergente.")
    comparison = receipt["comparison"]
    pnl, days, entries = Counter(), Counter(), []
    traded_steps = 0
    for d in comparison["deals"]:
        value = sum(d[k] for k in ("profit", "commission", "swap", "fee"))
        traded_steps += d["steps"]
        days[datetime.fromtimestamp(d["msc"] // 1000, timezone.utc).date().isoformat()] += value
        pnl[d["position"]] += value
        if d["entry"] == "DEAL_ENTRY_IN":
            entries.append([d["owner"], d["msc"], d["type"]])
    journal = root / receipt["journal"]
    if ultra_market._sha(journal) != receipt["journal_sha256"]:
        raise ValueError("Hash do journal de vetos divergente.")
    refusals = []
    for line in mt5_tools.read_log_text(journal).splitlines():
        if "MT5X_ULTRA_PORTFOLIO|ENTRY|" in line:
            fields = line.partition("MT5X_ULTRA_PORTFOLIO|ENTRY|")[2].split("|", 9)
            if fields[3] not in ("4", "5"):
                refusals.append([int(fields[0]), int(fields[1]), "DEAL_TYPE_BUY" if fields[2] == "0" else "DEAL_TYPE_SELL", fields[-1]])
    contract = receipt["execution_market"]["specifications"]["current"]
    if contract["currency_profit"] != receipt["execution_market"]["account"]["currency"]:
        raise ValueError("A sensibilidade exige conversão histórica para a moeda da conta.")
    cost_per_step = (float(contract["trade_contract_size"]) * float(contract["trade_tick_size"])
                     * float(contract["volume_step"]) * 100)
    sensitivity = {str(ticks): sum(pnl.values()) - ticks * traded_steps * cost_per_step for ticks in (1, 2)}
    groups = {}
    parity = comparison.get("pattern_parity") or {}
    for o in parity.get("occurrences", []):
        day = datetime.fromtimestamp(o["tick_msc"] // 1000, timezone.utc).date().isoformat()
        values = [r["value"] for r in o["rules"]]
        verdict = "invalidated" if True in values else "unknown" if None in values else "confirmed"
        group = groups.setdefault(day, {}).setdefault(verdict, dict(count=0, censored=0, pnl_ticks=[]))
        group["count"] += 1
        if o["next_minute"] is None:
            group["censored"] += 1
        else:
            group["pnl_ticks"].append(o["next_minute"]["spread_included_ticks"])
    return dict(net_cents=sum(pnl.values()), cycles=len(pnl), closed_cycle_cents=list(pnl.values()),
                one_minute_labels_by_session=groups,
                daily_net_cents=dict(days), extra_ticks_per_side_net_cents=sensitivity, equity_drawdown_pct=drawdown(mt5_tools.read_log_text(report)),
                simultaneous=comparison["peak"]["chronological"], **profit_factor.fields(pnl.values()),
                entries=entries, refusals=refusals, receipt=receipt["receipt_path"],
                policy_sha256=receipt["preset_sha256"], journal_sha256=receipt["journal_sha256"])


def marginal(full, reduced):
    added = Counter(map(tuple, full["entries"]))
    displaced = Counter(map(tuple, reduced["entries"])) - added
    reasons = {tuple(row[:3]): row[3] for row in full["refusals"]}
    missed = [dict(magic=k[0], tick_msc=k[1], direction=k[2], count=n, reason=reasons.get(k))
              for k, n in displaced.items()]
    capital = re.compile(r"orçamento (?:global excedido|do robô .+ excedido)|margem necessária .+ excede disponível", re.I)
    loss_days = {day: full.get("daily_net_cents", {}).get(day, 0) - value
                 for day,value in reduced.get("daily_net_cents", {}).items() if value < 0}
    return dict(net_cents=full["net_cents"]-reduced["net_cents"], marginal_cents_on_losing_days=loss_days,
                equity_drawdown_pct=full["equity_drawdown_pct"]-reduced["equity_drawdown_pct"],
                profit_factor=(None if full["profit_factor"] is None or reduced["profit_factor"] is None else
                               full["profit_factor"]-reduced["profit_factor"]),
                simultaneous=full["simultaneous"]-reduced["simultaneous"], displaced=missed,
                capital_vetoes=sum(r["count"] for r in missed if r["reason"] and capital.search(r["reason"])))


def choose(results, plan):
    eligible, reasons = [], {}
    for name, metrics_ in results.items():
        failures = []
        if metrics_["cycles"] < plan["min_cycles"]:
            failures.append("insufficient_cycles")
        if metrics_["net_cents"] < plan["min_net_cents"]:
            failures.append("below_net_threshold")
        if metrics_["equity_drawdown_pct"] > plan["max_equity_drawdown_pct"]:
            failures.append("drawdown_limit")
        references = {"conditional": ("static", "companion", "filtered_signal"),
                      "static": ("companion", "signal"), "filtered_signal": ("signal",)}.get(name, ())
        failures.extend("no_increment_over_" + ref for ref in references
                        if metrics_["net_cents"] <= results[ref]["net_cents"])
        reasons[name] = failures
        if not failures:
            eligible.append(name)
    return max(eligible, key=lambda name: results[name]["net_cents"], default="cash"), reasons


def _save(path, value):
    stage = path.with_name(uuid4().hex + ".tmp")
    preset.write(stage, value)
    stage.replace(path)


def run(config, training, plan, output, *, timeout, progress):
    from ..robots.ultra_market_runner import run as tester
    # Rejeite reserva e validação pelo cabeçalho, antes de abrir qualquer bloco de cotações.
    envelope = json.loads((Path(training) / "manifest.json").read_text(encoding="utf-8"))
    _check_training_window(envelope, plan)
    manifest = ultra_market.verify(training)
    policies = arms(config, manifest, plan)
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    frozen = dict(plan=plan, training_manifest_sha256=manifest["manifest_hash"], training_end_msc=manifest["time"]["end_msc"],
                  policies={k: p["preset_sha256"] for k, p in policies.items()})
    with mt5_tools.tester_lease():
        target = root / "plan.json"
        if target.exists():
            if json.loads(target.read_text(encoding="utf-8")) != frozen:
                raise ValueError("Plano, fita ou fonte mudou desde a tentativa anterior.")
        else:
            preset.write(target, frozen)
        checkpoint = root / "progress.json"
        state = (json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else
                 dict(attempts=[], results={}, economic_approval=False, last_completed="plan_frozen"))
        def publish(status, **updates):
            state.update(status=status, **updates)
            _save(checkpoint, state)
            if progress:
                progress(status, checkpoint)
            return state
        for name, policy in policies.items():
            if name in state["results"]:
                r = json.loads(Path(state["results"][name]["receipt"]).read_text(encoding="utf-8"))
                if (r.get("manifest_hash") != manifest["manifest_hash"] or r.get("preset_sha256") != policy["preset_sha256"]
                        or r["status"] not in _SUCCESS or metrics(r) != state["results"][name]):
                    raise ValueError("Recibo do braço concluído diverge do checkpoint.")
                continue
            if len(state["attempts"]) >= plan["max_attempts"]:
                return publish("budget_exhausted")
            if state["attempts"] and state["attempts"][-1]["arm"] == name:
                state["attempts"][-1].setdefault("outcome", "interrupted")
            attempt = dict(arm=name, status="started")
            state["attempts"].append(attempt)
            publish("training", candidate=name)
            def update(status, receipt_path):
                attempt.update(status=status, receipt=str(receipt_path))
                publish("training")
            receipt = tester(config, training, root / name, mode="portfolio", preset=policy, timeout=timeout, progress=update)
            if receipt["status"] not in _SUCCESS:
                attempt["outcome"] = "failed"
                return publish("training_incomplete", reason=receipt.get("reason") or receipt["status"])
            attempt["outcome"] = "completed"
            state["results"][name] = metrics(receipt)
            publish("training", last_completed=name)
        results = state["results"]
        chosen, reasons = choose(results, plan)
        cash = dict(net_cents=0, cycles=0, equity_drawdown_pct=0, profit_factor=None, simultaneous=0, entries=[], refusals=[])
        pairs = {"add_filtered_signal": ("conditional", "companion"), "add_companion": ("conditional", "filtered_signal"),
                 "apply_filter": ("conditional", "static"), "filter_alone": ("filtered_signal", "signal"),
                 "static_add_signal": ("static", "companion"), "static_add_companion": ("static", "signal")}
        selection = dict(**frozen, results=results, cash=cash, chosen=chosen, reasons=reasons, economic_approval=False,
                         best_individual=max((k for k in ("companion", "signal", "filtered_signal") if not reasons[k]),
                                             key=lambda k: results[k]["net_cents"], default="cash"),
                         marginal={k: marginal(results[a], results[b]) for k,(a,b) in pairs.items()})
        policy_path = None
        if chosen != "cash":
            selected = policies[chosen]
            modules = selected["profile"]["modules"]
            selected = preset.portable(config, manifest, strategies=[m["key"] for m in modules],
                pattern=next((m["pattern"] for m in modules if "pattern" in m), None),
                research=dict(selection_sha256=digest(selection), training_manifest_sha256=manifest["manifest_hash"],
                              training_end_msc=manifest["time"]["end_msc"]))
            policy_path = root / "policy.json"
            _save(policy_path, selected)
            selection["policy_sha256"] = selected["preset_sha256"]
        _save(root / "selection.json", selection)
        return publish("no_candidate" if chosen == "cash" else "selected", last_completed="selection_frozen",
                       chosen=chosen, reason=None, policy_path=str(policy_path) if policy_path else None,
                       selection_path=str(root/"selection.json"))
