"""Campanha Ultra em ticks no Laboratório. Recibo e posição avançam na mesma transação."""
from decimal import Decimal
import json
from importlib import import_module

import pandas as pd
from sqlalchemy import inspect, text

from ..analytics import data_eligibility, datasets
from ..analytics.tick_fidelity import quote_masks
from ..composition import ultra as portfolio
from ..composition.registry import STRATEGY_PARAMETERS
from ..experiments import store
from ..experiments.schema import closed, digest, encoded, token
from ..strategies import get_strategy

VERSION = 1
SIGNALS = ("donchian", "s09_reference")
ACCOUNT_FIELDS = ("cash", "risk_limit", "margin", "fee", "fill_steps_per_tick")
MODULE_FIELDS = ("owner", "strategy", "params", "steps", "stop", "target", "breakeven",
                 "invalidate", "ttl_ms", "max_losses", "daily_goal")


def capabilities():
    return {"version": VERSION, "signals": list(SIGNALS), "timeframe": "M1", "session": "UTC_day",
            "account_mode": "modeled_hedging", "sizing": "integer_volume_steps",
            "entry": "marketable_limit", "management": ["fixed_stop_target", "breakeven"],
            "liquidity": "declared_shared_steps_per_quote", "native_status": "capability_absent",
            "economic_status": "unqualified", "money_unit": "hundredth_of_declared_currency"}


def _integer(value, minimum, label):
    if type(value) is not int or not minimum <= value <= 10**12:
        raise ValueError(f"{label} deve ser inteiro entre {minimum} e 10^12.")


def _units(value, unit):
    quotient = Decimal(str(value)) / Decimal(str(unit))
    if not quotient.is_finite() or quotient != quotient.to_integral_value():
        raise ValueError("Preço/valor incompatível com a grade de tick ou moeda.")
    return int(quotient)


def source_identity():
    from ..presets.execution import app_source_identity
    names = ("laboratory.ultra", "composition.ultra", "composition.registry", "analytics.datasets",
             "analytics.tick_fidelity", "analytics.data_eligibility", "strategies.base",
             "strategies.indicators", "strategies.donchian", "strategies.s09", "strategies.specs",
             "strategies.backtester", "analytics.instruments")
    modules = {f"mt5_extracao.{name}": import_module(f"mt5_extracao.{name}") for name in names}
    return app_source_identity(modules)["digest"]


def _policy(definition):
    closed(definition, ("version", "manifest_id", "sample_kind", "currency", "session",
                        "account_mode", "modules", *ACCOUNT_FIELDS), "Campanha Ultra")
    if type(definition["version"]) is not int or definition["version"] != VERSION:
        raise ValueError("Versão de campanha Ultra desconhecida.")
    if definition["session"] != "UTC_day" or definition["account_mode"] != "modeled_hedging":
        raise ValueError("Sessão/modo de conta sem capacidade de replay.")
    if definition["sample_kind"] not in ("synthetic", "market_history"):
        raise ValueError("Declare a origem sintética ou histórica dos dados.")
    token(definition["currency"], "moeda econômica")
    for key in ACCOUNT_FIELDS:
        _integer(definition[key], 0 if key == "fee" else 1, key)
    seen = set()
    if not isinstance(definition["modules"], list):
        raise ValueError("Módulos exigem lista em ordem de prioridade.")
    for module in definition["modules"]:
        closed(module, MODULE_FIELDS, "Módulo Ultra")
        owner = token(module["owner"], "dono")
        if owner in seen:
            raise ValueError("Dono duplicado na carteira.")
        seen.add(owner)
        key = module["strategy"]
        if key not in SIGNALS:
            raise ValueError(f"Execução Ultra ainda sem capacidade para {key}.")
        params = module["params"]
        closed(params, STRATEGY_PARAMETERS[key], "Parâmetros congelados")
        if not all(spec.accepts(params[name]) for name, spec in STRATEGY_PARAMETERS[key].items()):
            raise ValueError("Parâmetro da estratégia fora do catálogo.")
        get_strategy(key, **params)
        for name in MODULE_FIELDS[3:]:
            _integer(module[name], 0 if name in ("breakeven", "invalidate") else 1, name)
        if module["breakeven"] >= module["target"]:
            raise ValueError("A transição deve ocorrer antes do alvo.")
    signature = source_identity()
    return dict(definition, revision=digest([definition, signature]), engine_hash=signature)


def _qualify(snapshot, policy):
    manifest, spec = snapshot.manifest, snapshot.spec
    issues = []
    if manifest["timeframe"] != "M1" or manifest["time"].get("offset_state") != "declared":
        issues.append("m1_declared_clock_required")
    if not spec.known or spec.provenance.get("specification_status") != "known":
        issues.append("instrument_validity_or_sizing_unknown")
    if spec.currency_profit != policy["currency"]:
        issues.append("currency_conversion_unverified")
    required = ["ohlcv", "executable_quotes"]
    if policy["sample_kind"] == "market_history":
        derived = snapshot.derived_bars
        # Este executor ainda não reproduz correções tardias de um indicador recursivo.
        # Aceitar somente o caso cujo prefixo já estava disponível no fechamento.
        if derived.empty or not (
            pd.to_datetime(derived.computed_at_utc, utc=True) <= pd.to_datetime(derived.bar_end_utc, utc=True)
        ).all():
            issues.append("bar_availability_at_decision_unproven")
        required.extend(("observed_ticks", "observed_session_bars"))
    gate = data_eligibility.evaluate(snapshot.quality, required)
    issues.extend(gate["reasons"])
    if spec.known:
        for module in policy["modules"]:
            lot = Decimal(module["steps"]) * Decimal(str(spec.volume_step))
            if not Decimal(str(spec.volume_min)) <= lot <= Decimal(str(spec.volume_max)):
                issues.append("volume_outside_contract")
        try:
            value = _units(Decimal(str(spec.volume_step)) * Decimal(str(spec.tick_value)), "0.01")
            if value <= 0:
                issues.append("tick_value_unrepresentable")
        except ValueError:
            issues.append("tick_value_unrepresentable")
    return issues


def _inputs(snapshot, policy):
    spec, bars = snapshot.spec, snapshot.bars
    ticks = snapshot.market_frame("ticks").sort_values(["time_msc", "seq"], kind="stable")
    bid, ask, quoted, crossed = quote_masks(ticks)
    valid = quoted & ~crossed
    ticks = ticks.loc[valid, ["time_msc", "seq"]].copy()
    ticks["bid"] = [_units(v, spec.tick_size) for v in bid[valid]]
    ticks["ask"] = [_units(v, spec.tick_size) for v in ask[valid]]
    offset = pd.Timedelta(hours=snapshot.manifest["time"]["broker_utc_offset_hours"])
    ends = pd.to_datetime(bars.time) - offset + pd.Timedelta(minutes=1)
    starts = ticks.time_msc.to_numpy()
    scheduled = {}
    for module in policy["modules"]:
        strategy = get_strategy(module["strategy"], **module["params"])
        sig = strategy.gerar_sinais(bars)
        eligible = sig.entry_long | sig.entry_short
        for i in eligible.to_numpy().nonzero()[0]:
            available = int(ends.iloc[i].value // 1_000_000)
            index = int(starts.searchsorted(available))
            signals = scheduled.setdefault(index, {})
            if module["owner"] in signals:
                raise ValueError("Sinais no intervalo sem cotação exigem calendário/cobertura de ticks.")
            signals[module["owner"]] = {"side": 1 if sig.entry_long.iloc[i] else -1,
                                        "reference": _units(bars.close.iloc[i], spec.tick_size),
                                        "available_msc": available}
    return ticks.to_dict("records"), scheduled


def _tables(conn):
    conn.execute(text("CREATE TABLE IF NOT EXISTS _ultra_replays (receipt TEXT PRIMARY KEY, "
                      "policy TEXT NOT NULL, state TEXT NOT NULL, status TEXT NOT NULL, reasons TEXT NOT NULL)"))
    conn.execute(text("CREATE TABLE IF NOT EXISTS _ultra_frames (receipt TEXT NOT NULL, ordinal BIGINT NOT NULL, "
                      "events TEXT NOT NULL, mark TEXT NOT NULL, PRIMARY KEY(receipt, ordinal))"))


def freeze(db, definition):
    policy = _policy(definition)
    snapshot = datasets.load(db, policy["manifest_id"])
    issues = _qualify(snapshot, policy)
    status = "insufficient_data" if issues else "frozen" if policy["modules"] else "no_candidate"
    if not issues:
        spec = snapshot.spec
        policy["tick_cash"] = _units(Decimal(str(spec.volume_step)) * Decimal(str(spec.tick_value)), "0.01")
    with store.transaction(db) as conn:
        _tables(conn)
        conn.execute(text("INSERT INTO _ultra_replays VALUES(:r,:p,:s,:status,:why) ON CONFLICT DO NOTHING"),
                     dict(r=policy["revision"], p=encoded(policy), s=encoded(portfolio.initial_state(policy["cash"])),
                          status=status, why=encoded(issues)))
    return inspect_receipt(db, policy["revision"])


def _read(conn, receipt):
    row = conn.execute(text("SELECT * FROM _ultra_replays WHERE receipt=:r"), {"r": receipt}).mappings().first()
    if row is None:
        raise ValueError("Recibo Ultra desconhecido.")
    return json.loads(row["policy"]), json.loads(row["state"]), row["status"], json.loads(row["reasons"])


def inspect_receipt(db, receipt, *, timeline=False):
    with db.engine.connect() as conn:
        if not inspect(conn).has_table("_ultra_replays"):
            raise ValueError("Recibo Ultra desconhecido.")
        policy, state, status, reasons = _read(conn, receipt)
        result = {"version": VERSION, "receipt": receipt, "manifest_id": policy["manifest_id"],
                  "sample_kind": policy["sample_kind"], "status": status, "reasons": reasons,
                  "progress": state["cursor"], "account": state, "policy": policy,
                  "native_status": "capability_absent", "economic_status": "unqualified"}
        if timeline:
            rows = conn.execute(text("SELECT events, mark FROM _ultra_frames WHERE receipt=:r ORDER BY ordinal"),
                                {"r": receipt})
            result["timeline"] = [{"events": json.loads(events), "mark": json.loads(mark)} for events, mark in rows]
    return result


def replay(db, receipt, *, until=None):
    """O limite é ordinal absoluto. Nova chamada/processo respeita o cursor já confirmado."""
    if until is not None:
        _integer(until, 0, "until")
    current = inspect_receipt(db, receipt)
    if current["status"] not in ("frozen", "running"):
        return current
    policy = current["policy"]
    if policy.get("engine_hash") != source_identity():
        raise ValueError("Motor de replay divergente. Retome com a revisão de origem do recibo.")
    try:
        ticks, signals = _inputs(datasets.load(db, policy["manifest_id"]), policy)
    except ValueError as error:
        with store.transaction(db) as conn:
            conn.execute(text("UPDATE _ultra_replays SET status='insufficient_data', reasons=:why "
                              "WHERE receipt=:r"), {"r": receipt, "why": encoded([str(error)])})
        return inspect_receipt(db, receipt)
    end = min(until, len(ticks)) if until is not None else len(ticks)
    while True:
        with store.transaction(db) as conn:
            _, state, status, _ = _read(conn, receipt)
            if state["cursor"] >= end or status == "complete":
                break
            batch_end = min(end, state["cursor"] + 256)
            batch = []
            for i in range(state["cursor"], batch_end):
                events, mark = portfolio.advance(state, policy, ticks[i], signals.get(i, {}))
                batch.append(dict(r=receipt, i=i, events=encoded(events), mark=encoded(mark)))
            conn.execute(text("INSERT INTO _ultra_frames VALUES(:r,:i,:events,:mark)"), batch)
            if batch_end == len(ticks):
                state["unexecuted_signals"] = signals.get(len(ticks), {})
                status = "complete"
            else:
                status = "running"
            conn.execute(text("UPDATE _ultra_replays SET state=:s, status=:status WHERE receipt=:r"),
                         dict(s=encoded(state), status=status, r=receipt))
    return inspect_receipt(db, receipt)
