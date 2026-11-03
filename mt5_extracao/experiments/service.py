"""Uma avaliação explícita, com orçamento existente e journal antes do resultado."""
import configparser
from dataclasses import asdict, replace
import json
import math
import uuid

import pandas as pd
from sqlalchemy import text

from ..analytics import datasets
from ..composition import BaseAdapter, Composition, compatibility
from ..composition.registry import BASE_MODES
from ..robots import backtest, budget as budgets, catalog, store as runs
from ..strategies import get_strategy, research
from .schema import PatternCampaignRevision, ProtocolRevision, ROLES, digest, token, encoded
from . import store


def create(db, definition):
    revision = ProtocolRevision(definition)
    context, previous = {"roles": {}}, None
    for name in ROLES:
        role = revision.to_dict()["roles"].get(name)
        if role is None:
            continue
        snapshot = datasets.load(db, role["data_manifest_id"])
        manifest, bars = snapshot.manifest, snapshot.bars
        times = pd.to_datetime(bars["time"])
        start = pd.Timestamp(role["evaluation_start"])
        try:
            if times.empty or times.isna().any() or not times.is_monotonic_increasing or times.duplicated().any() or not times.iloc[0] <= start <= times.iloc[-1]:
                raise ValueError("Papel sem intervalo de avaliação válido; confira evaluation_start e snapshot.")
            selected = times[times >= start]
            if previous is not None and selected.iloc[0] <= previous:
                raise ValueError("Intervalos de avaliação devem ser ordenados e não sobrepostos; aquecimento pode sobrepor.")
        except TypeError:
            raise ValueError("Relógios dos papéis são incompatíveis; declare a mesma convenção temporal.") from None
        identity = dict(symbol=manifest["symbol"], timeframe=manifest["timeframe"],
                        origin=manifest["source"]["namespace"], time=manifest["time"])
        if "identity" in context and identity != context["identity"]:
            raise ValueError("Papéis exigem a mesma origem, ativo, timeframe e convenção temporal.")
        context["identity"] = identity
        context["roles"][name] = dict(origin_key=digest({k: v for k, v in identity.items() if k != "time"}),
                                      first=selected.iloc[0].isoformat(), last=selected.iloc[-1].isoformat())
        previous = selected.iloc[-1]
    return store.put_protocol(db, revision, context)


def _mapping(composition, executor="base_v1"):
    if executor == "composition_v1":
        return BaseAdapter(composition).strategy()
    if executor != "base_v1":
        raise ValueError("Executor desconhecido; não reinterpretar um pedido de outra versão.")
    value = composition.to_dict()
    if any(value[slot]["mode"] != mode for slot, mode in BASE_MODES.items()) or value["filters"]:
        raise ValueError("Avaliação inicial aceita somente composição BASE sem filtros ou overrides.")
    if not value["sizing"]["params"]["compound"]:
        raise ValueError("run_python inicial exige compound=True; compound=False não pode ser mapeado fielmente.")
    problems = compatibility(composition)
    if problems:
        raise ValueError("; ".join(p.message for p in problems))
    return get_strategy(value["strategy"]["key"], **value["strategy"]["params"])


def request(db, protocol_id, key, composition, *, role="train", origin="manual", purpose="evaluation",
            use_cache=True, discard_reason="", config=None, executor="base_v1"):
    token(key, "Chave do pedido")
    p = store.protocol(db, protocol_id)
    if p['definition'].get('type') in ('laboratory_associations_v1', 'pattern_campaign_v1'):
        raise ValueError('Este protocolo não é composição BASE; use o avaliador do seu tipo, sem executor de estratégias.')
    if executor not in ("base_v1", "composition_v1"):
        raise ValueError("Executor desconhecido; use base_v1 ou composition_v1 explicitamente.")
    if role not in p["definition"]["roles"] or origin not in ("manual", "automatic") or purpose not in ("evaluation", "tuning") or type(use_cache) is not bool:
        raise ValueError("Declare papel existente, origem manual/automatic, finalidade evaluation/tuning e cache booleano.")
    value = composition.to_dict() if isinstance(composition, Composition) else json.loads(encoded(composition))
    payload = dict(composition=value, role=role, origin=origin, purpose=purpose, use_cache=use_cache,
                   discard_reason=str(discard_reason), settings=None)
    if executor != "base_v1":
        payload["executor"] = executor
    error = ""
    try:
        composition = Composition(value)
        value = composition.to_dict()
        payload["composition"] = value
        settings = replace(research.backtest_config(config), risk_per_trade=value["sizing"]["params"]["risk_fraction"])
        if executor == "composition_v1":
            settings = BaseAdapter(composition).backtest_config(settings)
        if any(type(number) in (int, float) and not math.isfinite(number) for number in asdict(settings).values()):
            raise ValueError("Configuração econômica exige números finitos.")
        payload["settings"] = asdict(settings)
        _mapping(composition, executor)
        if value["context"] != {"symbol": p["context"]["identity"]["symbol"], "timeframe": p["context"]["identity"]["timeframe"]}:
            raise ValueError("Composição não corresponde ao ativo/timeframe dos papéis.")
    except ValueError as exc:
        error = str(exc)
    alternative = digest(dict(composition=value, data=p["definition"]["roles"][role], settings=payload["settings"]))
    return store.put_request(db, protocol_id, key, payload, alternative, error, bool(discard_reason))


def holdout_access(db, protocol_id, key, *, role="holdout", purpose="evaluation"):
    token(key, "Chave de acesso")
    p = store.protocol(db, protocol_id)
    if role not in p["context"]["roles"] or purpose not in ("evaluation", "tuning"):
        raise ValueError("Acesso exige papel registrado e finalidade evaluation/tuning.")
    with store.transaction(db) as conn:
        store.access(conn, protocol_id, role, p["context"]["roles"][role], key, purpose)
    return dict(recorded=True, qualified_final_holdout=False)


class _Publication:
    """Adiciona journal na conexão que já grava run + vínculo do manifesto."""
    def __init__(self, db, item, job, execution_id, identity):
        self.db, self.item, self.job = db, item, job
        self.execution_id, self.identity, self.run_id = execution_id, identity, job.run_id

    def check(self, **kwargs):
        self.job.check(**kwargs)

    def publication(self):
        return self.job.publication()

    def result(self, state, reason="", conn=None):
        if conn is None:
            raise ValueError("Publicação de experimento exige a transação do run.")
        store.lock(conn)
        row = conn.execute(text("SELECT metrics_json FROM _robot_runs WHERE run_id=:i"), {"i": self.run_id}).scalar_one()
        actual = json.loads(row).get("execution_identity")
        if state != "completed" or actual != self.identity:
            raise ValueError("Identidade mudou durante a avaliação; publicação recusada.")
        self.job.result(state, reason, conn)
        store.finish(conn, self.item, "run_recorded", self.run_id, self.execution_id, actual)


def _delivery(db, item):
    found = runs.load_runs(db, run_id=item["run_id"])
    if len(found) != 1:
        raise ValueError("Run referenciado ausente; restaure o registro, sem recalcular silenciosamente.")
    return dict(request=item, run=found[0], ledger_qualified=False, statistical_approval=False, qualified_final_holdout=False)


def _evaluate(db, request_id, *, config=None, budget=None):
    item = store.request(db, request_id)
    if item["state"] in ("rejected", "discarded"):
        return dict(request=item, ledger_qualified=False, statistical_approval=False, qualified_final_holdout=False)
    p, payload = store.protocol(db, item["protocol_id"]), item["payload"]
    # A transação de acesso termina ANTES do cálculo, busca em cache ou entrega.
    with store.transaction(db) as conn:
        store.access(conn, item["protocol_id"], payload["role"], p["context"]["roles"][payload["role"]],
                     "request-"+request_id, payload["purpose"], request_id)
    if item["state"] in ("run_recorded", "reused"):
        checkpoint = budgets.checkpoint(db, item["batch_id"])
        if checkpoint is not None and checkpoint["status"] != "completed":
            # Recuperação após commit e antes de finish: nunca recalcula o run.
            cleanup = budgets.Budget(db, config, limits=budgets.Limits(**p["definition"]["budget"]["limits"]), batch_id=item["batch_id"])
            try:
                cleanup.begin(checkpoint["plan"])
            except (ValueError, budgets.Interrupted):
                pass  # Executor ainda vivo/guarda ativa: não roubar posse para limpar status.
            else:
                cleanup.finish()
        return _delivery(db, item)
    composition = Composition(payload["composition"])
    executor = payload.get("executor", "base_v1")
    strategy = _mapping(composition, executor)
    adapter = BaseAdapter(composition) if executor == "composition_v1" else None
    role = p["definition"]["roles"][payload["role"]]
    snapshot = datasets.load(db, role["data_manifest_id"])
    frozen = configparser.ConfigParser()
    frozen["STRATEGY"] = {key: str(payload["settings"][key]) for key in ("initial_equity", "risk_per_trade", "commission_per_lot", "slippage_points")}
    settings = research.backtest_config(frozen)
    if adapter is not None:
        settings = adapter.backtest_config(settings)
    if asdict(settings) != payload["settings"]:
        raise ValueError("Configuração econômica não é mapeável para run_python.")
    identity = backtest._python_execution_identity(snapshot, strategy, settings, trade_from=role["evaluation_start"],
                                                   **({"composition": composition} if adapter is not None else {}))
    symbol, timeframe = p["context"]["identity"]["symbol"], p["context"]["identity"]["timeframe"]
    robot = next(r for r in catalog.python_robots() if r.key == "python:"+strategy.key)
    job_key = item["request_id"]
    plan = dict(version=1, signature=digest(dict(request=request_id, identity=identity, payload=payload)),
                jobs=[dict(key=job_key, robot=robot.key, symbol=symbol, timeframe=timeframe, kind=catalog.PYTHON, data_manifest_id=snapshot.manifest_id)])
    limits = budgets.Limits(**p["definition"]["budget"]["limits"])
    parent = budget if budget is not None else budgets.Budget(db, config, limits=limits, batch_id=item["batch_id"])
    if (type(parent) is not budgets.Budget or parent.batch_id != item["batch_id"] or parent.db is not db
            or parent.config is not config or parent.limits != limits or parent._claimed):
        raise ValueError("Budget deve usar o mesmo banco/configuração/batch e limites congelados, sem posse ativa.")
    execution_id, job, claimed = None, None, False
    try:
        parent.begin(plan)
        claimed = True
        recovered = parent.recovered(job_key)
        if recovered is not None:
            raise ValueError("Checkpoint possui run sem estado correspondente no journal; preserve a divergência.")
        # Depois de admitido, o pedido retoma a própria reserva; cache alheio
        # não pode trocar o run_id do journal nem do checkpoint existente.
        if payload["use_cache"] and identity.get("cache_eligible") and job_key not in parent._jobs:
            found = next((r for r in runs.load_runs(db, robot.key) if r["metrics"].get("execution_identity") == identity), None)
            if found is not None:
                parent.check()
                with store.transaction(db) as conn:
                    budgets.JobBudget(parent, job_key, found["run_id"]).result("completed", conn=conn)
                    store.finish(conn, item, "reused", found["run_id"], identity=identity)
                return _delivery(db, store.request(db, request_id))
        job = parent.admit(job_key)
        execution_id = uuid.uuid4().hex
        with store.transaction(db) as conn:
            previous = conn.execute(text("SELECT execution_id,run_id FROM _experiment_executions WHERE request_id=:i AND state='running'"), {"i": request_id}).mappings().all()
            for old in previous:
                store.event(conn, "abandoned:"+old["execution_id"], item["protocol_id"], request_id,
                            "execution_interrupted", dict(execution_id=old["execution_id"], run_id=old["run_id"], reason="executor anterior sem publicação"))
            conn.execute(text("UPDATE _experiment_executions SET state='interrupted',error='execução anterior sem publicação' WHERE request_id=:i AND state='running'"), {"i": request_id})
            store.start(conn, item, job.run_id, identity, execution_id)
        control = _Publication(db, item, job, execution_id, identity)
        execution = {"composition": composition} if adapter is not None else {"strategy": strategy}
        backtest.run_python(db, frozen, robot, symbol, timeframe, **execution,
                            data_manifest_id=snapshot.manifest_id, trade_from=role["evaluation_start"], control=control)
        return _delivery(db, store.request(db, request_id))
    except BaseException as exc:
        current = store.request(db, request_id)
        if current["state"] in ("run_recorded", "reused"):
            # A fronteira transacional já confirmou o run; o importer posterior não é qualificado aqui.
            with store.transaction(db) as conn:
                store.event(conn, "post:"+uuid.uuid4().hex, item["protocol_id"], request_id, "post_publication_error", dict(error=str(exc)))
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            return _delivery(db, current)
        state = "interrupted" if isinstance(exc, (budgets.Interrupted, KeyboardInterrupt, SystemExit)) else "failed"
        with store.transaction(db) as conn:
            if claimed:
                if job is not None:
                    job.result(state, str(exc), conn)
                store.finish(conn, item, state, job.run_id if job else item["run_id"], execution_id, error=str(exc))
            else:
                store.event(conn, "refused:"+uuid.uuid4().hex, item["protocol_id"], request_id, "evaluation_refused", dict(error=str(exc)))
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return dict(request=store.request(db, request_id), evaluation_error=str(exc), ledger_qualified=False,
                    statistical_approval=False, qualified_final_holdout=False)
    finally:
        if claimed:
            parent.finish(failed=store.request(db, request_id)["state"] not in ("run_recorded", "reused"))


def evaluate(db, request_id, *, config=None, budget=None):
    item = store.request(db, request_id)
    if store.protocol(db, item["protocol_id"])["definition"].get("type") == "pattern_campaign_v1":
        raise ValueError("Tentativa de padrão exige avaliação T13.4; executor de composição não pode consumi-la.")
    try:
        return _evaluate(db, request_id, config=config, budget=budget)
    except Exception as exc:
        item = store.request(db, request_id)
        with store.transaction(db) as conn:
            store.event(conn, "prepare:"+uuid.uuid4().hex, item["protocol_id"], request_id, "preparation_refused", dict(error=str(exc)))
            if item["state"] not in ("run_recorded", "reused", "running", "rejected", "discarded"):
                store.finish(conn, item, "failed", item["run_id"], error=str(exc))
        return dict(request=store.request(db, request_id), evaluation_error=str(exc), ledger_qualified=False,
                    statistical_approval=False, qualified_final_holdout=False)


def create_pattern_campaign(db, definition):
    """Congela roster, cortes e orçamento no protocolo existente; sem ler labels."""
    revision = PatternCampaignRevision(definition)
    body = revision.to_dict()
    context = {"campaign_roles": {}, "coverage": {}, "prior_external_exposure": "unknown"}
    snapshots = []
    for source in body["roster"]:
        snapshot = datasets.load(db, source["data_manifest_id"])
        manifest = snapshot.manifest
        actual = (manifest["source"]["namespace"], manifest["symbol"], manifest["timeframe"])
        declared = (source["source_namespace"], source["symbol"], source["timeframe"])
        if actual != declared:
            raise ValueError("Roster diverge da identidade do snapshot; campanha recusada.")
        if manifest["time"]["offset_state"] != "declared":
            raise ValueError("Campanha exige relógio congelado com offset comprovado.")
        bars = snapshot.bars
        if bars.empty:
            raise ValueError("Campanha exige barras congeladas, não snapshot vazio ou só de features.")
        offset = manifest["time"]["broker_utc_offset_hours"]
        times = pd.to_datetime(bars.time, format="ISO8601").dt.tz_localize("UTC") - pd.Timedelta(hours=offset)
        snapshots.append((source, times))
    for role, window in body["roles"].items():
        scopes, coverage = [], []
        first, last = pd.Timestamp(window["start_utc"]), pd.Timestamp(window["end_utc"])
        for source, times in snapshots:
            origin_key = digest({"symbol": source["symbol"], "timeframe": source["timeframe"],
                                 "origin": source["source_namespace"]})
            scopes.append(dict(origin_key=origin_key, first=first.isoformat(), last=last.isoformat()))
            count = int(((times >= first) & (times < last)).sum())
            coverage.append(dict(source_namespace=source["source_namespace"], symbol=source["symbol"],
                                 timeframe=source["timeframe"], rows=count, available=count > 0))
        context["campaign_roles"][role] = scopes
        context["coverage"][role] = coverage
    return store.put_protocol(db, revision, context)


def register_pattern_attempt(db, protocol_id, key, definition, *, role="train", origin="manual",
                             purpose="evaluation", discard_reason=""):
    """Conta tentativas novas no orçamento global; idênticas preservam a identidade."""
    from mt5_extracao.analytics.hypotheses import Definition

    token(key, "Chave da tentativa")
    if not isinstance(definition, Definition):
        raise ValueError("Compile a definição tipada antes de registrar a tentativa.")
    campaign = store.protocol(db, protocol_id)
    body = campaign["definition"]
    if body.get("type") != "pattern_campaign_v1" or role not in ROLES or origin not in ("manual", "automatic") or purpose not in ("evaluation", "tuning"):
        raise ValueError("Campanha, papel, origem ou finalidade inválidos.")
    if not isinstance(discard_reason, str):
        raise ValueError("Motivo de descarte precisa ser texto explícito.")
    roster = body["roster"]
    rule = definition.body
    if rule["timeframe"] not in {source["timeframe"] for source in roster} or not set(rule["universe_definition"]["symbols"]) <= {source["symbol"] for source in roster}:
        raise ValueError("Definição fora do roster congelado da campanha.")
    candidate_id = digest([definition.definition_hash, roster, protocol_id])
    payload = dict(candidate_id=candidate_id, definition_hash=definition.definition_hash,
                   definition=definition.export(), role=role, origin=origin, purpose=purpose,
                   discard_reason=discard_reason)
    return store.put_pattern_attempt(db, protocol_id, key, payload, discard_reason=discard_reason)


def pattern_campaign_access(db, protocol_id, key, *, role="holdout", purpose="evaluation"):
    token(key, "Chave de acesso")
    if purpose not in ("evaluation", "tuning"):
        raise ValueError("Finalidade de acesso inválida.")
    return store.pattern_role_access(db, protocol_id, role, key, purpose)


def pattern_campaign_status(db, protocol_id):
    result = store.status(db, protocol_id)
    if result["protocol"]["definition"].get("type") != "pattern_campaign_v1":
        raise ValueError("Protocolo não é campanha de padrões.")
    return result
