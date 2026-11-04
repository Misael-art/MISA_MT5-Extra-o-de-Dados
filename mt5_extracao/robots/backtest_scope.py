"""Plano revisável de TFs; snapshots, journal e execução pertencem aos serviços atuais."""
import configparser
import copy
from dataclasses import asdict
from datetime import datetime
import hashlib
import json

from mt5_extracao import timeframes
from mt5_extracao.analytics import datasets, data_eligibility
from mt5_extracao.strategies import get_strategy, research
from . import budget, catalog, controls, runner

ALL = "Todos"
VERSION = 1


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _descriptor(robot, preset):
    return dict(key=robot.key, kind=robot.kind, source=catalog.fingerprint(robot),
                manifest=robot.manifest, timeframes=list(robot.timeframes),
                inputs=[dict(name=i.name, type=i.type, default=str(i.default)) for i in robot.inputs],
                effective_values=controls.effective_values(robot, preset))


def _config(config, deposit=None):
    child = configparser.ConfigParser(interpolation=None)
    if config is not None:
        child.read_dict({section: dict(config[section]) for section in config.sections()})
    if deposit is not None:
        if not child.has_section("STRATEGY"):
            child.add_section("STRATEGY")
        child["STRATEGY"]["initial_equity"] = str(deposit)
    return child


def _selection(scope):
    if scope in (ALL, "all"):
        return list(timeframes.Timeframe)
    tf = timeframes.parse(scope)
    if tf is None:
        raise ValueError("Escolha um timeframe válido ou Todos para o backtest.")
    return [tf]


def _python_params(robot, preset):
    strategy = get_strategy(robot.key.split(":", 1)[1])
    values = controls.effective_values(robot, preset)
    params = {name: type(default)(values[name]) for name, default in strategy.params.items()}
    return get_strategy(strategy.key, **params)


def preview(db, config, robot, symbol, *, scope=ALL, start, end, preset=None,
            manifest_path=None, guard=lambda: None):
    """Captura referências locais e grava um plano pendente, sem executar/admitir jobs.

    O histórico do Tester é distinto dos snapshots locais e não ganha revisão
    congelada por esta prévia. Seu modelo e eventual download ficam explícitos.
    """
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Escolha um ativo antes de preparar o plano.")
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("Base indisponível; conecte a base antes de preparar o plano.")
    try:
        begin, finish = datetime.fromisoformat(str(start)), datetime.fromisoformat(str(end))
        if begin >= finish or (begin.tzinfo is None) != (finish.tzinfo is None):
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("Informe uma janela ISO explícita, com início anterior ao fim.") from None
    start, end = begin.isoformat(), finish.isoformat()
    if robot.kind not in (catalog.PYTHON, catalog.MQL5):
        raise ValueError("Tipo de robô indisponível para o backtest.")
    if robot.kind == catalog.MQL5 and (begin.tzinfo is not None or begin.time() != datetime.min.time() or finish.time() != datetime.min.time()):
        raise ValueError("O Tester exige datas de calendário sem horário ou fuso; informe AAAA-MM-DD.")
    package, strategy = None, None
    if manifest_path is not None:
        if robot.kind != catalog.MQL5:
            raise ValueError("Pacote nativo exige um robô MQL5.")
        from ..presets.compiler import read_package
        package = read_package(manifest_path, robot)
        test = package.manifest["test_context"]
        if test is None or any(datetime.fromisoformat(test[k]) != value for k, value in (("start", begin), ("end", finish))):
            raise ValueError("Janela difere do pacote; exporte a revisão no contexto escolhido.")
        if package.manifest["composition"]["context"]["symbol"] != symbol:
            raise ValueError("Ativo difere do pacote; escolha o contexto original.")
        child = _config(config, test["deposit"])
        identity = dict(package.manifest["identity"], content_hash=package.package_hash,
                        execution_hash=package.execution_hash, config_hash=package.manifest["config_hash"])
        execution = dict(mode="preset_package", manifest_path=str(manifest_path), package_hash=package.package_hash)
        model = test["model"]
        minimum = max(1, int(package.manifest["composition"]["strategy"]["params"].get("warmup_bars", 0)) + 1)
    else:
        from ..presets import NATIVE_PRESET_KEYS
        if robot.key in NATIVE_PRESET_KEYS:
            raise ValueError("Escolha Default ou Manual e exporte o pacote antes de preparar Todos.")
        child = _config(config)
        descriptor = _descriptor(robot, preset)
        identity = dict(id=preset or "code-defaults", label=preset or controls.DEFAULT_PRESET,
                        revision=None, revision_basis="content_hash", content_hash=_hash(descriptor))
        if robot.kind == catalog.PYTHON:
            strategy = _python_params(robot, preset)
            execution = dict(mode="python", params=strategy.params)
            model, minimum = "python_bar_backtest", strategy.min_bars
        else:
            execution = dict(mode="native_private", preset=preset,
                             params=controls.native_values(robot, preset), model="4")
            model, minimum = "4", 1
    control = budget.Budget(db, child)
    def checked():
        guard()
        control.check()
    checked()
    descriptor_hash = _hash(_descriptor(robot, preset))
    rows, jobs, snapshots = [], [], {}
    supported = {timeframes.parse(value) for value in robot.timeframes}
    for tf in _selection(scope):
        checked()
        row = dict(timeframe=tf.name, label=tf.label, eligible=False, bars=0, minimum_bars=minimum,
                   data_manifest_id=None, reason="", limitations=[])
        rows.append(row)
        if package is not None and package.manifest["composition"]["context"]["timeframe"] != tf.name:
            row["reason"] = "Preset congelado em outro TF; os parâmetros internos não foram alterados."
            continue
        if robot.kind == catalog.MQL5 and package is None and tf not in supported:
            row["reason"] = "TF não declarado pelo perfil nativo."
            continue
        if robot.kind == catalog.MQL5 and not robot.has_source:
            row["reason"] = "Fonte indisponível para conferir e observar a sessão privada."
            continue
        if not db.find_table_for_symbol(symbol, tf.label):
            row["reason"] = "Sem série local; extraia o histórico antes de preparar outro plano."
            continue
        try:
            snapshot = datasets.capture(db, symbol, tf.name, begin, finish, guard=checked)
            row.update(bars=len(snapshot.bars), data_manifest_id=snapshot.manifest_id)
            if row["bars"] < minimum:
                row["reason"] = f"Amostra insuficiente: {row['bars']} barras; mínimo {minimum}."
                continue
            required = sorted(set(data_eligibility.requirements(dict(source_id="bars",
                required_columns=strategy.required_columns if strategy else ("open", "high", "low", "close")))) | {"ohlcv"})
            quality = data_eligibility.evaluate(snapshot.quality, required)
            row["limitations"] = quality["limitations"]
            if not quality["eligible"]:
                row["reason"] = " ".join(quality["reasons"])
                continue
            if robot.kind == catalog.MQL5:
                row["limitations"].append("Série local é referência; revisão/cobertura do histórico nativo e aquecimento dependem do perfil/Tester.")
            row["eligible"] = True
            jobs.append((robot, symbol, tf.name))
            snapshots[tf.name] = snapshot.manifest_id
        except (ValueError, OSError) as error:
            row["reason"] = str(error)
    checked()
    if _hash(_descriptor(robot, preset)) != descriptor_hash:
        raise ValueError("Código/preset mudou durante a prévia; prepare outro plano.")
    review = dict(version=VERSION, scope="all" if scope in (ALL, "all") else "single",
                  symbol=symbol, robot=robot.key, robot_name=robot.name, kind=robot.kind,
                  start=start, end=end, preset=preset, preset_identity=identity, model=model,
                  settings=asdict(research.backtest_config(child)), rows=rows,
                  eligible=sum(r["eligible"] for r in rows), excluded=sum(not r["eligible"] for r in rows),
                  descriptor_hash=descriptor_hash, manifest_path=str(manifest_path) if manifest_path is not None else None,
                  base_namespace=datasets.identity(db)["namespace"],
                  native_history_revision="unavailable" if robot.kind == catalog.MQL5 else None,
                  cost_basis="python_settings" if robot.kind == catalog.PYTHON else "native_report; custos efetivos não são certificados pelas configurações Python",
                  native_download_notice="O Tester pode baixar histórico da corretora; confirmar este plano autoriza esse preparo, sem negociação ao vivo."
                    if robot.kind == catalog.MQL5 else None)
    provenance = {k: review[k] for k in ("version", "scope", "symbol", "robot", "start", "end", "preset_identity",
                                        "model", "settings", "base_namespace", "native_history_revision", "cost_basis")}
    context = dict(review=review, provenance=provenance)
    # Reutilizar a chave de job da fila, sem outro catálogo de combinações.
    manifest_ids = {budget.job_key(robot.key, symbol, tf): ident for tf, ident in snapshots.items()}
    options = dict(start=begin, end=finish, preset=preset, execution=execution, context=context,
                   manifest_ids=manifest_ids if robot.kind == catalog.PYTHON else None)
    control.prepare(jobs, claim=False, **options)
    return inspect_plan(db, control.batch_id)


def inspect_plan(db, batch_id):
    checkpoint = budget.checkpoint(db, batch_id)
    if checkpoint is None or checkpoint["plan"].get("context", {}).get("review", {}).get("version") != VERSION:
        raise ValueError("Plano de TFs não encontrado ou de versão incompatível.")
    heads = {item["job_key"]: item for item in checkpoint["jobs"]}
    executions = [dict(timeframe=entry["timeframe"], **heads[entry["key"]])
                  for entry in checkpoint["plan"]["jobs"] if entry["key"] in heads]
    return dict(batch_id=batch_id, status=checkpoint["status"], reason=checkpoint["reason"], signature=checkpoint["plan"]["signature"], executions=executions,
                **checkpoint["plan"]["context"]["review"])


def create_runner(db, config, robot, batch_id, *, on_event=None):
    """Confere o plano e devolve o executor existente; o chamador decide quando iniciar."""
    review = inspect_plan(db, batch_id)
    if review["robot"] != robot.key or _hash(_descriptor(robot, review["preset"])) != review["descriptor_hash"]:
        raise ValueError("Código/preset mudou desde a revisão; prepare outro plano.")
    if review["base_namespace"] != datasets.identity(db)["namespace"]:
        raise ValueError("Plano pertence a outra base lógica.")
    if not review["eligible"]:
        raise ValueError("Nenhuma combinação elegível; confira os motivos de exclusão.")
    child = _config(config, review["settings"]["initial_equity"] if review["manifest_path"] else None)
    if asdict(research.backtest_config(child)) != review["settings"]:
        raise ValueError("Capital/custos mudaram desde a revisão; prepare outro plano.")
    checkpoint = budget.checkpoint(db, batch_id)
    entries, context = checkpoint["plan"]["jobs"], checkpoint["plan"]["context"]
    execution = entries[0]["execution"]
    if review["manifest_path"]:
        from ..presets.compiler import read_package
        if read_package(review["manifest_path"], robot).package_hash != review["preset_identity"]["content_hash"]:
            raise ValueError("Pacote mudou desde a revisão; preserve a referência e prepare outro plano.")
    for row in review["rows"]:
        if row["eligible"]:
            datasets.load(db, row["data_manifest_id"])
    actor = copy.deepcopy(robot)

    def validate_job(job, entry):
        if asdict(research.backtest_config(child)) != review["settings"]:
            raise ValueError("Capital/custos mudaram; execução recusada neste plano.")
        if _hash(_descriptor(robot, review["preset"])) != review["descriptor_hash"]:
            raise ValueError("Código/preset mudou; execução recusada antes de iniciar este job.")
        if review["manifest_path"]:
            from ..presets.compiler import read_package
            if read_package(review["manifest_path"], robot).package_hash != execution["package_hash"]:
                raise ValueError("Pacote mudou; preserve a revisão e prepare outro plano.")

    def validate_run(key, record):
        entry = next(item for item in entries if item["key"] == key)
        validate_job(None, entry)
        if (record["robot_key"] != robot.key or record["symbol"] != review["symbol"] or
                record["timeframe"] != entry["timeframe"]):
            raise ValueError("Resultado não corresponde à combinação revisada.")
        if execution["mode"] == "python" and (record["params"] != execution["params"] or
                                               record["data_manifest_id"] != entry["data_manifest_id"]):
            raise ValueError("Dados/parâmetros diferem do plano; resultado não publicado.")
        if robot.kind == catalog.MQL5 and record["metrics"].get("source_fingerprint") != catalog.fingerprint(robot):
            raise ValueError("Fonte nativa difere da revisão; resultado não publicado.")

    def validate_recovered(record, entry):
        expected = record["metrics"].get("backtest_scope") or {}
        if expected.get("batch_id") != batch_id or expected.get("plan_signature") != checkpoint["plan"]["signature"]:
            raise ValueError("Resultado não corresponde ao plano revisado; reconcilie antes de retomar.")
        if execution["mode"] == "preset_package":
            from ..presets.execution import load_evidence
            proofs = load_evidence(db, execution_hash=review["preset_identity"]["execution_hash"])
            if not any(p["run_id"] == record["run_id"] and p["status"] in ("contract_only", "reconciled_backtest") for p in proofs):
                raise ValueError("Conferência nativa pendente/divergente; preserve o resultado sem repetir o Tester.")

    options = dict(start=datetime.fromisoformat(review["start"]), end=datetime.fromisoformat(review["end"]),
                   preset=review["preset"], execution=execution, context=context,
                   manifest_ids={entry["key"]: entry["data_manifest_id"] for entry in entries} if robot.kind == catalog.PYTHON else None)
    return runner.BatchRunner(db, child, [runner.Job(actor, entry["symbol"], entry["timeframe"]) for entry in entries],
                              budget=budget.Budget(db, child, batch_id=batch_id, validate_run=validate_run), on_event=on_event,
                              prepare_options=options, validate_job=validate_job, validate_recovered=validate_recovered)
