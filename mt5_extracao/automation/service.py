"""Workflow local: adaptadores dos serviços atuais, sem motor/fila/store financeiros novos."""
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys

from . import PROTOCOL_VERSION
from . import charts, protocol
from .sessions import SessionController


def run_cli(arguments, *, config_path, db_override=None):
    """Invoca somente o CLI deste ambiente por argv, nunca via shell."""
    from .catalog import command_tree
    if not isinstance(arguments, list) or not arguments or any(not isinstance(x, str) for x in arguments):
        raise ValueError("arguments exige lista de argumentos textuais do CLI.")
    names = {row["command"] for row in command_tree()["subcommands"]} - {"workflow"}
    if arguments[0] not in names:
        raise ValueError("Use comando público do catálogo; workflow recursivo não é permitido.")
    command = [sys.executable, "-m", "mt5_extracao.cli", "--json", "--config", str(config_path)]
    if db_override is not None:
        command += ["--db", str(db_override)]
    result = subprocess.run(command + arguments, capture_output=True, text=True, encoding="utf-8", errors="replace")
    try:
        payload = protocol.loads(result.stdout)
    except (ValueError, TypeError):
        return dict(ok=False, command=arguments[0], exit_code=result.returncode or 1,
                    data={}, messages=[], errors=[result.stderr.strip() or "CLI não devolveu envelope JSON."])
    # O doctor preserva seu contrato de bootstrap: lista de checks, sem envelope.
    if arguments[0] == "doctor" and isinstance(payload, list):
        return dict(ok=result.returncode == 0, command="doctor", exit_code=result.returncode,
                    data=dict(checks=payload, source_format="bootstrap.checks"), messages=[],
                    errors=[] if result.returncode == 0 else ["Doctor encontrou falhas; consulte data.checks."])
    if not isinstance(payload, dict) or payload.get("exit_code") != result.returncode:
        raise RuntimeError("Saída do CLI diverge do código de saída; preserve o recibo.")
    return payload


def _reader():
    from ..mt5_backend import get_mt5
    from ..robots import controls
    provider = get_mt5()
    if provider is None:
        raise ValueError("Backend MT5 indisponível; conecte o backend ou forneça read_account do connector existente.")
    return controls.account_identity(provider)


class Workflow:
    def __init__(self, db, config, *, read_account=None, config_path=None, db_override=None):
        self.db, self.config = db, config
        self.config_path, self.db_override = config_path, db_override
        self.controller = SessionController(db, config, read_account or _reader)
        self.receipts = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @staticmethod
    def methods():
        return dict(protocol=PROTOCOL_VERSION, transport="local JSON/JSONL; no network server",
            lifecycle="serve mantém sessões; EOF/context manager encerra somente as próprias",
            methods=[
                dict(method="commands", required=[], description="árvore completa do argparse atual"),
                dict(method="cli.run", required=["arguments"], description="dados, pesquisa, laboratório, backtest, install/compile/check/market pelos comandos existentes"),
                dict(method="session.review", required=["robot", "symbol", "timeframe"], optional=["preset"], description="conta/carteira/risco/fonte; não inicia processo"),
                dict(method="session.open", required=["mode"], description="play exige review/accept_review/confirm_account; histórico exige robot+janela/contexto ou manifest de preset", modes=["play", "replay", "backtest"]),
                dict(method="session.status", required=[], optional=["session"]),
                dict(method="session.wait", required=["session"], optional=["until", "timeout"], until=["running", "ready", "finished"]),
                dict(method="session.stop", required=["session"], description="STOP próprio, sem liquidar posições"),
                dict(method="chart.list", required=["session"]),
                dict(method="chart.open", required=["session", "symbol", "timeframe"], description="gráfico adicional sem Expert"),
                dict(method="chart.close", required=["session", "chart"], description="somente auxiliar próprio"),
                dict(method="chart.select", required=["session", "chart"]),
                dict(method="chart.receipt", required=["session"], description="recibo tardio sem repetir efeito"),
                dict(method="chart.style", required=["session", "style"], optional=["chart"], styles=["light", "dark", "light_gray"]),
                dict(method="chart.snapshot", required=["session"], optional=["chart", "width", "height", "backend"], backends=["native", "owned_window"], description="PLAY: native; REPLAY: janela própria pela API do SO, com resolução real"),
                dict(method="indicator.add", required=["session", "kind"], optional=["chart", "period", "mean", "deviation"], kinds=["sma", "ema", "atr", "rsi", "adx", "super_atr_10"]),
                dict(method="indicator.remove", required=["session", "indicator"], optional=["chart"]),
                dict(method="panel.set", required=["session", "field", "value"], fields=["theme", "timer", "probability", "gradient", "watermark", "clean", "bars"]),
                dict(method="panel.get", required=["session"]),
                dict(method="presets.default", required=["robot", "symbol", "timeframe"]),
                dict(method="presets.list", required=["robot", "symbol", "timeframe"]),
                dict(method="presets.get", required=["preset_id"], optional=["revision"]),
                dict(method="presets.save", required=["robot", "source_id", "name"], optional=["source_revision", "composition"]),
                dict(method="presets.revise", required=["robot", "preset_id", "composition", "expected_revision"]),
                dict(method="presets.rename", required=["preset_id", "name"]),
                dict(method="presets.select", required=["robot", "symbol", "timeframe", "preset_id", "revision"]),
                dict(method="presets.export", required=["robot", "preset_id", "directory"], optional=["revision", "test_context"]),
                dict(method="presets.import", required=["robot", "manifest"]),
            ], limits=dict(sessions=16, charts_per_session=16, indicators_per_session=64, steps=128),
            boundaries=["Não há novas ordens Python, seleção automática, otimização, waiver de risco ou capacidade financeira.",
                        "Deploy local=install/compile/revisar/start; publicação Market continua contrato manual existente.",
                        "Método gráfico incompatível é recusado; não é substituído por automação de mouse."])

    def call(self, request):
        protocol.fields(request, ("id", "method"), ("params",))
        key = protocol.text(request["id"], "id", 80)
        method = protocol.text(request["method"], "method", 80)
        params = request.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("params deve ser objeto.")
        signature = protocol.digest(request)
        if key in self.receipts:
            previous, result = self.receipts[key]
            if previous != signature:
                raise ValueError("ID de pedido reutilizado com conteúdo divergente.")
            return result
        try:
            data = self._dispatch(method, dict(params))
            if method == "session.wait" and (data.get("wait_timed_out") or data.get("state") == "failed" or
                    params.get("until", "running") != "finished" and data.get("state") != "running"):
                result = dict(id=key, ok=False, data=data, errors=[data.get("error") or "A sessão não atingiu o estado solicitado."],
                              error_code="wait_timeout" if data.get("wait_timed_out") else "session_not_ready")
            elif method == "cli.run" and data.get("ok") is not True:
                result = dict(id=key, ok=False, data=data, errors=data.get("errors") or ["O CLI recusou ou falhou."], error_code="cli_failed")
            else:
                result = dict(id=key, ok=True, data=data, errors=[])
        except (ValueError, TypeError, OSError, RuntimeError) as error:
            result = dict(id=key, ok=False, data={}, errors=[str(error) or type(error).__name__],
                          error_code="invalid_request" if isinstance(error, (ValueError, TypeError)) else "operation_failed")
        self.receipts[key] = signature, result
        return result

    def _dispatch(self, method, params):
        if method == "commands":
            protocol.fields(params)
            from .catalog import command_tree
            return command_tree()
        if method == "cli.run":
            protocol.fields(params, ("arguments",))
            if self.config_path is None:
                raise ValueError("cli.run exige config_path explícito; SDK em memória pode chamar os serviços diretamente.")
            return run_cli(params["arguments"], config_path=self.config_path, db_override=self.db_override)
        if method in ("session.review", "session.open", "session.status", "session.wait", "session.stop"):
            return getattr(self.controller, method.split(".")[1])(params)
        if method == "chart.receipt":
            protocol.fields(params, ("session",))
            self.controller.job(params["session"])
            return self.controller.channels[params["session"]].receipt()
        if method in charts.OPERATIONS:
            session = params.pop("session", None)
            job = self.controller.job(session)
            if not job.visible or job.mode == "backtest":
                raise ValueError("Sessão não visual: reabra com visible=true; REPLAY sempre exibe.")
            timeout = protocol.number(params.pop("timeout", 15), "timeout", .1, 120)
            if method == "chart.snapshot":
                backend = params.pop("backend", "owned_window" if job.mode == "replay" else "native")
                if backend == "owned_window":
                    from .snapshots import capture
                    return capture(job, params, timeout=timeout)
                if backend != "native" or job.mode == "replay":
                    raise ValueError("Backend de captura indisponível; REPLAY usa owned_window, sem width/height.")
            return self.controller.channels[session].request(method, params, timeout)
        if method.startswith("presets."):
            return self._preset(method[8:], params)
        raise ValueError("Método não reconhecido; consulte workflow methods.")

    @staticmethod
    def _preset_data(record):
        return dict(identity=asdict(record.identity), composition=record.composition.to_dict(), manifest=record.manifest)

    def _preset(self, method, params):
        from ..presets import defaults, manual, store
        def current(item):
            return manual.get_manual(self.db, item.identity.id, item.identity.revision) if item.identity.category == "Manual" else item
        def robot():
            return self.controller._robot(params["robot"])
        if method in ("default", "list"):
            protocol.fields(params, ("robot", "symbol", "timeframe"))
            if method == "default":
                return self._preset_data(defaults.ensure_default(self.db, robot(), symbol=params["symbol"], timeframe=params["timeframe"]))
            return dict(presets=[self._preset_data(current(item)) for item in store.list_presets(self.db, robot().key, params["symbol"], params["timeframe"])])
        if method == "get":
            protocol.fields(params, ("preset_id",), ("revision",))
            item = store.get_preset(self.db, params["preset_id"], params.get("revision"))
            if item is None:
                raise ValueError("Preset/revisão não encontrado.")
            return self._preset_data(current(item))
        if method == "save":
            protocol.fields(params, ("robot", "source_id", "name"), ("source_revision", "composition"))
            item = manual.save_as_manual(self.db, robot(), params["source_id"], params["name"],
                source_revision=params.get("source_revision"), composition=params.get("composition"))
            return self._preset_data(item)
        if method == "revise":
            protocol.fields(params, ("robot", "preset_id", "composition", "expected_revision"))
            return self._preset_data(manual.save_revision(self.db, robot(), params["preset_id"], params["composition"], expected_revision=params["expected_revision"]))
        if method == "rename":
            protocol.fields(params, ("preset_id", "name"))
            return self._preset_data(manual.rename_manual(self.db, params["preset_id"], params["name"]))
        if method == "select":
            protocol.fields(params, ("robot", "symbol", "timeframe", "preset_id", "revision"))
            store.select_preset(self.db, robot().key, params["symbol"], params["timeframe"], params["preset_id"], params["revision"])
            item = store.selected_preset(self.db, robot().key, params["symbol"], params["timeframe"])
            return dict(selected=self._preset_data(current(item)))
        if method == "import":
            protocol.fields(params, ("robot", "manifest"))
            return self._preset_data(manual.import_manual(self.db, robot(), params["manifest"]))
        if method == "export":
            protocol.fields(params, ("robot", "preset_id", "directory"), ("revision", "test_context"))
            from ..presets.compiler import TestContext
            context = TestContext(**params["test_context"]) if params.get("test_context") is not None else None
            result = manual.export_preset(self.db, robot(), params["preset_id"], params["directory"],
                revision=params.get("revision"), test_context=context)
            return dict(set_path=str(result["set_path"]), manifest_path=str(result["manifest_path"]),
                        manifest=result["package"].manifest)
        raise ValueError("Operação de preset desconhecida.")

    def run(self, manifest):
        protocol.fields(manifest, ("version", "steps"))
        if type(manifest["version"]) is not int or manifest["version"] != PROTOCOL_VERSION:
            raise ValueError("Versão de workflow não suportada.")
        steps = manifest["steps"]
        if not isinstance(steps, list) or not 1 <= len(steps) <= 128:
            raise ValueError("Workflow exige1..128 passos.")
        identifiers = []
        # Valida a forma de TODOS os passos antes de qualquer efeito.
        for step in steps:
            protocol.fields(step, ("id", "method"), ("params",))
            identifiers.append(protocol.text(step["id"], "id", 80))
            protocol.text(step["method"], "method", 80)
            if not isinstance(step.get("params", {}), dict):
                raise ValueError("params deve ser objeto.")
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("IDs repetidos no workflow.")
        results, receipts = {}, []
        for step in steps:
            try:
                call = dict(step, params=protocol.resolve(step.get("params", {}), results))
                receipt = self.call(call)
            except ValueError as error:
                receipt = dict(id=step["id"], ok=False, data={}, errors=[str(error)], error_code="invalid_reference")
            receipts.append(receipt)
            if not receipt["ok"] or (step["method"] == "cli.run" and not receipt["data"].get("ok")):
                return dict(ok=False, steps=receipts, stopped_at=step["id"], rollback_performed=False)
            results[step["id"]] = receipt["data"]
        return dict(ok=True, steps=receipts)

    def close(self):
        return self.controller.close()
