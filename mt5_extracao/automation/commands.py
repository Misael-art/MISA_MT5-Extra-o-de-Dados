"""CLI e transporte JSONL do workflow. A saída nunca pede interação humana."""
import json
from pathlib import Path
import sys

from . import open_workflow, protocol
from .service import Workflow


def add_parser(sub):
    p = sub.add_parser("workflow", help="SDK por JSON: sessões, gráficos, indicadores, presets e comandos sem Tk")
    s = p.add_subparsers(dest="workflow_action", required=True)
    s.add_parser("methods", help="contratos disponíveis; não abre DB, MT5 ou GUI")
    q = s.add_parser("run", help="executa manifesto versionado; passos e artefatos em JSON")
    q.add_argument("--file", required=True, help="JSON {version:1,steps:[...]} com IDs únicos e referências estruturadas")
    s.add_parser("serve", help="JSONL por stdin/stdout; EOF encerra somente sessões próprias, sem servidor de rede")
    p.set_defaults(func=run)


def _read_account(ctx):
    ctx.connector()
    from ..mt5_backend import get_mt5
    from ..robots import controls
    return controls.account_identity(get_mt5())


def serve(workflow, incoming, outgoing):
    """Um recibo por linha. Duplicatas nesta instância não repetem o efeito."""
    while True:
        line = incoming.readline(1048577)
        if not line:
            return
        request = None
        try:
            if len(line) > 1048576:
                while line and not line.endswith("\n"):
                    line = incoming.readline(1048577)
                raise ValueError("Pedido excede1MiB.")
            request = protocol.loads(line)
            result = workflow.call(request)
        except (ValueError, TypeError) as error:
            key = request.get("id") if isinstance(request, dict) else None
            result = dict(id=key if isinstance(key, str) else None, ok=False, data={}, errors=[str(error)])
        from ..cli import _jsonable
        outgoing.write(json.dumps(_jsonable(result), ensure_ascii=False, allow_nan=False) + "\n")
        outgoing.flush()


def run(args, ctx):
    from .. import cli
    if args.workflow_action == "methods":
        cli._data(**Workflow.methods())
        cli._print("Workflow1: comandos, sessões, apresentação e presets por JSON; sem mouse/teclado.")
        return cli.EXIT_OK
    with open_workflow(ctx.db, ctx.config, read_account=lambda: _read_account(ctx),
                       config_path=args.config, db_override=args.db) as workflow:
        if args.workflow_action == "serve":
            cli._OUT["raw"] = True  # JSONL já contém seus recibos; não emitir envelope extra no EOF.
            try:
                serve(workflow, sys.stdin, sys.stdout)
            except KeyboardInterrupt:
                return cli.EXIT_FAIL
            return cli.EXIT_OK
        manifest = protocol.loads(Path(args.file).read_text(encoding="utf-8-sig"))
        result = workflow.run(manifest)
        cli._data(**result)
        cli._print("Workflow concluído." if result["ok"] else "Workflow interrompido no passo " + result["stopped_at"])
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL
