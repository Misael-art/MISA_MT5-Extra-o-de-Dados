"""CLI estreita do registro; nenhum comando inicializa MetaTrader."""
import json
from pathlib import Path

from . import service, store


def _read(path):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("JSON contém chave duplicada; preserve uma definição única.")
            value[key] = item
        return value

    def invalid(value):
        raise ValueError("JSON contém número não finito: "+value)

    return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=pairs, parse_constant=invalid)


def add_parser(sub):
    parser = sub.add_parser("experiments", help="registro auditável inicial de avaliações Python; sem seleção automática")
    commands = parser.add_subparsers(dest="experiment_action", required=True)
    p = commands.add_parser("create", help="registra protocolo imutável e papéis de snapshots existentes")
    p.add_argument("--file", required=True)
    p = commands.add_parser("request", help="registra alternativa/pedido explícito, inclusive descarte ou recusa")
    p.add_argument("--protocol", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--composition", required=True)
    p.add_argument("--role", choices=("train", "validation", "holdout"), default="train")
    p.add_argument("--origin", choices=("manual", "automatic"), default="manual")
    p.add_argument("--purpose", choices=("evaluation", "tuning"), default="evaluation")
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--discard-reason", default="")
    p.add_argument("--executor", choices=("base_v1", "composition_v1"), default="base_v1",
                   help="base_v1 preserva o inicial; composition_v1 aplica modos Python já suportados")
    p = commands.add_parser("evaluate", help="avalia o pedido com o executor declarado, orçamento e journal prévios")
    p.add_argument("request_id")
    p = commands.add_parser("status", help="consulta journal e exposição; não entrega métricas financeiras")
    p.add_argument("protocol_id")
    p = commands.add_parser("holdout_access", help="registra acesso antes de consumir o resultado reservado")
    p.add_argument("protocol_id")
    p.add_argument("--key", required=True)
    p.add_argument("--role", choices=("train", "validation", "holdout"), default="holdout")
    p.add_argument("--purpose", choices=("evaluation", "tuning"), default="evaluation")
    parser.set_defaults(func=run)


def run(args, ctx):
    from .. import cli
    action = args.experiment_action
    if action == "create":
        definition = _read(args.file)
        result = service.create(ctx.db, definition)
    elif action == "request":
        composition = _read(args.composition)
        result = service.request(ctx.db, args.protocol, args.key, composition, role=args.role,
                                 origin=args.origin, purpose=args.purpose, use_cache=not args.no_cache,
                                 discard_reason=args.discard_reason, config=ctx.config, executor=args.executor)
    elif action == "evaluate":
        result = service.evaluate(ctx.db, args.request_id, config=ctx.config)
    elif action == "status":
        result = store.status(ctx.db, args.protocol_id)
    else:
        result = service.holdout_access(ctx.db, args.protocol_id, args.key, role=args.role, purpose=args.purpose)
    cli._data(**result)
    item = result.get("request", result)
    error = result.get("evaluation_error") or item.get("error")
    if error:
        cli._err(error)
        return cli.EXIT_FAIL
    cli._print("Registro de experimento: "+str(item.get("state", item.get("protocol_id", "consultado"))))
    return cli.EXIT_OK
