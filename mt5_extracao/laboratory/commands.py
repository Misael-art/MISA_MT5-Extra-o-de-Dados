"""Comandos do mesmo Laboratório usado pela UI; sem dependência de desktop."""
from pathlib import Path
import json

from mt5_extracao.analytics import datasets
from . import service


def add_parser(sub):
    parser = sub.add_parser("laboratory", help="séries, confluências e comparação exploratória entre timeframes")
    actions = parser.add_subparsers(dest="laboratory_action", required=True)
    actions.add_parser("catalog", help="todos os indicadores e estratégias canônicos, sem abrir a base").set_defaults(func=run)
    inventory = actions.add_parser("inventory", help="inventário real: registry, definições de Dados e fatos")
    inventory.add_argument("--query", default="", help="busca por id, nome, família, unidade ou parâmetro")
    inventory.add_argument("--family")
    inventory.add_argument("--state", choices=["STORED", "COMPUTABLE", "STORED_ONLY", "UNAVAILABLE"])
    inventory.add_argument("--symbol")
    inventory.add_argument("--tf", dest="timeframe")
    inventory.add_argument("--page-size", type=int, default=50)
    inventory.add_argument("--cursor")
    inventory.set_defaults(func=run)
    actions.add_parser("sources", help="tabelas OHLCV em TFs nativos; somente leitura").set_defaults(func=run)
    freeze = actions.add_parser("freeze", help="congela seleção explícita no store de snapshots existente")
    freeze.add_argument("--symbol", required=True)
    freeze.add_argument("--tf", action="append", required=True, help="TF da base; repita para comparar")
    freeze.add_argument("--start", help="início ISO no relógio broker; define a semente da janela")
    freeze.add_argument("--end", help="fim ISO no relógio broker")
    freeze.set_defaults(func=run)
    analyze = actions.add_parser("analyze", help="diagnóstico de snapshots congelados; não executa operações")
    analyze.add_argument("--manifest", action="append", required=True, help="ID canônico; repita para cada TF")
    selected = analyze.add_mutually_exclusive_group()
    selected.add_argument("--feature", action="append", help="feature_id do catálogo; repita para cada indicador")
    selected.add_argument("--strategy-only", action="store_true", help="compara apenas as intenções da estratégia")
    analyze.add_argument("--strategy", choices=list(service.REGISTRY))
    analyze.add_argument("--params", default="{}", help="JSON dos parâmetros da estratégia")
    analyze.add_argument("--as-of", required=True, help="corte ISO com fuso explícito")
    analyze.add_argument("--out", help="novo arquivo JSON; recusa sobrescrever")
    analyze.set_defaults(func=run)


def run(args, ctx):
    from mt5_extracao import cli
    if args.laboratory_action == "catalog":
        result = service.catalog()
    elif args.laboratory_action == "inventory":
        from . import inventory
        result = inventory.search(ctx.db, query=args.query, family=args.family, state=args.state,
            symbol=args.symbol, timeframe=args.timeframe, page_size=args.page_size, cursor=args.cursor)
    elif args.laboratory_action == "sources":
        result = dict(sources=service.sources(ctx.db),
            source_scope="OHLCV tables supported by datasets.capture and timeframes.Timeframe; derived facts use Analytics")
    elif args.laboratory_action == "freeze":
        snapshots = service.capture_selection(ctx.db, symbol=args.symbol, timeframes_selected=args.tf,
                                              start=args.start, end=args.end)
        result = dict(manifests=[dict(id=item.manifest_id, symbol=item.manifest["symbol"],
                                     timeframe=item.manifest["timeframe"]) for item in snapshots])
    else:
        from mt5_extracao.automation.protocol import loads
        params = loads(args.params)
        if not isinstance(params, dict):
            raise ValueError("--params exige objeto JSON de parâmetros.")
        result = service.analyze([datasets.load(ctx.db, ident) for ident in args.manifest],
            feature_ids=[] if args.strategy_only else args.feature if args.feature is not None else service.DEFAULT_FEATURES,
            strategy_key=args.strategy, strategy_params=params, as_of=args.as_of)
        if args.out:
            try:
                with Path(args.out).expanduser().open("x", encoding="utf-8") as handle:
                    json.dump(result, handle, ensure_ascii=False, allow_nan=False)
            except OSError as error:
                raise ValueError("Escolha um arquivo novo em diretório gravável: " + str(error)) from error
    cli._OUT["data"].update(result)
    cli._print("Laboratório: " + args.laboratory_action + " concluído.")
    return cli.EXIT_OK
