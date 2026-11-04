"""Comandos do mesmo Laboratório usado pela UI; sem dependência de desktop."""
from pathlib import Path
import json

import pandas as pd

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
    derived_read = actions.add_parser(
        "derived", help="lê barras de dia exato já gravadas em _bars_derived; não agrega nem extrai")
    derived_read.add_argument("--symbol", help="se omitido, lista as séries gravadas")
    derived_read.add_argument("--tf", action="append", help="tempo sintético já gravado, por exemplo M7; repita para vários")
    derived_read.add_argument("--start", help="início no relógio da corretora, sem fuso")
    derived_read.add_argument("--end", help="fim no relógio da corretora, sem fuso")
    derived_read.add_argument("--head", type=int, default=0, help="inclui as primeiras N barras; o restante fica só na contagem")
    derived_read.add_argument("--relations", action="store_true",
                              help="compara dois ou mais tempos na grade do mais curto; não grava snapshot")
    derived_read.add_argument("--feature", action="append", help="feature_id; sem este argumento usa os cinco padrões")
    derived_read.add_argument("--strategy", choices=list(service.REGISTRY))
    derived_read.add_argument("--as-of", help="corte ISO com fuso; omissão usa o instante atual")
    derived_read.set_defaults(func=run)
    freeze = actions.add_parser("freeze", help="congela seleção explícita no store de snapshots existente")
    freeze.add_argument("--symbol", required=True)
    freeze.add_argument("--tf", action="append", required=True, help="TF da base; repita para comparar")
    freeze.add_argument("--start", help="início ISO no relógio broker; define a semente da janela")
    freeze.add_argument("--end", help="fim ISO no relógio broker")
    freeze.add_argument("--stored-feature", action="append", nargs=2, metavar=("ID", "VERSAO"),
                        help="congela fatos e contrato de uma versão persistida; repita para várias")
    freeze.set_defaults(func=run)
    analyze = actions.add_parser("analyze", help="diagnóstico de snapshots congelados; não executa operações")
    analyze.add_argument("--manifest", action="append", required=True, help="ID canônico; repita para cada TF")
    selected = analyze.add_mutually_exclusive_group()
    selected.add_argument("--feature", action="append", help="feature_id do catálogo; repita para cada indicador")
    selected.add_argument("--strategy-only", action="store_true", help="compara apenas as intenções da estratégia")
    analyze.add_argument("--stored-feature", action="append", nargs=2, metavar=("ID", "VERSAO"),
                         help="analisa fatos desta versão congelada; repita para várias")
    analyze.add_argument("--strategy", choices=list(service.REGISTRY))
    analyze.add_argument("--params", default="{}", help="JSON dos parâmetros da estratégia principal")
    analyze.add_argument("--compare", action="append", choices=list(service.REGISTRY),
                         help="outra estratégia, com parâmetros padrão; repita para comparar intenções no mesmo ativo")
    analyze.add_argument("--as-of", required=True, help="corte ISO com fuso explícito")
    analyze.add_argument("--links", help="JSON de vínculos explícitos: manifest_id/features/relationship, sem rede")
    analyze.add_argument("--timeline-basis", choices=["received", "publication"], default="received")
    analyze.add_argument("--out", help="novo arquivo JSON; recusa sobrescrever")
    analyze.set_defaults(func=run)
    report = actions.add_parser("report", help="relatório portátil de análise congelada; sem rede/recalcular fórmulas")
    report.add_argument("--analysis", required=True)
    report.add_argument("--out", required=True, help="arquivo HTML novo")
    report.set_defaults(func=run)
    candle = actions.add_parser("candle", help="preço, séries e recibos de um candle do relatório congelado")
    candle.add_argument("--analysis", required=True)
    candle.add_argument("--index", required=True, type=int)
    candle.set_defaults(func=run)
    crossings = actions.add_parser("crossings", help="todos os pares selecionados e horizontes do preço")
    crossings.add_argument("--analysis", required=True)
    crossings.set_defaults(func=run)
    study = actions.add_parser("study", help="valida e resume um estudo do gráfico contra a análise exportada")
    study.add_argument("--analysis", required=True, help="JSON gerado por analyze --out")
    study.add_argument("--study", required=True, help="estudo exportado pela UI")
    study.set_defaults(func=run)
    matrix = actions.add_parser("matrix", help="matriz indicador × indicador da análise exportada")
    matrix.add_argument("--analysis", required=True, help="JSON gerado por analyze --out")
    matrix.add_argument("--lag", type=int, default=0, help="defasagem fixa do relatório: 0, 1, 3 ou 6")
    matrix.set_defaults(func=run)
    target = actions.add_parser("target", help="indicador × retorno bruto do preço, horizonte fixo")
    target.add_argument("--analysis", required=True)
    target.add_argument("--horizon", type=int, default=1, help="1, 3 ou 6 barras da grade")
    target.set_defaults(func=run)
    preset_assets = actions.add_parser("preset-assets", help="mesma revisão de preset já gravada, em ativos diferentes")
    preset_assets.add_argument("--robot", required=True, help="chave do robô; não executa backtest")
    preset_assets.set_defaults(func=run)


def run(args, ctx):
    from mt5_extracao import cli
    if args.laboratory_action in ("report", "candle", "crossings"):
        from mt5_extracao import sdk
        path = Path(args.analysis).expanduser()
        if path.stat().st_size > 16*1024*1024:
            raise ValueError("Análise excede16MiB; reduza o recorte antes de exportar.")
        frozen = service.load_report(path.read_text(encoding="utf-8"))
        result = (sdk.export_laboratory_report(frozen, args.out) if args.laboratory_action == "report" else
                  sdk.laboratory_candle(frozen, args.index) if args.laboratory_action == "candle" else
                  sdk.laboratory_crossings(frozen))
    elif args.laboratory_action == "catalog":
        result = service.catalog()
    elif args.laboratory_action == "inventory":
        from . import inventory
        result = inventory.search(ctx.db, query=args.query, family=args.family, state=args.state,
            symbol=args.symbol, timeframe=args.timeframe, page_size=args.page_size, cursor=args.cursor)
    elif args.laboratory_action == "study":
        from .annotations import Study
        report = service.load_report(Path(args.analysis).expanduser().read_text(encoding="utf-8"))
        loaded = Study.from_json(Path(args.study).expanduser().read_text(encoding="utf-8"), report["analysis_id"],
                                 [channel["id"] for channel in report["channels"]])
        result = dict(analysis_id=loaded.analysis_id, symbol=report["symbol"], grid_timeframe=report["grid_timeframe"],
                      items=loaded.items, hidden_series=sorted(loaded.hidden), removed_series=sorted(loaded.removed),
                      visible_series=[c["id"] for c in report["channels"] if c["id"] not in loaded.hidden | loaded.removed],
                      view=loaded.view)
    elif args.laboratory_action == "matrix":
        report = service.load_report(Path(args.analysis).expanduser().read_text(encoding="utf-8"))
        result = service.relation_matrix(report, lag=args.lag)
    elif args.laboratory_action == "target":
        report = service.load_report(Path(args.analysis).expanduser().read_text(encoding="utf-8"))
        result = service.target_matrix(report, horizon=args.horizon)
    elif args.laboratory_action == "preset-assets":
        from . import preset_assets
        result = preset_assets.compare(ctx.db, args.robot)
    elif args.laboratory_action == "sources":
        result = dict(sources=service.sources(ctx.db),
            source_scope="OHLCV tables supported by datasets.capture and timeframes.Timeframe; derived facts use Analytics")
    elif args.laboratory_action == "derived":
        from . import derived
        if args.tf and args.relations:
            if not args.symbol:
                raise ValueError("Informe --symbol junto com --tf. Esta comparação não varre a conta.")
            if len(args.tf) < 2:
                raise ValueError("A comparação sincronizada pede ao menos dois tempos de dia exato.")
            report = derived.compare_stored(
                ctx.db, args.symbol, args.tf, start=args.start, end=args.end,
                feature_ids=args.feature if args.feature is not None else service.DEFAULT_FEATURES,
                strategy_key=args.strategy,
                as_of=args.as_of or pd.Timestamp.now(tz="UTC").isoformat())
            result = derived.public_comparison(report)
        elif args.tf:
            if not args.symbol:
                raise ValueError("Informe --symbol junto com --tf. Esta leitura não varre a conta.")
            result = derived.public_summary(derived.read_stored(
                ctx.db, args.symbol, args.tf, start=args.start, end=args.end), head=args.head)
        else:
            listed = derived.list_stored(ctx.db)
            if args.symbol:
                listed = [item for item in listed if item["symbol"] == args.symbol]
            result = dict(series=listed, note=derived.NOTE, kind="stored_derived_index")
        if result.get("kind") == "stored_exact_day_comparison":
            cli._print(f"{result['symbol']} · grade {result['grid_timeframe']} · {result['bars']} fechamentos · "
                       f"{result['grid_gap_count']} intervalos sem grupo completo. {result['note']}")
            cli._print("Comparação sincronizada no fechamento do tempo mais curto. Não cria candles nem congela snapshot.")
        elif result.get("kind") == "stored_derived":
            cli._print(result["note"])
            for item in result["series"]:
                cli._print(f"{result['symbol']} {item['timeframe']}: {item['rows']} barras completas, "
                           f"{item['gaps']} intervalos sem grupo completo. {result['note']}")
        else:
            cli._print(result["note"])
            if not result["series"]:
                cli._print("Nenhuma barra de dia exato gravada. Gere com resample --session-aware. "
                           "Esta leitura não cria candles.")
            for item in result["series"]:
                cli._print(f"{item['symbol']} {item['timeframe']}: {item['rows']} barras, "
                           f"{item['start']} → {item['end']}. {item['note']}")
    elif args.laboratory_action == "freeze":
        snapshots = service.capture_selection(ctx.db, symbol=args.symbol, timeframes_selected=args.tf,
                                              start=args.start, end=args.end, stored_features=args.stored_feature or ())
        result = dict(manifests=[dict(id=item.manifest_id, symbol=item.manifest["symbol"],
                                     timeframe=item.manifest["timeframe"],
                                     **({"stored_features":item.manifest["selection"]["stored_features"]}
                                        if item.manifest["schema_version"] == datasets.FEATURE_VERSION else {}))
                                for item in snapshots])
    else:
        from mt5_extracao.automation.protocol import loads
        params = loads(args.params)
        if not isinstance(params, dict):
            raise ValueError("--params exige objeto JSON de parâmetros.")
        stored = args.stored_feature or ()
        if args.strategy_only and stored:
            raise ValueError("--strategy-only não combina com --stored-feature; escolha os canais explicitamente.")
        feature_ids = [] if args.strategy_only else args.feature if args.feature is not None else [] if stored else service.DEFAULT_FEATURES
        extras = [key for key in (args.compare or []) if key != args.strategy]
        common = dict(feature_ids=feature_ids, as_of=args.as_of, stored_features=stored)
        if args.links:
            from mt5_extracao.automation.protocol import loads
            path = Path(args.links).expanduser()
            if path.stat().st_size > 64*1024:
                raise ValueError("Seleção de vínculos excede64KiB.")
            selected_links = loads(path.read_text(encoding="utf-8"))
            snapshots = [datasets.load(ctx.db, ident) for ident in args.manifest]
            if not snapshots:
                raise ValueError("Informe o snapshot do ativo de mercado.")
            common.update(linked_sources=service.capture_linked_sources(ctx.db,
                target_symbol=snapshots[0].manifest["symbol"], selections=selected_links, frozen_only=True), timeline_basis=args.timeline_basis)
        if extras:
            if args.strategy is None and params:
                raise ValueError("Parâmetros exigem estratégia explícita.")
            items = [dict(key=args.strategy, params=params)] if args.strategy is not None else []
            items.extend(extras)
            result = service.analyze([datasets.load(ctx.db, ident) for ident in args.manifest],
                strategies=items, **common)
        else:
            result = service.analyze([datasets.load(ctx.db, ident) for ident in args.manifest],
                strategy_key=args.strategy, strategy_params=params, **common)
        if args.out:
            try:
                with Path(args.out).expanduser().open("x", encoding="utf-8") as handle:
                    json.dump(result, handle, ensure_ascii=False, allow_nan=False)
            except OSError as error:
                raise ValueError("Escolha um arquivo novo em diretório gravável: " + str(error)) from error
    cli._OUT["data"].update(result)
    cli._print("Laboratório: " + args.laboratory_action + " concluído.")
    return cli.EXIT_OK
