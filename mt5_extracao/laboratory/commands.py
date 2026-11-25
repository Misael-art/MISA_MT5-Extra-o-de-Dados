"""Comandos do mesmo Laboratório usado pela UI; sem dependência de desktop."""
from pathlib import Path
import json

import pandas as pd

from mt5_extracao.analytics import datasets
from . import service


def add_parser(sub):
    parser = sub.add_parser("laboratory", help="séries, confluências e comparação exploratória entre timeframes")
    actions = parser.add_subparsers(dest="laboratory_action", required=True)
    actions.add_parser("ultra-capabilities", help="capacidades do replay Ultra offline").set_defaults(func=run)
    supported = actions.add_parser("ultra-tester-capabilities", help="capacidades do livro e do executor para o pacote")
    supported.add_argument("--package", help="pacote congelado a conferir")
    supported.set_defaults(func=run)
    market = actions.add_parser("ultra-market-capture", help="congela ticks do broker na DEMO já aberta")
    for option in ("symbol", "server", "start", "end", "out"):
        market.add_argument("--" + option, required=True)
    market.add_argument("--chunk-minutes", type=int, default=15)
    market.set_defaults(func=run)
    market = actions.add_parser("ultra-market-verify", help="verifica o manifesto e os blocos congelados")
    market.add_argument("--snapshot", required=True)
    market.set_defaults(func=run)
    market = actions.add_parser("ultra-market-export", help="exporta o Expert passivo que grava ticks do Tester")
    for option in ("snapshot", "out", "attempt"):
        market.add_argument("--" + option, required=True)
    market.set_defaults(func=run)
    market = actions.add_parser("ultra-market-compare", help="compara ticks do broker e do OnTick do Tester")
    for option in ("snapshot", "tape", "binding", "journal"):
        market.add_argument("--" + option, required=True)
    market.set_defaults(func=run)
    market = actions.add_parser("ultra-market-tester-run", help="compila e roda a sonda ou o controle S09 no Tester")
    market.add_argument("--snapshot", required=True)
    market.add_argument("--out", required=True)
    market.add_argument("--mode", choices=("ticks", "s09_cycle"), default="ticks")
    market.add_argument("--timeout", type=int, default=300)
    market.set_defaults(func=run)
    market = actions.add_parser("ultra-market-demo-export", help="gera observador do gráfico DEMO com ordens desativadas")
    for option in ("snapshot", "out", "attempt"):
        market.add_argument("--" + option, required=True)
    market.set_defaults(func=run)
    explain = actions.add_parser("ultra-explain", help="consulta decisões, regras e orçamento de um tick da carteira")
    explain.add_argument("--receipt", required=True)
    explain.add_argument("--ordinal", type=int, required=True, help="índice do tick contado a partir de zero")
    explain.set_defaults(func=run)
    batch = actions.add_parser("ultra-batch", help="avalia em sequência as definições por ativo")
    batch.add_argument("--definition", action="append", required=True, help="arquivo JSON por ativo, em ordem")
    batch.set_defaults(func=run)
    freeze = actions.add_parser("ultra-freeze", help="congela política e referência ao snapshot")
    freeze.add_argument("--definition", required=True)
    freeze.set_defaults(func=run)
    report = actions.add_parser("ultra-report", help="exporta linha do tempo e vetos da carteira")
    report.add_argument("--receipt", required=True)
    report.add_argument("--reference", help="recibo completo nas mesmas sessões, riscos e custos")
    report.add_argument("--out", required=True)
    report.set_defaults(func=run)
    actions.add_parser("ultra-list", help="lista recibos da carteira Ultra").set_defaults(func=run)
    pack = actions.add_parser("ultra-compile", help="empacota o replay da pesquisa para verificação")
    pack.add_argument("--receipt", required=True)
    pack.add_argument("--out", required=True)
    pack.set_defaults(func=run)
    verify = actions.add_parser("ultra-verify", help="confere o pacote com a fonte e o livro")
    verify.add_argument("--package", required=True)
    verify.set_defaults(func=run)
    entries = actions.add_parser("ultra-entry-compare", help="compara entrada direta e gradiente linear no livro compartilhado")
    entries.add_argument("--package", required=True)
    entries.add_argument("--spacing-ticks", type=int, required=True)
    entries.set_defaults(func=run)
    expert = actions.add_parser("ultra-tester-export", help="gera livro concorrente MQL5 e ticks do pacote para o Tester")
    expert.add_argument("--package", required=True)
    expert.add_argument("--out", required=True, help="nova pasta para fontes e cotações")
    expert.add_argument("--prepare-tester", action="store_true", help="gera o contrato e a série prévia do Tester")
    expert.add_argument("--restart-id", help="ID de 32 dígitos hexadecimais para gerar a sonda de retomada")
    expert.set_defaults(func=run)
    capture = actions.add_parser("ultra-tester-compare", help="compara eventos e saldo do journal MQL5")
    capture.add_argument("--package", required=True)
    capture.add_argument("--journal", required=True)
    capture.add_argument("--origin", choices=("strategy_tester", "terminal_history", "terminal_restart", "strategy_tester_orders", "strategy_tester_sliced"), default="strategy_tester")
    capture.add_argument("--out", help="novo relatório HTML navegável por ciclo para a origem de ordens")
    capture.set_defaults(func=run)
    parity = actions.add_parser("ultra-parity", help="confere o primeiro desvio no livro normalizado do Tester")
    parity.add_argument("--package", required=True)
    parity.add_argument("--trace", help="livro JSON do Tester")
    parity.add_argument("--journal", help="journal bruto vinculado ao livro")
    parity.set_defaults(func=run)
    study = actions.add_parser("ultra-study-freeze", help="registra cortes, critérios e orçamento antes da busca")
    study.add_argument("--receipt", required=True)
    study.add_argument("--plan", required=True)
    study.set_defaults(func=run)
    study = actions.add_parser("ultra-study-run", help="avalia política fixa e revisões a cada 7, 14 e 15 dias corridos")
    study.add_argument("--study", required=True)
    study.set_defaults(func=run)
    for name in ("ultra-replay", "ultra-inspect", "ultra-compare"):
        action = actions.add_parser(name, help="retoma ou consulta a carteira pelo recibo")
        action.add_argument("--receipt", required=True)
        if name == "ultra-replay":
            action.add_argument("--until", type=int, help="quantidade absoluta de cotações a confirmar")
        elif name == "ultra-inspect":
            action.add_argument("--timeline", action="store_true")
        action.set_defaults(func=run)
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
    actions.add_parser("pattern-sources", help="fontes locais para T13.4 com namespace da base").set_defaults(func=run)
    evaluations = actions.add_parser("pattern-evaluations", help="avaliações T13.4 persistidas, sem executar pesquisa")
    evaluations.add_argument("--protocol")
    evaluations.add_argument("--symbol")
    evaluations.add_argument("--tf")
    evaluations.add_argument("--as-of", help="corte UTC com fuso explícito")
    evaluations.add_argument("--html", help="novo relatório HTML de consulta")
    evaluations.set_defaults(func=run)
    evaluation = actions.add_parser("pattern-evaluation", help="inspeciona um recibo T13.4 persistido")
    evaluation.add_argument("--protocol", required=True)
    evaluation.add_argument("--request", required=True)
    evaluation.add_argument("--manifest", required=True)
    evaluation.add_argument("--as-of", help="corte UTC com fuso explícito")
    evaluation.add_argument("--facts", action="store_true", help="inclui fatos gravados de sinal e desfecho")
    evaluation.add_argument("--html", help="novo relatório HTML de consulta")
    evaluation.set_defaults(func=run)
    for action in ("pattern-qualify", "pattern-freeze"):
        candidate = actions.add_parser(action, help="qualifica recorte T13.4" if action.endswith("qualify")
                                       else "congela recorte elegível no store existente")
        candidate.add_argument("--symbol", required=True)
        candidate.add_argument("--tf", required=True)
        candidate.add_argument("--family", required=True,
                               choices=["reversal", "continuation_time", "gap", "breakout_failure"])
        candidate.add_argument("--start", help="início no relógio da base")
        candidate.add_argument("--end", help="fim no relógio da base")
        candidate.set_defaults(func=run)
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
    report.add_argument("--evaluation",help="recibo estatístico congelado opcional desta análise")
    report.add_argument('--execution',help='recibo observacional de execução na mesma revisão')
    report.set_defaults(func=run)
    candle = actions.add_parser("candle", help="preço, séries e recibos de um candle do relatório congelado")
    candle.add_argument("--analysis", required=True)
    candle.add_argument("--index", required=True, type=int)
    candle.add_argument('--execution',help='recibo observacional congelado opcional')
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
    runs=actions.add_parser('runs',help='índice paginado de execuções gravadas, sem curvas/backtest')
    runs.add_argument('--offset',type=int,default=0);runs.add_argument('--limit',type=int,default=50);runs.set_defaults(func=run)
    comparison=actions.add_parser('compare-runs',help='compara execuções gravadas por ativo, preset ou timeframe')
    comparison.add_argument('--run',action='append',required=True)
    comparison.add_argument('--mode',choices=['same_asset','same_preset','same_strategy'],required=True)
    comparison.add_argument('--out');comparison.add_argument('--html');comparison.set_defaults(func=run)
    linked_execution=actions.add_parser('execution-link',help='concilia observações do run com o snapshot da grade principal')
    linked_execution.add_argument('--analysis',required=True);linked_execution.add_argument('--run',required=True);linked_execution.add_argument('--out',required=True);linked_execution.set_defaults(func=run)
    for action in ('protocol-register','protocol-evaluate'):
        q=actions.add_parser(action,help='protocolo estatístico explícito; usa ledger/inferência existentes')
        q.add_argument('--analysis',required=True)
        if action=='protocol-register':q.add_argument('--definition',required=True)
        else:
            q.add_argument('--protocol',required=True);q.add_argument('--key',required=True)
            q.add_argument('--timeout-seconds',type=float,default=120);q.add_argument('--max-memory-mb',type=float,default=1024)
        q.add_argument('--out',help='novo recibo JSON; recusa sobrescrever')
        q.set_defaults(func=run)


def run(args, ctx):
    from mt5_extracao import cli
    if args.laboratory_action.startswith("ultra-"):
        from . import ultra
        if args.laboratory_action.startswith("ultra-market-"):
            from . import ultra_market
            action = args.laboratory_action
            if action == "ultra-market-capture":
                result = ultra_market.capture_configured(ctx.config, ctx.args.config, args.symbol, args.start,
                    args.end, args.out, expected_server=args.server, chunk_minutes=args.chunk_minutes)
            elif action == "ultra-market-verify":
                result = ultra_market.verify(args.snapshot)
            elif action == "ultra-market-export":
                result = ultra_market.export_probe(args.snapshot, args.out, attempt=args.attempt)
            elif action == "ultra-market-tester-run":
                from ..robots.ultra_market_runner import run as run_market
                result = run_market(ctx.config, args.snapshot, args.out, mode=args.mode, timeout=args.timeout)
            elif action == "ultra-market-demo-export":
                result = ultra_market.export_demo_observer(args.snapshot, args.out, attempt=args.attempt)
            else:
                from ..robots.mt5_tools import read_log_text
                result = ultra_market.compare_probe(args.snapshot, args.tape, binding=args.binding,
                    agent_journal=read_log_text(Path(args.journal)))
        elif args.laboratory_action == "ultra-capabilities":
            result = ultra.capabilities()
        elif args.laboratory_action == "ultra-tester-capabilities":
            from .ultra_tester import capabilities, package_capabilities
            result = package_capabilities(ctx.db, json.loads(Path(args.package).read_text(encoding="utf-8-sig"))) if args.package else capabilities()
        elif args.laboratory_action == "ultra-explain":
            from .ultra_report import explain
            result = explain(ctx.db, args.receipt, args.ordinal)
        elif args.laboratory_action == "ultra-batch":
            items = []
            for name in args.definition:
                try:
                    frozen = ultra.freeze(ctx.db, json.loads(Path(name).read_text(encoding="utf-8-sig")))
                    replay = ultra.replay(ctx.db, frozen["receipt"])
                    item = {key: replay[key] for key in ("receipt", "manifest_id", "status", "reasons", "progress")}
                except (OSError, ValueError) as exc:
                    item = {"status": "invalid_definition", "reason": str(exc)}
                items.append(dict(item, definition=name))
            result = {"items": items}
        elif args.laboratory_action == "ultra-freeze":
            result = ultra.freeze(ctx.db, json.loads(Path(args.definition).read_text(encoding="utf-8-sig")))
        elif args.laboratory_action == "ultra-replay":
            result = ultra.replay(ctx.db, args.receipt, until=args.until)
        elif args.laboratory_action == "ultra-list":
            from .ultra_report import list_receipts
            result = {"receipts": list_receipts(ctx.db)}
        elif args.laboratory_action == "ultra-entry-compare":
            from .ultra_entries import compare
            result = compare(ctx.db, json.loads(Path(args.package).read_text(encoding="utf-8-sig")), args.spacing_ticks)
        elif args.laboratory_action == "ultra-report":
            from .ultra_report import export
            result = export(ctx.db, args.receipt, args.out, reference=args.reference)
        elif args.laboratory_action == "ultra-compile":
            from ..presets.compiler import compile_ultra_research
            result = compile_ultra_research(ctx.db, args.receipt)
            with Path(args.out).open("x", encoding="utf-8") as handle:
                json.dump(result, handle, ensure_ascii=False, allow_nan=False, indent=2)
        elif args.laboratory_action in ("ultra-tester-export", "ultra-tester-compare"):
            from . import ultra_tester
            package = json.loads(Path(args.package).read_text(encoding="utf-8-sig"))
            if args.laboratory_action == "ultra-tester-export":
                result = ultra_tester.export(ctx.db, package, args.out, restart_id=args.restart_id, prepare_tester=args.prepare_tester)
            else:
                result = ultra_tester.compare(ctx.db, package, Path(args.journal).read_bytes(), origin=args.origin)
                if args.out:
                    from .ultra_cycle_report import write_report
                    result["cycle_report"] = write_report(result, args.out)
        elif args.laboratory_action == "ultra-parity":
            from .ultra_parity import compare_trace
            if bool(args.trace) != bool(args.journal):
                raise ValueError("Forneça o livro e seu journal juntos.")
            package = json.loads(Path(args.package).read_text(encoding="utf-8-sig"))
            trace = json.loads(Path(args.trace).read_text(encoding="utf-8-sig")) if args.trace else None
            result = compare_trace(ctx.db, package, trace, journal=Path(args.journal).read_bytes() if args.journal else None)
        elif args.laboratory_action == "ultra-verify":
            from ..presets.compiler import verify_ultra_research
            result = verify_ultra_research(ctx.db, json.loads(Path(args.package).read_text(encoding="utf-8-sig")))
        elif args.laboratory_action == "ultra-study-freeze":
            from .ultra_study import register
            result = register(ctx.db, args.receipt, json.loads(Path(args.plan).read_text(encoding="utf-8-sig")))
        elif args.laboratory_action == "ultra-study-run":
            from .ultra_study import evaluate
            result = evaluate(ctx.db, args.study)
        elif args.laboratory_action == "ultra-compare":
            from .ultra_evaluation import compare
            result = compare(ctx.db, args.receipt)
        else:
            result = ultra.inspect_receipt(ctx.db, args.receipt, timeline=args.timeline)
    elif args.laboratory_action in ("pattern-evaluations", "pattern-evaluation"):
        from mt5_extracao import sdk
        from .pattern_report import render
        result = (sdk.pattern_evaluations(ctx.db, protocol_id=args.protocol, symbol=args.symbol,
                                          timeframe=args.tf, as_of=args.as_of)
                  if args.laboratory_action == "pattern-evaluations" else
                  sdk.pattern_evaluation(ctx.db, args.protocol, args.request, args.manifest,
                                         as_of=args.as_of, include_facts=args.facts))
        if args.html:
            with Path(args.html).expanduser().open("x", encoding="utf-8") as handle:
                handle.write(render(result))
        if isinstance(result, list):
            result = {"evaluations": result}
    elif args.laboratory_action in ("pattern-sources", "pattern-qualify", "pattern-freeze"):
        from mt5_extracao import sdk
        result = (sdk.pattern_sources(ctx.db) if args.laboratory_action == "pattern-sources" else
                  (sdk.qualify_pattern_source if args.laboratory_action == "pattern-qualify"
                   else sdk.freeze_pattern_source)(ctx.db, symbol=args.symbol, timeframe=args.tf,
                                                   family=args.family, start=args.start, end=args.end))
    elif args.laboratory_action=='execution-link':
        from mt5_extracao import sdk
        path=Path(args.analysis).expanduser();out=Path(args.out).expanduser()
        if path.stat().st_size>16*1024*1024 or out.exists():raise ValueError('Análise exige até16MiB e saída nova.')
        frozen=service.load_report(path.read_text(encoding='utf-8'));result=sdk.link_laboratory_execution(ctx.db,frozen,args.run)
        with out.open('x',encoding='utf-8') as handle:json.dump(result,handle,ensure_ascii=False,allow_nan=False)
    elif args.laboratory_action=='runs':
        from mt5_extracao.robots import store
        result=dict(runs=store.run_index(ctx.db,limit=args.limit,offset=args.offset),opened_backtest=False)
    elif args.laboratory_action=='compare-runs':
        from mt5_extracao import sdk
        for path in (args.out,args.html):
            if path and Path(path).expanduser().exists():raise ValueError('Escolha arquivos novos para a comparação.')
        result=sdk.compare_laboratory_runs(ctx.db,args.run,mode=args.mode)
        if args.html:sdk.export_laboratory_runs(result,args.html)
        if args.out:
            with Path(args.out).expanduser().open('x',encoding='utf-8') as out:json.dump(result,out,ensure_ascii=False,allow_nan=False)
    elif args.laboratory_action in ('protocol-register','protocol-evaluate'):
        from mt5_extracao import sdk
        def read(path):
            p=Path(path).expanduser()
            if p.stat().st_size>16*1024*1024:raise ValueError('Entrada excede16MiB.')
            return json.loads(p.read_text(encoding='utf-8'))
        frozen=service.load_report(json.dumps(read(args.analysis),allow_nan=False))
        if args.out and Path(args.out).expanduser().exists():raise ValueError('Escolha saída nova antes de registrar/avaliar.')
        result=(sdk.register_laboratory_protocol(ctx.db,frozen,read(args.definition)) if args.laboratory_action=='protocol-register' else
                sdk.evaluate_laboratory_protocol(ctx.db,frozen,args.protocol,key=args.key,timeout_seconds=args.timeout_seconds,max_memory_mb=args.max_memory_mb))
        if args.out:
            with Path(args.out).expanduser().open('x',encoding='utf-8') as out:json.dump(result,out,ensure_ascii=False,allow_nan=False)
    elif args.laboratory_action in ("report", "candle", "crossings"):
        from mt5_extracao import sdk
        path = Path(args.analysis).expanduser()
        if path.stat().st_size > 16*1024*1024:
            raise ValueError("Análise excede16MiB; reduza o recorte antes de exportar.")
        frozen = service.load_report(path.read_text(encoding="utf-8"))
        evaluation=None
        if getattr(args,'evaluation',None):
            receipt_path=Path(args.evaluation).expanduser()
            if receipt_path.stat().st_size>16*1024*1024:raise ValueError('Recibo excede16MiB.')
            evaluation=json.loads(receipt_path.read_text(encoding='utf-8'))
        execution=None
        if getattr(args,'execution',None):
            receipt_path=Path(args.execution).expanduser()
            if receipt_path.stat().st_size>16*1024*1024:raise ValueError('Recibo excede16MiB.')
            execution=json.loads(receipt_path.read_text(encoding='utf-8'))
        result = (sdk.export_laboratory_report(frozen, args.out, evaluation=evaluation,execution=execution) if args.laboratory_action == "report" else
                  sdk.laboratory_candle(frozen, args.index,execution=execution) if args.laboratory_action == "candle" else
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
