"""`mt5x trades ...` (Laboratório, Fase 16). Registrado em cli.build_parser por `add_parser`; a saída usa os auxiliares
do cli. Cada tarefa da Fase 16 acrescenta aqui o seu subcomando (T16.12 fecha o conjunto)."""
import argparse
from mt5_extracao import profit_factor


def add_parser(sub) -> None:
    p = sub.add_parser("trades", help="Laboratório: livro de operações dos robôs (importar, consultar, analisar)")
    ts = p.add_subparsers(dest="trades_command", required=True)
    q = ts.add_parser("import", help="traz para o livro os backtests já gravados que ainda não estão nele")
    q.add_argument("--include-hidden", action="store_true", help="inclui execuções auxiliares (bateria, paridade)")
    ts.add_parser("runs", help="lista os runs do livro (robô, ativo, timeframe, operações, completo ou parcial)")
    c = ts.add_parser("calendar", help="calendário econômico de várias fontes (CSV e MT5)")
    cs = c.add_subparsers(dest="calendar_command", required=True)
    q = cs.add_parser("import", help="importa uma fonte para o calendário unido")
    q.add_argument("--source", required=True, choices=("csv", "mt5"))
    q.add_argument("--path", help="arquivo (padrão: [SCREENER] events_file, ou a exportação mais recente do MT5)")
    cs.add_parser("status", help="eventos por fonte, período coberto e quantos de impacto alto")
    cs.add_parser("install", help="instala e compila o script MT5X_CalendarExport no MT5")
    q = cs.add_parser("export", help="pede ao MT5 a exportação do calendário (abre o terminal com o script)")
    q.add_argument("--days-back", type=int, help="padrão: [TRADE_ANALYTICS] calendar_days_back")
    q.add_argument("--days-forward", type=int, help="padrão: [TRADE_ANALYTICS] calendar_days_forward")
    q.add_argument("--countries", help="ex.: BR,US (padrão: [TRADE_ANALYTICS] calendar_countries; vazio = todos)")
    q.add_argument("--close-terminal", action="store_true", help="fecha o MT5 antes, se estiver aberto")
    for name, helptext in (("report", "números do robô (todos os ativos) ou do ativo (todos os robôs), por dimensão"),
                           ("insights", "procura insights (validados só se confirmados na reserva final)"),
                           ("simulate", "simula regras de filtro: procura, provas do walk-forward e reserva final")):
        q = ts.add_parser(name, help=helptext)
        lens = q.add_mutually_exclusive_group(required=True)
        lens.add_argument("--robot", help="lente por robô, ex.: python:donchian")
        lens.add_argument("--symbol", help="lente por ativo, ex.: WIN$N")
        if name == "report":
            q.add_argument("--by", default="day_of_week,time_bucket,event_impact,volatility_bucket",
                           help="dimensões separadas por vírgula")
        if name == "insights":
            q.add_argument("--save", action="store_true", help="grava em _trade_insights")
        if name == "simulate":
            q.add_argument("--exclude-days", default="", help="dias a excluir, 0 = segunda ... 4 = sexta (ex.: 4)")
            q.add_argument("--window", help="operar só entre HH:MM-HH:MM")
            q.add_argument("--no-news", action="store_true", help="excluir notícia de impacto alto")
            q.add_argument("--no-high-vol", action="store_true", help="excluir volatilidade alta")
            q.add_argument("--rules", help="regras em JSON (lista), como as do arquivo exportado")
            q.add_argument("--export", action="store_true", help="grava a proposta em exports/propostas/")
    q = ts.add_parser("compare", help="o mesmo run re-simulado em vários modos de execução")
    q.add_argument("--run", required=True, help="run_id (veja mt5x trades runs)")
    q.add_argument("--modes", default="", help="modos separados por vírgula (padrão: os de saída)")
    q.add_argument("--preset", help="preset do robô (para as travas do martingale)")
    p.set_defaults(func=run)


def run(args: argparse.Namespace, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import importers, ledger
    out, data = cli._print, cli._data
    db = ctx.db
    if args.trades_command == "import":
        done = importers.import_existing_runs(db, include_hidden=args.include_hidden)
        out(f"{done['runs']} run(s) importado(s), {done['trades']} operação(ões); {done['skipped']} já estavam no "
            "livro ou são auxiliares.")
        data(**done)
        return cli.EXIT_OK
    if args.trades_command == "runs":
        items = ledger.runs(db)
        for r in items:
            out(f"{r['run_id'][:12]}\t{r['robot_key']}\t{r['symbol']}\t{r['timeframe']}\t{r['trades']} operações\t"
                f"{r['completeness']}")
        out(f"\n{len(items)} run(s) no livro.")
        data(runs=items)
        return cli.EXIT_OK
    if args.trades_command == "calendar":
        return _calendar(args, ctx)
    if args.trades_command in ("report", "insights", "simulate"):
        return _analysis(args, ctx)
    if args.trades_command == "compare":
        return _compare(args, ctx)
    raise ValueError(f"subcomando desconhecido: {args.trades_command}")


def _calendar(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import calendar as cal
    from mt5_extracao.trade_analytics.calendar import mt5_export
    out, data = cli._print, cli._data
    config = ctx.config
    sub = args.calendar_command
    if sub == "import":
        count = cal.import_source(ctx.db, config, args.source, args.path)
        out(f"{count} evento(s) importado(s) da fonte {args.source}.")
        data(source=args.source, events=count)
        return cli.EXIT_OK
    if sub == "status":
        info = cal.status(ctx.db)
        for source, s in info.items():
            out(f"{source}: {s['events']} evento(s) de {s['first']:%Y-%m-%d} a {s['last']:%Y-%m-%d}, "
                f"{s['high_impact']} de impacto alto")
        if not info:
            out("Calendário vazio. Importe o CSV (--source csv) ou exporte do MT5 (mt5x trades calendar export).")
        merged = cal.merged(ctx.db)
        out(f"Unido: {len(merged)} evento(s) depois de juntar as fontes.")
        data(sources=info, merged_events=len(merged))
        return cli.EXIT_OK
    if sub == "install":
        result = mt5_export.install(config)
        out(f"Script instalado em {result['source']}: " + ("compilado." if result["ok"] else
                                                         "NÃO compilou: " + "; ".join(result["errors"])))
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL
    if sub == "export":
        from mt5_extracao.robots import mt5_tools
        get = lambda key, default: config.get("TRADE_ANALYTICS", key, fallback=default)  # noqa: E731
        days_back = args.days_back or int(get("calendar_days_back", "1095") or 1095)
        days_forward = args.days_forward or int(get("calendar_days_forward", "30") or 30)
        countries = args.countries if args.countries is not None else get("calendar_countries", "")
        request = mt5_export.write_request(config, days_back, days_forward, countries,
                                           int(get("calendar_min_importance", "0") or 0))
        if args.close_terminal and mt5_tools.terminal_running(config):
            mt5_tools.close_terminal(config=config)
        mt5_export.start(config)
        out(f"Pedido gravado em {request}. O MT5 abre e roda o script; quando terminar, importe com "
            "`mt5x trades calendar import --source mt5`.")
        data(request=str(request))
        return cli.EXIT_OK
    raise ValueError(f"subcomando desconhecido: {sub}")


def _lab(args, ctx):
    from mt5_extracao.trade_analytics import aggregate
    filters = {"robot_key": args.robot} if args.robot else {"symbol": args.symbol}
    lab = aggregate.frame(ctx.db, ctx.config, **filters)
    if lab.empty:
        raise ValueError("Nenhuma operação no livro para essa seleção (veja mt5x trades runs; importe com mt5x trades "
                         "import).")
    return lab, ("robot:" + args.robot) if args.robot else ("symbol:" + args.symbol)


def _analysis(args, ctx) -> int:
    import json
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import aggregate, context, export, insights, rules
    out, data = cli._print, cli._data
    lab, scope = _lab(args, ctx)
    population_context = context.population(lab, aggregate._initial_equity(ctx.config))
    if args.trades_command == "report":
        total = aggregate.summarize(lab, aggregate._initial_equity(ctx.config), aggregate.min_trades(ctx.config))
        out(f"{scope}: {total['n']} operações, acerto {total['win_rate'] * 100:.1f}%, fator de lucro "
            f"{profit_factor.text(total['profit_factor'])}, expectativa {total['expectancy']:.2f} (90%: {total['ci_low']} a "
            f"{total['ci_high']})")
        out(context.summary(population_context))
        out(context.details(population_context))
        tables = {}
        for dim in [d.strip() for d in args.by.split(",") if d.strip()]:
            if dim not in lab:
                raise ValueError(f"dimensão desconhecida: {dim}")
            table = aggregate.by_dimension(lab, [dim], ctx.config)
            tables[dim] = table.to_dict("records")
            out(f"\n{dim}:")
            for r in table.itertuples():
                out(f"  {getattr(r, dim)}\t{r.n} op.\tacerto {r.win_rate * 100:.0f}%\texpectativa {r.expectancy:.2f}"
                    + ("\t(amostra pequena)" if r.small else ""))
        lens = aggregate.robot_view(lab, ctx.config) if args.robot else aggregate.asset_view(lab, ctx.config)
        data(scope=scope, total=total, by=tables, lens=lens.to_dict("records"), context=population_context)
        return cli.EXIT_OK
    if args.trades_command == "insights":
        found = insights.discover(lab, ctx.config)
        for i in found:
            out(f"[{i.state}] {i.text}")
        if not found:
            out("Nenhum grupo com operações suficientes para testar.")
        if args.save:
            insights.save(ctx.db, scope, found)
        data(scope=scope, insights=[vars(i) for i in found])
        return cli.EXIT_OK
    chosen = json.loads(args.rules) if args.rules else []
    days = [int(d) for d in args.exclude_days.split(",") if d.strip()]
    if days:
        chosen.append({"tipo": "excluir_dia", "dias": days})
    if args.window:
        start, end = args.window.split("-")
        chosen.append({"tipo": "janela_horaria", "inicio": start.strip(), "fim": end.strip()})
    if args.no_news:
        chosen.append({"tipo": "excluir_noticia", "impacto_min": 3})
    if args.no_high_vol:
        chosen.append({"tipo": "excluir_volatilidade", "faixas": ["alta"]})
    if not chosen:
        raise ValueError("informe ao menos uma regra (--exclude-days, --window, --no-news, --no-high-vol ou --rules)")
    ev = rules.evaluate(lab, chosen, ctx.config)
    for part, label in (("search", "Procura"), ("holdout", "Reserva final")):
        b, a = ev[part]["before"], ev[part]["after"]
        out(f"{label}: expectativa {b['expectancy']:.2f} -> {a['expectancy']:.2f}; fator de lucro "
            f"{profit_factor.text(b['profit_factor'])} -> {profit_factor.text(a['profit_factor'])}; queda {b['max_drawdown_pct'] * 100:.1f}% -> "
            f"{a['max_drawdown_pct'] * 100:.1f}%")
    out(f"Provas do walk-forward: melhorou em {ev['folds_improved']} de {len(ev['folds'])}. {ev['note']}")
    path = export.proposal(scope, chosen, ev, context=population_context) if args.export else None
    if path:
        out(f"Proposta gravada em {path} (proposta: para virar preset passa por validate, bateria e paridade).")
    data(scope=scope, rules=chosen, evaluation=ev, context=population_context, proposal=str(path) if path else None)
    return cli.EXIT_OK


def _compare(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.robots import catalog
    from mt5_extracao.trade_analytics import ledger
    from mt5_extracao.trade_analytics.execution import runner
    out, data = cli._print, cli._data
    rows = ledger.load(ctx.db, run_id=args.run)
    if rows.empty:
        raise ValueError(f"run {args.run} não está no livro (veja mt5x trades runs)")
    robot = None
    try:
        robot = catalog.get_robot(catalog.load_catalog(ctx.config, ctx.db), str(rows["robot_key"].iloc[0]))
    except Exception:
        robot = None
    wanted = [m.strip() for m in args.modes.split(",") if m.strip()] or None
    table = runner.compare(ctx.db, ctx.config, args.run, wanted, robot=robot, preset=args.preset)
    result = {}
    for key, item in table.items():
        m = item["metrics"]
        out(f"{item['name']}: {m['n']} op., acerto {m['win_rate'] * 100:.1f}%, fator de lucro {profit_factor.text(m['profit_factor'])}, "
            f"expectativa {m['expectancy']:.2f}, queda {m['max_drawdown_pct'] * 100:.1f}%"
            + ("  [só estudo]" if item.get("study_only") else ""))
        result[key] = {"name": item["name"], "metrics": m, "study_only": item.get("study_only", False),
                       "triggers": (item.get("sequence") or {}).get("triggers"),
                       "safety_notes": item.get("safety_notes")}
    data(run=args.run, modes=result)
    return cli.EXIT_OK
