"""`mt5x trades ...` (Laboratório, Fase 16). Registrado em cli.build_parser por `add_parser`; a saída usa os auxiliares
do cli. Cada tarefa da Fase 16 acrescenta aqui o seu subcomando (T16.12 fecha o conjunto)."""
import argparse


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
