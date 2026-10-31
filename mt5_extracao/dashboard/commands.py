"""Entrada CLI aditiva, sem construir conector nem banco para abrir a casca."""
import math
import threading


def add_parser(sub):
    parser = sub.add_parser("dashboard", help="Dashboard Contínuo local, somente leitura")
    parser.add_argument("--port", type=int, help="porta local; padrão 5055 configurável")
    parser.add_argument("--no-browser", action="store_true", help="mostra URL sem abrir navegador")
    parser.add_argument("--seconds", type=float, help="encerra após N segundos (QA); padrão até Ctrl+C")
    parser.add_argument("--symbol", help="ativo a ler, sem coleta/cálculo")
    parser.add_argument("--tf", help="timeframe nativo da seleção")
    parser.add_argument("--as-of", help="corte com fuso explícito; obrigatório com ativo")
    parser.add_argument("--from", dest="date_from", help="início da janela, com fuso; padrão30dias")
    parser.add_argument("--to", dest="date_to", help="fim exclusivo, até o corte")
    parser.add_argument("--version", nargs=2, action="append", metavar=("ID", "VERSAO"),
                        help="resolve versão explícita de um indicador; repetível")
    parser.add_argument("--fixture-label", help="identifica explicitamente uma fotografia de QA sintética")
    parser.add_argument("--html", help="arquiva o mesmo documento autossuficiente; destino novo em exports/")
    parser.set_defaults(func=run)


def run(args, ctx):
    from mt5_extracao import cli
    from .server import DashboardServer
    from .opener import open_dashboard
    if args.seconds is not None and (not math.isfinite(args.seconds) or args.seconds <= 0):
        raise ValueError("--seconds precisa ser um número positivo e finito.")
    if args.port is not None and not 1 <= args.port <= 65535:
        raise ValueError("--port precisa estar entre 1 e 65535.")
    snapshot = None
    if any((args.symbol, args.tf, args.as_of, args.date_from, args.date_to, args.version)):
        if not all((args.symbol, args.tf, args.as_of)):
            raise ValueError("Informe --symbol, --tf e --as-of juntos.")
        from .generator import generate
        pairs = args.version or []
        if len({pair[0] for pair in pairs}) != len(pairs):
            raise ValueError("Cada ID precisa de uma única --version.")
        snapshot = generate(ctx.db, symbol=args.symbol, timeframe=args.tf, as_of=args.as_of,
                            start=args.date_from, end=args.date_to, versions=dict(pairs), fixture_label=args.fixture_label)
        for error in snapshot["errors"]:
            cli._print(error["message"])
    if args.html:
        if snapshot is None:
            raise ValueError("Para arquivar informe --symbol, --tf e --as-of.")
        from .document import export
        receipt = export(snapshot, args.html)
        cli._print("Fotografia arquivada: " + receipt["path"])
        cli._data(archive=receipt)
    with DashboardServer(ctx.config, port=args.port, snapshot=snapshot) as server:
        if not args.no_browser:
            result = open_dashboard(server, ctx.config)
            cli._print(result.message)
        cli._print(server.url)
        # CLI normal precisa mostrar a URL enquanto atende, não só ao encerrar.
        if not cli._OUT["json"]:
            import sys
            sys.stdout.flush()
        cli._data(url=server.url, port=server.port, phase="DASH-02" if snapshot else "DASH-01",
                  snapshot_id=snapshot["snapshot_id"] if snapshot else None)
        try:
            threading.Event().wait(args.seconds)
        except KeyboardInterrupt:
            pass
    return cli.EXIT_OK
