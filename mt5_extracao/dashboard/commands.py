"""Entrada CLI aditiva, sem construir conector nem banco para abrir a casca."""
import math
import threading


def add_parser(sub):
    parser = sub.add_parser("dashboard", help="Dashboard Contínuo local, somente leitura")
    parser.add_argument("--port", type=int, help="porta local; padrão 5055 configurável")
    parser.add_argument("--no-browser", action="store_true", help="mostra URL sem abrir navegador")
    parser.add_argument("--seconds", type=float, help="encerra após N segundos (QA); padrão até Ctrl+C")
    parser.set_defaults(func=run)


def run(args, ctx):
    from mt5_extracao import cli
    from .server import DashboardServer
    from .opener import open_dashboard
    if args.seconds is not None and (not math.isfinite(args.seconds) or args.seconds <= 0):
        raise ValueError("--seconds precisa ser um número positivo e finito.")
    if args.port is not None and not 1 <= args.port <= 65535:
        raise ValueError("--port precisa estar entre 1 e 65535.")
    with DashboardServer(ctx.config, port=args.port) as server:
        if not args.no_browser:
            result = open_dashboard(server, ctx.config)
            cli._print(result.message)
        cli._print(server.url)
        # CLI normal precisa mostrar a URL enquanto atende, não só ao encerrar.
        if not cli._OUT["json"]:
            import sys
            sys.stdout.flush()
        cli._data(url=server.url, port=server.port, phase="DASH-01")
        try:
            threading.Event().wait(args.seconds)
        except KeyboardInterrupt:
            pass
    return cli.EXIT_OK
