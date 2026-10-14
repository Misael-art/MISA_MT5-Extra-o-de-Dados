"""`mt5x trades ...` (Laboratório, Fase 16). Registrado em cli.build_parser por `add_parser`; a saída usa os auxiliares
do cli. Cada tarefa da Fase 16 acrescenta aqui o seu subcomando (T16.12 fecha o conjunto)."""
import argparse


def add_parser(sub) -> None:
    p = sub.add_parser("trades", help="Laboratório: livro de operações dos robôs (importar, consultar, analisar)")
    ts = p.add_subparsers(dest="trades_command", required=True)
    q = ts.add_parser("import", help="traz para o livro os backtests já gravados que ainda não estão nele")
    q.add_argument("--include-hidden", action="store_true", help="inclui execuções auxiliares (bateria, paridade)")
    ts.add_parser("runs", help="lista os runs do livro (robô, ativo, timeframe, operações, completo ou parcial)")
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
    raise ValueError(f"subcomando desconhecido: {args.trades_command}")

