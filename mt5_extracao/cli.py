"""
Linha de comando do MT5 Extração (sem interface gráfica).

    mt5x doctor
    mt5x symbols [--group "*WIN*"]
    mt5x tables
    mt5x extract --symbols WIN$N,WDO$N --tf M1 --from 2024-01-01 [--to 2024-06-30] [--indicators]
    mt5x update  --symbols WIN$N --tf M1 [--indicators]
    mt5x export  --table win_n_1_minuto --format csv|excel [--out arquivo]
    mt5x quality --table win_n_1_minuto [--details]
    mt5x schedule --every 15m --symbols WIN$N --tf M1

Sem o pacote instalado: python -m mt5_extracao.cli ... (ou ./run.sh --cli ... / run.bat --cli ...).
Códigos de saída: 0 sucesso, 1 falha, 2 uso incorreto.
"""
import argparse
import logging
import os
import shlex
import sys
import threading
from datetime import datetime
from pathlib import Path

from mt5_extracao import services, timeframes

EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _print(msg=""):
    print(msg, flush=True)


def _err(msg):
    print(f"ERRO: {msg}", file=sys.stderr, flush=True)




class Context:
    """Componentes criados sob demanda a partir do config.ini."""

    def __init__(self, args):
        self.args = args
        self._config = self._db = self._connector = None

    @property
    def config(self):
        if self._config is None:
            self._config = services.load_config(self.args.config)
        return self._config

    @property
    def db(self):
        if self._db is None:
            self._db = services.create_db_manager(self.config, self.args.db)
            if not self._db.is_connected():
                raise RuntimeError("Não foi possível abrir o banco de dados (veja logs/).")
        return self._db

    def connector(self):
        if self._connector is None:
            from mt5_extracao.mt5_connector import MT5Connector
            connector = MT5Connector(config_path=self.args.config)
            if not connector.initialize():
                raise RuntimeError("Não foi possível conectar ao MetaTrader 5. "
                                   "Rode 'mt5x doctor' para diagnosticar.")
            self._connector = connector
        return self._connector

    def extractor(self):
        from mt5_extracao.indicator_calculator import IndicatorCalculator
        return services.create_extractor(self.config, self.connector(), self.db, IndicatorCalculator())


# --- utilidades -------------------------------------------------------------------

def _parse_symbols(text):
    symbols = [s.strip() for s in (text or "").split(",") if s.strip()]
    if not symbols:
        raise ValueError("informe ao menos um símbolo em --symbols (separados por vírgula)")
    return symbols


def _parse_tf(text):
    tf = timeframes.parse(text)
    if tf is None:
        raise ValueError(f"timeframe inválido: {text} (use M1, M5, M15, M30, H1, H4, D1, W1 ou MN1)")
    return tf


def _parse_date(text, end_of_day=False):
    try:
        d = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"data inválida: {text} (use AAAA-MM-DD)")
    return d.replace(hour=23, minute=59, second=59) if end_of_day else d


def _run_blocking(start):
    """Executa uma extração assíncrona do HistoricalExtractor e espera o fim. Retorna (ok, falhas, cancelados)."""
    done = threading.Event()
    result = {}
    last = {"pct": -1}

    def progress(pct, msg):
        pct = int(pct)
        if pct != last["pct"] or "Falha" in msg or "Erro" in msg:
            _print(f"[{pct:3d}%] {msg}")
            last["pct"] = pct

    def finished(ok, failed, canceled):
        result.update(ok=ok, failed=failed, canceled=canceled)
        done.set()

    start(progress, finished)
    try:
        while not done.wait(0.5):
            pass
    except KeyboardInterrupt:
        _print("Cancelando... (aguarde o bloco atual terminar)")
        raise
    return result["ok"], result["failed"], result["canceled"]


# --- comandos -------------------------------------------------------------------------

def cmd_doctor(args, ctx):
    from mt5_extracao.bootstrap.__main__ import main as bootstrap_main
    argv = ["doctor"] + (["--json"] if args.json else [])
    return bootstrap_main(argv)


def cmd_symbols(args, ctx):
    items = ctx.connector().get_symbols(args.group) or []
    names = sorted(getattr(s, "name", str(s)) for s in items)
    for name in names:
        _print(name)
    _print(f"\n{len(names)} símbolo(s).")
    return EXIT_OK


def cmd_tables(args, ctx):
    tables = ctx.db.get_all_tables()
    for table in tables:
        last = ctx.db.get_last_timestamp(table)
        _print(f"{table}\túltimo registro: {last or '-'}")
    _print(f"\n{len(tables)} tabela(s).")
    return EXIT_OK


def cmd_quality(args, ctx):
    from mt5_extracao import data_quality
    reports = ctx.db.quality_reports(args.table)
    if not reports:
        _print(f"Nenhum relatório de qualidade para '{args.table}' (extraia dados primeiro).")
        return EXIT_OK
    problems = 0
    for block_start, report in reports:
        flag = "!" if data_quality.has_problems(report) else " "
        problems += flag == "!"
        if flag == "!" or args.all:
            _print(f"{flag} {block_start[:10]}  {data_quality.summarize(report)}")
            if flag == "!" and args.details:
                for key in ("invalid_ohlc", "zero_volume", "duplicate_time", "gaps"):
                    for ex in report[key]["examples"]:
                        _print(f"      {key}: {ex}")
    _print(f"\n{len(reports)} bloco(s) verificados, {problems} com problemas.")
    return EXIT_OK


def _extract_common(args):
    return _parse_symbols(args.symbols), _parse_tf(args.tf)


def cmd_extract(args, ctx):
    symbols, tf = _extract_common(args)
    start = _parse_date(args.date_from)
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else datetime.now()
    if end <= start:
        raise ValueError("--to deve ser posterior a --from")
    extractor = ctx.extractor()
    _print(f"Extraindo {', '.join(symbols)} {tf.name} de {start:%Y-%m-%d} a {end:%Y-%m-%d %H:%M}...")
    ok, failed, canceled = _run_blocking(lambda p, f: extractor.extract_data(
        symbols, tf.value, tf.label, start, end, include_indicators=args.indicators, overwrite=args.overwrite,
        max_workers=args.workers, update_progress_callback=p, finished_callback=f))
    _print(f"Concluído: {ok} ok, {failed} com falha, {canceled} cancelado(s).")
    return EXIT_OK if failed == 0 and canceled == 0 else EXIT_FAIL


def cmd_update(args, ctx):
    symbols, tf = _extract_common(args)
    extractor = ctx.extractor()
    _print(f"Atualizando {', '.join(symbols)} {tf.name} até agora...")
    ok, failed, canceled = _run_blocking(lambda p, f: extractor.update_symbols(
        symbols, tf.value, tf.label, include_indicators=args.indicators, default_days=args.days_if_empty,
        max_workers=args.workers, update_progress_callback=p, finished_callback=f))
    _print(f"Concluído: {ok} ok, {failed} com falha, {canceled} cancelado(s).")
    return EXIT_OK if failed == 0 and canceled == 0 else EXIT_FAIL


def cmd_export(args, ctx):
    from mt5_extracao.data_exporter import DataExporter
    tables = ctx.db.get_all_tables()
    if args.table not in tables:
        raise ValueError(f"tabela '{args.table}' não existe. Tabelas: {', '.join(tables) or '(nenhuma)'}")
    exporter = DataExporter(ctx.db)
    out = os.path.abspath(args.out) if args.out else None
    if args.format == "csv":
        path = exporter.export_to_csv(args.table, caminho_arquivo=out)
    else:
        path = exporter.export_to_excel(args.table, caminho_arquivo=out)
    if not path:
        _err("nada exportado (tabela vazia?)")
        return EXIT_FAIL
    _print(f"Exportado: {path}")
    return EXIT_OK


def _parse_every(text):
    text = text.strip().lower()
    if len(text) < 2 or text[-1] not in "mh" or not text[:-1].isdigit():
        raise ValueError("--every deve ser como 15m ou 2h")
    n, unit = int(text[:-1]), text[-1]
    if (unit == "m" and not 1 <= n <= 59) or (unit == "h" and not 1 <= n <= 23):
        raise ValueError("--every: minutos entre 1 e 59 ou horas entre 1 e 23")
    return n, unit


def schedule_commands(every, symbols, tf, hours="9-18", days="1-5", root=PROJECT_ROOT, indicators=False):
    """Linhas prontas para cron (Linux) e schtasks (Windows)."""
    n, unit = _parse_every(every)
    extra = " --indicators" if indicators else ""
    joined = ",".join(symbols)
    minute_field = f"*/{n}" if unit == "m" else "0"
    hour_field = hours if unit == "m" else f"{hours}/{n}" if "-" in hours else f"*/{n}"
    # No cron o comando passa pelo shell: aspas simples evitam que "$N" em "WIN$N" seja expandido
    cron = (f"{minute_field} {hour_field} * * {days} cd {shlex.quote(Path(root).as_posix())} && "
            f"./run.sh --cli update --symbols {shlex.quote(joined)} --tf {tf.name}{extra} >> logs/cron.log 2>&1")
    sc = "MINUTE" if unit == "m" else "HOURLY"
    schtasks = (f'schtasks /Create /SC {sc} /MO {n} /TN "MT5 Extracao Update {tf.name}" '
                f'/TR "\\"{str(root)}\\run.bat\\" --cli update --symbols {joined} --tf {tf.name}{extra}"')
    return cron, schtasks


def cmd_schedule(args, ctx):
    symbols, tf = _extract_common(args)
    cron, schtasks = schedule_commands(args.every, symbols, tf, args.hours, args.days, indicators=args.indicators)
    if os.name == "nt":
        _print("Windows (Agendador de Tarefas): execute no Prompt de Comando:")
        _print(f"  {schtasks}\n")
    else:
        _print("Linux (cron): execute 'crontab -e' e acrescente a linha:")
        _print(f"  {cron}\n")
    _print("O comando acima NÃO foi instalado automaticamente; revise antes de usar.")
    return EXIT_OK


# --- argparse --------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(prog="mt5x", description="MT5 Extração — linha de comando")
    parser.add_argument("--config", default=services.DEFAULT_CONFIG_PATH, help="config.ini (padrão config/config.ini)")
    parser.add_argument("--db", help="caminho do banco SQLite (padrão: [DATABASE] path)")
    parser.add_argument("-v", "--verbose", action="store_true", help="mostra os logs detalhados")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("doctor", help="diagnóstico do ambiente")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("symbols", help="lista símbolos do MT5")
    p.add_argument("--group", default="*", help='filtro do MT5, ex.: "*WIN*"')
    p.set_defaults(func=cmd_symbols)

    p = sub.add_parser("tables", help="lista tabelas do banco e o último registro de cada uma")
    p.set_defaults(func=cmd_tables)

    def add_common(p):
        p.add_argument("--symbols", required=True, help="símbolos separados por vírgula, ex.: WIN$N,WDO$N")
        p.add_argument("--tf", required=True, help="timeframe: M1, M5, M15, M30, H1, H4, D1, W1, MN1")
        p.add_argument("--indicators", action="store_true", help="calcular indicadores técnicos")
        p.add_argument("--workers", type=int, default=4, help="símbolos em paralelo (padrão 4)")

    p = sub.add_parser("extract", help="extração histórica de um período")
    add_common(p)
    p.add_argument("--from", dest="date_from", required=True, help="data inicial AAAA-MM-DD")
    p.add_argument("--to", dest="date_to", help="data final AAAA-MM-DD (padrão: agora)")
    p.add_argument("--overwrite", action="store_true", help="apaga o período antes de gravar")
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("update", help="atualização incremental até agora")
    add_common(p)
    p.add_argument("--days-if-empty", type=int, default=30, help="dias a buscar para símbolos sem dados (padrão 30)")
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("quality", help="relatório de qualidade dos blocos extraídos de uma tabela")
    p.add_argument("--table", required=True)
    p.add_argument("--all", action="store_true", help="mostrar também blocos sem problemas")
    p.add_argument("--details", action="store_true", help="listar exemplos de cada problema")
    p.set_defaults(func=cmd_quality)

    p = sub.add_parser("export", help="exporta uma tabela")
    p.add_argument("--table", required=True, help="nome da tabela (veja 'mt5x tables')")
    p.add_argument("--format", choices=["csv", "excel"], default="csv")
    p.add_argument("--out", help="arquivo de saída (padrão: exports/<tabela>_<data>.<ext>)")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("schedule", help="mostra o comando para agendar a atualização (cron / Agendador de Tarefas)")
    p.add_argument("--every", required=True, help="intervalo: 15m, 30m, 1h...")
    p.add_argument("--symbols", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--hours", default="9-18", help="horas do dia para cron (padrão 9-18)")
    p.add_argument("--days", default="1-5", help="dias da semana para cron (padrão 1-5 = seg a sex)")
    p.add_argument("--indicators", action="store_true")
    p.set_defaults(func=cmd_schedule)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.verbose:
        # Os módulos do núcleo registram INFO no console; na CLI só avisos e erros
        logging.disable(logging.INFO)
    ctx = Context(args)
    try:
        return args.func(args, ctx)
    except ValueError as e:
        _err(str(e))
        return EXIT_USAGE
    except (FileNotFoundError, RuntimeError) as e:
        _err(str(e))
        return EXIT_FAIL
    except KeyboardInterrupt:
        _err("interrompido pelo usuário")
        return EXIT_FAIL
    finally:
        logging.disable(logging.NOTSET)


if __name__ == "__main__":
    sys.exit(main())
