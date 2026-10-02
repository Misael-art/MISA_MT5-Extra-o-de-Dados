"""
Linha de comando do MT5 Extração (sem interface gráfica).

    mt5x doctor
    mt5x symbols [--group "*WIN*"]
    mt5x tables
    mt5x extract --symbols WIN$N,WDO$N --tf M1 --from 2024-01-01 [--to 2024-06-30] [--indicators]
    mt5x update  --symbols WIN$N --tf M1 [--indicators]
    mt5x ticks   --symbols WIN$N --from 2024-06-03 [--to 2024-06-07] [--chunk-hours 24]
    mt5x book    --symbols WIN$N [--interval 1] [--duration 3600]   # snapshots do book (DOM)
    mt5x export  --table win_n_1_minuto --format csv|excel|parquet|duckdb [--out arquivo]
    mt5x quality --table win_n_1_minuto [--details]
    mt5x schedule --every 15m --symbols WIN$N --tf M1

  Estratégias (pesquisa; não envia ordens):
    mt5x strategies                                    # as seis estratégias e o que evitar
    mt5x specs    --symbols WIN$N,WDO$N               # especificações de contrato (do MT5)
    mt5x backtest --symbol WIN$N --tf M15 --strategy orb [--param range_minutes=15]
    mt5x validate [--symbols ...] --tf H1 [--strategies all]   # Filtro C
    mt5x screen   [--symbols ...] --tf D1                       # Filtros A e B
    mt5x report   [--symbols ...] --tf D1 [--validate] [--open] # relatório HTML

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
from mt5_extracao.logging_setup import setup_logging, teardown_logging

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
        # config/config.ini relativo é procurado também na pasta do projeto (mt5x de qualquer pasta)
        args.config = services.resolve_config_path(args.config)
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


def cmd_ticks(args, ctx):
    from mt5_extracao.tick_extractor import TickExtractor
    symbols = _parse_symbols(args.symbols)
    start = _parse_date(args.date_from)
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else datetime.now()
    if end <= start:
        raise ValueError("--to deve ser posterior a --from")
    extractor = TickExtractor(ctx.connector(), ctx.db, chunk_hours=args.chunk_hours)
    failed = 0
    for symbol in symbols:
        _print(f"Ticks de {symbol} de {start:%Y-%m-%d} a {end:%Y-%m-%d %H:%M} (tabela {extractor.table_name(symbol)})...")
        r = extractor.extract(symbol, start, end, overwrite=args.overwrite,
                              progress=lambda pct, msg: _print(f"[{pct:3d}%] {msg}"))
        _print(f"{symbol}: {r['rows']} ticks gravados; blocos ok {r['ok']}, sem negócios {r['empty']}, "
               f"com falha {r['failed']}, já concluídos antes {r['skipped']}.")
        failed += r["failed"]
    if failed:
        _print("Rode o mesmo comando de novo para buscar só os blocos que falharam.")
    return EXIT_OK if failed == 0 else EXIT_FAIL


def cmd_book(args, ctx):
    from mt5_extracao.book_collector import BookCollector
    symbols = _parse_symbols(args.symbols)
    if args.interval <= 0 or (args.duration is not None and args.duration <= 0):
        raise ValueError("--interval e --duration devem ser maiores que zero")
    collector = BookCollector(ctx.connector(), ctx.db, interval=args.interval)
    limit = f"por {args.duration:g} s" if args.duration else "até Ctrl+C"
    _print(f"Coletando o book de {', '.join(symbols)} a cada {args.interval:g} s, {limit}...")
    summary = collector.run(symbols, duration=args.duration, progress=_print)
    ok = True
    for symbol, r in summary.items():
        if not r["subscribed"]:
            _err(f"{symbol}: o MT5 recusou o book (a corretora oferece book para este símbolo?)")
            ok = False
            continue
        _print(f"{symbol}: {r['snapshots']} snapshots, {r['rows']} níveis gravados em {collector.table_name(symbol)}"
               + (f", {r['failures']} leituras falharam" if r["failures"] else ""))
    return EXIT_OK if ok else EXIT_FAIL


def cmd_export(args, ctx):
    from mt5_extracao.data_exporter import DataExporter
    from mt5_extracao.error_handler import ExportError
    tables = ctx.db.get_all_tables()
    if args.table not in tables:
        raise ValueError(f"tabela '{args.table}' não existe. Tabelas: {', '.join(tables) or '(nenhuma)'}")
    exporter = DataExporter(ctx.db)
    out = os.path.abspath(args.out) if args.out else None
    try:
        path = _export(exporter, args, out)
    except ExportError as e:
        raise RuntimeError(str(e))
    if not path:
        _err("nada exportado (tabela vazia?)")
        return EXIT_FAIL
    _print(f"Exportado: {path}")
    return EXIT_OK


def _export(exporter, args, out):
    if args.format == "csv":
        return exporter.export_to_csv(args.table, caminho_arquivo=out)
    if args.format == "excel":
        return exporter.export_to_excel(args.table, caminho_arquivo=out)
    if args.format == "parquet":
        return exporter.export_to_parquet(args.table, caminho_arquivo=out)
    return exporter.export_to_duckdb([args.table], caminho_arquivo=out)


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


# --- estratégias ------------------------------------------------------------------------

def _research_symbols(args, ctx, tf):
    from mt5_extracao.strategies import research
    if getattr(args, "symbols", None):
        return _parse_symbols(args.symbols)
    symbols = research.symbols_with_data(ctx.db, tf)
    if not symbols:
        raise ValueError(f"nenhum símbolo com dados em {tf.name}. Informe --symbols ou extraia dados antes.")
    return symbols


def _parse_params(items):
    params = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"--param deve ser nome=valor (recebido: {item})")
        name, value = item.split("=", 1)
        try:
            number = float(value)
        except ValueError:
            raise ValueError(f"--param {name}: valor numérico esperado (recebido: {value})")
        params[name.strip()] = int(number) if number.is_integer() and "." not in value else number
    return params


def _fmt_pct(x):
    return f"{x * 100:.1f}%".replace(".", ",")


def _fmt_num(x, digits=2):
    return ("∞" if x == float("inf") else f"{x:.{digits}f}").replace(".", ",")


def cmd_strategies(args, ctx):
    from mt5_extracao.strategies import AVOID, REGISTRY
    _print("Estratégias disponíveis (use a chave em --strategy / --strategies):\n")
    for key, cls in REGISTRY.items():
        i = cls.info
        _print(f"  {key:<9} {i.name}")
        _print(f"            regime: {i.regime} · acerto típico: {i.win_rate} · risco:retorno {i.reward_risk} · "
               f"complexidade {i.complexity}")
        _print(f"            timeframes: {', '.join(i.timeframes)} · mercados: {i.markets}")
        _print(f"            {i.summary}\n")
    _print("Evite no início:")
    for name, why in AVOID:
        _print(f"  - {name}: {why}")
    _print("\nAviso: backtest não garante resultado futuro. Veja docs/estrategias.md.")
    return EXIT_OK


def cmd_specs(args, ctx):
    from mt5_extracao.strategies import specs
    symbols = _parse_symbols(args.symbols)
    result = specs.fetch_and_save_specs(ctx.connector(), ctx.db, symbols)
    for symbol, status in result.items():
        if status == "ok":
            s = specs.load_spec(ctx.db, symbol)
            _print(f"  {symbol}: tick {s.tick:g} = {s.tick_value:g} {s.currency_profit} por lote; "
                   f"lote mín. {s.volume_min:g} (passo {s.volume_step:g}); spread atual {s.spread_points:g} pontos")
        else:
            _print(f"  {symbol}: {status}")
    return EXIT_OK if all(v == "ok" for v in result.values()) else EXIT_FAIL


def cmd_backtest(args, ctx):
    from mt5_extracao.strategies import get_strategy, research, specs
    from mt5_extracao.strategies.backtester import run_backtest
    tf = _parse_tf(args.tf)
    strategy = get_strategy(args.strategy, **_parse_params(args.param))
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    df = ctx.db.load_ohlcv(args.symbol, tf.label, start, end)
    if df.empty:
        raise ValueError(research.missing_message([args.symbol], tf))
    spec = specs.load_spec(ctx.db, args.symbol)
    result = run_backtest(df, strategy, spec, research.backtest_config(ctx.config))
    m = result.metrics
    _print(f"{strategy.describe()} em {args.symbol} {tf.name}: {len(df)} barras "
           f"({df['time'].iloc[0]:%Y-%m-%d} a {df['time'].iloc[-1]:%Y-%m-%d})")
    if not spec.known:
        _print("  AVISO: especificação ausente; valores em dinheiro e lotes são aproximados. Rode 'mt5x specs'.")
    _print(f"  Operações: {m['trades']}   Acerto: {_fmt_pct(m['win_rate'])}   Profit factor: "
           f"{_fmt_num(m['profit_factor'])}   Payoff: {_fmt_num(m['payoff'])}")
    _print(f"  Expectativa: {_fmt_num(m['expectancy_r'])} R por operação   Resultado: {_fmt_num(m['net_profit'])} "
           f"({_fmt_pct(m['return_pct'])})   Drawdown máx.: {_fmt_pct(m['max_drawdown_pct'])}")
    if m["exit_reasons"]:
        _print("  Saídas: " + ", ".join(f"{k} {v}" for k, v in m["exit_reasons"].items()))
    if result.skipped_min_lot:
        _print(f"  {result.skipped_min_lot} sinal(is) ignorado(s): lote para o risco configurado abaixo do mínimo.")
    if args.trades_out:
        result.trades_frame().to_csv(args.trades_out, index=False)
        _print(f"  Operações salvas em {os.path.abspath(args.trades_out)}")
    _print("Um backtest isolado não valida a estratégia: use 'mt5x validate' (walk-forward e Monte Carlo).")
    return EXIT_OK


def cmd_validate(args, ctx):
    from mt5_extracao.strategies import parse_keys, research
    tf = _parse_tf(args.tf)
    keys = parse_keys(args.strategies)
    symbols = _research_symbols(args, ctx, tf)
    verdicts, missing = research.run_validation(ctx.db, symbols, tf, keys, ctx.config,
                                                progress=_print if args.verbose_progress else None)
    if missing:
        _err(research.missing_message(missing, tf))
    for v in verdicts:
        _print(f"{v['symbol']:<10} {v['strategy']:<9} {v['status']}")
    approved = sum(1 for v in verdicts if v["approved"])
    _print(f"\n{approved} de {len(verdicts)} combinação(ões) aprovada(s) no Filtro C. "
           "Resultados gravados; veja 'mt5x report'.")
    return EXIT_OK if verdicts else EXIT_FAIL


def cmd_screen(args, ctx):
    from mt5_extracao.strategies import asset_screener, research
    tf = _parse_tf(args.tf)
    symbols = _research_symbols(args, ctx, tf)
    rows, missing = research.run_screen(ctx.db, symbols, tf, ctx.config)
    if missing:
        _err(research.missing_message(missing, tf))
    if rows:
        _print(asset_screener.format_table(rows))
        _print(f"\n{sum(r['valid'] for r in rows)} de {len(rows)} ativo(s) aptos (Filtro A). "
               "Score = oportunidade agora (Filtro B).")
    return EXIT_OK if rows else EXIT_FAIL


def cmd_report(args, ctx):
    from mt5_extracao.strategies import parse_keys, research
    tf = _parse_tf(args.tf)
    symbols = _research_symbols(args, ctx, tf)
    if args.validate:
        _print("Validando estratégias (pode levar alguns minutos)...")
        research.run_validation(ctx.db, symbols, tf, parse_keys(args.strategies), ctx.config, progress=_print)
    path, rows, missing = research.build_report(ctx.db, symbols, tf, ctx.config, args.out)
    if missing:
        _err(research.missing_message(missing, tf))
    _print(f"Relatório salvo em {os.path.abspath(path)}")
    if args.open:
        import webbrowser
        webbrowser.open(Path(os.path.abspath(path)).as_uri())
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

    p = sub.add_parser("ticks", help="extrai ticks (bid/ask/last) por blocos, com retomada")
    p.add_argument("--symbols", required=True, help="símbolos separados por vírgula")
    p.add_argument("--from", dest="date_from", required=True, help="data inicial AAAA-MM-DD")
    p.add_argument("--to", dest="date_to", help="data final AAAA-MM-DD (padrão: agora)")
    p.add_argument("--chunk-hours", type=int, default=24, help="horas por bloco (padrão 24)")
    p.add_argument("--overwrite", action="store_true", help="busca de novo blocos já concluídos")
    p.set_defaults(func=cmd_ticks)

    p = sub.add_parser("book", help="coleta snapshots do book de ofertas (DOM) em intervalos regulares")
    p.add_argument("--symbols", required=True, help="símbolos separados por vírgula")
    p.add_argument("--interval", type=float, default=1.0, help="segundos entre snapshots (padrão 1)")
    p.add_argument("--duration", type=float, help="segundos de coleta (padrão: até Ctrl+C)")
    p.set_defaults(func=cmd_book)

    p = sub.add_parser("quality", help="relatório de qualidade dos blocos extraídos de uma tabela")
    p.add_argument("--table", required=True)
    p.add_argument("--all", action="store_true", help="mostrar também blocos sem problemas")
    p.add_argument("--details", action="store_true", help="listar exemplos de cada problema")
    p.set_defaults(func=cmd_quality)

    p = sub.add_parser("export", help="exporta uma tabela")
    p.add_argument("--table", required=True, help="nome da tabela (veja 'mt5x tables')")
    p.add_argument("--format", choices=["csv", "excel", "parquet", "duckdb"], default="csv",
                   help="parquet e duckdb exigem os pacotes opcionais pyarrow/duckdb")
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

    # Estratégias
    p = sub.add_parser("strategies", help="lista as seis estratégias, quando usar e o que evitar")
    p.set_defaults(func=cmd_strategies)

    p = sub.add_parser("specs", help="lê do MT5 as especificações de contrato (tick, lote, spread)")
    p.add_argument("--symbols", required=True)
    p.set_defaults(func=cmd_specs)

    p = sub.add_parser("backtest", help="backtest de uma estratégia em um símbolo")
    p.add_argument("--symbol", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--strategy", required=True, help="chave (veja 'mt5x strategies')")
    p.add_argument("--param", action="append", help="sobrescreve um parâmetro: nome=valor (pode repetir)")
    p.add_argument("--from", dest="date_from", help="data inicial AAAA-MM-DD")
    p.add_argument("--to", dest="date_to", help="data final AAAA-MM-DD")
    p.add_argument("--trades-out", help="salva as operações em CSV")
    p.set_defaults(func=cmd_backtest)

    def add_research(p):
        p.add_argument("--symbols", help="símbolos separados por vírgula (padrão: todos com dados no timeframe)")
        p.add_argument("--tf", required=True)

    p = sub.add_parser("validate", help="Filtro C: walk-forward, robustez e Monte Carlo (grava o veredito)")
    add_research(p)
    p.add_argument("--strategies", default="all", help="chaves separadas por vírgula ou 'all'")
    p.add_argument("--progress", dest="verbose_progress", action="store_true", help="mostra o andamento")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("screen", help="Filtros A e B: ativos aptos, regime, estratégia sugerida e score")
    add_research(p)
    p.set_defaults(func=cmd_screen)

    p = sub.add_parser("report", help="relatório HTML (triagem + matriz ativo × estratégia)")
    add_research(p)
    p.add_argument("--validate", action="store_true", help="roda a validação antes de gerar o relatório")
    p.add_argument("--strategies", default="all", help="usado com --validate")
    p.add_argument("--out", help="arquivo HTML (padrão: exports/relatorio_estrategias_<tf>_<data>.html)")
    p.add_argument("--open", action="store_true", help="abre o relatório no navegador")
    p.set_defaults(func=cmd_report)
    return parser


def _tolerant_streams():
    """No Windows, saída redirecionada usa cp1252: símbolos como ✅/❌ viram '?' em vez de erro."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass


def main(argv=None):
    _tolerant_streams()
    parser = build_parser()
    args = parser.parse_args(argv)
    # Arquivo de log sempre completo (INFO); no console só avisos e erros, salvo com -v
    setup_logging(console_level=logging.INFO if args.verbose else logging.WARNING)
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
        teardown_logging()


if __name__ == "__main__":
    sys.exit(main())
