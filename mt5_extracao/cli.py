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

  Robôs e operação por agente (veja docs/agente.md):
    mt5x resample --symbols X --base M1 --tf M5,M15,H1    # monta timeframes maiores de uma base menor
    mt5x robots [list|import|backtest] ...             # catálogo, importação e backtest de estratégia Python
    mt5x robots [install|compile] <robô>               # copia o robô MQL5 para o MT5 e compila no MetaEditor
    mt5x robots backtest <robô MQL5> --symbol EURUSD --tf H1 [--preset X]   # Strategy Tester do MT5
    mt5x robots backtest <robô> --symbols EURUSD,GBPUSD --tf H1   # o mesmo teste em vários ativos
    mt5x robots new <Nome> --dir <pasta>               # cria o esqueleto de um robô sobre a biblioteca de segurança
    mt5x robots generate <declaração.json> --name <Nome> --dir <pasta>   # robô MQL5 a partir de uma estratégia declarada
    mt5x robots validate <robô>                        # confere o robot.json contra o contrato do SDK
    mt5x robots check <robô> --symbol X --tf M5        # contrato, compilação, backtest e bateria em um comando
    mt5x robots parity <robô MQL5> --strategy <chave> --symbol X --tf M5   # mesmas operações que o Python?
    mt5x robots battery <robô> [--preset X] [--deep]   # bateria de segurança; --deep repete o Strategy Tester
    mt5x robots start <robô> --symbol X --tf M5        # inicia o robô em conta DEMO (travas conferidas antes)
    mt5x robots monitor <robô> | mt5x robots stop      # acompanha (somente leitura) | PARAR TUDO
    mt5x analyze --symbol WIN$N --tf D1 [--period "1 ano"]   # os números da aba Análise
    mt5x status                                        # base, robôs, validações e próximas ações sugeridas
    mt5x audit [--mt5] [--fail-on alto]                # segurança do projeto, patrimonial e do capital
    mt5x commands                                      # todos os comandos e argumentos (para agentes)

Sem o pacote instalado: python -m mt5_extracao.cli ... (ou ./run.sh --cli ... / run.bat --cli ...).
Códigos de saída: 0 sucesso, 1 falha, 2 uso incorreto.

Com --json (antes do comando), a saída padrão traz um único objeto JSON:
    {"ok": bool, "command": str, "exit_code": int, "data": {...}, "messages": [...], "errors": [...]}
Nenhum comando pede entrada pelo teclado.
"""
import argparse
import logging
import os
import subprocess
import re
import shlex
import sys
import threading
from datetime import datetime
from pathlib import Path

from mt5_extracao import services, timeframes
from mt5_extracao.logging_setup import setup_logging, teardown_logging

EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# Modo --json: nada é impresso durante o comando; texto, erros e dados saem juntos, num objeto só.
_OUT = {"json": False, "messages": [], "errors": [], "data": {}, "raw": False}


def _print(msg=""):
    if _OUT["json"]:
        _OUT["messages"].extend(str(msg).splitlines() or [""])
    else:
        print(msg, flush=True)


def _err(msg):
    if _OUT["json"]:
        _OUT["errors"].append(str(msg))
    else:
        print(f"ERRO: {msg}", file=sys.stderr, flush=True)


def _data(**values):
    """Dados estruturados do comando (só aparecem com --json)."""
    _OUT["data"].update(values)


def _jsonable(value):
    """Converte para tipos do JSON: datas viram texto, números do numpy viram int/float, NaN/infinito viram null."""
    import math
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if hasattr(value, "item") and not isinstance(value, (int, float)):      # escalares do numpy
        try:
            value = value.item()
        except (ValueError, AttributeError):
            return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, int):
        return value
    if hasattr(value, "__dataclass_fields__"):
        from dataclasses import asdict
        return _jsonable(asdict(value))
    return str(value)




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
            from mt5_extracao.robots import mt5_tools
            mt5_tools.use_config(self._config)           # modo oculto: os processos do Wine vão para a tela virtual
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
    as_json = args.json or _OUT["json"]
    _OUT["raw"] = as_json           # o doctor já imprime o próprio JSON
    return bootstrap_main(["doctor"] + (["--json"] if as_json else []))


def cmd_symbols(args, ctx):
    items = ctx.connector().get_symbols(args.group) or []
    names = sorted(getattr(s, "name", str(s)) for s in items)
    for name in names:
        _print(name)
    _data(symbols=names)
    _print(f"\n{len(names)} símbolo(s).")
    return EXIT_OK


def cmd_tables(args, ctx):
    from mt5_extracao import resample
    tables = ctx.db.get_all_tables()
    series = ctx.db.data_inventory()
    built = resample.origins(ctx.db)                      # séries montadas de um timeframe menor, não extraídas
    by_table = {}
    for item in series:
        tf = timeframes.parse(item["timeframe"])
        item["built_from"] = built.get((item["symbol"], tf.name if tf else ""), "")
        by_table[item["table"]] = item["built_from"]
    for table in tables:
        last = ctx.db.get_last_timestamp(table)
        _print(f"{table}\túltimo registro: {last or '-'}" +
               (f"\t(montada a partir de {by_table[table]})" if by_table.get(table) else ""))
    _data(tables=tables, series=series)
    _print(f"\n{len(tables)} tabela(s).")
    return EXIT_OK


def cmd_resample(args, ctx):
    """Monta timeframes maiores a partir de uma base menor já extraída (não fala com o MT5)."""
    from mt5_extracao import resample
    symbols, base = _parse_symbols(args.symbols), _parse_tf(args.base)
    targets = [_parse_tf(t) for t in args.tf.split(",") if t.strip()]
    if not targets:
        raise ValueError("informe em --tf os timeframes a montar, ex.: --tf M5,M15,H1")
    for target in targets:
        resample.check_pair(base, target)
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    done, failed = [], []
    for symbol in symbols:
        try:
            rows = resample.resample_symbol(ctx.db, symbol, base, targets, start, end)
        except (ValueError, RuntimeError) as e:
            failed.append({"symbol": symbol, "error": str(e)})
            _print(f"{symbol}: {e}")
            continue
        done += rows
        for row in rows:
            _print(f"{symbol} {row['timeframe']}: {row['bars']} barras montadas de {row['source_bars']} barras "
                   f"{base.name} ({row['data_start'][:16]} a {row['data_end'][:16]})")
    _data(resampled=done, failed=failed)
    _print(f"\nConcluído: {len(done)} série(s) montada(s), {len(failed)} ativo(s) com falha. As barras montadas "
           "seguem as regras do MT5 (spread = o menor do período) e aparecem em 'mt5x tables' como montadas.")
    return EXIT_OK if done and not failed else EXIT_FAIL


def cmd_mt5(args, ctx):
    """Estado do terminal desta instalação e, no modo oculto, uma captura do que ele está mostrando."""
    from mt5_extracao.bootstrap import display
    from mt5_extracao.robots import mt5_tools
    hidden = display.enabled(ctx.config)
    name = display.display_name(ctx.config)
    if args.action == "show":
        if not hidden:
            raise ValueError("O modo oculto está desligado: o MetaTrader 5 abre na sua tela. Para ligar: [MT5] hidden "
                             "= true em config/config.ini (ou MT5X_HIDDEN=1 só nesta execução).")
        try:
            path = display.screenshot(name, args.out or os.path.join("logs", "mt5_oculto.png"))
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as e:
            raise RuntimeError(str(e)) from e
        _data(screenshot=str(path), display=name)
        _print(f"Captura da tela virtual {name} gravada em {path}")
        return EXIT_OK
    owners = [owner for _, owner in mt5_tools.terminal_processes(ctx.config)]
    ours = sum(1 for o in owners if o == mt5_tools.OURS)
    _data(hidden=hidden, display=name if hidden else "", terminal_open=ours > 0,
          other_terminals=sum(1 for o in owners if o != mt5_tools.OURS), display_running=display.is_running(name))
    _print("MetaTrader 5 desta instalação: " + ("ABERTO" if ours else "fechado") +
           (f" ({len(owners) - ours} terminal(is) de outra instalação ou não identificado(s) também em execução)"
            if len(owners) > ours else ""))
    if hidden:
        _print(f"Modo oculto: LIGADO, tela virtual {name} " + ("ativa" if display.is_running(name) else "parada") +
               ". O terminal não aparece na sua tela; para ver o que ele mostra: mt5x mt5 show")
        if ours:
            _print("Atenção: há um terminal aberto fora da vista. Um robô pode estar operando nele.")
    else:
        _print("Modo oculto: desligado (o terminal abre na sua tela).")
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


_B3_SYMBOL = re.compile(r"^([A-Z]{3}\$[A-Z]?|[A-Z]{4}\d{1,2}F?|(WIN|WDO|IND|DOL)[A-Z]\d{2})$")


def _warn_missing_symbols(connector, symbols):
    """Explica, antes de extrair, quais símbolos o servidor da conta atual não tem e o que fazer."""
    check = getattr(connector, "missing_symbols", None)
    missing = check(symbols) if check else []
    if not missing:
        return
    server = connector.server_name() or "da conta atual"
    _print(f"AVISO: o servidor {server} não tem: {', '.join(missing)}.")
    if any(_B3_SYMBOL.match(s.upper()) for s in missing):
        _print("  Ativos da B3 (WIN, WDO, ações) só existem nos servidores de corretoras brasileiras;")
        _print("  a conta de demonstração da MetaQuotes tem Forex e CFDs (ex.: EURUSD).")
        _print("  Instale o MT5 da sua corretora, entre na conta dela e aponte o app para ele:")
        _print("    python -m mt5_extracao.bootstrap configure --mt5-path \"<pasta do MT5 da corretora>\"")
    _print("  Para ver os nomes que existem nesta conta: mt5x symbols --group \"*TEXTO*\"")


def cmd_extract(args, ctx):
    symbols, tf = _extract_common(args)
    start = _parse_date(args.date_from)
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else datetime.now()
    if end <= start:
        raise ValueError("--to deve ser posterior a --from")
    extractor = ctx.extractor()
    _warn_missing_symbols(ctx.connector(), symbols)
    _print(f"Extraindo {', '.join(symbols)} {tf.name} de {start:%Y-%m-%d} a {end:%Y-%m-%d %H:%M}...")
    ok, failed, canceled = _run_blocking(lambda p, f: extractor.extract_data(
        symbols, tf.value, tf.label, start, end, include_indicators=args.indicators, overwrite=args.overwrite,
        max_workers=args.workers, update_progress_callback=p, finished_callback=f))
    _data(ok=ok, failed=failed, canceled=canceled, symbols=symbols, timeframe=tf.name)
    _print(f"Concluído: {ok} ok, {failed} com falha, {canceled} cancelado(s).")
    return EXIT_OK if failed == 0 and canceled == 0 else EXIT_FAIL


def cmd_update(args, ctx):
    symbols, tf = _extract_common(args)
    extractor = ctx.extractor()
    _warn_missing_symbols(ctx.connector(), symbols)
    _print(f"Atualizando {', '.join(symbols)} {tf.name} até agora...")
    ok, failed, canceled = _run_blocking(lambda p, f: extractor.update_symbols(
        symbols, tf.value, tf.label, include_indicators=args.indicators, default_days=args.days_if_empty,
        max_workers=args.workers, update_progress_callback=p, finished_callback=f))
    _data(ok=ok, failed=failed, canceled=canceled, symbols=symbols, timeframe=tf.name)
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
    _warn_missing_symbols(ctx.connector(), symbols)
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


def _robot_dict(robot, full=False):
    data = {"key": robot.key, "kind": robot.kind, "name": robot.name, "version": robot.version,
            "regime": robot.regime, "timeframes": list(robot.timeframes), "markets": robot.markets,
            "stage": robot.stage, "stages": list(robot.stages), "next_step": robot.next_step,
            "risk": {"level": robot.risk.level, "reasons": robot.risk.reasons, "facts": robot.risk.facts},
            "preset_risks": {name: {"level": r.level, "reasons": r.reasons} for name, r in robot.preset_risks.items()},
            "has_source": robot.has_source, "dll_imports": robot.dll_imports, "notes": robot.notes,
            "path": robot.path, "headline": robot.headline, "validations": robot.results,
            "runs": [{k: v for k, v in run.items() if k not in ("equity", "trades")} for run in robot.runs],
            "battery": ({k: v for k, v in robot.checks[0].items() if k != "items"} if robot.checks else None)}
    if full:
        data.update(description=robot.description, files=robot.files, missing_includes=robot.missing_includes,
                    presets=robot.presets, inputs=[{"name": i.name, "type": i.type, "default": i.default,
                                                    "comment": i.comment, "group": i.group} for i in robot.inputs])
    return data


def _robots_backtest_many(args, ctx, robot, tf, start, end):
    """O mesmo backtest em vários ativos, com um quadro comparativo. Cada ativo é um teste independente."""
    from mt5_extracao.robots import backtest, catalog, mt5_tools
    symbols = _parse_symbols(args.symbols)
    mql5 = robot.kind == catalog.MQL5
    params = {}
    for item in args.param or []:
        if "=" not in item:
            raise ValueError(f"--param deve ser nome=valor (recebido: {item})")
        name, value = item.split("=", 1)
        params[name.strip()] = value.strip()
    if not mql5:
        params = _parse_params(args.param)
    bridge_was_running = False
    try:
        if mql5 and mt5_tools.terminal_running(ctx.config):
            if not args.close_terminal:       # um aviso só, em vez do mesmo erro repetido em cada ativo
                raise RuntimeError("O MetaTrader 5 está aberto e não roda o teste em uma segunda janela. Feche o MT5 "
                                   "(e pare a ponte: ./scripts/mt5-bridge.sh stop) ou repita com --close-terminal.")
            _print("Fechando o MetaTrader 5 para rodar os testes...")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        results = backtest.run_many(ctx.db, ctx.config, robot, symbols, tf.label, start, end, args.preset, params)
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    _data(runs=[{"symbol": r["symbol"], "error": r["error"],
                 "run": {k: v for k, v in r["run"].items() if k not in ("trades", "equity")} if r["run"] else None}
                for r in results],
          summary={"tested": sum(1 for r in results if r["run"]), "failed": sum(1 for r in results if not r["run"])})
    _print(f"{robot.name} em {tf.name}, {len(symbols)} ativo(s):")
    _print(f"  {'Ativo':<12}{'Operações':>10}{'Acerto':>9}{'Profit factor':>15}{'Resultado':>12}{'Queda máx.':>12}")
    for r in results:
        if r["run"] is None:
            _print(f"  {r['symbol']:<12}não testado: {r['error']}")
            continue
        m = r["run"]["metrics"]
        _print(f"  {r['symbol']:<12}{m['trades']:>10}{_fmt_pct(m['win_rate']):>9}{_fmt_num(m['profit_factor']):>15}"
               f"{_fmt_pct(m['return_pct']):>12}{_fmt_pct(m['max_drawdown_pct']):>12}")
    failed = [r["symbol"] for r in results if r["run"] is None]
    _print("Cada ativo é um teste independente (não há resultado de carteira). As execuções ficaram gravadas; a "
           "bateria avalia um ativo por vez: mt5x robots battery <robô> --symbol <ativo>.")
    if failed:
        _print("Sem teste: " + ", ".join(failed) + ".")
    return EXIT_FAIL if failed else EXIT_OK


def _robots_backtest(args, ctx, items):
    from mt5_extracao.robots import backtest, catalog
    if not args.path or not (args.symbol or args.symbols) or not args.tf:
        raise ValueError("uso: mt5x robots backtest <robô> --symbol WIN$N (ou --symbols A,B,C) --tf D1 "
                         "[--from AAAA-MM-DD] [--to AAAA-MM-DD]")
    if args.symbol and args.symbols:
        raise ValueError("use --symbol (um ativo) ou --symbols (vários), não os dois")
    robot = catalog.get_robot(items, args.path)
    tf = _parse_tf(args.tf)
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    if args.symbols:
        return _robots_backtest_many(args, ctx, robot, tf, start, end)
    if robot.kind == catalog.MQL5:
        return _robots_backtest_mql5(args, ctx, robot, tf, start, end)
    run = backtest.run_python(ctx.db, ctx.config, robot, args.symbol, tf.label, start, end,
                              _parse_params(args.param))
    m = run["metrics"]
    _data(run={k: v for k, v in run.items() if k != "trades"})
    _print(f"{robot.name} em {run['symbol']} {run['timeframe']}: {run['bars']} barras "
           f"({run['data_start'][:10]} a {run['data_end'][:10]})")
    _print(f"  Operações: {m['trades']}   Acerto: {_fmt_pct(m['win_rate'])}   Profit factor: "
           f"{_fmt_num(m['profit_factor'])}   Expectativa: {_fmt_num(m['expectancy_r'])} R")
    _print(f"  Resultado: {_fmt_pct(m['return_pct'])}   Queda máxima: {_fmt_pct(m['max_drawdown_pct'])}")
    _print("Execução gravada (aparece no card do robô). Um backtest isolado não valida a estratégia: use 'mt5x validate'.")
    return EXIT_OK


def _robots_backtest_mql5(args, ctx, robot, tf, start, end):
    """Robô MQL5: Strategy Tester do MT5 (simulação; nenhuma ordem é enviada a conta alguma)."""
    from mt5_extracao.robots import backtest, mt5_tools
    params = {}
    for item in args.param or []:
        if "=" not in item:
            raise ValueError(f"--param deve ser nome=valor (recebido: {item})")
        name, value = item.split("=", 1)
        params[name.strip()] = value.strip()
    bridge_was_running = False
    try:
        if args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar o teste...")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        run = backtest.run_mql5(ctx.db, ctx.config, robot, args.symbol, tf.label, start, end, args.preset, params)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    m = run["metrics"]
    _data(run={k: v for k, v in run.items() if k != "trades"})
    _print(f"{robot.name} em {run['symbol']} {run['timeframe']} pelo Strategy Tester "
           f"({run['data_start'][:10]} a {run['data_end'][:10]}, modelo: {m['tester_model']})")
    _print(f"  Operações: {m['trades']}   Acerto: {_fmt_pct(m['win_rate'])}   Profit factor: "
           f"{_fmt_num(m['profit_factor'])}   Resultado médio: {_fmt_num(m['expectancy'])} por operação")
    _print(f"  Resultado: {_fmt_num(m['net_profit'])} ({_fmt_pct(m['return_pct'])})   "
           f"Queda máxima: {_fmt_pct(m['max_drawdown_pct'])}")
    _print(f"  Operações lidas de: {m['trades_source']}. Relatório completo: {m['report']}")
    if not m["trades"]:
        _print("  Nenhuma operação no período: confira o ativo, o período e os parâmetros do robô.")
    _print("Execução gravada (aparece no card do robô). Um backtest isolado não valida o robô.")
    return EXIT_OK


def cmd_analyze(args, ctx):
    """Os números da aba Análise (study.py) para um ativo e timeframe, sem interface."""
    from mt5_extracao import study
    tf = _parse_tf(args.tf)
    periods = dict(study.PERIODS)
    if args.period not in periods:
        raise ValueError(f"período inválido: {args.period} (use um de: {', '.join(periods)})")
    item = next((i for i in ctx.db.data_inventory() if i["symbol"] == args.symbol and i["timeframe"] == tf.label), None)
    if item is None or not item["rows"]:
        raise ValueError(f"sem barras de {args.symbol} {tf.name} na base. Rode: mt5x extract --symbols {args.symbol} "
                         f"--tf {tf.name} --from AAAA-MM-DD")
    df = ctx.db.load_ohlcv(args.symbol, tf.label, study.period_start(item["end"], periods[args.period]))
    summary = study.summarize(df)
    freshness = study.freshness(item["end"], tf.minutes)
    season = {name: frame.reset_index().to_dict("records") for name, frame in study.seasonality(df).items()}
    _data(symbol=args.symbol, timeframe=tf.name, period=args.period, bars=len(df), freshness=freshness,
          summary=summary, seasonality=season)
    _print(f"{args.symbol} {tf.name}, {args.period}: {len(df)} barras, dados {freshness}")
    for label, key, pct in (("Último preço", "last", False), ("Retorno no período", "return_pct", True),
                            ("Volatilidade anualizada", "volatility_pct", True),
                            ("Queda máxima", "max_drawdown_pct", True),
                            ("Amplitude média por barra", "mean_range_pct", True), ("Barras de alta", "up_bars_pct", True)):
        value = summary.get(key)
        _print(f"  {label}: " + ("—" if value is None else (f"{value:.2f}%".replace(".", ",") if pct else f"{value:g}")))
    return EXIT_OK


def cmd_status(args, ctx):
    """Retrato do projeto para decidir o próximo passo sem olhar a tela."""
    from mt5_extracao import agent
    state = agent.status(ctx.config, ctx.db)
    _data(**state)
    base = state["base"]
    _print(f"Base: {base['symbols']} ativo(s), {base['series']} série(s), {base['bars']} barras; "
           f"{len(base['stale'])} série(s) desatualizada(s).")
    r = state["robots"]
    _print(f"Robôs: {r['total']} ({r['mql5']} MQL5, {r['python']} Python); por estágio: "
           + ", ".join(f"{k} {v}" for k, v in r["by_stage"].items()))
    v = state["validations"]
    _print(f"Validações: {v['total']} gravada(s), {v['approved']} aprovada(s).")
    _print("Próximas ações sugeridas:")
    for action in state["next_actions"]:
        _print(f"  [{action['priority']}] {action['why']}")
        _print(f"      {action['command']}")
    return EXIT_OK


def cmd_audit(args, ctx):
    """Auditoria de segurança do projeto, patrimonial e da gestão do capital."""
    from mt5_extracao import audit
    account = ctx.connector().account_summary() if args.mt5 else None
    try:
        db = ctx.db
    except RuntimeError:
        db = None
    findings = audit.run_audit(PROJECT_ROOT, ctx.config, db, account)
    blocking = audit.failures(findings, args.fail_on)
    summary = audit.summarize(findings)
    _data(summary=summary, findings=[f.to_dict() for f in findings], blocking=[f.id for f in blocking])
    marks = {audit.FAIL: "✗", audit.OK: "✓", audit.SKIPPED: "–"}
    for f in findings:
        _print(f"{marks[f.status]} [{f.area}/{f.severity}] {f.id} {f.title}" + (f": {f.detail}" if f.detail else ""))
        if f.status == audit.FAIL and f.fix:
            _print(f"    Como corrigir: {f.fix}")
    _print(f"\n{summary['failed']} achado(s) em {summary['total']} verificações "
           f"({summary['skipped']} não verificada(s)). Bloqueiam (≥ {args.fail_on}): {len(blocking)}.")
    return EXIT_FAIL if blocking else EXIT_OK


def cmd_commands(args, ctx):
    """Catálogo dos comandos e argumentos, lido do próprio analisador (não fica desatualizado)."""
    parser = build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    helps = {a.dest: a.help for a in sub._choices_actions}
    catalog = []
    for name, p in sub.choices.items():
        arguments = [{"flags": a.option_strings or [a.dest], "required": bool(a.required) if a.option_strings
                      else a.nargs not in ("?", "*"), "default": None if a.default is argparse.SUPPRESS else a.default,
                      "choices": list(a.choices) if a.choices else None, "help": a.help or ""}
                     for a in p._actions if a.dest != "help"]
        catalog.append({"command": name, "help": helps.get(name, ""), "arguments": arguments})
        _print(f"{name:<11} {helps.get(name, '')}")
    _data(commands=catalog, exit_codes={"0": "sucesso", "1": "falha", "2": "uso incorreto"},
          global_flags=["--config", "--db", "--json", "-v"])
    return EXIT_OK


def _robots_mt5(args, ctx, items):
    """install: copia o robô para a pasta de dados do MT5. compile: copia e compila no MetaEditor."""
    from mt5_extracao.robots import catalog, mt5_tools, store
    if not args.path:
        raise ValueError(f"uso: mt5x robots {args.action} <robô> (veja as chaves em 'mt5x robots list')")
    robot = catalog.get_robot(items, args.path)
    try:
        if args.action == "install":
            installed, result = mt5_tools.install_robot(ctx.config, robot, overwrite_presets=args.overwrite_presets,
                                                        upgrade_safety=args.upgrade_safety), None
        else:
            installed, result = mt5_tools.compile_robot(ctx.config, robot, overwrite_presets=args.overwrite_presets,
                                                        upgrade_safety=args.upgrade_safety)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    _data(robot=robot.key, files=[{"source": str(i.source), "target": str(i.target), "status": i.status}
                                  for i in installed])
    from mt5_extracao.robots import safety_lib
    wanted = safety_lib.required_version(robot)
    if wanted:
        _data(safety_lib=wanted)
        _print(f"  biblioteca de segurança {wanted} instalada na pasta do MT5")
    for item in installed:
        _print(f"  {item.status:<8} {item.target}")
    kept = [i.target.name for i in installed if i.status == mt5_tools.KEPT]
    if kept:
        _print("Preset(s) já existentes no MT5 com outro conteúdo foram mantidos: " + ", ".join(kept) +
               ". Para trocar pelos da biblioteca, repita com --overwrite-presets.")
    if result is None:
        _print(f"{robot.name}: arquivos na pasta do MT5. Para compilar: mt5x robots compile {args.path}")
        return EXIT_OK
    _data(compile=result.to_dict())
    for message in result.messages:
        _print(f"  {message}")
    _print(f"{robot.name}: {result.summary}")
    if not result.ok:
        return EXIT_FAIL
    from mt5_extracao.robots import pipeline
    pipeline.record_compiled(ctx.db, robot, result)          # não apaga o que outras etapas gravaram
    return EXIT_OK


def _robots_validate(args, ctx, items):
    from mt5_extracao.robots import catalog, contract
    if not args.path:
        raise ValueError("uso: mt5x robots validate <robô> (veja as chaves em 'mt5x robots list')")
    robot = catalog.get_robot(items, args.path)
    if robot.kind != catalog.MQL5:
        raise ValueError(f"{robot.name} é uma estratégia Python: o contrato vale para robôs MQL5.")
    problems = contract.validate(robot)
    _data(robot=robot.key, contract=contract.CONTRACT_VERSION, valid=not problems,
          problems=[vars(p) for p in problems])
    if not problems:
        _print(f"{robot.name}: contrato {contract.CONTRACT_VERSION} válido.")
        return EXIT_OK
    _print(f"{robot.name}: {len(problems)} problema(s) no contrato (veja docs/sdk/contrato.md):")
    for problem in problems:
        _print(f"  ✗ {problem}")
    return EXIT_FAIL


def _robots_check(args, ctx, items):
    """Ciclo completo do desenvolvedor; código de saída 0 só se todas as etapas passarem (bom para CI)."""
    from mt5_extracao.robots import catalog, mt5_tools, pipeline
    if not args.path or not args.symbol or not args.tf:
        raise ValueError("uso: mt5x robots check <robô> --symbol EURUSD --tf H1 [--from AAAA-MM-DD] [--to AAAA-MM-DD] "
                         "[--preset NOME] [--report arquivo.xml]")
    robot = catalog.get_robot(items, args.path)
    tf = _parse_tf(args.tf)
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    bridge_was_running = False
    try:
        if args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar o teste...")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        result = pipeline.check(ctx.db, ctx.config, robot, items, args.symbol, tf.label, start, end, args.preset)
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    if args.report:
        pipeline.write_junit(result, args.report)
    _data(check=result.to_dict(), report=args.report or "")
    icons = {pipeline.PASSED: "✓", pipeline.WARNED: "⚠", pipeline.FAILED: "✗", pipeline.SKIPPED: "–"}
    _print(f"Conferência de {robot.name}:")
    for step in result.steps:
        _print(f"  {icons[step.status]} {step.name} ({step.status}, {_fmt_num(step.seconds, 1)} s)" +
               (f": {step.detail}" if step.detail else ""))
    if result.report:
        for item in (i for i in result.report.items if i.status in ("falha", "aviso")):
            _print(f"      {item.icon} {item.name}: {item.detail}")
            if item.fix:
                _print(f"          O que fazer: {item.fix}")
    if args.report:
        _print(f"Relatório JUnit: {args.report}")
    _print("Tudo passou. Aprovação não é garantia de resultado." if result.ok
           else "Há etapa reprovada: corrija e rode de novo.")
    return EXIT_OK if result.ok else EXIT_FAIL


def _robots_parity(args, ctx, items):
    """Compara as operações do robô MQL5 (Strategy Tester) com as da estratégia Python no mesmo período."""
    from mt5_extracao.robots import catalog, mt5_tools, parity
    robot = catalog.get_robot(items, args.path) if args.path else None
    from mt5_extracao.robots import generate
    key = args.strategy or (robot.manifest.get("parity_with", "") if robot is not None else "")
    if robot is None or not key or not args.symbol or not args.tf:
        raise ValueError("uso: mt5x robots parity <robô MQL5> --strategy <chave da estratégia Python> --symbol X "
                         "--tf M5 [--from AAAA-MM-DD] [--to AAAA-MM-DD] (ou \"parity_with\" no robot.json)")
    declared = key == generate.DECLARED            # robô gerado: compara com a própria declaração
    strategy = None if declared else catalog.get_robot(items, key)
    if robot.kind != catalog.MQL5 or (strategy is not None and strategy.kind != catalog.PYTHON):
        raise ValueError("A paridade compara um robô MQL5 com uma estratégia Python (nessa ordem).")
    strategy_name = generate.load_spec(robot).name if declared else strategy.name
    tf = _parse_tf(args.tf)
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    bridge_was_running = False
    try:
        if args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar o teste...")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        if declared:
            outcome = parity.run_declared(ctx.db, ctx.config, robot, args.symbol, tf.label, start, end)
        else:
            outcome = parity.run(ctx.db, ctx.config, robot, strategy, args.symbol, tf.label, start, end, args.preset)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    report = outcome["report"]
    _data(parity=report.to_dict(), python_run=outcome["python_run"]["run_id"], mql5_run=outcome["mql5_run"]["run_id"])
    difference = report.result_difference
    _print(f"Paridade de {robot.name} (MQL5) com {strategy_name} (Python), {args.symbol} {tf.name}:")
    _print(f"  Operações: {report.python_trades} no Python, {report.mql5_trades} no MQL5; {report.matched} casadas "
           f"({_fmt_pct(report.matched_share)}; mínimo {_fmt_pct(parity.MIN_MATCHED_SHARE)}), "
           f"{report.same_sign} com o mesmo sinal de resultado")
    _print(f"  Par a par: {report.pairs_close} de {report.matched} com resultado até "
           f"{_fmt_pct(parity.PAIR_TOLERANCE)} um do outro ({_fmt_pct(report.pairs_close_share)}; mínimo "
           f"{_fmt_pct(parity.MIN_PAIRS_CLOSE_SHARE)}); mediana da razão MQL5/Python {_fmt_num(report.median_ratio, 3)} "
           f"(de {_fmt_num(1 - parity.MAX_MEDIAN_BIAS, 2)} a {_fmt_num(1 + parity.MAX_MEDIAN_BIAS, 2)})")
    _print(f"  Resultado: {_fmt_num(report.python_result)} no Python e {_fmt_num(report.mql5_result)} no MQL5; a "
           f"diferença é {_fmt_pct(report.difference_of_gross)} do giro bruto (máximo "
           f"{_fmt_pct(parity.MAX_DIFFERENCE_OF_GROSS)})" +
           (f" e {_fmt_pct(difference)} do resultado líquido (só informativo)" if difference is not None else ""))
    _print(f"  Modelo do Tester: {outcome['mql5_run']['metrics']['tester_model']}. A paridade compara a lógica; o "
           "custo com ticks reais é maior do que o do backtester Python (veja docs/sdk/exatidao.md).")
    if report.warmup_until:
        _print(f"  Os dois lados começam a operar em {report.warmup_until[:16]}, com o mesmo capital (as barras "
               "anteriores só aquecem os indicadores do Python).")
    for label, trades in (("Só no Python", report.only_python), ("Só no MQL5", report.only_mql5)):
        if trades:
            _print(f"  {label} ({len(trades)}): " + ", ".join(str(t[0])[:16] for t in trades[:8]) +
                   (" ..." if len(trades) > 8 else ""))
    _print("Paridade dentro do critério." if report.ok else
           "Fora do critério: confira as operações sem par e os pares que divergem antes de confiar no robô.")
    return EXIT_OK if report.ok else EXIT_FAIL


def _robots_battery(args, ctx, items):
    from mt5_extracao.robots import battery, catalog
    if not args.path:
        raise ValueError("uso: mt5x robots battery <robô> [--preset NOME] (veja as chaves em 'mt5x robots list')")
    robot = catalog.get_robot(items, args.path)
    bridge_was_running = False
    if args.deep:
        from mt5_extracao.robots import backtest, mt5_tools, store
        runs = [r for r in store.load_runs(ctx.db, robot.key) if r["source"] != backtest.DEEP_SOURCE]
        if robot.kind == catalog.MQL5 and runs and runs[0].get("trades"):
            count = len(battery.deep_plan(robot, runs[0]))
            _print(f"Bateria completa: {count} testes no Strategy Tester, cerca de "
                   f"{max(1, round(count * battery.SECONDS_PER_TEST / 60))} min. O MetaTrader 5 fica fechado enquanto isso.")
        if args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar os testes...")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
    try:
        report = battery.run_battery(ctx.db, ctx.config, robot, items, args.preset, deep=args.deep,
                                     symbol=args.symbol)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    finally:
        if bridge_was_running:
            from mt5_extracao.robots import mt5_tools
            mt5_tools.restart_bridge()
    _data(battery=report.to_dict())
    _print(f"Bateria de segurança de {robot.name}" + (f", preset {report.preset}" if report.preset else "") +
           (f", sobre o backtest de {report.symbol} {report.timeframe}" if report.symbol else ""))
    for group, status in ((battery.STATIC, report.static_status), (battery.STATISTICAL, report.statistical_status)):
        _print(f"\n{group.capitalize()}: {status}")
        for item in (i for i in report.items if i.group == group):
            _print(f"  {item.icon} {item.name}: {item.detail}")
            if item.fix:
                _print(f"      O que fazer: {item.fix}")
    if report.approved:
        _print("\nAprovada: o robô pode rodar em conta de demonstração." +
               ("" if report.allows_real else " Sem a parte de código conferida, nunca em conta real."))
        _print("Aprovação não é garantia de resultado. Confira os avisos (⚠) antes de seguir.")
        return EXIT_OK
    _print("\nReprovada: corrija os itens marcados com ✗ e rode a bateria de novo.")
    return EXIT_FAIL


def _robots_start(args, ctx, items):
    """Inicia o robô em conta de demonstração. Quem opera é o robô, dentro do MT5; aqui nenhuma ordem é enviada."""
    from mt5_extracao.robots import catalog, live, mt5_tools
    if not args.path or not args.symbol or not args.tf:
        raise ValueError("uso: mt5x robots start <robô> --symbol WIN$N --tf M5 [--preset NOME] --close-terminal")
    robot = catalog.get_robot(items, args.path)
    tf = _parse_tf(args.tf)
    account = ctx.connector().account_summary()
    gates = live.demo_gates(ctx.config, robot, account, args.preset, args.symbol, tf.name)
    _data(robot=robot.key, gates=[vars(g) for g in gates])
    _print(f"Exigências para iniciar {robot.name} em conta demo:")
    for gate in gates:
        _print(f"  {'✓' if gate.ok else '✗'} {gate.name}" + (f": {gate.detail}" if gate.detail else ""))
        if gate.fix:
            _print(f"      O que fazer: {gate.fix}")
    if not all(g.ok for g in gates):
        _print("\nO robô não foi iniciado.")
        return EXIT_FAIL
    if not args.close_terminal:
        _print("\nTudo certo, mas o robô só entra no gráfico quando o programa reinicia o MetaTrader 5. Isso "
               "interrompe o que estiver rodando nele. Para seguir, repita com --close-terminal.")
        return EXIT_FAIL
    bridge_was_running = False
    try:
        if mt5_tools.terminal_running(ctx.config):
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        state = live.start_demo(ctx.db, ctx.config, robot, account, args.symbol, tf.label, args.preset)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    _data(live=state)
    _print(f"\n{robot.name} iniciado na conta demo {state['account']} ({state['server']}), {state['symbol']} "
           f"{state['timeframe']}" + (f", preset {state['preset']}" if state["preset"] else "") + ".")
    _print(f"Acompanhe: mt5x robots monitor {args.path}   Parar tudo: mt5x robots stop")
    return EXIT_OK


def _robots_monitor(args, ctx, items):
    from mt5_extracao import mt5_connector
    from mt5_extracao.robots import catalog, live
    if not args.path:
        raise ValueError("uso: mt5x robots monitor <robô>")
    robot = catalog.get_robot(items, args.path)
    state = live.live_state(ctx.db, robot)
    if not state:
        raise ValueError(f"{robot.name} nunca foi iniciado por aqui. Use: mt5x robots start {args.path} ...")
    if not str(state.get("magic", "")).isdigit():
        raise RuntimeError(f"{robot.name} não tem magic number: não dá para separar as operações dele.")
    account = ctx.connector().account_summary()
    since = datetime.strptime(state["started_at"], "%Y-%m-%d %H:%M:%S")
    numbers = live.status(mt5_connector.mt5, state["magic"], since)
    killed = live.stop_active(ctx.config)
    _data(live=state, status=numbers, account=account, stop_active=killed)
    _print(f"{robot.name} em {state['symbol']} {state['timeframe']}, desde {state['started_at']}" +
           (f" (parado em {state['stopped_at']})" if state.get("stopped_at") else ""))
    if account and str(account.get("login")) != str(state["account"]):
        _print(f"  ! A conta conectada agora ({account.get('login')}) não é a do início ({state['account']}).")
    _print(f"  Posições abertas: {numbers['open_positions']} ({_fmt_num(numbers['open_volume'])} lotes), "
           f"resultado aberto {_fmt_num(numbers['open_profit'])}")
    if numbers["positions_without_stop"]:
        _print(f"  ! {numbers['positions_without_stop']} posição(ões) sem stop.")
    _print(f"  Operações fechadas: {numbers['closed_trades']} ({numbers['wins']} com lucro, {numbers['losses']} com "
           f"perda), resultado {_fmt_num(numbers['closed_result'])}")
    if killed:
        _print("  PARAR TUDO está ativo: os robôs com a camada de segurança não abrem novas operações.")
    return EXIT_OK


def _robots_stop(args, ctx, items):
    from mt5_extracao.robots import catalog, live, mt5_tools
    if args.release:
        existed = live.release_stop(ctx.config)
        _data(stop_active=False)
        _print("PARAR TUDO liberado: os robôs voltam a poder operar." if existed else "PARAR TUDO já estava desligado.")
        return EXIT_OK
    path = live.stop_all(ctx.config)
    for robot in items:
        if robot.kind == catalog.MQL5 and robot.stage == catalog.DEMO:
            live.mark_stopped(ctx.db, robot)
    closed = False
    if args.close_terminal and mt5_tools.terminal_running(ctx.config):
        mt5_tools.release_terminal(ctx.config)
        closed = True
    _data(stop_active=True, kill_file=path, terminal_closed=closed)
    _print(f"PARAR TUDO acionado: {path}")
    _print("Os robôs com a camada de segurança do HADOUKEN param de operar no próximo ciclo. Posições abertas "
           "NÃO são fechadas por este programa: confira no MetaTrader 5.")
    _print("MetaTrader 5 fechado." if closed else
           "Robô sem essa camada (ex.: só .ex5) não obedece ao arquivo: feche o MT5 ou repita com --close-terminal.")
    _print("Para liberar depois: mt5x robots stop --release")
    return EXIT_OK


def cmd_robots(args, ctx):
    from mt5_extracao import robots
    from mt5_extracao.robots import catalog
    if args.action == "import":
        if not args.path:
            raise ValueError("informe o arquivo ou a pasta: mt5x robots import <arquivo.mq5, .ex5 ou pasta>")
        if os.path.isdir(os.path.expanduser(args.path)):
            found = robots.import_library(ctx.db, args.path)
            _data(robots=[_robot_dict(r) for r in found])
            _print(f"Pasta adicionada ao catálogo: {len(found)} robô(s): " + ", ".join(r.name for r in found))
            return EXIT_OK
        robot = robots.import_robot(ctx.db, args.path)
        _data(robots=[_robot_dict(robot)])
        _print(f"Robô importado: {robot.name} ({robot.key}). Risco: {robot.risk.level}.")
        for note in robot.notes:
            _print(f"  {note}")
        return EXIT_OK
    items = robots.load_catalog(ctx.config, ctx.db)
    if args.action == "new":
        from mt5_extracao.robots import scaffold
        if not args.path or not args.dir:
            raise ValueError("uso: mt5x robots new <Nome> --dir <pasta de robôs> (ex.: --dir ~/robos)")
        try:
            created = scaffold.create(args.path, args.dir, items, ctx.config)
        except (OSError, RuntimeError) as e:
            raise RuntimeError(str(e)) from e
        robot = robots.robot_from_file(created[0])
        _data(robot=_robot_dict(robot), files=[str(p) for p in created])
        _print(f"Robô {robot.name} criado:")
        for path in created:
            _print(f"  {path}")
        root = Path(args.dir).expanduser().resolve()
        from mt5_extracao.robots import store
        known = [Path(p).resolve() for p in catalog.library_dirs(ctx.config)] + [
            Path(r["path"]).resolve() for r in store.load_records(ctx.db).values() if r["kind"] == catalog.LIBRARY]
        if root not in known:
            robots.import_library(ctx.db, root)
            _print(f"A pasta {root} foi adicionada ao catálogo.")
        _print(f"Agora escreva o sinal em {created[0].name} (veja o README ao lado) e rode: "
               f"mt5x robots compile {robot.key.split(':', 1)[1]}")
        return EXIT_OK
    if args.action == "generate":
        from mt5_extracao.robots import generate
        if not args.path or not args.name or not args.dir:
            raise ValueError("uso: mt5x robots generate <declaração.json> --name <Nome> --dir <pasta de robôs>")
        try:
            created = generate.create(generate.read_spec_file(args.path), args.name, args.dir, items, ctx.config)
        except (OSError, RuntimeError) as e:
            raise RuntimeError(str(e)) from e
        robot = robots.robot_from_file(created[0])
        slug = robot.key.split(":", 1)[1]
        _data(robot=_robot_dict(robot), files=[str(p) for p in created])
        _print(f"Robô {robot.name} gerado a partir da declaração:")
        for path in created:
            _print(f"  {path}")
        from mt5_extracao.robots import store
        root = Path(args.dir).expanduser().resolve()
        known = [Path(p).resolve() for p in catalog.library_dirs(ctx.config)] + [
            Path(r["path"]).resolve() for r in store.load_records(ctx.db).values() if r["kind"] == catalog.LIBRARY]
        if root not in known:
            robots.import_library(ctx.db, root)
            _print(f"A pasta {root} foi adicionada ao catálogo.")
        _print(f"Compile e prove a paridade com a declaração: mt5x robots compile {slug} && "
               f"mt5x robots parity {slug} --symbol <ativo> --tf <timeframe>")
        return EXIT_OK
    if args.action == "backtest":
        return _robots_backtest(args, ctx, items)
    if args.action in ("install", "compile"):
        return _robots_mt5(args, ctx, items)
    if args.action == "battery":
        return _robots_battery(args, ctx, items)
    if args.action == "validate":
        return _robots_validate(args, ctx, items)
    if args.action == "check":
        return _robots_check(args, ctx, items)
    if args.action == "parity":
        return _robots_parity(args, ctx, items)
    if args.action in ("start", "monitor", "stop"):
        return {"start": _robots_start, "monitor": _robots_monitor, "stop": _robots_stop}[args.action](args, ctx, items)
    if args.path:
        items = [robots.get_robot(items, args.path)]
    _data(robots=[_robot_dict(r, full=bool(args.path)) for r in items])
    for robot in items:
        version = f" v{robot.version}" if robot.version else ""
        _print(f"{robot.key:<28} {robot.kind_label:<7}{robot.name}{version}")
        details = [d for d in (robot.regime and f"regime: {robot.regime}",
                               robot.timeframes and "timeframes: " + ", ".join(robot.timeframes),
                               f"estágio: {robot.stage_label}") if d]
        _print("    " + " · ".join(details))
        _print(f"    risco: {robot.risk.level}" + (f" ({robot.risk.summary})" if robot.risk.reasons else ""))
        for name, rating in robot.preset_risks.items():
            _print(f"      preset {name}: {rating.level}" + (f" ({rating.summary})" if rating.reasons else ""))
        if args.path:
            _print(f"    arquivo: {robot.path}")
            for group in robot.groups or [""]:
                if group:
                    _print(f"    {group}")
                for item in (i for i in robot.inputs if i.group == group):
                    _print(f"      {item.name} = {item.default}" + (f"   ({item.comment})" if item.comment else ""))
        for note in robot.notes:
            _print(f"    ! {note}")
    if not catalog.library_dirs(ctx.config):
        _print("\nPara ver robôs MQL5, indique a pasta em [ROBOTS] library_dirs (config/config.ini) "
               "ou use 'mt5x robots import <arquivo>'.")
    _print("\nA classe de risco vem dos parâmetros e do preset; não é garantia. Veja docs/robos.md.")
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
    _data(symbol=args.symbol, timeframe=tf.name, strategy=strategy.key, params=strategy.params, bars=len(df),
          data_start=df["time"].iloc[0], data_end=df["time"].iloc[-1], metrics=m, spec_known=spec.known,
          skipped_min_lot=result.skipped_min_lot)
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
    _data(timeframe=tf.name, approved=approved, missing=missing, verdicts=[
        {k: v.get(k) for k in ("symbol", "strategy", "approved", "status", "params", "checks", "oos_metrics",
                               "monte_carlo", "data_start", "data_end", "bars")} for v in verdicts])
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
    _data(timeframe=tf.name, rows=rows, missing=missing)
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
    _data(path=os.path.abspath(path), rows=rows, missing=missing)
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
    parser.add_argument("--json", dest="json_output", action="store_true",
                        help="saída em um único objeto JSON (para agentes e scripts); use antes do comando")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("doctor", help="diagnóstico do ambiente")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("symbols", help="lista símbolos do MT5")
    p.add_argument("--group", default="*", help='filtro do MT5, ex.: "*WIN*"')
    p.set_defaults(func=cmd_symbols)

    p = sub.add_parser("mt5", help="estado do terminal e, no modo oculto, captura da tela virtual")
    p.add_argument("action", nargs="?", choices=["status", "show"], default="status")
    p.add_argument("--out", help="show: arquivo PNG da captura (padrão: logs/mt5_oculto.png)")
    p.set_defaults(func=cmd_mt5)

    p = sub.add_parser("resample", help="monta timeframes maiores a partir de uma base menor (ex.: M5 e H1 de M1)")
    p.add_argument("--symbols", required=True, help="símbolos separados por vírgula")
    p.add_argument("--base", required=True, help="timeframe já extraído, o menor (ex.: M1)")
    p.add_argument("--tf", required=True, help="timeframes a montar, separados por vírgula (ex.: M5,M15,H1)")
    p.add_argument("--from", dest="date_from", help="início AAAA-MM-DD (padrão: toda a base)")
    p.add_argument("--to", dest="date_to", help="fim AAAA-MM-DD")
    p.set_defaults(func=cmd_resample)

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

    p = sub.add_parser("robots", help="catálogo de robôs (MQL5 e Python): tipo, risco e estágio")
    p.add_argument("action", nargs="?", choices=["list", "import", "backtest", "install", "compile", "battery", "validate", "check", "parity",
                                                    "generate",
                                                    "new",
                                                    "start",
                                                    "monitor",
                                                    "stop"],
                   default="list")
    p.add_argument("path", nargs="?", help="list/backtest/install/compile: chave do robô; "
                                           "import: arquivo .mq5/.ex5 ou pasta de robôs")
    p.add_argument("--overwrite-presets", action="store_true",
                   help="install/compile: troca presets que já existem no MT5 pelos da biblioteca")
    p.add_argument("--symbol", help="backtest: ativo; battery: de qual ativo é o backtest avaliado")
    p.add_argument("--symbols", help="backtest: vários ativos separados por vírgula (um teste por ativo)")
    p.add_argument("--tf", help="backtest: timeframe (ex.: D1)")
    p.add_argument("--from", dest="date_from", help="backtest: início AAAA-MM-DD")
    p.add_argument("--to", dest="date_to", help="backtest: fim AAAA-MM-DD")
    p.add_argument("--param", action="append", default=[], help="backtest: parâmetro nome=valor (repetível)")
    p.add_argument("--strategy", help="parity: chave da estratégia Python a comparar (ex.: donchian)")
    p.add_argument("--deep", action="store_true",
                   help="battery (robô MQL5): repete o Strategy Tester para medir fora da amostra e robustez ±20%%")
    p.add_argument("--report", help="check: grava um relatório JUnit XML neste arquivo (para CI)")
    p.add_argument("--name", help="generate: nome do robô a criar")
    p.add_argument("--dir", help="new/generate: pasta de robôs onde criar (layout Experts/ Presets/)")
    p.add_argument("--upgrade-safety", action="store_true",
                   help="install/compile: troca a biblioteca de segurança instalada no MT5 pela versão do clone")
    p.add_argument("--preset", help="backtest de robô MQL5 e battery: nome do preset (.set) a usar")
    p.add_argument("--close-terminal", action="store_true",
                   help="backtest de robô MQL5, start e stop: fecha o MT5 (e para a ponte) se estiver aberto; "
                        "backtest e start religam a ponte depois")
    p.add_argument("--release", action="store_true", help="stop: desliga o PARAR TUDO (os robôs voltam a poder operar)")
    p.set_defaults(func=cmd_robots)

    p = sub.add_parser("analyze", help="números de estudo de um ativo (retorno, volatilidade, queda, sazonalidade)")
    p.add_argument("--symbol", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--period", default="1 ano", help='"1 mês", "3 meses", "6 meses", "1 ano" ou "Tudo"')
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("status", help="retrato da base, dos robôs e das validações, com as próximas ações sugeridas")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("audit", help="auditoria de segurança: projeto, patrimônio e gestão do capital")
    p.add_argument("--mt5", action="store_true", help="consulta o MT5 para saber se a conta conectada é demo ou real")
    p.add_argument("--fail-on", default="alto", choices=["crítico", "alto", "médio", "baixo"],
                   help="gravidade mínima que faz o comando sair com código 1 (padrão: alto)")
    p.set_defaults(func=cmd_audit)

    p = sub.add_parser("commands", help="lista todos os comandos e argumentos (para agentes)")
    p.set_defaults(func=cmd_commands)

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
    _OUT.update(json=bool(args.json_output), messages=[], errors=[], data={}, raw=False)
    ctx = Context(args)
    try:
        code = _run_command(args, ctx)
        if _OUT["json"] and not _OUT["raw"]:
            import json
            print(json.dumps(_jsonable({"ok": code == EXIT_OK, "command": args.command, "exit_code": code,
                                        "data": _OUT["data"], "messages": _OUT["messages"],
                                        "errors": _OUT["errors"]}), ensure_ascii=False), flush=True)
        return code
    finally:
        _OUT["json"] = False
        teardown_logging()


def _run_command(args, ctx):
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


if __name__ == "__main__":
    sys.exit(main())
