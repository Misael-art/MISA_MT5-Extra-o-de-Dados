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
    mt5x resample --symbols X --base M1 --tf M5,M15,H1    # mantém o resample pelo relógio já existente
    mt5x resample --symbols X --base M1 --tf M3,1D,1440min --session-aware
                                                        # barras analíticas por sessão observada
    mt5x analytics features list|show|values|affected     # características versionadas e consulta point-in-time
    mt5x analytics features compute --symbol X --tf M5    # volume, RVOL, volatilidade, regime e estrutura (T13.3)
    mt5x panel [--open]                                # painel visual: ativos x timeframes x robôs
    mt5x panel --serve --open                          # painel vivo: atualiza sozinho e tem botões de ação
    mt5x robots [list|import|backtest|market] ...     # catálogo, importação, backtest e pré-check Market
    mt5x robots [install|compile] <robô>               # copia o robô MQL5 para o MT5 e compila no MetaEditor
    mt5x robots backtest <robô MQL5> --symbol EURUSD --tf H1 [--preset X]   # Strategy Tester do MT5
    mt5x robots backtest <robô> --symbols EURUSD,GBPUSD --tf H1   # o mesmo teste em vários ativos
    mt5x robots backtest <r1,r2> --symbols A,B --tfs M15,H1 [--resume]   # lote: robôs x ativos x timeframes
    mt5x robots new <Nome> --dir <pasta>               # cria o esqueleto de um robô sobre a biblioteca de segurança
    mt5x robots generate <declaração.json> --name <Nome> --dir <pasta>   # robô MQL5 a partir de uma estratégia declarada
    mt5x robots validate <robô>                        # confere o robot.json contra o contrato do SDK
    mt5x robots check <robô> --symbol X --tf M5        # contrato, compilação, backtest e bateria em um comando
    mt5x robots parity <robô MQL5> --strategy <chave> --symbol X --tf M5   # mesmas operações que o Python?
    mt5x robots battery <robô> [--preset X] [--deep]   # bateria de segurança; --deep repete o Strategy Tester
    mt5x robots market <robô> [--pack <pasta>]        # pré-check local e pacote manual do MQL5 Market
    mt5x robots start <robô> --symbol X --tf M5        # inicia o robô em conta DEMO (travas conferidas antes)
    mt5x robots monitor <robô> | mt5x robots stop      # acompanha (somente leitura) | PARAR TUDO
    mt5x robots monitor <robô> --watch                 # um pop-up a cada operação do robô
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


def _notify(ctx, title, text="", level=None):
    """Pop-up do sistema para uma ação concluída ([APP] notifications). Nunca falha o comando."""
    from mt5_extracao import notify
    try:
        notify.send(ctx.config, title, text, level or notify.INFO)
    except Exception:           # nem a leitura da configuração pode derrubar o comando por causa de um aviso
        pass


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
    """Monta barras legadas offline ou, em --session-aware, barras analíticas com metadata do MT5."""
    from mt5_extracao import resample
    symbols, base = _parse_symbols(args.symbols), _parse_tf(args.base)
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None

    if getattr(args, "session_aware", False):
        from mt5_extracao.analytics import resampling
        if base is not timeframes.Timeframe.M1:
            raise ValueError("--session-aware exige --base M1; as barras por sessão usam M1 como origem.")
        targets = [value.strip() for value in args.tf.split(",") if value.strip()]
        if not targets:
            raise ValueError("informe em --tf os timeframes a montar, ex.: --tf M3,M5,1D,1440min")
        for target in targets:
            resampling.parse_target(target)
        connector = ctx.connector()
        done, failed = [], []
        for symbol in symbols:
            try:
                info = connector.get_symbol_info(symbol)
                if info is None:
                    raise RuntimeError(f"symbol_info do MT5 não disponível para {symbol}.")
                rows = resampling.resample_symbol(
                    ctx.db, symbol, targets, symbol_info=info,
                    session_gap_minutes=args.session_gap_minutes,
                    session_anchor=args.session_anchor,
                    partial_last_bar=args.partial_last_bar, start=start, end=end)
            except (ValueError, RuntimeError) as e:
                failed.append({"symbol": symbol, "error": str(e)})
                _print(f"{symbol}: {e}")
                continue
            done += rows
            for row in rows:
                note = ""
                if row.get("synthetic"):
                    note = (f"; {row['rejected_groups']} janelas recusadas; sintético, âncora 00:00 do "
                            "calendário da corretora, não é tempo do MT5")
                _print(f"{symbol} {row['timeframe']}: {row['bars']} barras observadas de {row['source_bars']} M1; "
                       f"{row['inserted']} novas, {row['corrected']} corrigidas, {row['unchanged']} inalteradas"
                       f"{note}")
        _data(session_aware=True, resampled=done, failed=failed)
        _print(f"\nConcluído: {len(done)} série(s) por sessão, {len(failed)} ativo(s) com falha. "
               "O calendário é inferido das barras M1 observadas; nenhuma barra sem dados foi criada.")
        return EXIT_OK if done and not failed else EXIT_FAIL

    targets = [_parse_tf(t) for t in args.tf.split(",") if t.strip()]
    if not targets:
        raise ValueError("informe em --tf os timeframes a montar, ex.: --tf M5,M15,H1")
    for target in targets:
        resample.check_pair(base, target)
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
    if args.action in ("layout", "restore-layout"):
        return _mt5_layout(args, ctx)
    if args.action == "view":
        from mt5_extracao.robots import view
        try:
            tf = _parse_tf(args.tf).name if args.tf else ""
            # o painel só entra na conta conferida: com o terminal aberto dá para conferir antes de fechá-lo
            account = None
            if args.close_terminal and mt5_tools.terminal_running(ctx.config):
                try:
                    account = ctx.connector().account_summary()
                except Exception as e:           # sem a conta conferida a visão abre só com o gráfico
                    logging.getLogger(__name__).debug(f"Conta não conferida para a visão: {e}")
            result = view.open_view(ctx.config, close_terminal=args.close_terminal, symbol=args.symbol or "",
                                    timeframe=tf, db=ctx.db, account=account, confirm=mt5_tools.confirm_startup)
        except view.TerminalOpen as e:
            raise ValueError(str(e)) from e
        except OSError as e:
            raise RuntimeError(str(e)) from e
        _data(**result)
        _print(view.describe(result))
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


def _mt5_layout(args, ctx):
    """Estado da organização da janela do MT5 (`layout`) ou devolução do arranjo anterior (`restore-layout`)."""
    from mt5_extracao.robots import layout, mt5_tools
    data = mt5_tools.data_dir(ctx.config)
    common = mt5_tools.common_files_dir(ctx.config)
    if args.action == "restore-layout":
        if mt5_tools.terminal_running(ctx.config):
            raise ValueError("O MetaTrader 5 está aberto: ele regravaria o arranjo ao fechar. Feche o terminal e rode "
                             "de novo: mt5x mt5 restore-layout")
        restored = layout.restore(data)
        _data(restored=restored)
        _print("Arranjo anterior da janela do MT5 devolvido." if restored else
               "Não há arranjo anterior guardado: a janela do MT5 está como você deixou.")
        if restored and layout.enabled(ctx.config, common):
            _print("A organização continua ligada: o próximo 'mt5x robots start' organiza a janela de novo. Para "
                   "desligar: [MT5] manage_layout = false em config/config.ini (ou Arquivo > Configurações).")
        return EXIT_OK
    state = layout.status(ctx.config, data, common)
    _data(**state)
    if state["panel_off"]:
        _print("Organização da janela: DESLIGADA pelo botão TELA do painel do HADOUKEN.")
    elif not state["enabled"]:
        _print("Organização da janela: DESLIGADA em [MT5] manage_layout.")
    else:
        _print("Organização da janela: LIGADA. Vale na próxima vez que o programa abrir o MT5 para iniciar um robô.")
    for key, _attr, label, _hint in layout.OPTION_KEYS:
        _print(f"  [{'x' if state['options'][key] else ' '}] {label}  ({key})")
    _print(f"Disposição do painel do HADOUKEN: {layout.PANEL_LAYOUTS[state['panel_layout']][0]}  (panel_layout = "
           f"{state['panel_layout']})")
    _print("Escondido agora no terminal: " + (", ".join(state["hidden_now"]) or "nada"))
    _print("Arranjo anterior guardado: " + ("sim (para devolver: mt5x mt5 restore-layout, com o MT5 fechado)"
                                             if state["has_arrangement"] else "não"))
    _print("Para aplicar agora: mt5x mt5 view --close-terminal")
    return EXIT_OK


def _panel_serve(args, ctx):
    """Painel vivo: servidor local (só 127.0.0.1, com chave) que atualiza a página e executa as ações da lista."""
    import time
    from mt5_extracao.robots import panel_server
    try:
        server = panel_server.PanelServer(ctx.db, ctx.config, args.port or 0,
                                          notifier=lambda title, text, level=None: _notify(ctx, title, text, level),
                                          connect=ctx.connector)
    except OSError as e:
        raise RuntimeError(f"Não foi possível abrir a porta {args.port}: {e}. Tente outra com --port.") from e
    server.start()
    _data(url=server.url, port=server.port)
    _print(f"Painel vivo em {server.url}")
    _print("Só este computador o alcança, e só com a chave do endereço acima (ela muda a cada vez). Ele se atualiza "
           "sozinho e os botões rodam uma ação por vez. Ctrl+C para encerrar.")
    if args.open:
        import webbrowser
        webbrowser.open(server.url)
    try:
        deadline = time.monotonic() + args.seconds if args.seconds else None
        while deadline is None or time.monotonic() < deadline:
            time.sleep(0.2)
    except KeyboardInterrupt:
        _print("\nPainel encerrado.")
    finally:
        server.stop()
    return EXIT_OK


def cmd_panel(args, ctx):
    """Painel visual (um arquivo HTML) com a matriz ativo x timeframe x robô. Só leitura."""
    from mt5_extracao.robots import matrix, panel
    if args.serve:
        return _panel_serve(args, ctx)
    data = matrix.build(ctx.db, ctx.config)
    out = args.out or str(PROJECT_ROOT / "exports" / "painel_resultados.html")
    path = panel.write(data, out)
    counts = {}
    for cell in data["cells"]:
        counts[cell["status"]] = counts.get(cell["status"], 0) + 1
    _data(panel=str(path), symbols=len(data["symbols"]), timeframes=data["timeframes"], robots=len(data["robots"]),
          cells=len(data["cells"]), by_status=counts)
    _print(f"Painel gravado em {path}")
    _print(f"  {len(data['symbols'])} ativo(s), {len(data['timeframes'])} timeframe(s), {len(data['robots'])} robô(s) e "
           f"estratégia(s), {len(data['cells'])} combinação(ões) testada(s)" +
           ("" if data["cells"] else ". Ainda não há backtests: rode 'mt5x robots backtest ...' e gere de novo."))
    if counts:
        _print("  " + " · ".join(f"{n} {status}" for status, n in sorted(counts.items(), key=lambda kv: -kv[1])))
    if args.open:
        import webbrowser
        webbrowser.open(path.resolve().as_uri())
    else:
        _print("Abra o arquivo no navegador (ou repita com --open).")
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
                for key in ("invalid_ohlc", "nonfinite_ohlc", "invalid_time", "unordered_time",
                            "invalid_volume", "zero_volume", "duplicate_time", "gaps"):
                    for ex in report.get(key, {}).get("examples", []):
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
    _notify(ctx, "Extração concluída" if failed == 0 and canceled == 0 else "Extração com falhas",
            f"{', '.join(symbols[:6])} {tf.name}: {ok} ok, {failed} com falha, {canceled} cancelado(s)",
            None if failed == 0 and canceled == 0 else "aviso")
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
    _notify(ctx, "Extração concluída" if failed == 0 and canceled == 0 else "Extração com falhas",
            f"{', '.join(symbols[:6])} {tf.name}: {ok} ok, {failed} com falha, {canceled} cancelado(s)",
            None if failed == 0 and canceled == 0 else "aviso")
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
    if x is None:
        return "indefinido"
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
        if mql5 and args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar os testes...")
            _notify(ctx, "MetaTrader 5 fechado para um teste", "Ele é fechado só durante o teste; a ponte é religada depois.", "aviso")
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
    _notify(ctx, f"{robot.name}: backtest em {len(symbols)} ativo(s)",
            f"{len(symbols) - len(failed)} testado(s), {len(failed)} sem teste, {tf.name}", "aviso" if failed else None)
    return EXIT_FAIL if failed else EXIT_OK


def _robots_backtest_batch(args, ctx, items):
    """Lote: robôs × ativos × timeframes, em fila. Preenche a matriz do painel (mt5x panel)."""
    from mt5_extracao.robots import backtest, catalog, mt5_tools
    from mt5_extracao.robots.budget import Budget
    robots = [catalog.get_robot(items, key) for key in args.path.split(",") if key.strip()]
    symbols = _parse_symbols(args.symbols or args.symbol)
    tfs = [_parse_tf(t) for t in (args.tfs or args.tf or "").split(",") if t.strip()]
    if not robots or not tfs:
        raise ValueError("uso: mt5x robots backtest <robô1,robô2> --symbols A,B --tfs M15,H1 [--from] [--to] "
                         "[--preset NOME] [--resume] [--close-terminal]")
    if args.param:
        raise ValueError("--param não vale em lote: cada robô tem os próprios parâmetros. Use --preset.")
    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to, end_of_day=True) if args.date_to else None
    total = len(robots) * len(symbols) * len(tfs)
    names = {r.key: r.name for r in robots}
    mql5 = any(r.kind == catalog.MQL5 for r in robots)
    _print(f"Lote: {len(robots)} robô(s) × {len(symbols)} ativo(s) × {len(tfs)} timeframe(s) = {total} teste(s), "
           "um por vez." + (" Robôs MQL5 usam o Strategy Tester em um perfil privado, um teste por vez."
                            if mql5 else ""))
    count = {"n": 0}

    def progress(item):
        count["n"] += 1
        head = f"  [{count['n']}/{total}] {names[item['robot']]} {item['symbol']} {item['timeframe']}: "
        if item["run"] is None:
            _print(head + "não testado: " + item["error"])
            return
        m = item["run"]["metrics"]
        reused = " (checkpoint recuperado)" if item.get("reuse_basis") == "checkpoint" else " (identidade conferida; pulado)"
        _print(head + f"{m['trades']} operações, profit factor {_fmt_num(m['profit_factor'])}, resultado "
               f"{_fmt_pct(m['return_pct'])}" + (reused if item["skipped"] else ""))
    budget = Budget(ctx.db, ctx.config, batch_id=getattr(args, "batch_id", None))
    bridge_was_running = False
    try:
        if mql5 and args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar os testes...")
            _notify(ctx, "MetaTrader 5 fechado para um lote de testes", "A ponte é religada no fim.", "aviso")
            bridge_was_running = mt5_tools.release_terminal(ctx.config)
        results = backtest.run_batch(ctx.db, ctx.config, robots, symbols, tfs, start, end, args.preset,
                                     resume=args.resume, progress=progress, budget=budget,
                                     batch_id=getattr(args, "batch_id", None))
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    _print(f"Checkpoint do lote: {budget.batch_id}. Retome com --batch-id {budget.batch_id}.")
    failed = [r for r in results if r["run"] is None]
    skipped = sum(1 for r in results if r["skipped"])
    _data(batch=[{**{k: r[k] for k in ("robot", "symbol", "timeframe", "error", "skipped")},
                  "reuse_basis": r.get("reuse_basis", ""),
                  "run": {k: v for k, v in r["run"].items() if k not in ("trades", "equity")} if r["run"] else None}
                 for r in results],
          summary={"total": total, "tested": len(results) - len(failed) - skipped, "skipped": skipped, "failed": len(failed)})
    partial = bool(budget.reason) or len(results) < total
    _data(batch_id=budget.batch_id, batch_status="interrupted" if partial else "failed" if failed else "completed",
          reason=budget.reason, remaining=total-sum(r["run"] is not None for r in results), resources=budget.measurements)
    label = "Lote interrompido" if partial else "Lote concluído"
    _print(f"\n{label}: {len(results) - len(failed) - skipped} testado(s), {skipped} pulado(s), {len(failed)} sem "
           "teste. Veja o conjunto com: mt5x panel")
    _notify(ctx, "Lote de backtests interrompido" if partial else "Lote de backtests concluído",
            f"{len(results) - len(failed) - skipped} testado(s), {skipped} pulado(s), {len(failed)} sem teste",
            "aviso" if failed else None)
    return EXIT_FAIL if failed or partial else EXIT_OK


def _robots_backtest(args, ctx, items):
    from mt5_extracao.robots import backtest, catalog
    if args.path and (args.tfs or "," in args.path):
        if not (args.symbol or args.symbols):
            raise ValueError("informe os ativos do lote em --symbols A,B,C")
        return _robots_backtest_batch(args, ctx, items)
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
    _notify(ctx, f"{robot.name}: backtest concluído",
            f"{run['symbol']} {run['timeframe']}: {m['trades']} operações, profit factor {_fmt_num(m['profit_factor'])}")
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
            _notify(ctx, "MetaTrader 5 fechado para um teste", "Ele é fechado só durante o teste; a ponte é religada depois.", "aviso")
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
    _notify(ctx, f"{robot.name}: backtest concluído",
            f"{run['symbol']} {run['timeframe']}: {m['trades']} operações, profit factor {_fmt_num(m['profit_factor'])}")
    return EXIT_OK


def cmd_analyze(args, ctx):
    """Os números da aba Análise (study.py) para um ativo e timeframe, sem interface."""
    from mt5_extracao import data_quality, study
    tf = _parse_tf(args.tf)
    periods = dict(study.PERIODS)
    if args.period not in periods:
        raise ValueError(f"período inválido: {args.period} (use um de: {', '.join(periods)})")
    item = next((i for i in ctx.db.data_inventory() if i["symbol"] == args.symbol and i["timeframe"] == tf.label), None)
    if item is None or not item["rows"]:
        raise ValueError(f"sem barras de {args.symbol} {tf.name} na base. Rode: mt5x extract --symbols {args.symbol} "
                         f"--tf {tf.name} --from AAAA-MM-DD")
    df = ctx.db.load_ohlcv(args.symbol, tf.label, study.period_start(item["end"], periods[args.period]))
    quality = data_quality.check_quality(df, tf.minutes,
        ctx.config.get("APP", "session_start", fallback="09:00"),
        ctx.config.get("APP", "session_end", fallback="18:30"))
    _data(symbol=args.symbol, timeframe=tf.name, period=args.period, bars=len(df),
          quality=quality, time_context=ctx.db.time_context())
    if df.empty:
        raise RuntimeError("Sem barras na janela consultada; atualize o inventário ou amplie o período.")
    if data_quality.blocks_price_analysis(quality):
        raise RuntimeError("Análise indisponível: preços ou horários inválidos. Confira o relatório de qualidade "
                           "e a fonte antes de analisar; nenhum valor foi preenchido.")
    summary = study.summarize(df)
    freshness = study.freshness(item["end"], tf.minutes)
    season = {name: frame.reset_index().to_dict("records") for name, frame in study.seasonality(df, bar_minutes=tf.minutes).items()}
    _data(symbol=args.symbol, timeframe=tf.name, period=args.period, bars=len(df), freshness=freshness,
          summary=summary, seasonality=season)
    _print(f"{args.symbol} {tf.name}, {args.period}: {len(df)} barras, dados {freshness}")
    for label, key, pct in (("Último preço", "last", False), ("Retorno no período", "return_pct", True),
                            ("Volatilidade anualizada", "volatility_pct", True),
                            ("Queda máxima", "max_drawdown_pct", True),
                            ("Amplitude média por barra", "mean_range_pct", True), ("Barras de alta", "up_bars_pct", True)):
        value = summary.get(key)
        _print(f"  {label}: " + ("—" if value is None else (f"{value:.2f}%".replace(".", ",") if pct else f"{value:g}")))
    _print(f"Retornos: {summary['return_pairs_used']}/{summary['return_pairs_total']} pares válidos; "
           f"{summary['return_pairs_excluded']} excluídos por divisão indefinida.")
    for name, reason in summary["unavailable_reasons"].items():
        _print(f"  {name} indisponível: {reason}.")
    return EXIT_OK


def cmd_portfolio(args, ctx):
    """Carteira da conta: todas as posições e ordens, de quem são e quanto arriscam. Somente leitura."""
    from mt5_extracao import mt5_connector
    from mt5_extracao.robots import catalog, portfolio
    if args.guardian:
        data = portfolio.read_guardian(ctx.config)
        if data is None:
            _print("Não há retrato do guardião. Coloque o HDK_AccountGuardian em um gráfico do MetaTrader 5 "
                   "(ele grava o retrato a cada poucos segundos) ou rode sem --guardian, pela ponte.")
            return EXIT_FAIL
        _data(portfolio=data, source="guardian")
        _print(f"Retrato do HDK_AccountGuardian, conta {data.get('login')}, gravado em {data.get('updated')} "
               f"(horário do servidor): estado {data.get('state')}" + (f" ({data.get('reason')})" if data.get("reason") else ""))
        _print(f"  Saldo {_fmt_num(data.get('balance', 0))}   Patrimônio {_fmt_num(data.get('equity', 0))}   Flutuante "
               f"{_fmt_num(data.get('floating', 0))}   Risco até o stop "
               f"{_fmt_num(data.get('risk_open', 0) + data.get('risk_pending', 0))}"
               + ("" if data.get("risk_complete") else " (incompleto: há posição ou ordem sem stop)"))
        _print(f"  {len(data.get('positions', []))} posição(ões), {len(data.get('orders', []))} ordem(ns) pendente(s).")
        _print_portfolio_exposures(data)
        return EXIT_OK
    ctx.connector()
    limit = ctx.config.getfloat("ROBOTS", "max_account_risk_pct", fallback=0.0)
    fixed_groups = ctx.config.get("ROBOTS", "account_exposure_groups", fallback="")
    data = portfolio.snapshot(mt5_connector.mt5, catalog.load_catalog(ctx.config, ctx.db), limit, fixed_groups)
    _data(portfolio=data, source="bridge")
    t = data["totals"]
    _print(f"Carteira da conta {data['login']} ({data['server']}, {'DEMO' if data['demo'] else 'REAL'}, "
           f"{'hedging' if data['hedging'] else 'netting: uma posição por ativo'}), em {data['currency']}:")
    _print(f"  Estado: {data['state']}" + (f" ({data['reason']})" if data["reason"] else ""))
    _print(f"  Saldo {_fmt_num(t['balance'])}   Patrimônio {_fmt_num(t['equity'])}   Margem livre "
           f"{_fmt_num(t['margin_free'])}   Flutuante {_fmt_num(t['floating'])}")
    _print(f"  Risco até o stop: {_fmt_num(t['risk_open'])} nas posições e {_fmt_num(t['risk_pending'])} nas ordens "
           f"pendentes ({_fmt_num(t['risk_pct'])}% do saldo" + (")" if t["risk_complete"] else ", INCOMPLETO)"))
    _print(f"  {t['positions']} posição(ões), {t['orders']} ordem(ns) pendente(s), {t['without_stop']} sem stop, "
           f"{t['manual']} manual(is) ou sem dono.")
    for group in data["by_owner"]:
        _print(f"  - {group['owner']}: {group['positions']} posição(ões), {group['orders']} ordem(ns), "
               f"{_fmt_num(group['volume'])} lote(s) em {', '.join(group['symbols'])}; resultado "
               f"{_fmt_num(group['profit'])}, risco {_fmt_num(group['risk_money'])}")
    for item in data["positions"]:
        risk = "SEM STOP" if not item["has_stop"] else ("risco não calculado" if item["risk_money"] is None
                                                        else f"risco {_fmt_num(item['risk_money'])}")
        _print(f"    #{item['ticket']} {item['symbol']} {'COMPRA' if item['direction'] > 0 else 'VENDA'} "
               f"{_fmt_num(item['volume'])} ({item['owner']}): resultado {_fmt_num(item['profit'])}, {risk}")
    for symbol, who in data["shared_symbols"].items():
        _print(f"  ATENÇÃO: {symbol} tem mais de um dono ({', '.join(who)})"
               + ("." if data["hedging"] else ": em conta netting eles dividem UMA posição."))
    _print_portfolio_exposures(data)
    _print("Somente leitura: este comando não envia, altera nem cancela ordens.")
    return EXIT_OK


def _print_portfolio_exposures(data):
    """Mostra a consolidação por ativo e grupo dos retratos do mt5x e do guardião."""
    exposures = data.get("exposures", [])
    if exposures:
        complete = data.get("risk_complete", data.get("totals", {}).get("risk_complete", True))
        note = " (parcial: há risco não calculado)" if not complete else ""
        _print(f"Exposição por ativo; risco na moeda da conta{note}:")
        for item in exposures:
            buy_count = item.get("buy_positions", 0)
            buy_orders = item.get("buy_orders", 0)
            sell_count = item.get("sell_positions", 0)
            sell_orders = item.get("sell_orders", 0)
            _print(f"  {item.get('symbol')}: compra {buy_count} posição(ões)/{buy_orders} ordem(ns), "
                   f"{_fmt_num(item.get('buy_volume', 0))} lote(s), risco {_fmt_num(item.get('buy_risk_money', 0))}; "
                   f"venda {sell_count} posição(ões)/{sell_orders} ordem(ns), "
                   f"{_fmt_num(item.get('sell_volume', 0))} lote(s), risco {_fmt_num(item.get('sell_risk_money', 0))} "
                   f"{item.get('currency', data.get('currency', ''))}.")
    groups = data.get("groups", [])
    if groups:
        complete = data.get("risk_complete", data.get("totals", {}).get("risk_complete", True))
        note = " (parcial: há risco não calculado)" if not complete else ""
        _print(f"Exposição por grupo{note}:")
        for item in groups:
            _print(f"  {item.get('group')}: compra {_fmt_num(item.get('buy_risk_money', 0))}, "
                   f"venda {_fmt_num(item.get('sell_risk_money', 0))} {item.get('currency', data.get('currency', ''))}.")


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
    from .automation.catalog import command_tree
    tree = command_tree()
    catalog = tree["subcommands"]
    for item in catalog:
        _print(f"{item['command']:<11} {item['help']}")
    _data(commands=catalog, exit_codes={"0": "sucesso", "1": "falha", "2": "uso incorreto"},
          global_flags=["--config", "--db", "--json", "-v"], schema_version=1,
          global_arguments=tree["arguments"])
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
    _notify(ctx, f"{robot.name}: compilado" if result.ok else f"{robot.name}: erro de compilação", result.summary,
            None if result.ok else "aviso")
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
            _notify(ctx, "MetaTrader 5 fechado para um teste", "Ele é fechado só durante o teste; a ponte é religada depois.", "aviso")
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
    stopped = next((s.name for s in result.steps if s.status == pipeline.FAILED), "")
    _notify(ctx, f"{robot.name}: conferência {'aprovada' if result.ok else 'reprovada'}",
            "Contrato, compilação, backtest e bateria passaram." if result.ok else f"Parou em: {stopped}",
            None if result.ok else "aviso")
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
            _notify(ctx, "MetaTrader 5 fechado para um teste", "Ele é fechado só durante o teste; a ponte é religada depois.", "aviso")
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
                   f"{max(1, round(count * battery.SECONDS_PER_TEST / 60))} min. Os testes usam um perfil privado, um por vez.")
        if args.close_terminal and mt5_tools.terminal_running(ctx.config):
            _print("Fechando o MetaTrader 5 para rodar os testes...")
            _notify(ctx, "MetaTrader 5 fechado para um teste", "Ele é fechado só durante o teste; a ponte é religada depois.", "aviso")
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
    failed_items = [i.name for i in report.items if i.status == battery.FAIL]
    _notify(ctx, f"{robot.name}: bateria {'APROVADA' if report.approved else 'REPROVADA'}",
            "Pode rodar em conta de demonstração." if report.approved else "Reprovou em: " + "; ".join(failed_items[:4]),
            None if report.approved else "aviso")
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
        state = live.start_demo(ctx.db, ctx.config, robot, account, args.symbol, tf.label, args.preset,
                                confirm=mt5_tools.confirm_startup)
    except (OSError, RuntimeError) as e:
        raise RuntimeError(str(e)) from e
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
    _data(live=state)
    _print(f"\n{robot.name} iniciado na conta demo {state['account']} ({state['server']}), {state['symbol']} "
           f"{state['timeframe']}" + (f", preset {state['preset']}" if state["preset"] else "") + ".")
    _print(f"Acompanhe: mt5x robots monitor {args.path} --watch   Parar tudo: mt5x robots stop")
    from mt5_extracao import notify
    _notify(ctx, f"{robot.name} iniciado em conta DEMO",
            f"{state['symbol']} {state['timeframe']}, conta {notify.mask_account(state['account'])}. "
            "O MetaTrader 5 foi reiniciado.", "aviso")
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
    if not args.watch:
        return EXIT_OK

    from mt5_extracao import notify

    def emit(title, text, level):
        _print(f"{datetime.now():%H:%M:%S}  {title}: {text}")
        _notify(ctx, f"{robot.name}: {title}", text, level)
    _print(f"\nAcompanhando {robot.name} a cada {args.interval:g} s (somente leitura). Um aviso por operação. "
           "Ctrl+C para parar; isso não para o robô.")
    if not notify.enabled(ctx.config):
        _print("Os pop-ups estão desligados ([APP] notifications): os avisos aparecem só aqui e no log.")
    try:
        emitted = live.watch(mt5_connector.mt5, state["magic"], emit, since, args.interval, args.cycles,
                             kill_active=lambda: live.stop_active(ctx.config))
    except KeyboardInterrupt:
        _print("\nAcompanhamento encerrado. O robô continua como estava.")
        return EXIT_OK
    _data(watch={"notices": emitted, "cycles": args.cycles})
    return EXIT_OK


def _robots_stop(args, ctx, items):
    from mt5_extracao.robots import catalog, live, mt5_tools
    if args.release:
        existed = live.release_stop(ctx.config)
        _data(stop_active=False)
        _print("PARAR TUDO liberado: os robôs voltam a poder operar." if existed else "PARAR TUDO já estava desligado.")
        if existed:
            _notify(ctx, "PARAR TUDO liberado", "Os robôs voltam a poder operar.", "aviso")
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
    _notify(ctx, "PARAR TUDO acionado", "Os robôs com a camada de segurança não abrem novas operações. Posições "
            "abertas NÃO foram fechadas." + (" MetaTrader 5 fechado." if closed else ""), "crítico")
    return EXIT_OK


def _robots_market(args, ctx, items):
    """Pré-checa um EA para revisão e, opcionalmente, monta um pacote local."""
    from mt5_extracao.robots import catalog, market, store
    robot = catalog.get_robot(items, args.path)
    if robot.kind != catalog.MQL5:
        raise ValueError("`mt5x robots market` só aceita um Expert Advisor MQL5.")
    if args.with_source and not args.pack:
        raise ValueError("`--with-source` exige `--pack <pasta>`. Nada foi copiado.")
    compile_result = market.last_compile_result(ctx.config, robot)
    checks = market.check(robot, compile_result, store.load_runs(ctx.db, robot.key))
    ready = market.ready(checks)
    _data(market={"robot_key": robot.key, "ready": ready, "compile": compile_result.to_dict(),
                  "items": [dict(vars(item), icon=item.icon) for item in checks]})
    _print(f"Pré-checagem local do MQL5 Market: {robot.name}")
    for item in checks:
        _print(f"  {item.icon} {item.name}: {item.detail}")
        if item.fix:
            _print(f"      O que fazer: {item.fix}")
    if not ready:
        _print("\nPré-checagem reprovada. O envio manual e a validação oficial continuam com o vendedor.")
        return EXIT_FAIL
    if args.pack:
        files = market.pack(robot, compile_result, args.pack, with_source=args.with_source)
        _data(package=[str(path) for path in files])
        _print(f"\nPacote local criado em {Path(args.pack).expanduser()}; revise os avisos e o LEIA-ME.txt.")
    else:
        _print("\nPré-checagem sem falhas. O envio e a validação oficial continuam manuais.")
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
    if args.action == "market":
        if not args.path:
            raise ValueError("uso: mt5x robots market <robô> [--pack <pasta>] [--with-source]")
        return _robots_market(args, ctx, items)
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


def _specs_calibrate(args, ctx, symbols):
    """Mede o spread real nos ticks já extraídos e grava ao lado da especificação. Não usa o MT5."""
    from mt5_extracao.strategies import specs
    failed, data = False, {}
    for symbol in symbols:
        try:
            profile = specs.calibrate_spread(ctx.db, symbol)
        except ValueError as e:
            _print(f"  {e}")
            failed = True
            continue
        data[symbol] = profile
        hours = [v for v in profile["hours"] if v is not None]
        opens = [v for v in profile["open"] if v is not None]
        worst = max(range(24), key=lambda h: profile["open"][h] if profile["open"][h] is not None else -1)
        _print(f"  {symbol}: spread medido em {profile['ticks']} ticks de {profile['days']} dia(s) "
               f"({profile['from'][:10]} a {profile['to'][:10]}): média {_fmt_num(profile['mean'], 1)} pontos; por hora, de "
               f"{_fmt_num(min(hours), 1)} a {_fmt_num(max(hours), 1)}; na abertura da hora, até "
               f"{_fmt_num(max(opens), 1)} (às {worst:02d}h).")
        if profile["days"] < 20:
            _print(f"    Só {profile['days']} dia(s) de ticks: a medida fica instável. Extraia pelo menos 20 dias, "
                   "espalhados pelo ano (mt5x ticks), e repita.")
    _data(spread=data)
    if data:
        _print("O backtest, a validação e a triagem em Python passam a cobrar esse spread em cada execução.")
    return EXIT_FAIL if failed else EXIT_OK


def cmd_specs(args, ctx):
    from mt5_extracao.strategies import specs
    symbols = _parse_symbols(args.symbols)
    if args.calibrate:
        return _specs_calibrate(args, ctx, symbols)
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
          real_spread=bool(spec.spread_by_hour), skipped_min_lot=result.skipped_min_lot)
    _print(f"{strategy.describe()} em {args.symbol} {tf.name}: {len(df)} barras "
           f"({df['time'].iloc[0]:%Y-%m-%d} a {df['time'].iloc[-1]:%Y-%m-%d})")
    if not spec.known:
        _print("  AVISO: especificação ausente; valores em dinheiro e lotes são aproximados. Rode 'mt5x specs'.")
    elif not spec.spread_by_hour:
        _print(f"  AVISO: custo otimista. O spread usado é o das barras, que é o menor de cada período. Meça o real: "
               f"'mt5x ticks --symbols {args.symbol} --from ... --to ...' e 'mt5x specs --symbols {args.symbol} "
               "--calibrate'.")
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
    from .automation.commands import add_parser as workflow_parser
    workflow_parser(sub)

    p = sub.add_parser("doctor", help="diagnóstico do ambiente")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("symbols", help="lista símbolos do MT5")
    p.add_argument("--group", default="*", help='filtro do MT5, ex.: "*WIN*"')
    p.set_defaults(func=cmd_symbols)

    p = sub.add_parser("panel", help="painel visual (HTML) com os resultados por ativo, timeframe e robô")
    p.add_argument("--out", help="arquivo HTML a gravar (padrão: exports/painel_resultados.html)")
    p.add_argument("--open", action="store_true", help="abre o painel no navegador")
    p.add_argument("--serve", action="store_true",
                   help="painel vivo: servidor local (só este computador) que atualiza a página e executa ações")
    p.add_argument("--port", type=int, help="--serve: porta (padrão: uma livre)")
    p.add_argument("--seconds", type=float, help="--serve: encerra sozinho depois desse tempo (padrão: até Ctrl+C)")
    p.set_defaults(func=cmd_panel)

    p = sub.add_parser("mt5", help="estado do terminal, organização da janela e, no modo oculto, captura da tela "
                                   "virtual")
    p.add_argument("action", nargs="?", choices=["status", "show", "layout", "restore-layout", "view"],
                   default="status",
                   help="status (padrão); show: captura da tela virtual; layout: o que a organização da janela faz "
                        "e o que está escondido; restore-layout: devolve o arranjo anterior (MT5 fechado); view: "
                        "abre o MT5 com a organização escolhida aplicada")
    p.add_argument("--close-terminal", action="store_true",
                   help="view: se o MT5 estiver aberto, fecha e abre de novo (interrompe o que roda nele)")
    p.add_argument("--symbol", help="view: ativo do gráfico, se o perfil de gráficos próprio ainda não tiver nenhum")
    p.add_argument("--tf", help="view: timeframe desse gráfico (padrão: M15)")
    p.add_argument("--out", help="show: arquivo PNG da captura (padrão: logs/mt5_oculto.png)")
    p.set_defaults(func=cmd_mt5)

    p = sub.add_parser("resample", help="monta timeframes legados ou barras analíticas por sessão a partir de M1")
    p.add_argument("--symbols", required=True, help="símbolos separados por vírgula")
    p.add_argument("--base", required=True, help="timeframe já extraído, o menor (ex.: M1)")
    p.add_argument("--tf", required=True,
                   help="timeframes a montar, separados por vírgula (ex.: M5,M15,H1). "
                        "No modo --session-aware, M7 e M9 são sintéticos de dia exato e não são tempos do MT5")
    p.add_argument("--from", dest="date_from", help="início AAAA-MM-DD (padrão: toda a base)")
    p.add_argument("--to", dest="date_to", help="fim AAAA-MM-DD")
    p.add_argument("--session-aware", action="store_true",
                   help="deriva barras analíticas respeitando sessões inferidas de M1 e registra symbol_info do MT5")
    p.add_argument("--session-gap-minutes", type=int, default=60,
                   help="--session-aware: gap M1 maior que este valor abre outra sessão inferida (padrão: 60)")
    p.add_argument("--session-anchor", default="00:00",
                   help="--session-aware: começo do dia de sessão em HH:MM do horário da corretora (padrão: 00:00). "
                        "M7/M9 recusam âncora diferente de 00:00: outra âncora é outro ensaio")
    p.add_argument("--partial-last-bar", choices=["include", "drop"], default="include",
                   help="--session-aware: inclui (padrão) ou descarta o candle final parcial de cada sessão. "
                        "M7/M9 nunca gravam candle curto")
    p.set_defaults(func=cmd_resample)

    p = sub.add_parser("analytics", help="características analíticas versionadas e consultas point-in-time")
    analytics_sub = p.add_subparsers(dest="analytics_domain", required=True)
    p_features = analytics_sub.add_parser("features", help="registro, dependências e valores disponíveis por data")
    features_sub = p_features.add_subparsers(dest="features_action", required=True)

    q = features_sub.add_parser("recover-jobs", help="reconcilia jobs de processos locais encerrados; preserva ativos e desconhecidos")
    q.add_argument("--symbol", help="restringe ao ativo informado")
    q.add_argument("--tf", help="restringe ao timeframe informado")
    q.add_argument("--family", help="restringe à família exata do journal")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("list", help="lista as definições de características registradas")
    q.add_argument("--all-versions", action="store_true", help="mostra também versões antigas do algoritmo")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("catalog", help="catálogo canônico: fórmulas e todas as versões registradas na base")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("show", help="mostra a definição e as dependências de uma característica")
    q.add_argument("feature_id")
    q.add_argument("--algorithm-version", help="versão do algoritmo (padrão: a definição mais recente)")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("values", help="consulta o último valor que já estava disponível no instante dado")
    q.add_argument("--as-of", required=True, help="instante de decisão com fuso, por exemplo 2026-10-14T17:25:00Z")
    q.add_argument("--symbol", required=True)
    q.add_argument("--tf", required=True, help="timeframe do valor, por exemplo M1")
    q.add_argument("--feature", dest="feature_id", help="limita a uma característica")
    q.add_argument("--algorithm-version", help="limita a uma versão do algoritmo")
    q.add_argument("--from", dest="date_from", help="início inclusivo de observation_time, com fuso")
    q.add_argument("--to", dest="date_to", help="fim exclusivo de observation_time, com fuso")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("series", help="consulta pública (envelope com base, versões, qualidade e cursor)")
    q.add_argument("--symbol", required=True)
    q.add_argument("--tf", required=True)
    q.add_argument("--feature", dest="feature_ids", action="append", required=True,
                   help="característica do catálogo; repita para várias")
    q.add_argument("--as-of", required=True, help="instante de decisão com fuso, por exemplo 2026-10-14T17:25:00Z")
    q.add_argument("--algorithm-version", help="consulta somente esta versão, sem escolher implicitamente a mais recente")
    q.add_argument("--from", dest="date_from", help="início inclusivo de observation_time, com fuso")
    q.add_argument("--to", dest="date_to", help="fim exclusivo de observation_time, com fuso")
    q.add_argument("--page-size", type=int, default=500)
    q.add_argument("--cursor", help="cursor devolvido pela página anterior")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("compute", help="calcula volume, RVOL, volatilidade, regime e estrutura da sessão")
    q.add_argument("--symbol", required=True)
    q.add_argument("--tf", required=True, help="timeframe de _bars_derived (gere antes com resample --session-aware)")
    q.add_argument("--families", help="lista separada por vírgula (padrão: todas): "
                                      "volume,rvol,volatility,regime,structure,calendar")
    q.add_argument("--volume-source", choices=("auto", "real", "tick"), default="auto",
                   help="auto usa o volume real só se todas as barras o conhecem; nunca mistura os dois")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("affected", help="lista características dependentes de fontes alteradas")
    q.add_argument("--changed", required=True,
                   help="chaves separadas por vírgula, por exemplo bars:M1,bars_derived:M15,feature:returns")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("materialize", help="materializa o registry canônico em barras derivadas fechadas")
    q.add_argument("--symbol", required=True)
    q.add_argument("--tf", required=True)
    q.add_argument("--features", help="IDs do registry separados por vírgula; padrão: todos")
    q.add_argument("--batch-size", type=int, default=5000)
    q.set_defaults(func=cmd_analytics)

    from mt5_extracao.analytics import features as analytical_features
    q = features_sub.add_parser("define-composite", help="registra receita tipada; não materializa fatos")
    q.add_argument("--id", required=True, dest="feature_id")
    q.add_argument("--name", required=True)
    q.add_argument("--algorithm-version", required=True)
    q.add_argument("--operation", choices=analytical_features.composition_operations(), required=True)
    q.add_argument("--input", action="append", nargs=2, metavar=("ID", "VERSAO"), required=True,
                   help="entrada ID/versão; repita em ordem para difference")
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("materialize-composite", help="grava eventos da cadeia congelada; não usa fonte live")
    q.add_argument("--manifest", required=True)
    q.add_argument("--as-of", required=True, help="corte UTC já ocorrido; não antecipa eventos futuros")
    q.add_argument("--target", action="append", nargs=2, metavar=("ID", "VERSAO"), required=True)
    q.add_argument("--batch-size", type=int, default=5000)
    q.set_defaults(func=cmd_analytics)

    q = features_sub.add_parser("compose", help="avalia receitas no contexto congelado; não grava valores")
    q.add_argument("--manifest", required=True)
    q.add_argument("--observation", help="observação UTC fixa; lê suas revisões, incluindo recibos posteriores às barras")
    q.add_argument("--target", action="append", nargs=2, metavar=("ID", "VERSAO"), required=True)
    q.add_argument("--as-of", action="append", required=True, help="decisão UTC; repita para vários instantes")
    q.set_defaults(func=cmd_analytics)

    for action in ("context", "export"):
        q = features_sub.add_parser(action, help="consulta por decisão UTC" if action == "context" else
                                   "exporta colunas versionadas do store, com estados e proveniência")
        q.add_argument("--symbol", required=True)
        q.add_argument("--tf", required=True)
        q.add_argument("--as-of", required=True, help="instante com fuso, por exemplo 2026-10-14T17:25:00Z")
        q.add_argument("--features", help="IDs do catálogo separados por vírgula; padrão: registry local"
                       + (" ou IDs congelados com --manifest" if action == "context" else ""))
        q.add_argument("--algorithm-version", help="versão registrada explícita; obrigatória sem fórmula local"
                       + ("; no snapshot, obrigatória se ambígua" if action == "context" else ""))
        if action == "context":
            q.add_argument("--manifest", help="usa somente fatos e contratos deste snapshot congelado")
            q.add_argument("--observation", help="exige --manifest; seleciona esta observação UTC e seus recibos congelados")
        if action == "export":
            q.add_argument("--out", required=True, help="destino CSV")
            q.add_argument("--from", dest="date_from")
            q.add_argument("--to", dest="date_to")
        q.set_defaults(func=cmd_analytics)

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
    p.add_argument("action", nargs="?", choices=["list", "import", "backtest", "install", "compile", "battery", "validate", "check", "parity", "market",
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
    p.add_argument("--batch-id", help="backtest em lote: retoma o checkpoint deste identificador, com os mesmos dados")
    p.add_argument("--tfs", help="backtest em lote: vários timeframes separados por vírgula (ex.: M15,H1,D1)")
    p.add_argument("--resume", action="store_true",
                   help="backtest em lote: pula a combinação que já tem execução gravada da mesma configuração")
    p.add_argument("--tf", help="backtest: timeframe (ex.: D1)")
    p.add_argument("--from", dest="date_from", help="backtest: início AAAA-MM-DD")
    p.add_argument("--to", dest="date_to", help="backtest: fim AAAA-MM-DD")
    p.add_argument("--param", action="append", default=[], help="backtest: parâmetro nome=valor (repetível)")
    p.add_argument("--strategy", help="parity: chave da estratégia Python a comparar (ex.: donchian)")
    p.add_argument("--pack", help="market: pasta vazia onde montar o pacote local do produto")
    p.add_argument("--with-source", action="store_true",
                   help="market: inclui .mq5/.mqh no pacote (por padrão só vai o .ex5)")
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
    p.add_argument("--watch", action="store_true",
                   help="monitor: fica acompanhando e avisa (pop-up) a cada operação do robô; Ctrl+C para parar")
    p.add_argument("--interval", type=float, default=15.0, help="monitor --watch: segundos entre as consultas")
    p.add_argument("--cycles", type=int, help="monitor --watch: número de consultas antes de sair (padrão: sem fim)")
    p.set_defaults(func=cmd_robots)

    p = sub.add_parser("analyze", help="números de estudo de um ativo (retorno, volatilidade, queda, sazonalidade)")
    p.add_argument("--symbol", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--period", default="1 ano", help='"1 mês", "3 meses", "6 meses", "1 ano" ou "Tudo"')
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("portfolio", help="carteira da conta: posições e ordens de todos os robôs e manuais, com o "
                                         "risco até o stop (somente leitura)")
    p.add_argument("--guardian", action="store_true",
                   help="lê o retrato gravado pelo HDK_AccountGuardian, sem usar a ponte")
    p.set_defaults(func=cmd_portfolio)

    from mt5_extracao.resources import commands as resources_commands
    resources_commands.add_parser(sub)
    from mt5_extracao.experiments import commands as experiments_commands
    experiments_commands.add_parser(sub)
    from mt5_extracao.trade_analytics import commands as trades_commands
    trades_commands.add_parser(sub)
    from mt5_extracao.laboratory import commands as laboratory_commands
    laboratory_commands.add_parser(sub)

    p = sub.add_parser("status", help="retrato da base, dos robôs e das validações, com as próximas ações sugeridas")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("audit", help="auditoria de segurança: projeto, patrimônio e gestão do capital")
    p.add_argument("--mt5", action="store_true", help="consulta o MT5 para saber se a conta conectada é demo ou real")
    p.add_argument("--fail-on", default="alto", choices=["crítico", "alto", "médio", "baixo"],
                   help="gravidade mínima que faz o comando sair com código 1 (padrão: alto)")
    p.set_defaults(func=cmd_audit)

    p = sub.add_parser("commands", help="lista todos os comandos e argumentos (para agentes)")
    p.set_defaults(func=cmd_commands)

    p = sub.add_parser("specs", help="lê do MT5 as especificações de contrato (tick, lote, spread, swap)")
    p.add_argument("--symbols", required=True)
    p.add_argument("--calibrate", action="store_true",
                   help="mede o spread real nos ticks já extraídos (mt5x ticks) e passa a usá-lo nos backtests; "
                        "não usa o MT5")
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


def cmd_analytics(args, ctx):
    """Inspeciona o registro de características e consulta valores point-in-time."""
    import json
    from mt5_extracao.analytics import feature_store, features

    if args.features_action == "recover-jobs":
        from mt5_extracao import sdk, timeframes
        timeframe = None
        if args.tf is not None:
            parsed = timeframes.parse(args.tf)
            if parsed is None:
                raise ValueError("Timeframe inválido; confira --tf antes de recuperar jobs.")
            timeframe = parsed.name
        result = sdk.recover_producer_jobs(ctx.db, symbol=args.symbol, timeframe=timeframe, family=args.family)
        _data(recovery=result)
        _print(f"{len(result['recovered'])} jobs interrompidos identificados; {result['preserved']} registros preservados.")
        return EXIT_OK

    if args.features_action == "define-composite":
        created = features.register_composite(ctx.db, args.feature_id, args.name, args.algorithm_version,
                                             operation=args.operation, inputs=args.input)
        _data(created=created, definition=features.get_feature(ctx.db, args.feature_id, args.algorithm_version))
        _print("Receita registrada; congele também as versões das entradas antes de avaliar.")
        return EXIT_OK
    if args.features_action == "materialize-composite":
        from mt5_extracao.resources import guard
        result = features.materialize_snapshot(ctx.db, args.manifest, targets=args.target, as_of=args.as_of, batch_size=args.batch_size,
                                               write_guard=guard.make(ctx.db, ctx.config))
        _data(materialization=result)
        _print(f"Snapshot congelado: {result['inserted']} novos, {result['revised']} revisados; {result['job']['status']}.")
        return EXIT_OK

    if args.features_action == "compose":
        import pandas as pd
        from mt5_extracao.analytics import datasets
        result = features.compose_snapshot(datasets.load(ctx.db, args.manifest),
            pd.DataFrame(dict(decision_time=args.as_of)), targets=args.target, observation_time=args.observation)
        _data(composition=result)
        if not args.json_output and result["context"]:
            if result["context_scope"]["observation_time"] is not None:
                _print("Observação congelada: " + result["context_scope"]["observation_time"])
            columns = ["decision_time"] + [ident + "::" + field for ident, _version in result["targets"]
                                           for field in ("value", "unit", "state")]
            _print(pd.DataFrame(result["context"])[columns].head(20).to_string(index=False, na_rep="—"))
            if len(result["context"]) > 20:
                _print("Mostrando as primeiras 20 decisões; use --json para o contexto completo.")
        _print(f"{len(result['executed'])} composições avaliadas no snapshot; contexto exploratório, sem materialização.")
        return EXIT_OK

    if args.features_action in {"materialize", "context", "export"}:
        import pandas as pd
        from mt5_extracao.analytics import indicator_context, indicator_materializer
        from mt5_extracao.resources import guard

        ids = [item.strip() for item in (args.features or "").split(",") if item.strip()] or None
        if args.features_action == "materialize":
            result = indicator_materializer.materialize(ctx.db, args.symbol, args.tf, feature_ids=ids,
                                                        batch_size=args.batch_size,
                                                        write_guard=guard.make(ctx.db, ctx.config))
            _print(f"Registry: {result['inserted']} novos, {result['revised']} revisados; {result['job']['status']}.")
            _data(materialization=result)
        elif args.features_action == "context":
            decisions = pd.DataFrame({"decision_time": [args.as_of]})
            if args.observation and not args.manifest:
                raise ValueError("--observation exige --manifest; a consulta usa somente revisões congeladas.")
            if args.manifest:
                from mt5_extracao.analytics import datasets
                from mt5_extracao import timeframes
                snapshot = datasets.load(ctx.db, args.manifest)
                tf = timeframes.parse(args.tf)
                if snapshot.manifest["symbol"] != args.symbol or tf is None or snapshot.manifest["timeframe"] != tf.name:
                    raise ValueError("Ativo/timeframe divergem do snapshot; confira o manifest escolhido.")
                frame = indicator_context.from_snapshot(snapshot, decisions, feature_ids=ids,
                                                         algorithm_version=args.algorithm_version,
                                                         observation_time=args.observation)
            else:
                frame = indicator_context.context(ctx.db, decisions, symbol=args.symbol, timeframe=args.tf,
                                                  feature_ids=ids, algorithm_version=args.algorithm_version)
            _print(frame.to_string(index=False))
            scope = frame.attrs.get("snapshot_context_scope")
            _data(schema_version=1, context=indicator_context.records(frame),
                  **({"snapshot_context_scope": scope} if scope is not None else {}))
        else:
            from mt5_extracao.data_exporter import DataExporter
            path = DataExporter(ctx.db).export_indicator_values(
                args.out, symbol=args.symbol, timeframe=args.tf, as_of=args.as_of, feature_ids=ids,
                algorithm_version=args.algorithm_version, start=args.date_from, end=args.date_to,
                write_guard=guard.make(ctx.db, ctx.config))
            _print(f"Indicadores exportados: {path}")
            _data(path=path)
        return EXIT_OK

    if args.features_action == "catalog":
        from mt5_extracao.analytics import query_service
        envelope = query_service.catalog(ctx.db)
        if envelope["status"] != "ok":
            for error in envelope["errors"]:
                _print(f"[{error['code']}] {error['message']}")
            _data(envelope=envelope)
            return EXIT_USAGE
        for item in envelope["definitions"]:
            _print(f"{item['feature_id']}@{item['algorithm_version']}\t{item['name']}\t"
                   f"{item['origin']}\t{item['metadata_state']}\t{item['name_status']}")
        for reason in envelope["reasons"]:
            _print(reason)
        _data(envelope=envelope)
        return EXIT_OK

    if args.features_action == "list":
        items = features.list_features(ctx.db, include_versions=args.all_versions)
        if not items:
            _print("Nenhuma característica registrada.")
        else:
            for item in items:
                name_note = (" [nome legado não verificável]" if
                             item.get("name_status") == "UNVERIFIED_LEGACY_NAME" else "")
                _print(f"{item['feature_id']}@{item['algorithm_version']}\t{item['name']}{name_note}\t"
                       f"dependências: {', '.join(item['dependencies']) or 'nenhuma'}")
        _print(f"\n{len(items)} definição(ões).")
        _data(features=items)
        return EXIT_OK

    if args.features_action == "show":
        item = features.get_feature(ctx.db, args.feature_id, args.algorithm_version)
        _print(json.dumps(_jsonable(item), ensure_ascii=False, indent=2))
        _data(feature=item)
        return EXIT_OK

    if args.features_action == "values":
        frame = feature_store.query_as_of(
            ctx.db, args.as_of, symbol=args.symbol, timeframe=args.tf, feature_id=args.feature_id,
            algorithm_version=args.algorithm_version, start=args.date_from, end=args.date_to,
        )
        if frame.empty:
            _print("Nenhum valor disponível para os filtros e o instante informado.")
        else:
            _print(frame.to_string(index=False))
        _data(values=frame.to_dict("records"))
        return EXIT_OK

    if args.features_action == "series":
        from mt5_extracao.analytics import query_service

        envelope = query_service.series(ctx.db, symbol=args.symbol, timeframe=args.tf, feature_ids=args.feature_ids,
                                        as_of=args.as_of, start=args.date_from, end=args.date_to,
                                        algorithm_version=args.algorithm_version,
                                        page_size=args.page_size, cursor=args.cursor)
        if envelope["status"] != "ok":
            for error in envelope["errors"]:
                _print(f"[{error['code']}] {error['message']}")
            _data(envelope=envelope)
            return EXIT_USAGE
        base = envelope["base"]
        _print(f"Base {base['namespace'][:8]} ({base['provider'] or 'fonte não declarada'}"
               f"{'@' + base['server'] if base['server'] else ''}); fuso: {envelope['time_context']['offset_state']}")
        for row in envelope["values"]:
            _print(f"{row['feature_id']}\t{row['observation_time']}\t{row['value']}\t{row['quality']}\t"
                   f"rev {row['revision']}\t{row['algorithm_version']}")
        for reason in envelope["reasons"]:
            _print(reason)
        if envelope["page"]["next_cursor"]:
            _print(f"Mais resultados: --cursor {envelope['page']['next_cursor']}")
        _data(envelope=envelope)
        return EXIT_OK

    if args.features_action == "compute":
        from mt5_extracao.analytics import producers
        from mt5_extracao.resources import guard

        families = [item.strip() for item in (args.families or "").split(",") if item.strip()] or None
        summary = producers.compute(ctx.db, args.symbol, args.tf, families=families,
                                    volume_source=args.volume_source, write_guard=guard.make(ctx.db, ctx.config))
        for item in summary:
            _print(f"{item['family']}\t{item['features']} características\t{item['values']} valores "
                   f"({item['inserted']} novos, {item['revised']} revisados, {item['unchanged']} inalterados)"
                   + (f"\tvolume: {item['volume_source']}" if item["volume_source"] else ""))
        _print("\nValores históricos são reconstruídos: disponíveis no fim da barra ou da sessão, "
               "como se as barras de hoje já existissem então.")
        _data(summary=summary)
        return EXIT_OK

    if args.features_action == "affected":
        changed = [item.strip() for item in args.changed.split(",") if item.strip()]
        if not changed:
            raise ValueError("informe ao menos uma fonte em --changed")
        items = features.affected_by(ctx.db, changed)
        if not items:
            _print("Nenhuma característica depende das fontes informadas.")
        else:
            for item in items:
                _print(f"{item['feature_id']}@{item['algorithm_version']}\t{item['name']}")
        _data(changed=changed, affected=items)
        return EXIT_OK

    raise ValueError(f"subcomando analytics desconhecido: {args.features_action}")


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
    # Com `python -m mt5_extracao.cli` este arquivo roda como __main__; os subcomandos de outros módulos
    # (resources, trades) importam `mt5_extracao.cli`. Delegar para ele mantém um estado de saída só (--json).
    from mt5_extracao import cli as _cli
    sys.exit(_cli.main())
