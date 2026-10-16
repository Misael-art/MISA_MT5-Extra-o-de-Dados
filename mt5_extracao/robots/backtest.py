"""
Backtest pelo card do robô.

Estratégias Python usam o backtester do projeto (barras da base, custos e lote pelo risco) e a execução
fica gravada em `_robot_runs`, com a curva de capital. Robôs MQL5 são testados pelo Strategy Tester do
MT5 (`run_mql5`): o app gera o .ini, roda o terminal, lê as operações do relatório (ou do
`HDK_trades_<magic>.csv` do HADOUKEN) e calcula aqui as mesmas métricas. Um backtest isolado não valida a
estratégia: isso é papel da validação e da bateria.
"""
import math
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from mt5_extracao import profit_factor
from . import catalog, contract, mql, store

MAX_CURVE_POINTS = 240


def equity_curve(trades, initial_equity: float, start=None) -> List[list]:
    """[[horário ISO, capital], ...] das operações fechadas, começando no capital inicial."""
    points = [[str(start) if start is not None else "", float(initial_equity)]]
    equity = float(initial_equity)
    for trade in trades:
        equity += float(trade.pnl)
        points.append([str(trade.exit_time), equity])
    return points


def downsample(points: List[list], max_points: int = MAX_CURVE_POINTS) -> List[list]:
    """Reduz a curva mantendo o primeiro, o último e os extremos de cada trecho (não esconde o pior drawdown)."""
    if len(points) <= max_points:
        return points
    size = math.ceil((len(points) - 2) / ((max_points - 2) / 2))
    kept = [points[0]]
    for i in range(1, len(points) - 1, size):
        chunk = points[i:i + size]
        low, high = min(chunk, key=lambda p: p[1]), max(chunk, key=lambda p: p[1])
        kept.extend(sorted({id(low): low, id(high): high}.values(), key=chunk.index))
    kept.append(points[-1])
    return kept


def run_python(db, config, robot, symbol: str, timeframe: str, start=None, end=None,
               params: Optional[dict] = None, strategy=None, source: str = "backtest", trade_from=None,
               real_spread: bool = True) -> dict:
    """
    Roda e grava o backtest de uma estratégia Python. `timeframe` é o rótulo da base (ex.: '1 dia').
    `strategy` permite passar a estratégia já montada (ex.: uma DeclaredStrategy), em vez de buscá-la pela chave
    do robô; nesse caso a execução é gravada na chave de `robot` e `robot` pode ser de qualquer tipo.
    `trade_from`: as barras anteriores a esse horário servem só para aquecer os indicadores; a estratégia começa
    a operar nele, sem posição e com o capital inicial (é como o Strategy Tester começa em FromDate).
    `real_spread=False` cobra o spread das barras, e não o medido nos ticks: é o que o modelo "cada tick" do
    Strategy Tester faz, e por isso é o que a paridade usa (ela compara a lógica, não o custo).
    """
    from mt5_extracao import timeframes
    from mt5_extracao.strategies import get_strategy, research, specs
    from mt5_extracao.strategies.backtester import run_backtest
    if strategy is None and robot.kind != catalog.PYTHON:
        raise ValueError("O backtest de robôs MQL5 roda no Strategy Tester do MetaTrader 5: use run_mql5.")
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    df = db.load_ohlcv(symbol, tf.label, start, end)
    if df.empty:
        raise ValueError(research.missing_message([symbol], tf))
    if strategy is None:
        strategy = get_strategy(robot.key.split(":", 1)[1], **(params or {}))
    if len(df) < strategy.min_bars:
        raise ValueError(f"Poucas barras para {strategy.info.name}: {len(df)} no período (mínimo {strategy.min_bars}). "
                         "Escolha um período maior ou extraia mais histórico.")
    spec = specs.load_spec(db, symbol)
    settings = research.backtest_config(config)
    if not real_spread:
        import dataclasses
        settings = dataclasses.replace(settings, real_spread=False)
    start_index = 0
    if trade_from is not None:
        import pandas as pd
        start_index = int((pd.to_datetime(df["time"]) < pd.Timestamp(trade_from)).sum())
        if start_index >= len(df):
            raise ValueError(f"Não há barras a partir de {trade_from} no período pedido.")
    result = run_backtest(df, strategy, spec, settings, start_index=start_index)
    metrics = {k: v for k, v in result.metrics.items() if k != "exit_reasons"}
    metrics.update(exit_reasons=result.metrics["exit_reasons"], skipped_min_lot=int(result.skipped_min_lot),
                   spec_known=bool(spec.known), real_spread=bool(settings.real_spread and spec.spread_by_hour),
                   initial_equity=float(settings.initial_equity),
                   risk_per_trade=float(settings.risk_per_trade))
    run = {"run_id": uuid.uuid4().hex, "robot_key": robot.key, "source": source, "symbol": symbol,
           "timeframe": tf.name, "data_start": str(df["time"].iloc[0]), "data_end": str(df["time"].iloc[-1]),
           "bars": int(len(df)), "params": dict(strategy.params), "metrics": metrics,
           "equity": downsample(equity_curve(result.trades, settings.initial_equity, df["time"].iloc[0])),
           "trades": [[str(t.exit_time), float(t.pnl), float(t.volume)] for t in result.trades],
           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    store.save_run(db, run)
    from mt5_extracao.trade_analytics import importers       # Laboratório (T16.2): operações completas no livro
    importers.safely(importers.record_python_run, db, run, result.trades)
    return run


# --- MQL5: Strategy Tester ------------------------------------------------------------

REPORT_SOURCE, CSV_SOURCE = "relatório do Strategy Tester", "HDK_trades.csv"
TESTER_SOURCE = "strategy tester"
DEEP_SOURCE = "strategy tester (bateria)"    # execuções extras da bateria: não são o "último backtest" do card
PARITY_SOURCE = "backtest python (paridade)" # o lado Python de uma paridade, gravado na chave do robô MQL5
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_TIME = re.compile(r"^\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}(:\d{2})?$")
_DEAL_COLUMNS = 13       # horário, oferta, ativo, tipo, direção, volume, preço, ordem, comissão, swap, lucro, saldo, comentário


def _number(text: str) -> float:
    """
    Número de um relatório ou CSV do MT5. Aceita milhar com espaço ("10 000.00"), vírgula decimal ("1 234,56") e
    os dois separadores juntos: o que aparece por último é o decimal ("1,234.56" e "1.234,56").
    """
    text = text.replace("\xa0", "").replace(" ", "")
    if not text:
        return 0.0
    if "," in text and "." in text:
        thousands = "," if text.rfind(".") > text.rfind(",") else "."
        text = text.replace(thousands, "")
    return float(text.replace(",", "."))


def entries_without_stop(text: str) -> Tuple[int, int]:
    """
    (entradas, entradas sem stop) lidas do relatório: cada negócio de entrada (direção "in") aponta para a ordem
    que o abriu, e a coluna S/L dessa ordem tem de estar preenchida. É a prova, pelo teste, de que o stop é
    obrigatório; a leitura do código só mostra a intenção.
    """
    orders, entries = {}, []
    for row in _ROW.findall(text):
        cells = [_TAG.sub("", c).replace("&nbsp;", " ").strip() for c in _CELL.findall(row)]
        if not cells or not _TIME.match(cells[0]):
            continue
        if len(cells) == 11 and cells[3].lower().split()[0:1] in (["buy"], ["sell"]):
            orders[cells[1]] = cells[6]
        elif len(cells) == _DEAL_COLUMNS and cells[3].lower() in ("buy", "sell") and cells[4].lower() == "in":
            entries.append(cells[7])
    return len(entries), sum(1 for order in entries if not _number(orders.get(order, "") or "0"))


def parse_report(text: str) -> Tuple[Optional[float], List[list]]:
    """
    Lê a tabela de negócios (deals) do relatório .htm do Strategy Tester. Devolve (depósito inicial, operações
    fechadas como [horário, resultado, volume]), com resultado = lucro + swap + comissão. Os títulos do relatório mudam
    com o idioma do terminal; por isso a leitura vai pela forma das linhas (13 colunas, tipo e direção em inglês).
    """
    deposit, results = None, []
    for row in _ROW.findall(text):
        cells = [_TAG.sub("", c).replace("&nbsp;", " ").strip() for c in _CELL.findall(row)]
        if len(cells) != _DEAL_COLUMNS or not _TIME.match(cells[0]):
            continue
        kind, direction = cells[3].lower(), cells[4].lower()
        if kind == "balance":
            if deposit is None:
                deposit = _number(cells[10])
        elif kind in ("buy", "sell") and direction not in ("", "in"):
            results.append([cells[0], _number(cells[10]) + _number(cells[9]) + _number(cells[8]),
                            _number(cells[5])])
    return deposit, results


def read_hdk_trades(path) -> List[list]:
    """
    Operações fechadas do HADOUKEN (Common\\Files\\HDK_trades_<magic>.csv): separador ';' e colunas
    time;symbol;type;volume;result. Devolve [horário, resultado, volume].
    """
    lines = [line.strip() for line in mql.read_text(path).splitlines() if line.strip()]
    if not lines:
        return []
    header = [h.strip().lower() for h in lines[0].split(";")]
    if "time" not in header or "result" not in header:
        raise ValueError(f"{Path(path).name} não tem as colunas esperadas (time;symbol;type;volume;result).")
    time_at, result_at = header.index("time"), header.index("result")
    volume_at = header.index("volume") if "volume" in header else None
    return [[cells[time_at].strip(), _number(cells[result_at]),
             _number(cells[volume_at]) if volume_at is not None and volume_at < len(cells) else 0.0]
            for cells in (line.split(";") for line in lines[1:]) if len(cells) > max(time_at, result_at)]


def metrics_from_results(results: List[list], initial_equity: float) -> dict:
    """As métricas do card a partir dos resultados em dinheiro. Sem o risco inicial de cada operação, não há R."""
    pnl = [float(r[1]) for r in results]
    wins, losses = [x for x in pnl if x > 0], [x for x in pnl if x < 0]
    gross_profit, gross_loss = sum(wins), -sum(losses)
    equity = peak = float(initial_equity)
    drawdown = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        if peak > 0:
            drawdown = max(drawdown, (peak - equity) / peak)
    return {"trades": len(pnl), "win_rate": len(wins) / len(pnl) if pnl else 0.0,
            **profit_factor.fields(pnl),
            "payoff": (gross_profit / len(wins)) / (gross_loss / len(losses)) if wins and losses else 0.0,
            "expectancy_r": None, "expectancy": sum(pnl) / len(pnl) if pnl else 0.0,
            "net_profit": sum(pnl), "return_pct": sum(pnl) / initial_equity if initial_equity else 0.0,
            "max_drawdown_pct": drawdown, "initial_equity": float(initial_equity)}


def _iso(stamp: str) -> str:
    """'2025.01.07 16:00' (MT5) -> '2025-01-07 16:00:00', o formato das curvas das estratégias Python."""
    date, _, clock = stamp.partition(" ")
    return date.replace(".", "-") + " " + (clock if clock.count(":") == 2 else (clock or "00:00") + ":00")


def tester_values(robot, preset: Optional[str] = None, params: Optional[dict] = None) -> dict:
    """Parâmetros que o teste muda em relação ao código: os do preset, depois os informados, e a exportação de trades."""
    values = {}
    if preset:
        if preset not in robot.presets:
            raise ValueError(f"Preset desconhecido: '{preset}'. Disponíveis: " + (", ".join(robot.presets) or "nenhum") + ".")
        values.update(robot.presets[preset])
    known = {i.name for i in robot.inputs}
    for name, value in (params or {}).items():
        if known and name not in known:
            raise ValueError(f"{robot.name} não tem o parâmetro '{name}'. Veja: mt5x robots list " +
                             robot.key.split(":", 1)[1])
        values[name] = value
    export = contract.input_name(robot, "export_trades") or "InpExportTrades"
    if export in known:
        values[export] = "true"                  # o robô grava HDK_trades_<magic>.csv no fim do teste
    return values


def run_mql5(db, config, robot, symbol: str, timeframe: str, start=None, end=None, preset: Optional[str] = None,
             params: Optional[dict] = None, run: Optional[Callable] = None,
             is_running: Optional[Callable] = None, source: str = TESTER_SOURCE,
             model: Optional[str] = None) -> dict:
    """
    Roda o robô MQL5 no Strategy Tester (o terminal precisa estar fechado) e grava a execução. O robô tem de
    estar instalado e compilado na pasta de dados do MT5 ('mt5x robots compile').
    """
    from mt5_extracao import timeframes
    from mt5_extracao.strategies import research
    from . import mt5_tools
    if robot.kind != catalog.MQL5:
        raise ValueError("Estratégias Python usam o backtester do projeto: use run_python.")
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    end = end or datetime.now()
    start = start or end - timedelta(days=365)
    if start >= end:
        raise ValueError("O início do período precisa ser anterior ao fim.")
    slug = robot.key.split(":", 1)[1]
    data = mt5_tools.data_dir(config)
    source_file = mt5_tools.install_plan(robot, data)[0].target
    compiled = source_file.with_suffix(".ex5")
    if not compiled.is_file() or (source_file.suffix.lower() == ".mq5" and source_file.is_file()
                                  and compiled.stat().st_mtime < source_file.stat().st_mtime):
        raise ValueError(f"{robot.name} não está compilado na pasta do MT5. Rode antes: mt5x robots compile {slug}")
    # O que roda no Tester é o que está na pasta do MT5. Se o fonte ou um include mudou no disco depois da última
    # compilação, o teste seria de um código antigo e a bateria leria o novo: recusa e manda compilar.
    stale = [item.target.name for item in mt5_tools.install_plan(robot, data)
             if item.source.suffix.lower() in (".mq5", ".mqh") and
             (not item.target.is_file() or item.target.read_bytes() != item.source.read_bytes())]
    if stale:
        raise ValueError(f"O código de {robot.name} mudou desde a última compilação (" + ", ".join(stale[:4]) +
                         f"). Rode antes: mt5x robots compile {slug}")
    values = tester_values(robot, preset, params)
    name = f"mt5x_{slug}"
    parameters = mt5_tools.write_tester_set(data, name, values) if values else ""
    deposit = float(research.backtest_config(config).initial_equity)
    if model is not None and model not in mt5_tools.TESTER_MODELS:
        raise ValueError(f"Modelo do Strategy Tester inválido: {model}")
    model = model or mt5_tools.tester_model(config)      # `model` troca o de [ROBOTS] tester_model só nesta execução
    defaults = {i.name: i.default for i in robot.inputs}
    magic_input = contract.input_name(robot, "magic") or "InpMagicNumber"
    magic = str(values.get(magic_input, defaults.get(magic_input, ""))).split("|")[0].strip()
    common = mt5_tools.common_files_dir(config)
    trades_csv = common / f"HDK_trades_{magic}.csv" if common is not None and magic.isdigit() else None
    if trades_csv is not None:
        trades_csv.unlink(missing_ok=True)        # o de um teste anterior não pode passar por resultado deste
    run_id = uuid.uuid4().hex
    ini = mt5_tools.tester_ini(mt5_tools.expert_name(compiled, data), symbol, tf.name, start, end, deposit, name,
                               model, parameters)
    report = mt5_tools.run_tester(config, data, ini, name, run, is_running=is_running, run_id=run_id)
    report_text = mql.read_text(report)
    report_deposit, results = parse_report(report_text)
    entries, without_stop = entries_without_stop(report_text)
    trades_source = REPORT_SOURCE
    if trades_csv is not None and trades_csv.is_file():
        results, trades_source = read_hdk_trades(trades_csv), CSV_SOURCE
    initial = report_deposit or deposit
    metrics = metrics_from_results(results, initial)
    tester_log = mt5_tools.tester_run_log(data, run_id)
    metrics.update(trades_source=trades_source, tester_model=mt5_tools.TESTER_MODELS[model], report=str(report),
                   tester_model_code=model, entries=entries, entries_without_stop=without_stop,
                   source_fingerprint=catalog.fingerprint(robot),
                   tester_log=str(tester_log) if tester_log.is_file() else "",
                   tester_log_captured=tester_log.is_file())
    equity, points = float(initial), [[start.strftime("%Y-%m-%d %H:%M:%S"), float(initial)]]
    for stamp, value, _ in results:
        equity += float(value)
        points.append([_iso(stamp), equity])
    record = {"run_id": run_id, "robot_key": robot.key, "source": source, "symbol": symbol,
              "timeframe": tf.name, "data_start": start.strftime("%Y-%m-%d %H:%M:%S"),
              "data_end": end.strftime("%Y-%m-%d %H:%M:%S"), "bars": 0,
              "params": dict(values, **({"preset": preset} if preset else {})), "metrics": metrics,
              "equity": downsample(points), "trades": [[_iso(t), float(v), float(vol)] for t, v, vol in results],
              "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    store.save_run(db, record)
    from mt5_extracao.trade_analytics import importers       # Laboratório (T16.2): operações no livro
    importers.safely(importers.record_mql5_run, db, record,
                     trades_csv if trades_source == CSV_SOURCE else None)
    return record


# --- vários ativos --------------------------------------------------------------------

def run_many(db, config, robot, symbols, timeframe: str, start=None, end=None, preset: Optional[str] = None,
             params: Optional[dict] = None, run: Optional[Callable] = None,
             is_running: Optional[Callable] = None) -> List[dict]:
    """
    O mesmo backtest em vários ativos, um por vez. Cada ativo é um teste independente (não há resultado "de
    carteira"). Um ativo que falha não interrompe os outros. Devolve, na ordem pedida,
    [{"symbol", "run" (ou None), "error" (ou "")}].
    """
    results = []
    for symbol in symbols:
        try:
            if robot.kind == catalog.MQL5:
                record = run_mql5(db, config, robot, symbol, timeframe, start, end, preset, params, run=run,
                                  is_running=is_running)
            else:
                record = run_python(db, config, robot, symbol, timeframe, start, end, params)
            results.append({"symbol": symbol, "run": record, "error": ""})
        except (OSError, RuntimeError, ValueError) as e:
            results.append({"symbol": symbol, "run": None, "error": str(e)})
    return results


def already_tested(db, robot, symbol: str, timeframe_name: str, preset: Optional[str], model: str) -> Optional[dict]:
    """
    A execução gravada que dispensa repetir esta combinação num lote com retomada (None se é preciso rodar). Robô
    MQL5: mesmo código, mesmo preset, sem parâmetro avulso e mesmo modelo do Tester. Estratégia Python: qualquer
    backtest comum da combinação.
    """
    for record in store.load_runs(db, robot.key):
        if record["source"] in catalog.HIDDEN_RUN_SOURCES or record["timeframe"] != timeframe_name or \
                record["symbol"].lower() != symbol.lower():
            continue
        if robot.kind != catalog.MQL5:
            return record
        m, used = record["metrics"], record.get("params", {})
        export = contract.input_name(robot, "export_trades") or "InpExportTrades"
        expected = dict(robot.presets.get(preset, {})) if preset else {}
        extra = [k for k, v in used.items() if k not in ("preset", export) and str(expected.get(k)) != str(v)]
        if m.get("source_fingerprint") == catalog.fingerprint(robot) and used.get("preset", "") == (preset or "") \
                and not extra and str(m.get("tester_model_code", "")) == model:
            return record
    return None


def run_batch(db, config, robots, symbols, timeframes, start=None, end=None, preset: Optional[str] = None,
              resume: bool = False, progress: Optional[Callable] = None, run: Optional[Callable] = None,
              is_running: Optional[Callable] = None) -> List[dict]:
    """
    Lote: cada robô em cada ativo e em cada timeframe, em fila (um teste por vez). Uma falha não interrompe o
    resto. Com `resume`, a combinação que já tem execução gravada da mesma configuração é pulada. Devolve, na
    ordem em que rodou, [{"robot", "symbol", "timeframe", "run" (ou None), "error", "skipped"}]; `progress` é
    chamado com cada item assim que ele termina.
    """
    from . import mt5_tools
    results = []
    model = mt5_tools.tester_model(config) if any(r.kind == catalog.MQL5 for r in robots) else ""
    for robot in robots:
        for tf in timeframes:
            for symbol in symbols:
                item = {"robot": robot.key, "symbol": symbol, "timeframe": tf.name, "run": None, "error": "",
                        "skipped": False}
                done = already_tested(db, robot, symbol, tf.name, preset, model) if resume else None
                if done is not None:
                    item.update(run=done, skipped=True)
                else:
                    try:
                        if robot.kind == catalog.MQL5:
                            item["run"] = run_mql5(db, config, robot, symbol, tf.label, start, end, preset, run=run,
                                                   is_running=is_running)
                        else:
                            item["run"] = run_python(db, config, robot, symbol, tf.label, start, end)
                    except (OSError, RuntimeError, ValueError) as e:
                        item["error"] = str(e)
                results.append(item)
                if progress:
                    progress(item)
    return results
