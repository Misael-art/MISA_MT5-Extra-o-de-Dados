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


def _execution_code_digest():
    import hashlib
    import json
    import importlib
    import marshal
    import types
    from mt5_extracao.strategies import REGISTRY
    root = Path(__file__).resolve().parents[1]
    sources = {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
               for path in sorted(root.rglob("*.py"))}
    def normalized(code):
        return code.replace(co_filename="", co_consts=tuple(normalized(value) if isinstance(value, types.CodeType) else value
                                                            for value in code.co_consts))
    # O protocolo 0 grava valores sem flags de strings internadas/referências
    # compartilhadas. Essas flags de memória podem divergir após um restart,
    # embora o código/constantes sejam iguais; não fazem parte da identidade.
    loaded = {}
    modules = {cls.__module__ for cls in REGISTRY.values()} | {
        __name__, "mt5_extracao.strategies.base", "mt5_extracao.strategies.backtester",
        "mt5_extracao.strategies.indicators", "mt5_extracao.strategies.equity",
        "mt5_extracao.composition.adapter",
        "mt5_extracao.analytics.instruments", "mt5_extracao.analytics.data_eligibility"}
    for name in sorted(modules):
        module = importlib.import_module(name)
        for key, value in sorted(vars(module).items()):
            if isinstance(value, types.FunctionType):
                loaded[name + ":" + key] = hashlib.sha256(marshal.dumps(normalized(value.__code__), 0)).hexdigest()
            elif isinstance(value, type) and value.__module__ == name:
                for method, function in sorted(vars(value).items()):
                    if isinstance(function, (classmethod, staticmethod)):
                        function = function.__func__
                    if isinstance(function, types.FunctionType):
                        loaded[name + ":" + key + ":" + method] = hashlib.sha256(marshal.dumps(normalized(function.__code__), 0)).hexdigest()
    return hashlib.sha256(json.dumps(dict(sources=sources, loaded=loaded), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _python_execution_identity(snapshot, strategy, settings, *, source="backtest", trade_from=None,
                               real_spread=True, cashflows=None, composition=None):
    """Identidade do motor existente; não cria protocolo estatístico nem outro cache."""
    import hashlib
    import json
    import platform
    from dataclasses import asdict
    import numpy as np
    import pandas as pd
    from mt5_extracao.strategies import REGISTRY
    unknown = {"version": 1, "cache_eligible": False, "status": "unqualified"}
    if (type(strategy) is not REGISTRY.get(getattr(strategy, "key", None)) or set(vars(strategy)) - {"params"} or
            cashflows is not None or source != "backtest"):
        return dict(unknown, reason="custom_execution_without_reusable_contract")
    try:
        code = _execution_code_digest()
        inputs = dict(version=1, code=code, data=snapshot.manifest_id,
                      strategy=dict(key=strategy.key, params=strategy.params, rules=asdict(strategy.exit_rules()),
                                    min_bars=strategy.min_bars, required_columns=strategy.required_columns),
                      settings=asdict(settings), trade_from=str(trade_from) if trade_from is not None else None,
                      real_spread_requested=bool(real_spread), source=source,
                      runtime=dict(python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__))
        if composition is not None:
            inputs["composition"] = composition.to_dict()
        raw = json.dumps(inputs, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (OSError, TypeError, ValueError):
        return dict(unknown, reason="execution_identity_unavailable")
    return dict(version=1, cache_eligible=True, status="frozen_reference", data_manifest_id=snapshot.manifest_id,
                code_sha256=code, digest=hashlib.sha256(raw.encode()).hexdigest())


def run_python(db, config, robot, symbol: str, timeframe: str, start=None, end=None,
               params: Optional[dict] = None, strategy=None, source: str = "backtest", trade_from=None,
               real_spread: bool = True, cashflows=None, data_manifest_id=None, control=None,
               composition=None) -> dict:
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
    from mt5_extracao.analytics import datasets
    adapter = None
    if composition is not None:
        from mt5_extracao.composition import BaseAdapter, Composition
        if strategy is not None or params is not None:
            raise ValueError("Composição já contém estratégia/parâmetros; não combine com strategy ou params.")
        composition = composition if isinstance(composition, Composition) else Composition(composition)
        adapter = BaseAdapter(composition)
        strategy = adapter.strategy()
        if robot.kind != catalog.PYTHON or robot.key != "python:" + strategy.key:
            raise ValueError("Composição exige o robô Python registrado correspondente à estratégia.")
    if strategy is None and robot.kind != catalog.PYTHON:
        raise ValueError("O backtest de robôs MQL5 roda no Strategy Tester do MetaTrader 5: use run_mql5.")
    if control is not None:
        control.check()
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    if adapter is not None and composition.to_dict()["context"] != {"symbol": symbol, "timeframe": tf.name}:
        raise ValueError("Composição não corresponde ao ativo/timeframe do backtest.")
    snapshot = datasets.load(db, data_manifest_id) if data_manifest_id is not None else datasets.capture(
        db, symbol, tf.name, start, end)
    if snapshot.manifest["symbol"] != symbol or snapshot.manifest["timeframe"] != tf.name:
        raise ValueError("Manifesto não corresponde ao ativo/timeframe; escolha a revisão correta.")
    if data_manifest_id is not None:
        for key, value in (("start", start), ("end", end)):
            if value is not None and datasets.selection_time(value) != snapshot.manifest["selection"][key]:
                raise ValueError("Manifesto não corresponde ao período pedido; capture outra seleção de dados.")
    df = snapshot.bars
    if df.empty:
        raise ValueError(research.missing_message([symbol], tf))
    if strategy is None:
        strategy = get_strategy(robot.key.split(":", 1)[1], **(params or {}))
    if len(df) < strategy.min_bars:
        raise ValueError(f"Poucas barras para {strategy.info.name}: {len(df)} no período (mínimo {strategy.min_bars}). "
                         "Escolha um período maior ou extraia mais histórico.")
    spec = snapshot.spec
    from mt5_extracao.analytics import data_eligibility
    from mt5_extracao.strategies.base import REQUIRED_COLUMNS
    required = data_eligibility.requirements({"source_id": "bars", "required_columns": getattr(strategy, "required_columns", REQUIRED_COLUMNS)})
    required = sorted(set(required) | {"ohlcv"})  # O motor também usa OHLC para abertura/stop/alvo.
    data_quality = snapshot.quality
    data_quality["eligibility"] = data_eligibility.evaluate(data_quality, required)
    data_quality["data_manifest_id"] = snapshot.manifest_id
    if not data_quality["eligibility"]["eligible"]:
        raise ValueError("Dados incompatíveis com a estratégia: " + " ".join(data_quality["eligibility"]["reasons"]))
    settings = research.backtest_config(config)
    if adapter is not None:
        settings = adapter.backtest_config(settings)
    if not real_spread:
        import dataclasses
        settings = dataclasses.replace(settings, real_spread=False)
    start_index = 0
    if trade_from is not None:
        import pandas as pd
        start_index = int((pd.to_datetime(df["time"]) < pd.Timestamp(trade_from)).sum())
        if start_index >= len(df):
            raise ValueError(f"Não há barras a partir de {trade_from} no período pedido.")
    if adapter is None:
        result = run_backtest(df, strategy, spec, settings, start_index=start_index, cashflows=cashflows,
                              observation_timeframe=tf.name if source not in catalog.HIDDEN_RUN_SOURCES else None,
                              **({"check": control.check} if control is not None else {}))
    else:
        result = adapter.run(df, spec, settings, timeframe=tf.name,
                             time_basis=snapshot.manifest["time"].get("consumer_basis"),
                             start_index=start_index, cashflows=cashflows,
                             observation_timeframe=tf.name if source not in catalog.HIDDEN_RUN_SOURCES else None,
                             check=control.check if control is not None else None)
    metrics = {k: v for k, v in result.metrics.items() if k != "exit_reasons"}
    metrics.update(exit_reasons=result.metrics["exit_reasons"], skipped_min_lot=int(result.skipped_min_lot),
                   data_manifest_id=snapshot.manifest_id, data_provenance_status="frozen_reference",
                   data_quality=data_quality,
                   execution_identity=_python_execution_identity(snapshot, strategy, settings, source=source,
                                                                 trade_from=trade_from, real_spread=real_spread, cashflows=cashflows,
                                                                 **({"composition": composition} if composition is not None else {})),
                   instrument_context=dict(spec.provenance),
                   spec_historical_known=spec.provenance.get("specification_status") == "known",
                   series_role=spec.provenance.get("series_role", "unknown"),
                   spec_known=bool(spec.known), real_spread=bool(settings.real_spread and spec.spread_by_hour),
                   initial_equity=float(settings.initial_equity),
                   risk_per_trade=float(settings.risk_per_trade))
    if composition is not None:
        metrics.update(composition=composition.to_dict(), composition_hash=composition.config_hash,
                       composition_executor="composition_v1", compound=bool(settings.compound))
    run = {"run_id": control.run_id if control is not None else uuid.uuid4().hex, "robot_key": robot.key, "source": source, "symbol": symbol,
           "timeframe": tf.name, "data_start": str(df["time"].iloc[0]), "data_end": str(df["time"].iloc[-1]),
           "bars": int(len(df)), "params": dict(strategy.params), "metrics": metrics,
           "data_manifest_id": snapshot.manifest_id, "data_provenance_status": "frozen_reference",
           "equity": downsample(equity_curve(result.trades, settings.initial_equity, df["time"].iloc[0])),
           "account_balance": [[str(t), float(v)] for t, v in result.account_balance.items()],
           "floating_equity": [[str(t), float(v)] for t, v in result.floating_equity.items()],
           "performance_equity": [[str(t), float(v)] for t, v in result.performance_equity.items()],
           "account_cashflows": [[str(t), float(v)] for t, v in result.account_cashflows.items() if v != 0],
           "valuation": {**result.valuation, "stored_cashflow_encoding": "sparse_bar_close_events"},
           "trades": [[str(t.exit_time), float(t.pnl), float(t.volume)] for t in result.trades],
           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    store.save_run(db, run, **({"control": control} if control is not None else {}))
    from mt5_extracao.trade_analytics import importers       # Laboratório (T16.2): operações completas no livro
    importers.safely(importers.record_python_run, db, run, result.trades, trajectory=result.trajectory)
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


def report_cash_result(text: str) -> Optional[float]:
    """Variação do saldo no relatório nativo; inclui cobranças sem posição que o CSV pode não alocar."""
    deposit, final = None, None
    for row in _ROW.findall(text):
        cells = [_TAG.sub("", c).replace("&nbsp;", " ").strip() for c in _CELL.findall(row)]
        if len(cells) != _DEAL_COLUMNS or not _TIME.match(cells[0]):
            continue
        if cells[3].lower() == "balance" and deposit is None:
            deposit = _number(cells[10])
        if cells[11]:
            final = _number(cells[11])
    return final - deposit if deposit is not None and final is not None else None


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


def _installed_native(config, robot, compiled_override=None):
    from . import mt5_tools
    slug = robot.key.split(":", 1)[1]
    data = mt5_tools.data_dir(config)
    source_file = mt5_tools.install_plan(robot, data)[0].target
    compiled = Path(compiled_override) if compiled_override is not None else source_file.with_suffix(".ex5")
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
    return data, compiled


def _serialized_tester(function):
    from functools import wraps
    import inspect
    signature = inspect.signature(function)
    @wraps(function)
    def serialized(*args, **kwargs):
        from . import mt5_tools, sessions
        from mt5_extracao import timeframes
        arguments = signature.bind(*args, **kwargs)
        arguments.apply_defaults()
        values = arguments.arguments
        control = values["control"]
        config, robot = values["config"], values["robot"]
        with mt5_tools.tester_lease(control.check if control is not None else None):
            owner = getattr(config, "_mt5x_session", None)
            if owner is not None and owner.child_config is config and mt5_tools.install_dir(config).resolve() == owner.directory.resolve():
                return function(*args, **kwargs)
            if robot.kind != catalog.MQL5:
                return function(*args, **kwargs)
            tf = timeframes.parse(values["timeframe"])
            if tf is None:
                raise ValueError(f"Timeframe inválido: {values['timeframe']}")
            finish = values["end"] or datetime.now()
            begin = values["start"] or finish - timedelta(days=365)
            if begin >= finish:
                raise ValueError("O início do período precisa ser anterior ao fim.")
            _installed_native(config, robot, values["compiled_override"])
            tester_values(robot, values["preset"], values["params"])
            if values["model"] is not None and values["model"] not in mt5_tools.TESTER_MODELS:
                raise ValueError(f"Modelo do Strategy Tester inválido: {values['model']}")
            if values["is_running"] is not None and values["is_running"]():
                raise RuntimeError("O terminal informado pelo executor já está aberto; aguarde ou use outra sessão.")
            execution = sessions.Session(values["db"], config, robot, values["symbol"], tf.name,
                                         preset=values["preset"], mode="replay" if values["visual"] else "backtest",
                                         start=begin, end=finish, native_options=dict(values))
            execution.run()
            if execution.exception is not None:
                raise execution.exception
            if execution.record is None:
                raise RuntimeError(execution.error or "O teste isolado terminou sem resultado completo.")
            return execution.record
    return serialized


@_serialized_tester
def run_mql5(db, config, robot, symbol: str, timeframe: str, start=None, end=None, preset: Optional[str] = None,
             params: Optional[dict] = None, run: Optional[Callable] = None,
             is_running: Optional[Callable] = None, source: str = TESTER_SOURCE,
             model: Optional[str] = None, visual: Optional[bool] = None, read_csv: bool = True,
             trades_csv_override=None, compiled_override=None, export_trades: bool = True,
             tester_port: Optional[int] = None, control=None) -> dict:
    """
    Roda o robô MQL5 no Strategy Tester (o terminal precisa estar fechado) e grava a execução. O robô tem de
    estar instalado e compilado na pasta de dados do MT5 ('mt5x robots compile').
    """
    from mt5_extracao import timeframes
    from mt5_extracao.strategies import research
    from . import mt5_tools
    if control is not None:
        control.check()
    if robot.kind != catalog.MQL5:
        raise ValueError("Estratégias Python usam o backtester do projeto: use run_python.")
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    end = end or datetime.now()
    start = start or end - timedelta(days=365)
    if start >= end:
        raise ValueError("O início do período precisa ser anterior ao fim.")
    data, compiled = _installed_native(config, robot, compiled_override)
    values = tester_values(robot, preset, params)
    export_input = contract.input_name(robot, "export_trades") or "InpExportTrades"
    if not export_trades and export_input in {i.name for i in robot.inputs}:
        values[export_input] = "false"  # o observador da sessão publica seu próprio CSV, sem sobrescrever o legado
    run_id = control.run_id if control is not None else uuid.uuid4().hex
    artifact_id = run_id + "_" + uuid.uuid4().hex
    name = f"mt5x_{artifact_id}"
    if control is not None:
        control.check(path=data, reserve_bytes=len(str(values).encode("utf-8")) * 2)
    parameters = mt5_tools.write_tester_set(data, name, values) if values else ""
    deposit = float(research.backtest_config(config).initial_equity)
    if model is not None and model not in mt5_tools.TESTER_MODELS:
        raise ValueError(f"Modelo do Strategy Tester inválido: {model}")
    model = model or mt5_tools.tester_model(config)      # `model` troca o de [ROBOTS] tester_model só nesta execução
    defaults = {i.name: i.default for i in robot.inputs}
    magic_input = contract.input_name(robot, "magic") or "InpMagicNumber"
    magic = str(values.get(magic_input, defaults.get(magic_input, ""))).split("|")[0].strip()
    common = mt5_tools.common_files_dir(config)
    owner = getattr(config, "_mt5x_session", None)
    trades_csv = common / owner.trades_file if read_csv and common is not None and owner is not None and owner.instrumented else None
    if trades_csv_override is not None:
        trades_csv = Path(trades_csv_override)
        if trades_csv.exists():
            raise ValueError("Já existe um CSV para esta sessão. Use uma sessão nova; o arquivo foi preservado.")
    ini = mt5_tools.tester_ini(mt5_tools.expert_name(compiled, data), symbol, tf.name, start, end, deposit, name,
                               model, parameters, **({"visual": visual} if visual is not None else {}),
                               **({"port": tester_port} if tester_port is not None else {}))
    if control is not None:
        control.check(path=data, reserve_bytes=len(ini.encode("utf-16")))
    report = mt5_tools.run_tester(config, data, ini, name, run, is_running=is_running, run_id=artifact_id)
    if control is not None:
        control.check()
    report_text = mql.read_text(report)
    if trades_csv_override is not None and not trades_csv.is_file():
        raise RuntimeError("O Tester não publicou o livro desta sessão. O teste pode ter sido interrompido; "
                           "consulte o diário. Nenhum resultado foi habilitado para PLAY.")
    report_deposit, results = parse_report(report_text)
    entries, without_stop = entries_without_stop(report_text)
    trades_source = REPORT_SOURCE
    if trades_csv is not None and trades_csv.is_file():
        results, trades_source = read_hdk_trades(trades_csv), CSV_SOURCE
    initial = report_deposit or deposit
    metrics = metrics_from_results(results, initial)
    if trades_csv is not None and trades_csv.is_file():
        import csv
        rows = list(csv.DictReader(mql.read_text(trades_csv).splitlines(), delimiter=";"))
        currencies = {r.get("currency") for r in rows if r.get("currency")}
        if len(currencies) == 1:
            metrics["currency"] = currencies.pop()
        metrics["costs"] = {key: sum(_number(r.get(key, "0") or "0") for r in rows)
                            for key in ("commission", "swap", "fee")}
        metrics["trades_csv"] = str(trades_csv)
        metrics["observation_unit"] = "posição encerrada, incluindo custos de entrada e saída"
        native_cash = report_cash_result(report_text)
        if native_cash is None or not math.isclose(native_cash, metrics["net_profit"], rel_tol=1e-9, abs_tol=.01):
            raise ValueError("O livro da sessão não reconcilia com a variação do saldo no relatório MT5. "
                             "Confira taxas, ativos adicionais e o diário; nenhum resultado foi habilitado para PLAY.")
        metrics.update(native_cash_result=native_cash, cash_reconciled=True,
                       cash_reconciliation_difference=metrics["net_profit"] - native_cash)
    tester_log = mt5_tools.tester_run_log(data, artifact_id)
    from mt5_extracao.analytics import data_eligibility
    metrics["data_quality"] = data_eligibility.tester_report(report_text, model)
    metrics["execution_identity"] = dict(version=1, cache_eligible=False, status="unqualified",
                                         reason="native_history_revision_unavailable")
    if owner is not None:
        metrics.update(artifact_directory=str(owner.directory), artifact_report=str(owner.report),
                       native_profile_isolated=True,
                       native_detail_capture="observer" if owner.instrumented and trades_source == CSV_SOURCE else "report_or_explicit_csv")
    metrics.update(artifact_id=artifact_id, trades_source=trades_source, tester_model=mt5_tools.TESTER_MODELS[model], report=str(report),
                   tester_model_code=model, entries=entries, entries_without_stop=without_stop,
                   source_fingerprint=catalog.fingerprint(robot),
                   tester_log=str(tester_log) if tester_log.is_file() else "",
                   tester_log_captured=tester_log.is_file())
    if visual is not None:
        metrics.update(visual=visual, session_state="completed")
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
    store.save_run(db, record, **({"control": control} if control is not None else {}))
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


def already_tested(db, robot, symbol: str, timeframe_name: str, preset: Optional[str], model: str,
                   *, config=None, start=None, end=None, data_manifest_id=None) -> Optional[dict]:
    """Reutiliza só identidade Python comprovada. Recuperar checkpoint é outra operação.

    Dados nativos sem revisão não qualificam cache entre experimentos. Legados
    continuam legíveis, mas a falta de identidade não é preenchida retroativamente.
    """
    if robot.kind != catalog.PYTHON or preset:
        return None
    from mt5_extracao import timeframes
    from mt5_extracao.strategies import get_strategy, research
    from mt5_extracao.analytics import datasets
    tf = timeframes.parse(timeframe_name)
    if tf is None:
        return None
    try:
        snapshot = datasets.load(db, data_manifest_id) if data_manifest_id is not None else datasets.capture(db, symbol, tf.name, start, end)
        if snapshot.manifest["symbol"] != symbol or snapshot.manifest["timeframe"] != tf.name:
            return None
        expected = _python_execution_identity(snapshot, get_strategy(robot.key.split(":", 1)[1]), research.backtest_config(config))
    except (ValueError, OSError, KeyError):
        return None
    if not expected["cache_eligible"]:
        return None
    for record in store.load_runs(db, robot.key):
        if record["source"] != "backtest" or record["timeframe"] != tf.name or record["symbol"] != symbol:
            continue
        actual = record["metrics"].get("execution_identity", {})
        if actual.get("cache_eligible") is True and actual.get("version") == 1 and actual.get("digest") == expected["digest"]:
            return record
    return None


def run_batch(db, config, robots, symbols, timeframes, start=None, end=None, preset: Optional[str] = None,
              resume: bool = False, progress: Optional[Callable] = None, run: Optional[Callable] = None,
              is_running: Optional[Callable] = None, budget=None, batch_id=None) -> List[dict]:
    """Fila sequencial existente, com orçamento/checkpoint e retomada explícita por batch_id.

    --resume localiza o último checkpoint compatível; fora dele só reutiliza
    identidade de execução comprovada. Falhas anteriores ficam no histórico.
    """
    from . import budget as budget_mod, mt5_tools
    budget = budget or budget_mod.Budget(db, config, batch_id=batch_id)
    jobs = [(robot, symbol, tf.name) for robot in robots for tf in timeframes for symbol in symbols]
    results = []
    model = mt5_tools.tester_model(config) if any(r.kind == catalog.MQL5 for r in robots) else ""
    failed = False
    try:
        plan = budget.prepare(jobs, start=start, end=end, preset=preset, resume_latest=resume and batch_id is None)
        for (robot, symbol, timeframe), entry in zip(jobs, plan):
            item = {"robot": robot.key, "symbol": symbol, "timeframe": timeframe, "run": None, "error": "", "skipped": False}
            control = None
            try:
                budget.check()
                done = budget.recovered(entry["key"])
                reuse_basis = "checkpoint" if done is not None else ""
                if done is None and resume and batch_id is None:
                    done = already_tested(db, robot, symbol, timeframe, preset, model, config=config, start=start, end=end,
                                          data_manifest_id=entry.get("data_manifest_id"))
                    if done is not None:
                        reuse_basis = "execution_identity"
                if done is not None:
                    budget.remember(entry["key"], done)
                    item.update(run=done, skipped=True, reuse_basis=reuse_basis)
                else:
                    control = budget.admit(entry["key"])
                    if entry.get("preparation_error"):
                        raise ValueError(entry["preparation_error"])
                    if robot.kind == catalog.MQL5:
                        item["run"] = run_mql5(db, config, robot, symbol, timeframe,
                                              datetime.fromisoformat(entry["start"]), datetime.fromisoformat(entry["end"]),
                                              preset, run=run, is_running=is_running, control=control)
                    else:
                        item["run"] = run_python(db, config, robot, symbol, timeframe, start, end,
                                                data_manifest_id=entry.get("data_manifest_id"), control=control)
            except budget_mod.Interrupted as exc:
                item.update(error=str(exc), status="interrupted")
                if control is not None:
                    control.result("interrupted", str(exc))
                results.append(item)
                if progress:
                    progress(item)
                break
            except (OSError, RuntimeError, ValueError) as exc:
                item["error"] = str(exc)
                failed = True
                if control is not None:
                    control.result("failed", str(exc))
            results.append(item)
            if progress:
                progress(item)
    except KeyboardInterrupt:
        budget.cancel()
    except budget_mod.Interrupted:
        pass
    finally:
        budget.finish(failed=failed)
    return results
