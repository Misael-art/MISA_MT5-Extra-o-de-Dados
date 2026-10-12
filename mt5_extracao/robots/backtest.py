"""
Backtest pelo card do robô.

Estratégias Python usam o backtester do projeto (barras da base, custos e lote pelo risco) e a execução
fica gravada em `_robot_runs`, com a curva de capital. Robôs MQL5 são testados pelo Strategy Tester do
MT5 (T8.5). Um backtest isolado não valida a estratégia: isso é papel da validação e da bateria.
"""
import math
import uuid
from datetime import datetime
from typing import List, Optional

from . import catalog, store

MAX_CURVE_POINTS = 240
PF_CAP = 99.0     # profit factor sem nenhuma perda é infinito; guardamos um teto legível


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
               params: Optional[dict] = None) -> dict:
    """Roda e grava o backtest de uma estratégia Python. `timeframe` é o rótulo da base (ex.: '1 dia')."""
    from mt5_extracao import timeframes
    from mt5_extracao.strategies import get_strategy, research, specs
    from mt5_extracao.strategies.backtester import run_backtest
    if robot.kind != catalog.PYTHON:
        raise ValueError("O backtest de robôs MQL5 roda no Strategy Tester do MetaTrader 5 (ainda não disponível).")
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    df = db.load_ohlcv(symbol, tf.label, start, end)
    if df.empty:
        raise ValueError(research.missing_message([symbol], tf))
    strategy = get_strategy(robot.key.split(":", 1)[1], **(params or {}))
    if len(df) < strategy.min_bars:
        raise ValueError(f"Poucas barras para {strategy.info.name}: {len(df)} no período (mínimo {strategy.min_bars}). "
                         "Escolha um período maior ou extraia mais histórico.")
    spec = specs.load_spec(db, symbol)
    settings = research.backtest_config(config)
    result = run_backtest(df, strategy, spec, settings)
    metrics = {k: v for k, v in result.metrics.items() if k != "exit_reasons"}
    metrics["profit_factor"] = min(float(metrics["profit_factor"]), PF_CAP)
    metrics.update(exit_reasons=result.metrics["exit_reasons"], skipped_min_lot=int(result.skipped_min_lot),
                   spec_known=bool(spec.known), initial_equity=float(settings.initial_equity),
                   risk_per_trade=float(settings.risk_per_trade))
    run = {"run_id": uuid.uuid4().hex, "robot_key": robot.key, "source": "backtest", "symbol": symbol,
           "timeframe": tf.name, "data_start": str(df["time"].iloc[0]), "data_end": str(df["time"].iloc[-1]),
           "bars": int(len(df)), "params": dict(strategy.params), "metrics": metrics,
           "equity": downsample(equity_curve(result.trades, settings.initial_equity, df["time"].iloc[0])),
           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    store.save_run(db, run)
    return run
