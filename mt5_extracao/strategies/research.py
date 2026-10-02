"""
Orquestração da pesquisa (usada pela CLI e pela interface gráfica, sem Tkinter):
carrega os dados do banco, roda triagem/validação com a configuração do usuário e
gera o relatório HTML.
"""
import logging
import os
from datetime import datetime
from typing import Callable, List, Optional

import pandas as pd
from sqlalchemy import inspect, text

from . import REGISTRY, get_strategy
from . import asset_screener, specs, validation
from .backtester import BacktestConfig

log = logging.getLogger(__name__)

Progress = Optional[Callable[[str], None]]


def backtest_config(config) -> BacktestConfig:
    g = (lambda key, default: config.getfloat("STRATEGY", key, fallback=default)) if config is not None \
        else (lambda key, default: default)
    return BacktestConfig(initial_equity=g("initial_equity", 100_000.0), risk_per_trade=g("risk_per_trade", 0.01),
                          slippage_points=g("slippage_points", 0.0), commission_per_lot=g("commission_per_lot", 0.0))


def criteria(config) -> validation.Criteria:
    if config is None:
        return validation.Criteria()
    return validation.Criteria(
        min_profit_factor=config.getfloat("STRATEGY", "min_profit_factor", fallback=1.3),
        min_trades=config.getint("STRATEGY", "min_trades", fallback=100),
        max_drawdown=config.getfloat("STRATEGY", "max_drawdown", fallback=0.20))


def symbols_with_data(db, tf) -> List[str]:
    """Símbolos que já têm tabela no banco para o timeframe (registro em _symbol_tables)."""
    if not inspect(db.engine).has_table("_symbol_tables"):
        return []
    key = db._normalize_name(tf.label)
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT symbol FROM _symbol_tables WHERE timeframe = :t ORDER BY symbol"),
                            {"t": key}).fetchall()
    return [r[0] for r in rows if db.find_table_for_symbol(r[0], tf.label)]


def load_data(db, symbols: List[str], tf, start=None, end=None, progress: Progress = None):
    data, missing = {}, []
    for symbol in symbols:
        df = db.load_ohlcv(symbol, tf.label, start, end)
        if df.empty:
            missing.append(symbol)
        else:
            data[symbol] = df
            if progress:
                progress(f"{symbol} {tf.name}: {len(df)} barras ({df['time'].iloc[0]:%Y-%m-%d} a "
                         f"{df['time'].iloc[-1]:%Y-%m-%d})")
    return data, missing


def missing_message(symbols: List[str], tf) -> str:
    return (f"sem dados de {', '.join(symbols)} em {tf.name}. Extraia antes, ex.: "
            f"mt5x extract --symbols {','.join(symbols)} --tf {tf.name} --from 2020-01-01")


def run_screen(db, symbols, tf, config=None, progress: Progress = None):
    data, missing = load_data(db, symbols, tf, progress=progress)
    cfg = asset_screener.ScreenerConfig.from_config(config)
    events = asset_screener.load_events(config.get("SCREENER", "events_file", fallback="").strip()
                                        if config is not None else "")
    portfolio = [s.strip() for s in (config.get("SCREENER", "portfolio", fallback="") if config is not None
                                     else "").split(",") if s.strip()]
    rows = asset_screener.screen(data, {s: specs.load_spec(db, s) for s in data}, tf.minutes, cfg, events,
                                 portfolio, validation.load_validations(db, list(data), tf.name), tf.name)
    return rows, missing


def run_validation(db, symbols, tf, keys, config=None, progress: Progress = None):
    """Valida cada estratégia × símbolo, grava em _strategy_validation e devolve os vereditos."""
    data, missing = load_data(db, symbols, tf, progress=progress)
    bt, crit = backtest_config(config), criteria(config)
    verdicts = []
    for symbol, df in data.items():
        spec = specs.load_spec(db, symbol)
        for key in keys:
            if progress:
                progress(f"Validando {get_strategy(key).info.name} em {symbol} {tf.name}...")
            try:
                verdict = validation.validate(df, key, spec, bt, crit)
            except ValueError as e:
                verdicts.append({"symbol": symbol, "strategy": key, "approved": False, "status": f"⚠ {e}"})
                continue
            if not spec.known:
                verdict["status"] += " (especificação ausente: rode 'mt5x specs')"
            validation.save_validation(db, symbol, tf.name, verdict)
            verdicts.append({"symbol": symbol, **verdict})
    return verdicts, missing


def build_report(db, symbols, tf, config=None, out_path=None, progress: Progress = None):
    from .report import render_report
    rows, missing = run_screen(db, symbols, tf, config, progress)
    validations = validation.load_validations(db, symbols, tf.name)
    html = render_report(rows, validations, tf, missing)
    if out_path is None:
        os.makedirs("exports", exist_ok=True)
        out_path = os.path.join("exports", f"relatorio_estrategias_{tf.name}_{datetime.now():%Y%m%d_%H%M}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return out_path, rows, missing
