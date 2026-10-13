"""
Especificações de contrato dos símbolos (tamanho do tick, valor do tick, lotes, spread).

    fetch_and_save_specs(connector, db, ["WIN$N", "EURUSD"])   # lê do MT5 e grava em _symbol_specs
    spec = load_spec(db, "WIN$N")                              # usado pelo backtest e pela triagem

Sem especificação gravada, load_spec devolve um padrão genérico com known=False e os relatórios
avisam "especificação ausente: rode mt5x specs".
"""
import json
import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from .backtester import SymbolSpec

log = logging.getLogger(__name__)


def spec_from_symbol_info(info) -> dict:
    """Converte o objeto symbol_info do MT5 (ou equivalente) em dict de DatabaseManager.SPEC_FIELDS."""
    point = float(getattr(info, "point", 0) or 0.01)
    return {
        "point": point,
        "digits": int(getattr(info, "digits", 0) or 0),
        "tick_size": float(getattr(info, "trade_tick_size", 0) or point),
        "tick_value": float(getattr(info, "trade_tick_value", 0) or 0) or 1.0,
        "volume_min": float(getattr(info, "volume_min", 0) or 0) or 1.0,
        "volume_step": float(getattr(info, "volume_step", 0) or 0) or 1.0,
        "volume_max": float(getattr(info, "volume_max", 0) or 0) or 1e6,
        "contract_size": float(getattr(info, "trade_contract_size", 0) or 0) or 1.0,
        "currency_profit": str(getattr(info, "currency_profit", "") or ""),
        "spread": float(getattr(info, "spread", 0) or 0),
        "swap_long": float(getattr(info, "swap_long", 0) or 0),
        "swap_short": float(getattr(info, "swap_short", 0) or 0),
        "swap_mode": int(getattr(info, "swap_mode", 0) or 0),
        "swap_rollover3days": int(getattr(info, "swap_rollover3days", 3) or 0),
    }


def to_symbol_spec(symbol: str, row: dict) -> SymbolSpec:
    return SymbolSpec(symbol=symbol, point=row["point"], digits=int(row["digits"] or 0),
                      tick_size=row["tick_size"] or 0.0, tick_value=row["tick_value"],
                      volume_min=row["volume_min"], volume_step=row["volume_step"],
                      volume_max=row["volume_max"], contract_size=row["contract_size"] or 1.0,
                      currency_profit=row["currency_profit"] or "", spread_points=row["spread"] or 0.0,
                      swap_long=row.get("swap_long") or 0.0, swap_short=row.get("swap_short") or 0.0,
                      swap_mode=int(row.get("swap_mode") or 0),
                      swap_triple_day=int(row["swap_rollover3days"]) if row.get("swap_rollover3days") is not None else 3,
                      spread_by_hour=_hours(row.get("spread_profile"), "hours"),
                      spread_at_open=_hours(row.get("spread_profile"), "open"), known=True)


def _hours(raw, key: str):
    """Os 24 valores por hora gravados por calibrate_spread (None se não há calibração)."""
    if not raw:
        return None
    try:
        hours = json.loads(raw).get(key)
    except (ValueError, AttributeError):
        return None
    return tuple(hours) if isinstance(hours, list) and len(hours) == 24 else None


def spread_profile(ticks, point: float) -> dict:
    """
    Spread real por hora do dia (horário do servidor), em pontos, a partir de ticks com bid e ask.

    "hours": média de cada hora, ponderada pelo tempo em que cada cotação valeu (até 60 s), para um intervalo sem
    ticks não pesar. "open": média do spread no PRIMEIRO tick de cada hora, que é o que paga uma ordem executada
    na abertura da barra; na virada do dia ele é várias vezes maior que a média da hora.
    Devolve {"hours", "open" (24 valores ou None), "mean", "ticks", "days", "from", "to"}.
    """
    if ticks is None or len(ticks) == 0 or point <= 0:
        return {"hours": [None] * 24, "open": [None] * 24, "mean": None, "ticks": 0, "days": 0, "from": "", "to": ""}
    spread = ((ticks["ask"] - ticks["bid"]) / point).to_numpy(dtype=float)
    msc = ticks["time_msc"].to_numpy(dtype="int64")
    weight = np.append(np.clip(np.diff(msc), 1, 60_000), 1).astype(float)
    hour = (msc // 3_600_000) % 24
    hours = []
    for h in range(24):
        mask = hour == h
        hours.append(round(float(np.average(spread[mask], weights=weight[mask])), 3) if mask.any() else None)
    slot = msc // 3_600_000                                         # cada hora de cada dia
    first = np.append(True, slot[1:] != slot[:-1])
    opens = [round(float(spread[first & mask].mean()), 3) if (first & mask).any() else None
             for mask in (hour == h for h in range(24))]
    stamp = lambda value: pd.Timestamp(int(value), unit="ms").strftime("%Y-%m-%d %H:%M")
    return {"hours": hours, "open": opens, "mean": round(float(np.average(spread, weights=weight)), 3),
            "ticks": int(len(spread)), "days": int(len(np.unique(msc // 86_400_000))),
            "from": stamp(msc[0]), "to": stamp(msc[-1])}


def calibrate_spread(db, symbol: str) -> dict:
    """
    Mede o spread real do símbolo nos ticks já extraídos (mt5x ticks) e grava ao lado da especificação. O
    backtester passa a cobrar esse spread em cada execução, em vez do spread da barra (que é o menor do período).
    Levanta ValueError, com o que fazer, se falta a especificação ou os ticks.
    """
    row = db.get_symbol_spec(symbol)
    if row is None:
        raise ValueError(f"{symbol}: falta a especificação de contrato. Rode: mt5x specs --symbols {symbol}")
    table = db.get_table_name_for_symbol(symbol, "ticks")
    ticks = db.load_tick_spreads(table)
    if ticks.empty:
        raise ValueError(f"{symbol}: não há ticks na base. Extraia pelo menos uma semana: "
                         f"mt5x ticks --symbols {symbol} --from AAAA-MM-DD --to AAAA-MM-DD")
    profile = spread_profile(ticks, float(row["point"]))
    db.save_spread_profile(symbol, profile)
    return profile


def load_spec(db, symbol: str) -> SymbolSpec:
    row = db.get_symbol_spec(symbol)
    if row is None:
        return SymbolSpec(symbol=symbol, known=False)
    return to_symbol_spec(symbol, row)


def fetch_and_save_specs(connector, db, symbols: List[str]) -> Dict[str, str]:
    """Lê symbol_info de cada símbolo no MT5 e grava. Retorna {símbolo: 'ok' | mensagem de erro}."""
    result = {}
    for symbol in symbols:
        info = connector.get_symbol_info(symbol)
        if info is None:
            result[symbol] = "não encontrado no MT5 (confira o nome e se está na Observação de Mercado)"
            continue
        db.save_symbol_spec(symbol, spec_from_symbol_info(info))
        result[symbol] = "ok"
    return result
