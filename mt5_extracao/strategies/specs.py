"""
Especificações de contrato dos símbolos (tamanho do tick, valor do tick, lotes, spread).

    fetch_and_save_specs(connector, db, ["WIN$N", "EURUSD"])   # lê do MT5 e grava em _symbol_specs
    spec = load_spec(db, "WIN$N")                              # usado pelo backtest e pela triagem

Sem especificação gravada, load_spec devolve um padrão genérico com known=False e os relatórios
avisam "especificação ausente: rode mt5x specs".
"""
import logging
from typing import Dict, List

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
    }


def to_symbol_spec(symbol: str, row: dict) -> SymbolSpec:
    return SymbolSpec(symbol=symbol, point=row["point"], digits=int(row["digits"] or 0),
                      tick_size=row["tick_size"] or 0.0, tick_value=row["tick_value"],
                      volume_min=row["volume_min"], volume_step=row["volume_step"],
                      volume_max=row["volume_max"], contract_size=row["contract_size"] or 1.0,
                      currency_profit=row["currency_profit"] or "", spread_points=row["spread"] or 0.0,
                      known=True)


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
