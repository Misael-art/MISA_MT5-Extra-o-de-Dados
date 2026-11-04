"""Criptomoedas pela API pública de mercado da Binance (sem conta, sem chave): pares e barras OHLCV.

A conta de demonstração da MetaQuotes não tem criptomoedas. As velas da Binance à vista
(``/api/v3/klines``) cobrem desde a listagem de cada par, de M1 a MN1. Só velas fechadas são gravadas.
Os horários são UTC, como a Binance publica. Os dados vão para um banco próprio (``DEFAULT_DB``): a
base é vinculada a uma única fonte (``datasets.declare_source``).
"""
import os
import time as _time
from datetime import datetime, timezone

import pandas as pd
import requests

DEFAULT_DB = os.path.join("database", "cripto_binance.db")
SOURCE = {"provider": "binance", "server": "api.binance.com"}
BASE_URL = "https://api.binance.com"
LIMIT = 1000                                   # velas por requisição (máximo da API)
INTERVALS = {"M1": "1m", "M3": "3m", "M5": "5m", "M15": "15m", "M30": "30m", "H1": "1h", "H2": "2h",
             "H4": "4h", "H6": "6h", "H8": "8h", "H12": "12h", "D1": "1d", "W1": "1w", "MN1": "1M"}
_COLUMNS = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]


def _get(path, params, *, session=None, timeout=30, attempts=4):
    session = session or requests
    for attempt in range(attempts):
        response = session.get(BASE_URL + path, params=params, timeout=timeout)
        if response.status_code in (418, 429) and attempt + 1 < attempts:
            _time.sleep(int(response.headers.get("Retry-After") or 2 ** attempt))   # limite de uso da API
            continue
        response.raise_for_status()
        return response.json()
    response.raise_for_status()


def list_pairs(quote=None, *, session=None):
    """Pares negociáveis à vista: [{symbol, base, quote}], opcionalmente só de uma moeda de cotação."""
    info = _get("/api/v3/exchangeInfo", {"permissions": "SPOT"}, session=session)
    pairs = [{"symbol": s["symbol"], "base": s["baseAsset"], "quote": s["quoteAsset"]}
             for s in info["symbols"] if s.get("status") == "TRADING"]
    if quote:
        pairs = [p for p in pairs if p["quote"] == quote.upper()]
    return sorted(pairs, key=lambda p: p["symbol"])


def pair_spec(symbol, *, session=None):
    """Especificação do par no formato de ``DatabaseManager.SPEC_FIELDS`` (contrato = 1 unidade da base).

    Vem dos filtros públicos da Binance: passo de preço (PRICE_FILTER) e lote mínimo/passo/máximo
    (LOT_SIZE). Spot não tem swap; o spread real não é publicado nas velas e fica 0.
    """
    info = _get("/api/v3/exchangeInfo", {"symbol": symbol.upper()}, session=session)["symbols"][0]
    filters = {f["filterType"]: f for f in info["filters"]}
    tick = float(filters["PRICE_FILTER"]["tickSize"])
    lot = filters["LOT_SIZE"]
    digits = len(filters["PRICE_FILTER"]["tickSize"].rstrip("0").split(".")[1]) if tick < 1 else 0
    return {"point": tick, "digits": digits, "tick_size": tick, "tick_value": tick, "contract_size": 1.0,
            "volume_min": float(lot["minQty"]), "volume_step": float(lot["stepSize"]),
            "volume_max": float(lot["maxQty"]), "currency_profit": info["quoteAsset"], "spread": 0,
            "swap_long": 0.0, "swap_short": 0.0, "swap_mode": 0, "swap_rollover3days": 0}


def _ms(moment):
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def klines(symbol, tf_name, start, end, *, session=None, now=None):
    """Velas fechadas de ``symbol`` entre ``start`` e ``end`` (UTC), no formato do banco.

    ``tick_volume`` é o número de negócios. ``real_volume`` é o volume financeiro na moeda de cotação
    (USDT, BRL…), arredondado: a coluna do banco é inteira e a quantidade da moeda base é fracionária.
    """
    interval = INTERVALS.get(tf_name)
    if interval is None:
        raise ValueError(f"timeframe {tf_name} não existe na Binance (use {', '.join(INTERVALS)})")
    cursor, stop = _ms(start), _ms(end)
    closed_before = _ms(now or datetime.now(timezone.utc))
    rows = []
    while cursor <= stop:
        batch = _get("/api/v3/klines", {"symbol": symbol.upper(), "interval": interval, "startTime": cursor,
                                        "endTime": stop, "limit": LIMIT}, session=session)
        if not batch:
            break
        rows += [r for r in batch if r[6] < closed_before]
        cursor = batch[-1][0] + 1
        if len(batch) < LIMIT:
            break
    frame = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume", "close_time",
                                        "quote_volume", "trades", "taker_base", "taker_quote", "ignore"])
    bars = pd.DataFrame({"time": pd.to_datetime(frame["open_time"], unit="ms"),
                         **{c: frame[c].astype(float) for c in ("open", "high", "low", "close")},
                         "tick_volume": frame["trades"].astype("int64"), "spread": 0,
                         "real_volume": frame["quote_volume"].astype(float).round().astype("int64")})
    return bars.drop_duplicates("time").reset_index(drop=True)[_COLUMNS]


def save(db, symbol, timeframe_label, bars):
    """Grava as barras na base da Binance; ``False`` quando a base recusa (outra fonte, base de tempo)."""
    return db.save_ohlcv_data(symbol.upper(), timeframe_label, bars, source=dict(SOURCE))
