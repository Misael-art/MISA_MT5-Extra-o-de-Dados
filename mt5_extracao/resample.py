"""
Monta um timeframe maior a partir de uma base menor (M5 a partir de M1, H1 a partir de M5...), sem nova extração.

    built = resample_bars(m1_bars, Timeframe.M1, Timeframe.M5)
    summary = resample_symbol(db, "EURUSD", Timeframe.M1, [Timeframe.M5, Timeframe.H1])

As regras foram conferidas contra o MetaTrader 5 real (2026-10-12, EURUSD, duas semanas, MetaQuotes-Demo): as
barras M5, M15, H1, H4 e D1 montadas do M1 saíram IGUAIS às extraídas do MT5, barra a barra, em todas as colunas:
abertura da primeira barra, máxima, mínima, fechamento da última, soma do volume de ticks e do volume real, e
`spread` = o MENOR spread das barras que formam a barra maior. (O spread da barra do MT5 é o mínimo do período:
quem usa essa coluna como custo está usando o melhor caso; veja a T10.12 em docs/PLANO_DE_TRABALHO.md.)

Só de menor para maior, com o maior múltiplo do menor, até D1: semanal e mensal dependem do calendário da
corretora. O alinhamento é pelo relógio da base (horário da corretora, como as barras estão gravadas).
"""
import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from mt5_extracao.timeframes import Timeframe

log = logging.getLogger(__name__)

MAX_MINUTES = Timeframe.D1.minutes
COLUMNS = ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")
_CREATE = """
CREATE TABLE IF NOT EXISTS _resampled (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    source_timeframe TEXT NOT NULL,
    bars INTEGER,
    data_start TEXT,
    data_end TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (symbol, timeframe)
)
"""


def check_pair(source: Timeframe, target: Timeframe) -> None:
    """Levanta ValueError, com o motivo, se `target` não pode ser montado a partir de `source`."""
    if target.minutes <= source.minutes:
        raise ValueError(f"{target.name} não é maior que {source.name}: só dá para montar um timeframe maior a partir "
                         "de um menor.")
    if target.minutes > MAX_MINUTES:
        raise ValueError(f"{target.name} não pode ser montado: semanal e mensal dependem do calendário da corretora. "
                         "Extraia esse timeframe direto do MT5.")
    if target.minutes % source.minutes:
        raise ValueError(f"{target.name} ({target.minutes} min) não é múltiplo de {source.name} ({source.minutes} min).")


def resample_bars(df: pd.DataFrame, source: Timeframe, target: Timeframe) -> pd.DataFrame:
    """Barras do timeframe `target` montadas das barras `source` (colunas time, open, high, low, close, ...)."""
    check_pair(source, target)
    if df is None or df.empty:
        return pd.DataFrame(columns=[c for c in COLUMNS if df is None or c in df.columns])
    data = df.copy()
    data["time"] = pd.to_datetime(data["time"])
    data = data.sort_values("time").drop_duplicates("time", keep="last")
    rules = {"open": "first", "high": "max", "low": "min", "close": "last", "tick_volume": "sum",
             "spread": "min", "real_volume": "sum"}
    rules = {column: rule for column, rule in rules.items() if column in data.columns}
    missing = [c for c in ("open", "high", "low", "close") if c not in rules]
    if missing:
        raise ValueError("Faltam colunas para montar as barras: " + ", ".join(missing))
    grouped = data.groupby(data["time"].dt.floor(f"{target.minutes}min"), sort=True)
    built = grouped.agg(rules).reset_index()
    return built[[c for c in COLUMNS if c in built.columns]]


def resample_symbol(db, symbol: str, source: Timeframe, targets: Iterable[Timeframe], start=None, end=None) -> List[dict]:
    """
    Monta e grava (upsert, como a extração) cada timeframe de `targets` a partir das barras `source` do símbolo.
    Devolve um resumo por timeframe. Os indicadores gravados na tabela de origem não são copiados.
    """
    from sqlalchemy import text
    targets = list(targets)
    for target in targets:
        check_pair(source, target)
    bars = db.load_ohlcv(symbol, source.label, start, end)
    if bars.empty:
        raise ValueError(f"Não há barras {source.name} de {symbol} na base" +
                         (" nesse período" if start or end else "") +
                         f". Extraia antes: mt5x extract --symbols {symbol} --tf {source.name} --from AAAA-MM-DD")
    summary = []
    for target in targets:
        built = resample_bars(bars, source, target)
        if not db.save_ohlcv_data(symbol, target.label, built):
            raise RuntimeError(f"Não foi possível gravar as barras {target.name} de {symbol} (veja logs/).")
        row = {"symbol": symbol, "timeframe": target.name, "source_timeframe": source.name, "bars": int(len(built)),
               "source_bars": int(len(bars)), "data_start": str(built["time"].iloc[0]),
               "data_end": str(built["time"].iloc[-1])}
        with db.engine.begin() as conn:
            conn.execute(text(_CREATE))
            conn.execute(text(
                "INSERT INTO _resampled (symbol, timeframe, source_timeframe, bars, data_start, data_end, created_at) "
                "VALUES (:symbol, :timeframe, :source_timeframe, :bars, :data_start, :data_end, :created_at) "
                "ON CONFLICT(symbol, timeframe) DO UPDATE SET source_timeframe = excluded.source_timeframe, "
                "bars = excluded.bars, data_start = excluded.data_start, data_end = excluded.data_end, "
                "created_at = excluded.created_at"),
                {**{k: row[k] for k in ("symbol", "timeframe", "source_timeframe", "bars", "data_start", "data_end")},
                 "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        summary.append(row)
        log.info(f"{symbol} {target.name}: {len(built)} barras montadas de {len(bars)} barras {source.name}")
    return summary


def origins(db) -> dict:
    """{(símbolo, timeframe): timeframe de origem} das séries montadas (vazio se nenhuma)."""
    from sqlalchemy import inspect, text
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_resampled"):
        return {}
    with db.engine.connect() as conn:
        return {(r[0], r[1]): r[2] for r in conn.execute(text("SELECT symbol, timeframe, source_timeframe FROM _resampled"))}
