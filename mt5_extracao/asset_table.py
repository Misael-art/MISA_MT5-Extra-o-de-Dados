"""
Dados das tabelas de ativos (abas "Ativos" e "Minha base"), sem Tkinter: favoritos e benchmark gravados no banco,
volume médio diário, resultado dos robôs por ativo, situação da base e a ordenação das linhas.

    prefs = load_prefs(db)                      # {"favorites": [...], "benchmarks": {ativo: índice}} ou None
    cells = profit_cells(store.load_runs(db))   # o backtest mais recente de cada robô em cada ativo e timeframe
    rows = sort_rows(build_rows(...), "Volume")

Favoritos e benchmark ficam na tabela interna `_asset_prefs`, criada sob demanda (mudança aditiva: bases antigas
continuam abrindo).
"""
import math
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from mt5_extracao import study, timeframes
from mt5_extracao.robots import catalog

COL_FAVORITE, COL_SYMBOL, COL_VOLUME, COL_PROFIT = "★", "Ativo", "Volume", "Robô Lucrativo"
COL_STATUS, COL_BENCHMARK, COL_DETAILS, COL_SELECTED = "Status da Base", "Benchmark", "Detalhes na Base", "Seleção"
COLUMNS = (COL_FAVORITE, COL_SYMBOL, COL_VOLUME, COL_PROFIT, COL_STATUS, COL_BENCHMARK, COL_DETAILS, COL_SELECTED)

STAR_ON, STAR_OFF = "★", "☆"
CHECK_ON, CHECK_OFF = "☑", "☐"
PROFIT_NONE, PROFIT_NEGATIVE = "⚪ N/A", "🔴 Sem Lucro"
STATUS_OK, STATUS_STALE, STATUS_EMPTY = "🟢 Atualizada", "🔴 Precisa de atualização", "⚪ Sem dados"
BENCHMARK_YES, BENCHMARK_NO = "🎯 Sim", "❌ Não"
VOLUME_DAYS = 30

_CREATE = """
CREATE TABLE IF NOT EXISTS _asset_prefs (
    symbol TEXT PRIMARY KEY,
    favorite INTEGER NOT NULL DEFAULT 0,
    benchmark TEXT,
    updated_at TEXT
)
"""


# --- favoritos e benchmark (banco) ----------------------------------------------------

def _usable(db) -> bool:
    return db is not None and getattr(db, "engine", None) is not None


def load_prefs(db) -> Optional[dict]:
    """{"favorites": [ativos], "benchmarks": {ativo: benchmark}}; None se a tabela ainda não existe (ou sem banco)."""
    from sqlalchemy import inspect, text
    if not _usable(db) or not inspect(db.engine).has_table("_asset_prefs"):
        return None
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT symbol, favorite, benchmark FROM _asset_prefs ORDER BY symbol")).fetchall()
    return {"favorites": [r[0] for r in rows if r[1]], "benchmarks": {r[0]: r[2] for r in rows if r[2]}}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_favorites(db, symbols: Iterable[str]) -> None:
    """Grava a lista de favoritos inteira (quem saiu da lista deixa de ser favorito). O benchmark não muda."""
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(text("UPDATE _asset_prefs SET favorite = 0 WHERE favorite <> 0"))
        for symbol in dict.fromkeys(symbols):
            conn.execute(text("INSERT INTO _asset_prefs (symbol, favorite, updated_at) VALUES (:s, 1, :u) "
                              "ON CONFLICT(symbol) DO UPDATE SET favorite = 1, updated_at = excluded.updated_at"),
                         {"s": symbol, "u": _now()})


def set_benchmark(db, symbol: str, benchmark: str) -> None:
    """Vincula (ou, com texto vazio, desvincula) o índice de referência de um ativo."""
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(text("INSERT INTO _asset_prefs (symbol, favorite, benchmark, updated_at) VALUES (:s, 0, :b, :u) "
                          "ON CONFLICT(symbol) DO UPDATE SET benchmark = excluded.benchmark, "
                          "updated_at = excluded.updated_at"),
                     {"s": symbol, "b": (benchmark or "").strip() or None, "u": _now()})


# --- volume ---------------------------------------------------------------------------

def _volume_series(series: List[dict]) -> Optional[dict]:
    """A série usada para medir o volume: a de maior timeframe até o diário (menos linhas para ler)."""
    known = [(timeframes.parse(item["timeframe"]), item) for item in series if item["rows"] and item["end"]]
    known = [(tf, item) for tf, item in known if tf is not None]
    if not known:
        return None
    daily = [pair for pair in known if pair[0].minutes <= 1440]
    return max(daily, key=lambda p: p[0].minutes)[1] if daily else min(known, key=lambda p: p[0].minutes)[1]


def daily_volume(df: pd.DataFrame, bar_minutes: int) -> Optional[float]:
    """Volume médio por dia das barras: volume real quando a corretora informa, senão o volume de ticks."""
    if df is None or df.empty:
        return None
    column = next((c for c in ("real_volume", "tick_volume") if c in df.columns and df[c].fillna(0).sum() > 0), None)
    if column is None:
        return None
    if bar_minutes > 1440:
        return float(df[column].mean() / (bar_minutes / 1440))
    return float(df[column].groupby(pd.to_datetime(df["time"]).dt.date).sum().mean())


def volumes(db, inventory: List[dict]) -> Dict[str, float]:
    """ativo -> volume médio diário nos últimos VOLUME_DAYS dias de dados da base (ativos sem volume ficam de fora)."""
    by_symbol: Dict[str, List[dict]] = {}
    for item in inventory:
        by_symbol.setdefault(item["symbol"], []).append(item)
    result = {}
    for symbol, series in by_symbol.items():
        item = _volume_series(series)
        if item is None:
            continue
        tf = timeframes.parse(item["timeframe"])
        df = db.load_ohlcv(symbol, item["timeframe"], start=item["end"] - timedelta(days=VOLUME_DAYS))
        value = daily_volume(df, tf.minutes)
        if value is not None:
            result[symbol] = value
    return result


def fmt_volume(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    for limit, suffix in ((1e9, " bi"), (1e6, " mi"), (1e3, " mil")):
        if value >= limit:
            return f"{value / limit:.1f}".replace(".", ",") + suffix
    return f"{value:.0f}"


# --- robô lucrativo -------------------------------------------------------------------

def _metric_number(value) -> Optional[float]:
    """Métrica disponível, sem substituir ausência ou valor inválido por zero."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def add_run(cells: dict, run: dict) -> None:
    """Põe uma execução em `cells` como a mais recente do robô naquele ativo e timeframe."""
    m = run.get("metrics") or {}
    cells.setdefault((run["symbol"], run["timeframe"]), {})[run["robot_key"]] = (
        _metric_number(m.get("net_profit")), _metric_number(m.get("return_pct")))


def profit_cells(runs: List[dict]) -> dict:
    """
    {(ativo, timeframe): {robô: (lucro líquido, retorno)}} com o backtest MAIS RECENTE de cada robô (não o melhor).
    `runs` vem de store.load_runs, o mais recente primeiro; execuções auxiliares (bateria, paridade) não entram.
    """
    cells: dict = {}
    for run in reversed(runs):
        if run["source"] not in catalog.HIDDEN_RUN_SOURCES:
            add_run(cells, run)
    return cells


def profit_of(cells: dict, symbol: str, timeframe_name: Optional[str] = None) -> Optional[float]:
    """
    None: nenhum resultado com métricas disponíveis no ativo (ou naquele timeframe). Senão, o maior retorno
    entre os robôs com lucro positivo, ou negativo/zero quando nenhum resultado conhecido teve lucro.
    """
    results = [value for (sym, tf), robots in cells.items() for value in robots.values()
               if sym == symbol and timeframe_name in (None, tf)]
    results = [(net, ret) for net, ret in results if net is not None and ret is not None]
    if not results:
        return None
    winners = [ret for net, ret in results if net > 0]
    return max(winners) if winners else min(0.0, max(ret for _, ret in results))


def fmt_profit(value: Optional[float]) -> str:
    value = _metric_number(value)
    if value is None:
        return PROFIT_NONE
    if value == 0:
        return "⚪ 0,0%"
    amount = f"{abs(value) * 100:.1f}".replace(".", ",") + "%"
    return ("🟢 +" if value > 0 else "🔴 -") + amount


# --- situação da base e benchmark -----------------------------------------------------

def is_fresh(item: dict, now=None) -> bool:
    tf = timeframes.parse(item["timeframe"])
    return study.freshness(item["end"], tf.minutes if tf else 1, now) == "em dia"


def fmt_status(fresh: Optional[bool]) -> str:
    return STATUS_EMPTY if fresh is None else (STATUS_OK if fresh else STATUS_STALE)


def fmt_benchmark(benchmark) -> str:
    return BENCHMARK_YES if benchmark else BENCHMARK_NO


def series_text(series: List[dict]) -> str:
    """'M1 (6.489 barras), H1 (120 barras)' das séries de um ativo."""
    parts = []
    for item in series:
        tf = timeframes.parse(item["timeframe"])
        bars = f"{int(item['rows']):,}".replace(",", ".")
        parts.append(f"{tf.name if tf else item['timeframe']} ({bars} barras)")
    return ", ".join(parts)


# --- linhas e ordenação ---------------------------------------------------------------

_FIELDS = ("key", "symbol", "favorite", "volume", "profit", "fresh", "benchmark", "details", "bars", "minutes",
           "selected")


def build_rows(entries: Iterable[Tuple[str, str, List[dict]]], favorites: Iterable[str], volume_map: Dict[str, float],
               cells: dict, benchmarks: Dict[str, str], selected: Iterable[str], per_series: bool = False,
               now=None) -> pd.DataFrame:
    """
    Uma linha por entrada (chave da linha, ativo, séries da base). Na aba "Ativos" a entrada é o ativo com todas as
    suas séries; na "Minha base" (`per_series`), cada série (ativo x timeframe) é uma entrada, e o resultado dos
    robôs é o daquele timeframe.
    """
    favorites, selected = set(favorites), set(selected)
    rows = []
    for key, symbol, series in entries:
        series = [item for item in series if item["rows"]]
        single = timeframes.parse(series[0]["timeframe"]) if per_series and series else None
        rows.append((key, symbol, symbol in favorites, volume_map.get(symbol),
                     profit_of(cells, symbol, single.name if single else None),
                     all(is_fresh(item, now) for item in series) if series else None,
                     benchmarks.get(symbol) or "", series_text(series), sum(int(item["rows"]) for item in series),
                     single.minutes if single else 0, key in selected))
    frame = pd.DataFrame(rows, columns=_FIELDS)
    for column in ("volume", "profit"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def sort_rows(rows: pd.DataFrame, column: Optional[str] = None, reverse: bool = False) -> pd.DataFrame:
    """
    Ordena as linhas. Os favoritos ficam sempre no topo. Sem coluna (ordem padrão): maior volume primeiro e, entre
    os sem volume, ordem alfabética. Com coluna, o primeiro clique traz o que interessa para cima (lucro, base que
    precisa de atualização, maior volume, com benchmark) e `reverse` inverte só esse critério.
    """
    if rows.empty:
        return rows
    data = rows.assign(
        _fav=~rows["favorite"].astype(bool),
        _volume=-rows["volume"].fillna(-1.0),
        # com lucro (maior retorno primeiro), depois sem lucro, depois sem teste
        _profit=rows["profit"].map(lambda v: 2 if pd.isna(v) else (1 if v <= 0 else 0)),
        _return=-rows["profit"].fillna(0.0),
        _status=rows["fresh"].map(lambda v: 2 if v is None or pd.isna(v) else (1 if v else 0)),
        _benchmark=rows["benchmark"] == "",
        _bars=-rows["bars"],
        _selected=~rows["selected"].astype(bool))
    primary = {COL_VOLUME: ["_volume"], COL_PROFIT: ["_profit", "_return"], COL_STATUS: ["_status"],
               COL_BENCHMARK: ["_benchmark"], COL_DETAILS: ["_bars"], COL_SELECTED: ["_selected"],
               COL_SYMBOL: ["symbol"]}.get(column, [])
    if reverse and COL_VOLUME == column:
        data["_volume"] = rows["volume"].fillna(float("inf"))      # do menor para o maior; sem volume por último
        primary_order = [True]
    else:
        primary_order = [not reverse] * len(primary)
    tie = [c for c in ("_volume", "symbol", "minutes") if c not in primary]
    by = ["_fav"] + primary + tie
    ordered = data.sort_values(by, ascending=[True] + primary_order + [True] * len(tie), kind="stable")
    return ordered[list(rows.columns)].reset_index(drop=True)


def display(row) -> tuple:
    """Os textos de uma linha (de build_rows), na ordem de COLUMNS."""
    fresh = None if row.fresh is None or pd.isna(row.fresh) else bool(row.fresh)
    return (STAR_ON if row.favorite else STAR_OFF, row.symbol, fmt_volume(row.volume), fmt_profit(row.profit),
            fmt_status(fresh), fmt_benchmark(row.benchmark), row.details, CHECK_ON if row.selected else CHECK_OFF)
