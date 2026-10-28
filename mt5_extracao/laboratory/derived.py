"""Leitura das barras já gravadas em ``_bars_derived``.

Não agrega, não extrai, não grava e não cria tabela. M7/M9 e os demais tempos de
dia exato saem da última revisão fechada. Tempos do enum continuam na captura
nativa de ``datasets``.
"""
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.analytics.resampling import latest_bars, parse_target
from mt5_extracao.laboratory.service import MAX_BARS, MAX_TIMEFRAMES, sources

NOTE = "sintético, âncora 00:00 do calendário da corretora, não é tempo do MT5"


def selection_plan(items):
    """``native``, ``derived``, ``mixed`` ou ``empty`` a partir das fontes já listadas."""
    if not items:
        return "empty"
    flags = {bool(item.get("synthetic")) for item in items}
    if flags == {True}:
        return "derived"
    if flags == {False}:
        return "native"
    return "mixed"


def _table_ready(db):
    from sqlalchemy import inspect
    return db is not None and getattr(db, "engine", None) is not None and inspect(db.engine).has_table("_bars_derived")


def _minutes(item):
    parsed = timeframes.parse(item["timeframe"])
    if parsed is not None:
        return parsed.minutes
    try:
        target = parse_target(item["timeframe"])
    except ValueError:
        return 10**9
    return target.minutes if target.minutes is not None else 10**9


def list_stored(db):
    """Séries de dia exato que já existem. Tempos do MT5 gravados na mesma tabela ficam de fora."""
    if not _table_ready(db):
        return []
    from sqlalchemy import text
    with db.engine.connect() as conn:
        pairs = conn.execute(text("SELECT DISTINCT symbol, tf FROM _bars_derived ORDER BY symbol, tf")).fetchall()
    result = []
    for symbol, label in pairs:
        try:
            target = parse_target(label)
        except ValueError:
            continue
        if target.mode != "exact_day":
            continue
        frame = latest_bars(db, symbol, target.label, closed_only=True)
        if frame.empty:
            continue
        starts = pd.to_datetime(frame["bar_start_broker"])
        result.append(dict(
            symbol=str(symbol), timeframe=target.label, rows=int(len(frame)),
            start=starts.min().isoformat(sep=" "), end=starts.max().isoformat(sep=" "),
            synthetic=True, mt5_predefined=False, day_origin="broker_00:00",
            kind="bars_derived", note=NOTE))
    return result


def chart_sources(db):
    """Inventário nativo mais as séries de dia exato já gravadas. Não cria barras."""
    native = [{**item, "synthetic": False, "mt5_predefined": True, "kind": "native"} for item in sources(db)]
    return sorted([*native, *list_stored(db)], key=lambda row: (row["symbol"], _minutes(row), row["timeframe"]))


def _broker_bound(value):
    if value is None or value == "":
        return None
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        raise ValueError("A janela da leitura sintética usa o relógio da corretora, sem fuso.")
    return stamp


def read_stored(db, symbol, timeframes_selected, start=None, end=None):
    """Devolve OHLC da última revisão fechada. Recusa tempo nativo e não preenche buraco."""
    labels = list(timeframes_selected)
    if not labels or len(labels) > MAX_TIMEFRAMES or len(set(labels)) != len(labels):
        raise ValueError("Escolha de 1 a 8 tempos sintéticos distintos já gravados.")
    start_at, end_at = _broker_bound(start), _broker_bound(end)
    series = []
    for label in labels:
        target = parse_target(label)
        if target.mode != "exact_day":
            raise ValueError(
                f"{label} não é leitura de dia exato. Tempos do MT5 continuam na captura nativa.")
        frame = latest_bars(db, symbol, target.label, closed_only=True)
        if not frame.empty and (start_at is not None or end_at is not None):
            stamps = pd.to_datetime(frame["bar_start_broker"])
            mask = pd.Series(True, index=frame.index)
            if start_at is not None:
                mask &= stamps >= start_at
            if end_at is not None:
                mask &= stamps <= end_at
            frame = frame.loc[mask].reset_index(drop=True)
        if frame.empty:
            raise ValueError(
                f"{target.label}: nenhuma barra completa de {symbol} na janela. "
                "Amplie o intervalo ou gere as barras com resample --session-aware. "
                "Esta leitura não cria candles.")
        if int(frame["bar_count"].ne(target.minutes).sum()) or int(frame["is_partial"].ne(0).sum()):
            raise ValueError(
                f"{target.label}: a leitura só desenha grupo completo. Há barra curta ou parcial gravada.")
        if len(frame) > MAX_BARS:
            suggested = pd.Timestamp(frame["bar_start_broker"].iloc[-MAX_BARS]).isoformat(sep=" ")
            raise ValueError(
                f"{target.label}: {len(frame)} barras na janela; o limite é {MAX_BARS}. "
                f"Informe início a partir de {suggested} (últimas {MAX_BARS} barras) ou um fim anterior.")
        starts = pd.to_datetime(frame["bar_start_broker"])
        gaps = 0
        if len(starts) > 1:
            expected = starts.iloc[:-1] + pd.Timedelta(minutes=target.minutes)
            gaps = int((starts.iloc[1:].reset_index(drop=True) != expected.reset_index(drop=True)).sum())
        prices = []
        for row in frame.itertuples(index=False):
            prices.append([float(row.open), float(row.high), float(row.low), float(row.close)])
        series.append(dict(
            timeframe=target.label, minutes=int(target.minutes), rows=int(len(frame)), gaps=gaps,
            start=starts.iloc[0].isoformat(sep=" "), end=starts.iloc[-1].isoformat(sep=" "),
            times=[stamp.isoformat(sep=" ") for stamp in starts], prices=prices,
            bar_count_min=int(frame["bar_count"].min()), bar_count_max=int(frame["bar_count"].max()),
            partials=0))
    return dict(kind="stored_derived", symbol=str(symbol), synthetic=True, mt5_predefined=False,
                day_origin="broker_00:00", note=NOTE, series=series)


def public_summary(read, head=0):
    """O CLI não imprime milhares de preços. ``head`` pede só as primeiras barras."""
    shown = max(0, int(head))
    series = []
    for item in read["series"]:
        summary = {key: value for key, value in item.items() if key not in {"times", "prices"}}
        if shown:
            summary["head"] = [dict(time=item["times"][index], open=item["prices"][index][0],
                                     high=item["prices"][index][1], low=item["prices"][index][2],
                                     close=item["prices"][index][3])
                               for index in range(min(shown, item["rows"]))]
        series.append(summary)
    return {**{key: value for key, value in read.items() if key != "series"}, "series": series}
