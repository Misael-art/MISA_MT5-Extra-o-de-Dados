"""Comparação do ORB legado com o recorte de pregão qualificado por F19-PESQ-01."""
from collections import defaultdict
from math import isfinite

import pandas as pd

from mt5_extracao.analytics import opening_coverage
from mt5_extracao.strategies.orb import OpeningRangeBreakout


def _events(data, signals, time_utc, dates):
    events = defaultdict(list)
    for index in signals.index[signals.entry_long | signals.entry_short]:
        side = "long" if signals.at[index, "entry_long"] else "short"
        start = time_utc.loc[index]
        events[dates.loc[index]].append({"bar_open_utc": start.isoformat(),
                       "available_at_utc": (start + opening_coverage.MINUTE).isoformat(),
                       "side": side, "close": float(data.at[index, "close"]),
                       "stop": float(signals.at[index, "stop_" + side]),
                       "target": float(signals.at[index, "target_" + side])})
    return events


def compare(snapshot, calendar, *, as_of, range_minutes=30, target_mult=1.5, min_range_atr=0.0):
    """Compara sinais Python pontuais. SL, TP e decisão saem do mesmo ORB."""
    if (type(target_mult) not in (int, float) or not isfinite(target_mult) or target_mult <= 0 or
            type(min_range_atr) not in (int, float) or not isfinite(min_range_atr) or min_range_atr < 0):
        raise ValueError("O alvo precisa de múltiplo positivo e o filtro ATR de múltiplo não negativo.")
    coverage = opening_coverage.report([snapshot], calendar, as_of=as_of, opening_minutes=range_minutes)
    params = {"range_minutes": range_minutes, "target_mult": target_mult, "min_range_atr": min_range_atr}
    strategy = OpeningRangeBreakout(**params)
    data = snapshot.bars
    clock = snapshot.manifest["time"]["broker_utc_offset_hours"]
    time_utc = pd.to_datetime(data.time, format="ISO8601").dt.tz_localize("UTC") - pd.Timedelta(hours=clock)
    visible = time_utc + opening_coverage.MINUTE <= pd.Timestamp(coverage["as_of"])
    data, time_utc = data.loc[visible], time_utc.loc[visible]
    legacy = strategy.gerar_sinais(data)
    day = time_utc.dt.tz_convert(coverage["timezone"]).dt.strftime("%Y-%m-%d")
    qualified_sessions = {row["date"]: row for row in coverage["sessions"] if row["status"] == "eligible"}
    high = day.map({key: row["range"]["high"] for key, row in qualified_sessions.items()})
    low = day.map({key: row["range"]["low"] for key, row in qualified_sessions.items()})
    ready_at = pd.to_datetime(day.map({key: row["range"]["available_at_utc"]
                                       for key, row in qualified_sessions.items()}), utc=True)
    close = pd.to_datetime(day.map({key: row["expected_close_utc"] for key, row in qualified_sessions.items()}), utc=True)
    ready = (time_utc >= ready_at) & (time_utc < close)
    qualified = legacy.copy()
    atr = qualified.atr if min_range_atr else qualified.atr.fillna(0)
    strategy._breakouts(data, qualified, day, ready, high, low, atr)
    legacy_events = _events(data, legacy, time_utc, day)
    nominal_events = _events(data, qualified, time_utc, day)
    sessions = [{**row, "legacy_signals": legacy_events[row["date"]],
                 "qualified_signals": nominal_events[row["date"]]} for row in coverage["sessions"]]
    first = next((row for row in sessions if row["legacy_signals"] != row["qualified_signals"]), None)
    return {"version": 1, "contract": snapshot.manifest["symbol"], "params": params,
            "coverage_manifest_id": coverage["manifest_id"], "snapshot_id": snapshot.manifest_id,
            "first_divergent_session": first["date"] if first else None,
            "execution_evidence": "signals_python_only", "sessions": sessions}
