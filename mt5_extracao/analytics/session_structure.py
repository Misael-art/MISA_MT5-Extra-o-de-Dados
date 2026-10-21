"""Estrutura da sessão (T13.3): pregão anterior, gap, faixa inicial, rompimento e níveis anteriores.

Cada valor diz quando ficou disponível:

- retorno e máxima/mínima do pregão anterior: no fim da sessão anterior;
- gap da abertura: no fim da **primeira barra** da sessão (o preço de abertura só é conhecido
  depois do primeiro negócio; usar o fim da barra é conservador);
- faixa inicial (máxima/mínima dos primeiros ``opening_minutes``): no fim da última barra da janela;
- rompimento da faixa inicial: no fim da barra que rompe (``1`` acima, ``-1`` abaixo, primeira
  ocorrência);
- distância ao máximo/mínimo da sessão anterior, em ATR anterior: no fim de cada barra.

O gap descreve o que aconteceu **antes** de qualquer entrada na abertura: não é resultado
alcançável por quem compra ou vende na abertura (armadilha registrada na T13.4).
"""
import numpy as np
import pandas as pd

from mt5_extracao.strategies import indicators as ind


def session_summary(bars):
    grouped = bars.groupby("session_id", sort=False)
    table = pd.DataFrame({
        "open_utc": grouped["bar_start_utc"].min(), "close_utc": grouped["bar_end_utc"].max(),
        "first_end_utc": grouped["bar_end_utc"].min(),
        "open": grouped["open"].first(), "high": grouped["high"].max(), "low": grouped["low"].min(),
        "close": grouped["close"].last(),
        "complete": grouped["session_complete"].all() if "session_complete" in bars else True,
    }).sort_values("open_utc").reset_index()
    return table


def structure_features(bars, *, opening_minutes=30, atr_n=14, last_session_open=True):
    bars = bars.sort_values("bar_start_utc").reset_index(drop=True)
    if bars.empty:
        return pd.DataFrame(columns=["session_id", "observation_time", "available_at", "feature_id", "value",
                                     "metadata"])
    sessions = session_summary(bars)
    closed = sessions.iloc[:-1] if last_session_open else sessions
    rows = []
    prev = sessions.shift(1)
    for k in range(len(sessions)):
        s, p = sessions.iloc[k], prev.iloc[k]
        if k < len(closed) and k >= 1 and bool(s["complete"]) and bool(p["complete"]) and p["close"] > 0:
            rows.append((s["session_id"], s["open_utc"], s["close_utc"], "session.return",
                         float(s["close"] / p["close"] - 1), {}))
        if k >= 1 and bool(p["complete"]) and p["close"] > 0:
            rows.append((s["session_id"], s["open_utc"], s["first_end_utc"], "session.gap",
                         float(s["open"] / p["close"] - 1), {"unit": "fração do fechamento anterior"}))

    atr = ind.atr(bars, atr_n).shift(1)            # ATR conhecido antes da barra atual
    session_index = {sid: k for k, sid in enumerate(sessions["session_id"])}
    for sid, group in bars.groupby("session_id", sort=False):
        k = session_index[sid]
        open_time = group["bar_start_utc"].min()
        window_end = open_time + pd.Timedelta(minutes=opening_minutes)
        inside = group[group["bar_end_utc"] <= window_end]
        meta = {"opening_minutes": opening_minutes}
        if not inside.empty and inside["bar_end_utc"].max() >= window_end:
            high, low = float(inside["high"].max()), float(inside["low"].min())
            known_at = inside["bar_end_utc"].max()
            rows.append((sid, open_time, known_at, f"opening_range.high_{opening_minutes}", high, meta))
            rows.append((sid, open_time, known_at, f"opening_range.low_{opening_minutes}", low, meta))
            after = group[group["bar_start_utc"] >= known_at]
            broke = after[(after["high"] > high) | (after["low"] < low)]
            if not broke.empty:
                first = broke.iloc[0]
                up, down = first["high"] > high, first["low"] < low
                # As duas pontas na mesma barra: a ordem intrabarra é desconhecida, registra 0 (ambíguo).
                direction = 0 if (up and down) else (1 if up else -1)
                rows.append((sid, first["bar_start_utc"], first["bar_end_utc"],
                             f"opening_range.breakout_{opening_minutes}", float(direction),
                             dict(meta, ambiguous=bool(up and down))))
        if k >= 1 and bool(sessions.iloc[k - 1]["complete"]):
            p = sessions.iloc[k - 1]
            for idx, bar in group.iterrows():
                a = atr.iloc[idx]
                if not np.isfinite(a) or a <= 0:
                    continue
                rows.append((sid, bar["bar_start_utc"], bar["bar_end_utc"], "level.dist_prev_high_atr",
                             float((bar["close"] - p["high"]) / a), {}))
                rows.append((sid, bar["bar_start_utc"], bar["bar_end_utc"], "level.dist_prev_low_atr",
                             float((bar["close"] - p["low"]) / a), {}))
    return pd.DataFrame(rows, columns=["session_id", "observation_time", "available_at", "feature_id", "value",
                                       "metadata"])
