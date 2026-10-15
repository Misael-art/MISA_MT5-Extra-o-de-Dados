"""Volume por sessão e RVOL intradiário (T13.3), só com informação anterior à decisão.

Entrada: barras de ``resampling.latest_bars`` (uma sessão por ``session_id``). Saída: quadros
"longos" com ``observation_time``, ``available_at``, ``feature_id``, ``value`` e ``metadata``,
que ``analytics.producers`` grava na feature store.

Regras:

- **Volume real e volume de ticks nunca se misturam.** ``volume_source`` escolhe um deles;
  ``auto`` usa o real só se *todas* as barras o conhecem. Barra com volume desconhecido torna a
  sessão desconhecida (ausente), nunca zero.
- Médias, medianas e percentis comparam a sessão com as **anteriores** (a atual não entra na base
  de comparação). O valor da sessão só fica disponível no fim dela.
- ``rvol`` = volume acumulado da sessão até o fim da barra ÷ mediana do acumulado no mesmo minuto
  da sessão nas sessões anteriores. Sem ``min_sessions`` comparáveis o valor fica ausente; nunca
  1,0. A confiança declarada do RVOL é o número de sessões na mediana (``rvol.reference_sessions``).
"""
import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

VOLUME_SOURCES = ("auto", "real", "tick")


def resolve_source(bars, volume_source="auto"):
    """Devolve (série de volume com NaN onde desconhecido, nome da fonte usada)."""
    if volume_source not in VOLUME_SOURCES:
        raise ValueError(f"volume_source deve ser um de {', '.join(VOLUME_SOURCES)}.")
    real_known = bars["real_volume_known"].fillna(0).astype(int) == 1 if "real_volume_known" in bars else None
    if volume_source == "auto":
        volume_source = "real" if real_known is not None and len(bars) and bool(real_known.all()) else "tick"
    if volume_source == "real":
        if real_known is None:
            return pd.Series(np.nan, index=bars.index), "real"
        return pd.to_numeric(bars["real_volume"], errors="coerce").where(real_known), "real"
    return pd.to_numeric(bars["tick_volume"], errors="coerce"), "tick"


def theil_sen_slope(values):
    """Mediana das inclinações entre todos os pares (robusta a uma sessão atípica)."""
    y = np.asarray(values, dtype=float)
    x = np.arange(len(y), dtype=float)
    ok = ~np.isnan(y)
    x, y = x[ok], y[ok]
    if len(y) < 3:
        return np.nan
    i, j = np.triu_indices(len(y), k=1)
    return float(np.median((y[j] - y[i]) / (x[j] - x[i])))


def sessions_table(bars, volume_source="auto"):
    """Uma linha por sessão: início, fim, minutos com barra, volume e volume por minuto."""
    volume, source = resolve_source(bars, volume_source)
    work = bars.assign(_volume=volume,
                       _minutes=(bars["bar_end_utc"] - bars["bar_start_utc"]).dt.total_seconds() / 60.0)
    grouped = work.groupby("session_id", sort=False)
    table = pd.DataFrame({
        "open_utc": grouped["bar_start_utc"].min(),
        "close_utc": grouped["bar_end_utc"].max(),
        "minutes": grouped["_minutes"].sum(),
        # min_count = tamanho do grupo: um único volume desconhecido torna a sessão desconhecida.
        "volume": grouped["_volume"].apply(lambda s: s.sum() if s.notna().all() else np.nan),
        "max_revision": grouped["revision"].max() if "revision" in work else 1,
        "complete": grouped["session_complete"].all() if "session_complete" in work else True,
    }).sort_values("open_utc").reset_index()
    table.loc[~table["complete"], "volume"] = np.nan
    table["volume_per_minute"] = table["volume"] / table["minutes"].where(table["minutes"] > 0)
    table["weekday"] = table["open_utc"].dt.weekday
    return table, source


def session_volume_features(bars, *, volume_source="auto", windows=(5, 10, 20, 60), slope_window=15,
                            percentile_window=60, min_sessions=5, last_session_open=True):
    """Volume da sessão comparado às anteriores; disponível no fim da sessão."""
    if min_sessions < 1 or any(int(n) < 1 for n in windows) or slope_window < 1 or percentile_window < 1:
        raise ValueError("janelas e min_sessions precisam ser inteiros positivos.")
    table, source = sessions_table(bars, volume_source)
    if last_session_open and len(table):
        table = table.iloc[:-1]       # a última sessão da base pode não ter terminado
    if table.empty:
        return _empty(), source
    vpm = table["volume_per_minute"]
    prior = vpm.shift(1)
    rows = []

    def add(name, values, meta=None):
        for k, value in enumerate(values):
            if pd.notna(value) and np.isfinite(value):
                rows.append((table["session_id"].iloc[k], table["open_utc"].iloc[k], table["close_utc"].iloc[k],
                             name, float(value), dict(meta or {})))

    add(f"volume.session_per_minute.{source}", vpm)
    for n in windows:
        if min_sessions <= n:
            median = prior.rolling(n, min_periods=min_sessions).median()
            mean = prior.rolling(n, min_periods=min_sessions).mean()
        else:
            median = mean = pd.Series(np.nan, index=prior.index)
        add(f"volume.session_median_{n}.{source}", median)
        add(f"volume.session_mean_{n}.{source}", mean)
        add(f"volume.session_ratio_median_{n}.{source}", vpm / median.where(median > 0))
    log_vpm = np.log(vpm.where(vpm > 0))
    slope = log_vpm.rolling(slope_window, min_periods=slope_window).apply(theil_sen_slope, raw=True)
    add(f"volume.session_slope_{slope_window}.{source}", slope,
        {"unit": "variação do log do volume por sessão (Theil-Sen)"})

    def prior_percentile(window):
        current, history = window[-1], window[:-1]
        history = history[~np.isnan(history)]
        if np.isnan(current) or len(history) < min_sessions:
            return np.nan
        return 100.0 * (history < current).mean()

    pct = (vpm.rolling(percentile_window + 1, min_periods=min_sessions + 1).apply(prior_percentile, raw=True)
           if min_sessions <= percentile_window else pd.Series(np.nan, index=vpm.index))
    add(f"volume.session_percentile_{percentile_window}.{source}", pct, {"unit": "percentil 0-100 vs anteriores"})
    frame = pd.DataFrame(rows, columns=["session_id", "observation_time", "available_at", "feature_id", "value",
                                        "metadata"])
    return frame, source


def intraday_rvol(bars, *, volume_source="auto", lookback=20, min_sessions=5, same_weekday=False):
    """RVOL até o fim de cada barra, comparando o mesmo minuto da sessão em sessões anteriores."""
    if lookback < 1 or min_sessions < 1:
        raise ValueError("lookback e min_sessions precisam ser inteiros positivos.")
    volume, source = resolve_source(bars, volume_source)
    if bars.empty:
        return _empty(), source
    if min_sessions > lookback:
        return _empty(), source
    work = bars.assign(_volume=volume)
    opens = work.groupby("session_id")["bar_start_utc"].transform("min")
    work["offset_minutes"] = ((work["bar_end_utc"] - opens).dt.total_seconds() // 60).astype(int)
    # Acumulado desconhecido a partir da primeira barra sem volume na sessão.
    known = work["_volume"].notna().astype(int).groupby(work["session_id"]).cummin().astype(bool)
    work["_cum"] = work["_volume"].fillna(0).groupby(work["session_id"]).cumsum().where(known)
    session_order = work.groupby("session_id")["bar_start_utc"].min().sort_values()
    pivot = work.pivot_table(index="session_id", columns="offset_minutes", values="_cum", aggfunc="last")
    pivot = pivot.reindex(index=session_order.index, columns=sorted(pivot.columns))
    if "session_complete" in work:
        complete = work.groupby("session_id")["session_complete"].all().reindex(pivot.index).fillna(False)
        pivot.loc[~complete] = np.nan
    # Num minuto sem barra, o acumulado é o da última barra anterior da mesma sessão.
    pivot = pivot.ffill(axis=1)
    weekday = session_order.dt.weekday

    def reference(frame):
        shifted = frame.shift(1)
        return (shifted.rolling(lookback, min_periods=min_sessions).median(),
                shifted.notna().rolling(lookback, min_periods=1).sum())

    if same_weekday:
        medians, counts = [], []
        for _, idx in weekday.groupby(weekday).groups.items():
            med, cnt = reference(pivot.loc[idx])
            medians.append(med)
            counts.append(cnt)
        median = pd.concat(medians).reindex(pivot.index)
        count = pd.concat(counts).reindex(pivot.index)
    else:
        median, count = reference(pivot)
    stacked_median = median.stack(future_stack=True).rename("_median")
    stacked_count = count.stack(future_stack=True).rename("reference_count")
    work = work.join(stacked_median, on=["session_id", "offset_minutes"]).join(stacked_count, on=["session_id", "offset_minutes"])
    work["rvol_value"] = work["_cum"] / work["_median"].where(work["_median"] > 0)
    rows = []
    suffix = ".same_weekday" if same_weekday else ""
    for row in work.itertuples(index=False):
        if pd.notna(row.rvol_value) and np.isfinite(row.rvol_value):
            meta = {"minute_of_session": int(row.offset_minutes)}
            rows.append((row.session_id, row.bar_start_utc, row.bar_end_utc, f"rvol{suffix}.{source}",
                         float(row.rvol_value), meta))
            rows.append((row.session_id, row.bar_start_utc, row.bar_end_utc, f"rvol{suffix}.reference_sessions",
                         float(row.reference_count), meta))
    frame = pd.DataFrame(rows, columns=["session_id", "observation_time", "available_at", "feature_id", "value",
                                        "metadata"])
    return frame, source


def _empty():
    return pd.DataFrame(columns=["session_id", "observation_time", "available_at", "feature_id", "value", "metadata"])
