"""
Enriquecimento (T16.3): as colunas de contexto de cada operação do livro, para o Laboratório cruzar com o resultado.

    features = enrich(db, config, trades)     # trades = ledger.load(...); uma linha por operação, mesma ordem

Regra *point-in-time* (a mesma da Fase 13): toda característica de mercado usa só barras com `time < entry_time`. A
barra da entrada ainda não tinha fechado quando a decisão foi tomada. Volatilidade "baixa/média/alta" é o percentil do
ATR% da entrada entre as `VOL_LOOKBACK` barras anteriores, não um corte calculado com o histórico inteiro (que veria o
futuro). MAE e MFE usam as barras entre a entrada e a saída: são resultado, não contexto.

Operação `parcial` (sem horário de entrada) recebe só as colunas de calendário e horário, pela saída
(`time_ref = "saida"`); as de mercado ficam vazias.
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.strategies import indicators

log = logging.getLogger(__name__)

SECTION = "TRADE_ANALYTICS"
FEATURE_VERSION = 2            # T16.3b: barras fechadas, surpresa disponível e proveniência explícita
VOL_LOOKBACK = 250             # barras para o percentil de volatilidade
VOLUME_LOOKBACK = 20           # barras para o percentil de volume
VOL_LABELS = ("baixa", "média", "alta")

COLUMNS = ("run_id", "seq", "time_ref", "day_of_week", "day_of_month", "week_of_month", "month", "is_month_end",
           "hour", "time_bucket", "minutes_from_open", "event_impact", "minutes_to_event", "event_name",
           "event_surprise", "entry_volume", "volume_source", "volume_rank", "atr", "atr_pct", "bb_width",
           "prior_return_pct", "volatility_rank", "volatility_bucket", "win_flag", "duration_min", "mae", "mfe",
           "mae_mfe_resolution", "feature_version")


def _setting(config, key, default):
    try:
        return type(default)(config.get(SECTION, key, fallback=str(default)) or default)
    except (AttributeError, ValueError):
        return default


def calendar_columns(times: pd.Series, bucket_min: int = 30) -> pd.DataFrame:
    """Dia da semana (0 = segunda), do mês, semana do mês (1–5), mês, fim de mês (últimos 3 dias), hora e faixa."""
    t = pd.to_datetime(times)
    minutes = t.dt.hour * 60 + t.dt.minute
    bucket = (minutes // bucket_min) * bucket_min
    return pd.DataFrame({
        "day_of_week": t.dt.dayofweek, "day_of_month": t.dt.day, "week_of_month": (t.dt.day - 1) // 7 + 1,
        "month": t.dt.month, "is_month_end": (t.dt.days_in_month - t.dt.day) < 3, "hour": t.dt.hour,
        "time_bucket": [f"{int(b // 60):02d}:{int(b % 60):02d}" if pd.notna(b) else None for b in bucket],
    }, index=times.index)


def _bars(db, symbol: str, timeframe: str, cache: Dict) -> pd.DataFrame:
    key = (symbol, timeframe)
    if key not in cache:
        tf = timeframes.parse(timeframe) if timeframe else None
        frame = db.load_ohlcv(symbol, tf.label) if tf is not None else pd.DataFrame()
        if not frame.empty:
            frame = frame.sort_values("time").reset_index(drop=True)
            frame["time"] = pd.to_datetime(frame["time"])
            if tf.name == "MN1":
                frame["bar_end"] = frame["time"].map(lambda value: value + pd.DateOffset(months=1))
            else:
                frame["bar_end"] = frame["time"] + pd.Timedelta(minutes=tf.minutes)
            frame["atr"] = indicators.atr(frame, 14)
            frame["atr_pct"] = frame["atr"] / frame["close"]
            bands = indicators.bollinger(frame["close"], 20, 2.0)
            frame["bb_width"] = (bands["upper"] - bands["lower"]) / bands["mid"]
            real = pd.to_numeric(frame["real_volume"], errors="coerce") if "real_volume" in frame else None
            ticks = pd.to_numeric(frame["tick_volume"], errors="coerce") if "tick_volume" in frame else None
            if real is not None and real.notna().all():
                frame["vol"], frame["volume_source"] = real, "real_volume"
            elif ticks is not None:
                frame["vol"], frame["volume_source"] = ticks, "tick_volume"
            else:
                frame["vol"], frame["volume_source"] = np.nan, None
            frame["date"] = frame["time"].dt.normalize()
            frame["day_open"] = frame.groupby("date")["time"].transform("min")
        cache[key] = frame
    return cache[key]


def _percentile(window: np.ndarray, value: float) -> Optional[float]:
    window = window[~np.isnan(window)]
    if len(window) < 5 or np.isnan(value):
        return None
    return float((window <= value).mean())


def _events_index(db, config, symbols):
    """Eventos unidos por ativo (vazio se não houver calendário)."""
    from mt5_extracao.trade_analytics import calendar as cal
    events = cal.merged(db)
    if events.empty:
        return {}
    mapping = cal.parse_mapping(config.get(SECTION, "event_symbols", fallback="") if config is not None else "")
    out = {}
    for symbol in symbols:
        spec = db.get_symbol_spec(symbol) if hasattr(db, "get_symbol_spec") else None
        out[symbol] = cal.events_for(events, symbol, (spec or {}).get("currency_profit"), mapping) \
            .sort_values("event_time").reset_index(drop=True)
    return out


def enrich(db, config, trades: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por operação (mesmo índice de `trades`) com COLUMNS."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=COLUMNS)
    bucket_min = max(1, _setting(config, "time_bucket_min", 30))
    vol_period = max(1, _setting(config, "volatility_period", 14))
    before = _setting(config, "event_window_before_min", 30)
    after = _setting(config, "event_window_after_min", 30)
    entry = pd.to_datetime(trades["entry_time"])
    exit_ = pd.to_datetime(trades["exit_time"])
    ref = entry.fillna(exit_)
    out = calendar_columns(ref, bucket_min)
    out.insert(0, "time_ref", np.where(entry.notna(), "entrada", "saida"))
    out.insert(0, "seq", trades["seq"].values)
    out.insert(0, "run_id", trades["run_id"].values)
    for col in COLUMNS:
        if col not in out:
            out[col] = None
    out["win_flag"] = (trades["pnl"].astype(float) > 0).astype(int)
    out["duration_min"] = ((exit_ - entry).dt.total_seconds() / 60).where(entry.notna())
    out["feature_version"] = FEATURE_VERSION
    try:
        events = _events_index(db, config, trades["symbol"].dropna().unique())
    except Exception as e:
        log.warning(f"Laboratório: calendário econômico não lido ({e}); operações sem a dimensão de notícias.")
        events = {}
    cache: Dict = {}
    for i, row in trades.iterrows():
        when = ref.loc[i]
        evs = events.get(row["symbol"])
        if evs is not None and not evs.empty and pd.notna(when):
            delta = (evs["event_time"] - when).dt.total_seconds() / 60       # > 0: evento depois da entrada
            near = evs[(delta >= -before) & (delta <= after)]
            if not near.empty:
                top = near.sort_values(["impact", "event_time"], ascending=[False, True]).iloc[0]
                out.at[i, "event_impact"] = int(top["impact"])
                out.at[i, "minutes_to_event"] = float((top["event_time"] - when).total_seconds() / 60)
                out.at[i, "event_name"] = top["name"]
                available_at = top.get("surprise_available_at")
                if pd.notna(top["surprise"]) and pd.notna(available_at) and pd.Timestamp(available_at) <= when:
                    out.at[i, "event_surprise"] = top["surprise"]
            else:
                out.at[i, "event_impact"] = 0
        if pd.isna(entry.loc[i]) or not row.get("symbol"):
            continue
        bars = _bars(db, row["symbol"], row.get("timeframe"), cache)
        if bars.empty:
            continue
        k = int(bars["bar_end"].searchsorted(entry.loc[i], side="right")) - 1
        if k < 0:
            continue
        bar = bars.iloc[k]
        out.at[i, "minutes_from_open"] = float((entry.loc[i] - bar["day_open"]).total_seconds() / 60) \
            if bar["date"] == entry.loc[i].normalize() else None
        out.at[i, "entry_volume"] = float(bar["vol"])
        out.at[i, "volume_source"] = bar["volume_source"]
        out.at[i, "volume_rank"] = _percentile(bars["vol"].values[max(0, k - VOLUME_LOOKBACK + 1):k + 1].astype(float),
                                               float(bar["vol"]))
        for name in ("atr", "atr_pct", "bb_width"):
            value = bar[name]
            out.at[i, name] = None if pd.isna(value) else float(value)
        if k >= vol_period:
            out.at[i, "prior_return_pct"] = float(bar["close"] / bars["close"].iloc[k - vol_period] - 1)
        rank = _percentile(bars["atr_pct"].values[max(0, k - VOL_LOOKBACK + 1):k + 1].astype(float),
                           float(bar["atr_pct"]) if pd.notna(bar["atr_pct"]) else np.nan)
        out.at[i, "volatility_rank"] = rank
        out.at[i, "volatility_bucket"] = None if rank is None else VOL_LABELS[min(2, int(rank * 3))]
        if pd.notna(exit_.loc[i]) and pd.notna(row.get("entry_price")) and row.get("direction") in (1, -1):
            path = bars[(bars["time"] < exit_.loc[i]) & (bars["bar_end"] > entry.loc[i])]
            if not path.empty:
                d, price = int(row["direction"]), float(row["entry_price"])
                favorable = (path["high"].max() - price) if d == 1 else (price - path["low"].min())
                adverse = (price - path["low"].min()) if d == 1 else (path["high"].max() - price)
                out.at[i, "mfe"], out.at[i, "mae"] = max(0.0, float(favorable)), max(0.0, float(adverse))
                tf = timeframes.parse(row.get("timeframe"))
                out.at[i, "mae_mfe_resolution"] = f"{tf.name} OHLC (intrabar aproximado)" if tf else None
    return out[list(COLUMNS)]
