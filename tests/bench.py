"""BD02: bancada compacta, determinística e rotulada como sintética.

Cada cenário devolve barras no formato ``_bars_derived`` usado pelos produtores e
pelo registry. Nenhum cenário é dado observado: ``origin`` e ``synthetic`` acompanham
toda massa. Os tamanhos cobrem o maior aquecimento do registry com margem, sem
guardar arquivo persistente; a impressão digital é o SHA-256 do CSV canônico.
"""
import hashlib

import numpy as np
import pandas as pd

ORIGIN = "synthetic:bd02"
RECEIPT_DELAY = pd.Timedelta(seconds=2)  # recebimento/computação sempre após o fechamento
COLUMNS = ("symbol", "tf", "session_id", "bar_start_utc", "bar_end_utc", "open", "high", "low", "close",
           "tick_volume", "real_volume", "real_volume_known", "revision", "computed_at_utc")


def _frame(close, *, symbol="BENCH", minutes=5, bars_per_session=24, start="2026-09-01 08:00", wick=0.5,
           tick_volume=None, real_volume=None, real_known=True, session_gap_hours=None):
    close = np.asarray(close, dtype=float)
    n = len(close)
    open_ = np.concatenate([[close[0]], close[:-1]])
    spread = np.maximum(np.abs(close) * 1e-4, 1e-9) * wick / 0.5
    high = np.maximum(open_, close) + spread
    low = np.minimum(open_, close) - spread
    begin0 = pd.Timestamp(start, tz="UTC")
    step = pd.Timedelta(minutes=minutes)
    gap = pd.Timedelta(hours=session_gap_hours) if session_gap_hours is not None else pd.Timedelta(days=1)
    starts = [begin0 + (index // bars_per_session) * gap + (index % bars_per_session) * step for index in range(n)]
    ticks = np.full(n, 30.0) if tick_volume is None else np.asarray(tick_volume, dtype=float)
    real = np.full(n, 10.0) if real_volume is None else np.asarray(real_volume, dtype=object)
    frame = pd.DataFrame({
        "symbol": symbol, "tf": f"M{minutes}", "session_id": [f"s{index // bars_per_session}" for index in range(n)],
        "bar_start_utc": starts, "bar_end_utc": [value + step for value in starts],
        "open": open_, "high": high, "low": low, "close": close, "tick_volume": ticks,
        "real_volume": real if real_known else None, "real_volume_known": int(real_known),
        "revision": 1})
    frame["computed_at_utc"] = frame["bar_end_utc"] + RECEIPT_DELAY
    frame.attrs.update(origin=ORIGIN, synthetic=True)
    return frame


def trend(n=240, step=0.05, start_price=100.0, **kw):
    return _frame(start_price + step * np.arange(n), **kw)


def lateral(n=240, amplitude=0.5, period=12, **kw):
    return _frame(100.0 + amplitude * np.sin(2 * np.pi * np.arange(n) / period), **kw)


def flat(n=240, price=100.0, **kw):
    """Preço constante: empates, denominadores zero e correlação indefinida."""
    return _frame(np.full(n, price), wick=0.0, **kw)


def random_walk(n=240, seed=7, **kw):
    rng = np.random.default_rng(seed)
    return _frame(100.0 + np.cumsum(rng.normal(0, 0.1, n)), **kw)


def with_gap(frame, start, length):
    """Remove barras do meio: lacuna explícita, sem preencher valores."""
    result = frame.drop(frame.index[start:start + length]).reset_index(drop=True)
    result.attrs.update(frame.attrs)
    return result


def with_duplicate(frame, index):
    result = pd.concat([frame, frame.iloc[[index]]]).sort_values("bar_start_utc", kind="stable").reset_index(drop=True)
    result.attrs.update(frame.attrs)
    return result


def out_of_order(frame, first, second):
    result = frame.copy()
    result.iloc[[first, second]] = result.iloc[[second, first]].to_numpy()
    result.attrs.update(frame.attrs)
    return result


def with_revision(frame, index, close, received_at="2026-10-23"):
    """Correção tardia: nova revisão da mesma barra, com instante de recebimento próprio."""
    revised = frame.copy()
    revised.loc[index, ["close", "revision", "computed_at_utc"]] = [close, 2, pd.Timestamp(received_at, tz="UTC")]
    revised.loc[index, "high"] = max(revised.loc[index, "high"], close)
    revised.loc[index, "low"] = min(revised.loc[index, "low"], close)
    revised.attrs.update(frame.attrs)
    return revised


def with_missing(frame, index, columns=("close",)):
    result = frame.copy()
    result.loc[index, list(columns)] = np.nan
    result.attrs.update(frame.attrs)
    return result


def volume_states(n=48):
    """Zero conhecido, volume ausente e volume real desconhecido são estados distintos."""
    zero = _frame(np.linspace(100, 101, n), real_volume=np.zeros(n))
    absent = _frame(np.linspace(100, 101, n), real_known=False)
    partial = _frame(np.linspace(100, 101, n))
    partial.loc[n // 2, ["real_volume", "real_volume_known"]] = [None, 0]
    return {"zero_known": zero, "absent": absent, "partial_unknown": partial}


def price_scales(n=240):
    """Mesmo formato em escalas distintas (FX 1e0, índice 1e5, cripto pequena 1e-4)."""
    base = np.sin(np.arange(n) / 9.0) * 0.01 + 1.0
    return {scale: _frame(base * scale, symbol=f"SCALE_{index}") for index, scale in enumerate((1e-4, 1.0, 1e5))}


def two_bases_same_symbol(n=240):
    """Duas bases com o MESMO símbolo e dados diferentes; a identidade vem da base, não do texto."""
    a = trend(n, symbol="EURUSD")
    b = lateral(n, symbol="EURUSD")
    a.attrs.update(base_id="bench-a", provider="synthetic-feed-a")
    b.attrs.update(base_id="bench-b", provider="synthetic-feed-b")
    return a, b


def overnight_sessions(n=144):
    """Sessões que atravessam a meia-noite UTC (início 22:00, 6 h por sessão)."""
    return _frame(np.linspace(100, 102, n), bars_per_session=72, start="2026-09-01 22:00")


def dst_transition(zone="Europe/Athens", start_utc="2026-10-23 00:00", hours=96):
    """Barras H1 em fuso local real atravessando o fim do horário de verão (25/10/2026 04:00 → 03:00).

    O offset muda de +3 h para +2 h e a hora local 03:00–04:00 se repete (fold): duas barras UTC
    distintas têm o mesmo relógio local. ``bar_start_utc`` segue contínuo; ``utc_offset_hours``
    e ``bar_start_local`` carregam a resposta esperada para o teste de calendário (BD05).
    """
    frame = _frame(np.linspace(1.10, 1.12, hours), minutes=60, bars_per_session=hours, start=start_utc)
    local = frame["bar_start_utc"].dt.tz_convert(zone)
    frame["bar_start_local"] = local.dt.tz_localize(None)
    frame["utc_offset_hours"] = [value.utcoffset().total_seconds() / 3600 for value in local]
    frame.attrs.update(local_zone=zone)
    return frame


def invalid_computed_before_close(n=24):
    """Massa deliberadamente IMPOSSÍVEL: cálculo registrado antes do fechamento da barra.

    Fica fora de ``scenarios()``; serve só para exigir rejeição/qualidade de quem a receber.
    """
    frame = trend(n)
    frame["computed_at_utc"] = frame["bar_start_utc"]
    frame.attrs.update(valid=False, defect="computed_before_close")
    return frame


def temporal_violations(frame):
    """Índices em que o fato foi computado/recebido antes de existir (fechamento da barra)."""
    return frame.index[frame["computed_at_utc"] < frame["bar_end_utc"]].tolist()


def scenarios():
    gap_base = trend()
    out = {"trend": trend(), "lateral": lateral(), "flat": flat(), "random_walk": random_walk(),
           "gap": with_gap(gap_base, 100, 5), "duplicate": with_duplicate(gap_base, 50),
           "out_of_order": out_of_order(gap_base, 60, 61), "revision": with_revision(gap_base, 120, 200.0),
           "missing_close": with_missing(gap_base, 80), "overnight": overnight_sessions(), "dst": dst_transition()}
    out.update({f"volume_{key}": value for key, value in volume_states().items()})
    out.update({f"scale_{key:g}": value for key, value in price_scales().items()})
    a, b = two_bases_same_symbol()
    out.update(base_a=a, base_b=b)
    return out


def fingerprint(frame):
    payload = frame.loc[:, list(COLUMNS)].to_csv(index=False, float_format="%.12g", lineterminator="\n")
    return hashlib.sha256(payload.encode()).hexdigest(), len(payload.encode())
