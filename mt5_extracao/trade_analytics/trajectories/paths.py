"""Trajetória de preço observada, distinta de PnL e de uma sequência intrabarra inferida."""
import math
from numbers import Integral

import numpy as np
import pandas as pd

from .clocks import Clock, bar_intervals, complete_bars, trading_minutes

VERSION = 1
UTC = Clock("utc")


def _missing(value):
    return value is None or bool(pd.isna(value))


def _number(value):
    if isinstance(value, (bool, np.bool_)) or _missing(value):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _scale(unit, point, risk_distance):
    if unit == "price":
        return 1.0
    denominator = _number(point if unit == "points" else risk_distance)
    if unit not in ("points", "R") or denominator is None or denominator <= 0:
        raise ValueError("Unidade exige point ou risco inicial positivo declarado; confira a especificação.")
    scale = 1.0 / denominator
    if not math.isfinite(scale):
        raise ValueError("Unidade fora da precisão suportada; confira point ou risco inicial.")
    return scale


def _window(left, right, entry):
    return {"start_utc": left.isoformat(), "end_utc": right.isoformat(),
            "age_start_min": (left - entry).total_seconds() / 60,
            "age_end_min": (right - entry).total_seconds() / 60}


def _point_window(point):
    return {key: point[key] for key in ("start_utc", "end_utc", "age_start_min", "age_end_min",
                                        "source_sequence", "sequence_basis", "basis") if key in point}


def _quote_points(quotes, entry, finish, price, direction, scale, limitations, *, inclusive_end=True):
    required = {"event_time_utc", "bid", "ask"}
    if not required <= set(quotes.columns):
        limitations.append("missing_bid_ask_columns")
        return []
    data = quotes.copy(deep=True)
    data["_time"] = [UTC.timestamp(t) for t in data.event_time_utc]
    before_end = data._time <= finish if inclusive_end else data._time < finish
    data = data[(data._time >= entry) & before_end].sort_values("_time", kind="stable")
    sequenced = False
    if "seq" in data:
        orders = [_number(value) for value in data.seq]
        sequenced = all(v is not None and v >= 0 and v.is_integer() for v in orders)
        if sequenced:
            data["_order"] = [int(v) for v in orders]
            if data.duplicated(["_time", "_order"]).any():
                raise ValueError("Quotes conflitantes na mesma identidade temporal; concilie a sequência da fonte.")
            data = data.sort_values(["_time", "_order"], kind="stable")
        else:
            limitations.append("invalid_provider_sequence")
    if data._time.duplicated().any() and not sequenced:
        limitations.append("same_timestamp_order_unknown")
    points = []
    for row in data.itertuples(index=False, name=None):
        values = dict(zip(data.columns, row))
        bid, ask = _number(values["bid"]), _number(values["ask"])
        if bid is None or ask is None or ask < bid:
            limitations.append("invalid_or_missing_quote")
            continue
        value = direction * ((bid if direction == 1 else ask) - price) * scale
        if not math.isfinite(value):
            limitations.append("invalid_or_missing_quote")
            continue
        points.append({**_window(values["_time"], values["_time"], entry), "low": value, "high": value,
                       "close": value, "available_at_utc": values["_time"].isoformat(), "basis": "quote",
                       "sequence_basis": "provider_order" if sequenced else "timestamp_only"})
        if sequenced:
            points[-1]["source_sequence"] = values["_order"]
    return points


def _bar_points(bars, intervals, entry, finish, price, direction, scale, limitations, *, boundary_open=False):
    # Barras MT5 são bid. Não criar ask intrabarra usando o menor spread da barra.
    columns = ("open", "high", "low", "close") if direction == 1 else (
        "ask_open", "ask_high", "ask_low", "ask_close")
    if not set(columns) <= set(bars.columns):
        limitations.append("missing_bid_bars" if direction == 1 else "missing_ask_bars")
        return []
    # Só visitar barras que possam contribuir para esta posição. A validação
    # dos intervalos continua abrangendo a fonte completa, antes deste recorte.
    selected = ((intervals.end_utc > entry) & (intervals.start_utc <= finish)).to_numpy()
    bars, intervals = bars.iloc[selected], intervals.iloc[selected]
    # intervals e barras usam a mesma ordenação estável de abertura.
    points = []
    for (_, row), interval in zip(bars.iterrows(), intervals.itertuples(index=False)):
        left, right = interval
        op = _number(row[columns[0]])
        if op is not None and left >= entry and (left < finish or boundary_open and left == finish):
            value = direction * (op - price) * scale
            if math.isfinite(value):
                points.append({**_window(left, left, entry), "low": value, "high": value, "close": value,
                               "available_at_utc": left.isoformat(), "basis": "bar_open"})
        if left < entry or right > finish:
            if left < finish and right > entry:
                limitations.append("partial_boundary_bar_excluded")
            continue
        values = [_number(row[c]) for c in columns]
        if any(v is None for v in values):
            limitations.append("invalid_or_missing_bar")
            continue
        op, hi, lo, close = values
        if hi < max(op, close) or lo > min(op, close):
            limitations.append("invalid_or_missing_bar")
            continue
        extrema = [direction * (v - price) * scale for v in (lo, hi)]
        value = direction * (close - price) * scale
        if not all(math.isfinite(v) for v in [*extrema, value]):
            limitations.append("invalid_or_missing_bar")
            continue
        points.append({**_window(left, right, entry), "low": min(extrema), "high": max(extrema),
                       "close": value,
                       "available_at_utc": right.isoformat(), "basis": "ohlc_interval"})
    if points:
        limitations.append("intrabar_order_unknown")
    return points


def reconstruct(trade, *, clock, quotes=None, bars=None, as_of=None, unit="price", point=None,
                risk_distance=None, thresholds=(), sessions=None, session_coverage=None, exit_window=None,
                _bar_cache=None):
    """Desfechos de uma linha do ledger, sem consultar a base nem alterar a operação.

    Quotes usam event_time_utc/bid/ask; barras usam time no relógio declarado e o TF
    da operação. Só uma resolução por chamada. Pontos são observados; cobertura
    integral e primeiro cruzamento absoluto não são presumidos. OHLC conserva o
    intervalo dos extremos e sua disponibilidade no fechamento da barra.
    """
    if quotes is not None and bars is not None:
        raise ValueError("Escolha quotes ou barras para a trajetória; não misture resoluções implicitamente.")
    trade = dict(trade)
    ident = {k: trade.get(k) for k in ("run_id", "seq", "source", "symbol", "timeframe", "direction")}
    if (not isinstance(ident["run_id"], str) or not ident["run_id"] or
            not isinstance(ident["source"], str) or not ident["source"] or
            isinstance(ident["seq"], (bool, np.bool_)) or not isinstance(ident["seq"], Integral) or ident["seq"] < 0):
        raise ValueError("Trajetória exige identidade run_id/seq/source do livro de operações.")
    ident = {key: None if _missing(value) else value.item() if isinstance(value, np.generic) else value
             for key, value in ident.items()}
    scale = _scale(unit, point, risk_distance)
    levels = [_number(level) for level in thresholds]
    if any(level is None or level == 0 for level in levels) or len(set(levels)) != len(levels):
        raise ValueError("Limiares devem ser finitos, distintos e diferentes de zero, na unidade declarada.")
    result = {"schema_version": VERSION, "identity": ident, "unit": unit, "status": "unavailable",
              "unit_reference": {"basis": "price_difference" if unit == "price" else "caller_declared",
                                 "point": _number(point) if unit == "points" else None,
                                 "initial_risk_distance": _number(risk_distance) if unit == "R" else None},
              "coverage_state": "observed_only", "limitations": [], "points": [], "thresholds": [],
              "age_min": None, "complete_bars": None, "trading_min": None, "right_censored": None,
              "age_lower_min": None, "age_upper_min": None,
              "mfe_observed": None, "mae_observed": None, "mfe_time": None, "mae_time": None,
              "clock": {"basis": clock.basis, "broker_utc_offset_hours": clock.broker_utc_offset_hours},
              "timestamp_basis": "ledger_timestamp", "outcome_role": "post_entry_observation"}
    limits = result["limitations"]
    if _missing(trade.get("entry_time")):
        limits.append("missing_entry_time")
        return result
    entry = clock.timestamp(trade["entry_time"])
    exited = None if _missing(trade.get("exit_time")) else clock.timestamp(trade["exit_time"])
    if exited is not None and exited < entry:
        raise ValueError("Fim anterior à entrada; confira os horários da operação.")
    window = None
    if exit_window is not None:
        left, right = (clock.timestamp(exit_window[key]) for key in ("start_time", "end_time"))
        available = clock.timestamp(exit_window["available_at"])
        if left < entry or right < left or available < right:
            raise ValueError("Janela de execução incoerente; confira a origem do encerramento.")
        window = (left, right, available)
        exited = right
    if as_of is None and exited is None:
        limits.append("open_trade_requires_as_of")
        return result
    cutoff = clock.timestamp(as_of) if as_of is not None else exited
    if cutoff < entry:
        limits.append("entry_after_as_of")
        return result
    if window is not None and window[2] > cutoff:
        raise ValueError("Encerramento indisponível nesse corte; consulte apenas os estados já conhecidos.")
    censored = exited is None or exited > cutoff
    finish = cutoff if censored else exited
    prefix_end = window[0] if window is not None else finish
    lower = (prefix_end - entry).total_seconds() / 60
    upper = (finish - entry).total_seconds() / 60
    negotiated_lower = trading_minutes(entry, prefix_end, sessions, UTC, coverage=session_coverage)
    negotiated_upper = trading_minutes(entry, finish, sessions, UTC, coverage=session_coverage)
    result.update(age_min=lower if prefix_end == finish else None, age_lower_min=lower, age_upper_min=upper,
                  right_censored=censored,
                  start_utc=entry.isoformat(), end_utc=finish.isoformat(),
                  trading_min=negotiated_lower if negotiated_lower == negotiated_upper else None,
                  trading_lower_min=negotiated_lower, trading_upper_min=negotiated_upper)
    if window is not None:
        result["timestamp_basis"] = "provided_execution_window"
        result["exit_window"] = _window(window[0], window[1], entry)
        if prefix_end != finish:
            limits.append("uncertain_exit_interval_market_prices_excluded")
    price, direction = _number(trade.get("entry_price")), trade.get("direction")
    if price is None or _missing(direction) or direction not in (1, -1) or isinstance(direction, (bool, np.bool_)):
        limits.append("missing_entry_price_or_direction")
        return result
    direction = int(direction)
    if quotes is not None:
        points = _quote_points(quotes, entry, prefix_end, price, direction, scale, limits,
                               inclusive_end=censored)
    elif bars is not None:
        # Cache privado ao serviço e à consulta, nunca global nem persistente.
        # Guardar a referência original também impede reutilização de seu id.
        key = (id(bars), trade.get("timeframe"), clock)
        prepared = _bar_cache.get(key) if _bar_cache is not None else None
        if prepared is None:
            data = bars.copy(deep=True)
            data["_sort"] = [clock.timestamp(t) for t in data.time]
            data = data.sort_values("_sort", kind="stable").reset_index(drop=True)
            intervals = bar_intervals(data.time, trade.get("timeframe"), clock)
            if _bar_cache is not None:
                _bar_cache[key] = (bars, data, intervals)
        else:
            _, data, intervals = prepared
        result["complete_bars"] = complete_bars(entry, prefix_end, intervals, UTC)
        points = _bar_points(data, intervals, entry, prefix_end, price, direction, scale, limits,
                             boundary_open=exit_window is not None and exit_window.get("phase") == "intrabar")
    else:
        points = []
        limits.append("missing_path_source")
    market_points = len(points)
    exit_price = _number(exit_window.get("price", trade.get("exit_price")) if exit_window is not None else trade.get("exit_price"))
    if not censored and exit_price is not None:
        value = direction * (exit_price - price) * scale
        if math.isfinite(value):
            points.append({**_window(prefix_end, finish, entry), "low": value, "high": value,
                           "close": value, "available_at_utc": (window[2] if window is not None else finish).isoformat(),
                           "basis": "producer_execution_fill" if window is not None else "ledger_execution_endpoint"})
        else:
            limits.append("invalid_execution_endpoint")
    if not market_points and points:
        result["coverage_state"] = "execution_endpoints_only"
        limits.append("execution_endpoints_do_not_reconstruct_market_path")
    result["points"] = points
    if points:
        result["status"] = "observed"
        best = max(points, key=lambda p: p["high"])
        worst = min(points, key=lambda p: p["low"])
        result.update(mfe_observed=max(0.0, best["high"]), mae_observed=max(0.0, -worst["low"]))
        # Zero é o referencial da entrada, não um preço observado de liquidação.
        if best["high"] > 0:
            result["mfe_time"] = _point_window(best)
        if worst["low"] < 0:
            result["mae_time"] = _point_window(worst)
    for level in levels:
        hit = next((p for p in points if (p["high"] >= level if level > 0 else p["low"] <= level)), None)
        result["thresholds"].append({"level": level, "state": "first_observed_hit" if hit else "unknown",
                                     "window": None if hit is None else _point_window(hit),
                                     "available_at_utc": None if hit is None else hit["available_at_utc"],
                                     "first_absolute_crossing_known": False})
    limits.append("continuous_path_coverage_not_proven")
    result["limitations"] = list(dict.fromkeys(limits))
    return result
