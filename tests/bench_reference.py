"""BD02: referências independentes em Python puro (sem pandas/numpy nem strategies.indicators).

Cada função implementa a definição escrita, laço a laço, para servir de oráculo.
``None`` é ausência; nenhuma função converte ausência em zero.
"""
import math


def returns(close, horizon):
    out = [None] * len(close)
    for index in range(horizon, len(close)):
        previous = close[index - horizon]
        if previous is not None and close[index] is not None and previous > 0:
            out[index] = close[index] / previous - 1.0
    return out


def true_range(high, low, close):
    out = []
    for index in range(len(close)):
        candidates = [high[index] - low[index]]
        if index:
            candidates += [abs(high[index] - close[index - 1]), abs(low[index] - close[index - 1])]
        out.append(max(candidates))
    return out


def wilder(values, period, seed):
    """Suavização de Wilder: ``seed='first'`` parte da 1ª observação; ``'sma'`` da média das n primeiras."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    if seed == "sma":
        state = sum(values[:period]) / period
        out[period - 1] = state
        start = period
    else:
        state = values[0]
        for index in range(1, period):
            state += (values[index] - state) / period
        out[period - 1] = state
        start = period
    for index in range(start, len(values)):
        state += (values[index] - state) / period
        out[index] = state
    return out


def rsi(close, period, seed):
    deltas = [close[index] - close[index - 1] for index in range(1, len(close))]
    gains = wilder([max(value, 0.0) for value in deltas], period, seed)
    losses = wilder([max(-value, 0.0) for value in deltas], period, seed)
    out = [None]
    for gain, loss in zip(gains, losses):
        if gain is None:
            out.append(None)
        elif gain == 0 and loss == 0:
            out.append(50.0)
        elif loss == 0:
            out.append(100.0)
        else:
            out.append(100.0 - 100.0 / (1.0 + gain / loss))
    return out


def efficiency_ratio(close, period):
    out = [None] * len(close)
    for index in range(period, len(close)):
        path = sum(abs(close[k] - close[k - 1]) for k in range(index - period + 1, index + 1))
        out[index] = 0.0 if path == 0 else abs(close[index] - close[index - period]) / path
    return out


def bollinger_width(close, period, deviations):
    out = [None] * len(close)
    for index in range(period - 1, len(close)):
        window = close[index - period + 1:index + 1]
        mean = sum(window) / period
        std = math.sqrt(sum((value - mean) ** 2 for value in window) / period)
        out[index] = None if mean <= 0 else (2 * deviations * std) / mean
    return out


def donchian_upper(high, period, shift):
    out = [None] * len(high)
    for index in range(period - 1 + shift, len(high)):
        end = index - shift
        out[index] = max(high[end - period + 1:end + 1])
    return out
