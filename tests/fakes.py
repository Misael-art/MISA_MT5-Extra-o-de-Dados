"""
MT5 falso para testes (sem terminal MetaTrader 5).

FakeMT5 imita a API do módulo MetaTrader5 usada pelo projeto e gera barras
sintéticas determinísticas: pregão das 09:00 às 17:59 em dias úteis.
"""
import calendar
import collections
import datetime as _dt
import types

import numpy as np

# Valores oficiais das constantes de timeframe do MetaTrader5
TIMEFRAMES = {
    "TIMEFRAME_M1": 1, "TIMEFRAME_M2": 2, "TIMEFRAME_M3": 3, "TIMEFRAME_M4": 4,
    "TIMEFRAME_M5": 5, "TIMEFRAME_M6": 6, "TIMEFRAME_M10": 10, "TIMEFRAME_M12": 12,
    "TIMEFRAME_M15": 15, "TIMEFRAME_M20": 20, "TIMEFRAME_M30": 30,
    "TIMEFRAME_H1": 16385, "TIMEFRAME_H2": 16386, "TIMEFRAME_H3": 16387, "TIMEFRAME_H4": 16388,
    "TIMEFRAME_H6": 16390, "TIMEFRAME_H8": 16392, "TIMEFRAME_H12": 16396,
    "TIMEFRAME_D1": 16408, "TIMEFRAME_W1": 32769, "TIMEFRAME_MN1": 49153,
}
MINUTES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 10: 10, 12: 12, 15: 15, 20: 20, 30: 30,
           16385: 60, 16386: 120, 16387: 180, 16388: 240, 16390: 360, 16392: 480, 16396: 720,
           16408: 1440, 32769: 10080, 49153: 43200}

TICKS_DTYPE = [("time", "<i8"), ("bid", "<f8"), ("ask", "<f8"), ("last", "<f8"), ("volume", "<u8"),
               ("time_msc", "<i8"), ("flags", "<u4"), ("volume_real", "<f8")]

RATES_DTYPE = [("time", "<i8"), ("open", "<f8"), ("high", "<f8"), ("low", "<f8"), ("close", "<f8"),
               ("tick_volume", "<u8"), ("spread", "<i4"), ("real_volume", "<u8")]

SymbolInfo = collections.namedtuple(
    "SymbolInfo", "name spread visible path description point digits trade_tick_size trade_tick_value "
                  "volume_min volume_step volume_max trade_contract_size currency_profit",
    defaults=(5.0, 0, 5.0, 0.2, 1.0, 1.0, 1000.0, 0.2, "BRL"))  # padrão: contrato tipo WIN


def _epoch(v):
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return calendar.timegm(v.timetuple())
    return int(v)


def synthetic_bar_times(start_epoch, end_epoch, minutes):
    """Horários (epoch) de barras entre start e end (inclusive) no pregão 09:00-18:00 de dias úteis."""
    step = minutes * 60
    t = start_epoch - (start_epoch % step) if minutes < 1440 else start_epoch - (start_epoch % 86400)
    if t < start_epoch:
        t += step if minutes < 1440 else 86400
    out = []
    while t <= end_epoch:
        d = _dt.datetime.fromtimestamp(t, _dt.timezone.utc)
        if d.weekday() < 5 and (minutes >= 1440 or 9 <= d.hour < 18):
            out.append(t)
        t += step if minutes < 1440 else 86400
    return out


class FakeMT5(types.SimpleNamespace):
    """Objeto com a mesma API do módulo MetaTrader5 (o necessário para os testes)."""

    def __init__(self, symbols=("WIN$N", "WDO$N", "PETR4"), fail_ranges=None):
        super().__init__(**TIMEFRAMES)
        self.__version__ = "5.0.fake"
        self.COPY_TICKS_ALL = -1
        self.symbols = list(symbols)
        self.calls = []                     # (nome, args) de cada chamada
        self.fail_ranges = list(fail_ranges or [])  # [(inicio_epoch, fim_epoch)] que devolvem None
        self._last_error = (1, "Success")

    # --- conexão ---------------------------------------------------------------
    def initialize(self, *args, **kwargs):
        self.calls.append(("initialize", args, kwargs))
        return True

    def shutdown(self):
        return True

    def last_error(self):
        return self._last_error

    def terminal_info(self):
        return types.SimpleNamespace(connected=True, name="FakeMT5", path="C:\\Fake")

    def account_info(self):
        return types.SimpleNamespace(login=1, server="Fake-Demo", balance=0.0)

    def version(self):
        return (500, 5000, "01 Jan 2026")

    # --- símbolos -------------------------------------------------------------
    def symbol_info(self, symbol):
        if symbol not in self.symbols:
            return None
        return SymbolInfo(symbol, 5, True, "Fake\\" + symbol, symbol)

    def symbol_select(self, symbol, enable=True):
        return symbol in self.symbols

    def symbols_get(self, group="*"):
        return tuple(self.symbol_info(s) for s in self.symbols)

    def symbols_total(self):
        return len(self.symbols)

    # --- cotações -------------------------------------------------------------
    @staticmethod
    def price_at(t):
        return 100.0 + (t // 60) % 50  # determinístico, varia por minuto

    def _rates(self, times):
        rows = []
        for t in times:
            p = self.price_at(t)
            rows.append((t, p, p + 1.0, p - 1.0, p + 0.5, 10 + (t // 60) % 7, 1 + (t // 60) % 3, 100))
        return np.array(rows, dtype=RATES_DTYPE)

    def copy_rates_range(self, symbol, timeframe, date_from, date_to):
        a, b = _epoch(date_from), _epoch(date_to)
        self.calls.append(("copy_rates_range", (symbol, timeframe, a, b), {}))
        if symbol not in self.symbols:
            return None
        for fa, fb in self.fail_ranges:
            if a < fb and b > fa:
                self._last_error = (-1, "Terminal: falha simulada")
                return None
        self._last_error = (1, "Success")
        return self._rates(synthetic_bar_times(a, b, MINUTES[timeframe]))

    def copy_rates_from(self, symbol, timeframe, date_from, count):
        self.calls.append(("copy_rates_from", (symbol, timeframe, _epoch(date_from), count), {}))
        end = _epoch(date_from)
        times = synthetic_bar_times(end - 60 * 60 * 24 * 30, end, MINUTES[timeframe])
        return self._rates(times[-count:])

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        self.calls.append(("copy_rates_from_pos", (symbol, timeframe, start_pos, count), {}))
        end = _epoch(_dt.datetime(2024, 6, 28, 17, 59))
        times = synthetic_bar_times(end - 60 * 60 * 24 * 30, end, MINUTES[timeframe])
        return self._rates(times[-count - start_pos:len(times) - start_pos or None])

    def copy_ticks_range(self, symbol, date_from, date_to, flags):
        """3 ticks por minuto de pregão; os dois primeiros no mesmo milissegundo (acontece no MT5 real)."""
        a, b = _epoch(date_from), _epoch(date_to)
        self.calls.append(("copy_ticks_range", (symbol, a, b, flags), {}))
        if symbol not in self.symbols:
            return None
        for fa, fb in self.fail_ranges:
            if a < fb and b > fa:
                self._last_error = (-1, "Terminal: falha simulada")
                return None
        self._last_error = (1, "Success")
        rows = []
        for t in synthetic_bar_times(a, b, 1):
            p = self.price_at(t)
            for k, offset_ms in enumerate((0, 0, 500)):
                rows.append((t, p, p + 0.5, p + 0.25 * k, 1 + k, t * 1000 + offset_ms, 6 if k else 2, float(1 + k)))
        return np.array(rows, dtype=TICKS_DTYPE)

    def calls_named(self, name):
        return [c for c in self.calls if c[0] == name]
