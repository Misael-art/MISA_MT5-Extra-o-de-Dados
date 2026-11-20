"""BD03–05/T2.6: datas enviadas ao MT5 seguem o relógio do servidor, nunca o fuso do computador.

O MT5 grava barras/ticks no relógio do servidor como se fosse UTC (o mesmo número que volta em `time`). O módulo
nativo do Windows lia datetime sem fuso no fuso local e deslocava a janela (GMT-3: barras de 03:00 a 02:55).
"""
import calendar
import datetime as dt
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

from mt5_extracao import cli, mt5_backend

REPO = Path(__file__).resolve().parents[1]

# Processo filho com o fuso local trocado por TZ (o Windows e o Linux respeitam TZ por processo). Um módulo
# MetaTrader5 falso devolve a grade M5 entre os limites recebidos, como o terminal faria no eixo do servidor.
_CHILD = r"""
import calendar, datetime as dt, json, sys, time
sys.path.insert(0, sys.argv[1])
from mt5_extracao import mt5_backend

class Native:
    TIMEFRAME_M5 = 5
    def __init__(self):
        self.calls = []
    def copy_rates_range(self, symbol, timeframe, date_from, date_to):
        self.calls.append(["rates", date_from, date_to])
        if not isinstance(date_from, int):            # o nativo aceita datetime e o lê no fuso local
            date_from, date_to = int(time.mktime(date_from.timetuple())), int(time.mktime(date_to.timetuple()))
        first = -(-date_from // 300) * 300
        return [t for t in range(first, date_to + 1, 300)]
    def copy_ticks_range(self, symbol, date_from, date_to, flags):
        self.calls.append(["ticks", date_from, date_to])
        return []

native = Native()
wrapped = mt5_backend.LocalMT5(native)
start, end = dt.datetime(2026, 11, 10), dt.datetime(2026, 11, 17, 23, 59, 59)
bars = wrapped.copy_rates_range("EURUSD", wrapped.TIMEFRAME_M5, start, end)
wrapped.copy_ticks_range("EURUSD", start, end, 0)
raw = Native.copy_rates_range(Native(), "EURUSD", 5, start, end)
print(json.dumps({"calls": native.calls, "first": bars[0], "last": bars[-1], "count": len(bars),
                  "raw_first": raw[0], "local_midnight": time.mktime(start.timetuple()),
                  "dst_day": dt.datetime(2026, 3, 9).timestamp() - dt.datetime(2026, 3, 8).timestamp(),
                  "dst_epoch": mt5_backend._to_epoch(dt.datetime(2026, 3, 9)) - mt5_backend._to_epoch(dt.datetime(2026, 3, 8))}))
"""


def _child(tz):
    environment = {k: v for k, v in os.environ.items() if k != "TZ"}
    environment["TZ"] = tz
    done = subprocess.run([sys.executable, "-I", "-c", _CHILD, str(REPO)], capture_output=True, text=True,
                          env=environment, timeout=120)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_requested_window_is_the_same_on_every_host_timezone():
    utc, brazil, japan, newyork = (_child(tz) for tz in ("UTC0", "BRT3", "JST-9", "EST5EDT"))
    midnight, last = calendar.timegm((2026, 11, 10, 0, 0, 0)), calendar.timegm((2026, 11, 17, 23, 55, 0))
    for got in (utc, brazil, japan, newyork):
        assert got["calls"] == [["rates", midnight, midnight + 8 * 86400 - 1], ["ticks", midnight, midnight + 8 * 86400 - 1]]
        assert (got["first"], got["last"], got["count"]) == (midnight, last, 8 * 288)   # 00:00 .. 23:55, inclusivo
        assert got["dst_epoch"] == 86400                                                # sem DST inventado
    # O que o nativo fazia: a meia-noite pedida dependia do fuso do computador (o defeito reproduzido).
    assert {round(g["local_midnight"]) - midnight for g in (utc, brazil, japan)} == {0, 3 * 3600, -9 * 3600}
    assert brazil["raw_first"] == midnight + 3 * 3600
    assert newyork["dst_day"] == 23 * 3600


def test_naive_is_server_clock_and_aware_is_an_instant():
    assert mt5_backend._to_epoch(dt.datetime(2026, 11, 10)) == calendar.timegm((2026, 11, 10, 0, 0, 0))
    aware = dt.datetime(2026, 11, 10, tzinfo=dt.timezone(dt.timedelta(hours=-3)))
    assert mt5_backend._to_epoch(aware) == calendar.timegm((2026, 11, 10, 3, 0, 0))
    assert mt5_backend._to_epoch(dt.datetime(2026, 11, 10, tzinfo=dt.timezone.utc)) == calendar.timegm((2026, 11, 10, 0, 0, 0))


def test_get_mt5_wraps_the_native_module_with_the_bridge_contract(tmp_path, monkeypatch):
    received = {}
    fake = types.ModuleType("MetaTrader5")
    fake.COPY_TICKS_ALL = -1
    fake.__version__ = "5.0.teste"
    fake.copy_ticks_range = lambda *args: received.setdefault("args", args)
    fake.history_deals_get = lambda date_from, date_to, group=None: (date_from, date_to, group)
    monkeypatch.setitem(sys.modules, "MetaTrader5", fake)
    mt5_backend.reset()
    try:
        config = tmp_path / "config.ini"
        config.write_text("[BRIDGE]\nenabled = false\n", encoding="utf-8")
        module = mt5_backend.get_mt5(str(config))
        assert isinstance(module, mt5_backend.LocalMT5)
        assert module.COPY_TICKS_ALL == -1 and module.__version__ == "5.0.teste"
        module.copy_ticks_range("EURUSD", dt.datetime(2026, 11, 10), dt.datetime(2026, 11, 10, 23, 59, 59),
                                module.COPY_TICKS_ALL)
        midnight = calendar.timegm((2026, 11, 10, 0, 0, 0))
        assert received["args"] == ("EURUSD", midnight, midnight + 86399, -1)
        assert module.history_deals_get(dt.datetime(2026, 11, 10), dt.datetime(2026, 11, 11), group="*") == (
            midnight, midnight + 86400, "*")
    finally:
        mt5_backend.reset()


@pytest.mark.parametrize("text", ["2026-11-10T00:00-03:00", "10/11/2026", "2026-11-10 00:00", "2026-02-30"])
def test_ambiguous_or_invalid_dates_are_refused(text):
    with pytest.raises(ValueError, match="AAAA-MM-DD"):
        cli._parse_date(text)


def test_dates_cover_the_whole_server_day_inclusively():
    assert cli._parse_date("2026-11-10") == dt.datetime(2026, 11, 10, 0, 0, 0)
    assert cli._parse_date("2026-11-17", end_of_day=True) == dt.datetime(2026, 11, 17, 23, 59, 59)


def test_now_comes_from_the_server_tick_not_from_the_computer(connector, fake_mt5, monkeypatch):
    ticks = {"EURUSD": 1794268800 + 3600, "GBPUSD": 1794268800 + 7200, "XAUUSD": None}
    monkeypatch.setattr(fake_mt5, "symbol_info_tick", lambda symbol: None if ticks.get(symbol) is None else
                        types.SimpleNamespace(time=ticks[symbol]), raising=False)
    assert connector.server_now(["EURUSD", "GBPUSD", "XAUUSD"]) == dt.datetime(2026, 11, 10, 2, 0)
    assert connector.server_now(["XAUUSD"]) is None
    assert cli._server_now(connector, ["EURUSD"]) == dt.datetime(2026, 11, 10, 1, 0)
    with pytest.raises(ValueError, match="informe --to"):
        cli._server_now(connector, ["XAUUSD"])


def test_incremental_update_ends_at_the_server_clock(monkeypatch):
    from mt5_extracao.historical_extractor import HistoricalExtractor
    seen = {}

    class Connector:
        def server_now(self, symbols):
            seen["symbols"] = list(symbols)
            return dt.datetime(2026, 11, 19, 21, 30)

    class Db:
        def get_table_name_for_symbol(self, symbol, timeframe):
            return "eurusd_5_minutos"

        def get_last_timestamp(self, table):
            return dt.datetime(2026, 11, 19, 12, 0)

    extractor = HistoricalExtractor.__new__(HistoricalExtractor)
    extractor.connector, extractor.db_manager = Connector(), Db()
    monkeypatch.setattr(extractor, "extract_data", lambda *a, **k: seen.update(end=k["end_date"]), raising=False)
    extractor.update_symbols(["EURUSD"], 5, "5 minutos", include_indicators=False)
    assert seen == {"symbols": ["EURUSD"], "end": dt.datetime(2026, 11, 19, 21, 30)}
