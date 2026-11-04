"""Criptomoedas pela API pública da Binance: paginação, velas fechadas, especificação e CLI."""
import calendar
import sqlite3
from datetime import datetime

import pytest

from mt5_extracao import cli, crypto_public

T0 = calendar.timegm((2026, 11, 3, 0, 0, 0)) * 1000   # 03/11/2026 00:00 UTC
MINUTE = 60_000


def kline(i, close=100.0):
    open_ms = T0 + i * MINUTE
    return [open_ms, "100", "101", "99", str(close), "0.5", open_ms + MINUTE - 1, "50.4", 7, "0", "0", "0"]


class FakeBinance:
    def __init__(self, count):
        self.rows = [kline(i, 100.0 + i) for i in range(count)]
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params)))
        if url.endswith("/exchangeInfo"):
            return Response({"symbols": [{
                "symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT",
                "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                            {"filterType": "LOT_SIZE", "minQty": "0.00001000", "stepSize": "0.00001000",
                             "maxQty": "9000.00000000"}]}]})
        start, end = params["startTime"], params["endTime"]
        batch = [r for r in self.rows if start <= r[0] <= end][:params["limit"]]
        return Response(batch)


class Response:
    status_code = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


def test_klines_page_through_limit_and_drop_open_candle(monkeypatch):
    monkeypatch.setattr(crypto_public, "LIMIT", 3)
    fake = FakeBinance(8)
    start = datetime.utcfromtimestamp(T0 / 1000)
    now = datetime.utcfromtimestamp((T0 + 7 * MINUTE + 30_000) / 1000)   # a 8ª vela ainda está aberta
    bars = crypto_public.klines("btcusdt", "M1", start, now, session=fake, now=now)
    assert len(bars) == 7 and len([c for c in fake.calls if c[0].endswith("/klines")]) == 3
    assert bars["close"].tolist() == [100.0 + i for i in range(7)]
    assert bars["tick_volume"].tolist() == [7] * 7 and bars["real_volume"].tolist() == [50] * 7
    assert fake.calls[0][1]["symbol"] == "BTCUSDT" and fake.calls[0][1]["interval"] == "1m"


def test_unknown_timeframe_is_refused():
    with pytest.raises(ValueError, match="Binance"):
        crypto_public.klines("BTCUSDT", "M2", datetime(2026, 1, 1), datetime(2026, 1, 2), session=FakeBinance(0))


def test_pair_spec_comes_from_public_filters():
    spec = crypto_public.pair_spec("BTCUSDT", session=FakeBinance(0))
    assert (spec["tick_size"], spec["digits"], spec["volume_min"], spec["contract_size"], spec["currency_profit"]) == (
        0.01, 2, 0.00001, 1.0, "USDT")


def test_cli_extracts_into_dedicated_database_and_resumes(tmp_path, monkeypatch):
    fake = FakeBinance(5)
    monkeypatch.setattr(crypto_public, "requests", fake)
    db = tmp_path / "cripto.db"
    args = ["--db", str(db), "crypto", "extract", "--symbols", "BTCUSDT", "--tf", "M1"]
    assert cli.main(args + ["--from", "2026-11-03", "--to", "2026-11-03"]) == cli.EXIT_OK
    fake.rows += [kline(i, 100.0 + i) for i in range(5, 8)]
    assert cli.main(args + ["--to", "2026-11-03"]) == cli.EXIT_OK
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*), MAX(close) FROM btcusdt_1_minuto").fetchone() == (8, 107.0)
        assert conn.execute("SELECT volume_min, currency_profit FROM _symbol_specs").fetchall() == [(0.00001, "USDT")]
        origin = dict(conn.execute("SELECT slot, origin_id FROM _dataset_origin").fetchall())
    assert origin.get("server") == "api.binance.com"
    resumed = [c[1]["startTime"] for c in fake.calls if c[0].endswith("/klines")][-1]
    assert resumed == T0 + 5 * MINUTE


def test_cli_resume_without_history_asks_for_start(tmp_path):
    assert cli.main(["--db", str(tmp_path / "c.db"), "crypto", "extract", "--symbols", "BTCUSDT"]) != cli.EXIT_OK
