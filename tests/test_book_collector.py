"""T5.4: snapshots do book de ofertas."""
import pytest
from sqlalchemy import text

import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import cli
from mt5_extracao.book_collector import BookCollector
from mt5_extracao.database_manager import DatabaseManager


class FakeClock:
    def __init__(self, start=1_704_196_800.0):   # 2024-01-02 12:00 UTC
        self.t = start

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def collector(connector, db, **kw):
    clock = FakeClock()
    return BookCollector(connector, db, clock=clock, sleep=clock.sleep, **kw)


def test_connector_snapshot(connector):
    assert connector.book_subscribe("WIN$N")
    levels = connector.book_snapshot("WIN$N")
    assert len(levels) == 10 and {l["type"] for l in levels} == {1, 2}
    assert not connector.book_subscribe("NAO_EXISTE")


def test_collects_snapshots_and_releases(connector, fake_mt5, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    c = collector(connector, db, interval=1.0)
    summary = c.run(["WIN$N", "WDO$N"], max_snapshots=5)
    assert summary["WIN$N"]["snapshots"] == 5 and summary["WIN$N"]["rows"] == 50
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT time_utc, time_msc, type, price, volume FROM win_n_book "
                                 "ORDER BY time_msc, type, price")).fetchall()
        times = [r[0] for r in conn.execute(text("SELECT DISTINCT time_utc FROM wdo_n_book ORDER BY 1"))]
    assert len(rows) == 50
    assert rows[0][0].startswith("2024-01-02 12:00:00")
    assert len(times) == 5 and times[1].startswith("2024-01-02 12:00:01")   # um snapshot por segundo
    assert len(fake_mt5.calls_named("market_book_add")) == 2                # assina uma vez só
    assert len(fake_mt5.calls_named("market_book_release")) == 2


def test_duration_and_memory_bounded(connector, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    c = collector(connector, db, interval=2.0, flush_every=10)
    summary = c.run(["WIN$N"], duration=3600)          # 1 hora simulada
    assert summary["WIN$N"]["snapshots"] == 1800
    assert c.max_buffered <= 10 * 10                    # nunca mais que flush_every snapshots em memória
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM win_n_book")).scalar() == 18000


def test_unsupported_symbol_and_failures(connector, fake_mt5, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    summary = collector(connector, db).run(["NAO_EXISTE"], max_snapshots=3)
    assert summary["NAO_EXISTE"]["subscribed"] is False
    original = fake_mt5.market_book_get
    fake_mt5.market_book_get = lambda s: None
    try:
        summary = collector(connector, db).run(["WIN$N"], max_snapshots=3)
    finally:
        fake_mt5.market_book_get = original
    assert summary["WIN$N"]["failures"] == 3 and summary["WIN$N"]["rows"] == 0
    with pytest.raises(ValueError):
        BookCollector(connector, db, interval=0)


def test_cli_book(fake_mt5, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    def fake_initialize(self, *a, **k):
        self.is_initialized = True
        return True
    monkeypatch.setattr(connector_module.MT5Connector, "initialize", fake_initialize)
    import mt5_extracao.book_collector as bc
    monkeypatch.setattr(bc.time, "sleep", lambda s: None)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "book", "--symbols", "WIN$N,NAO_EXISTE", "--interval", "0.01",
                     "--duration", "0.05"]) == 1
    captured = capsys.readouterr()
    assert "níveis gravados em win_n_book" in captured.out
    assert "NAO_EXISTE: o MT5 recusou o book" in captured.err
