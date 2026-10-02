"""T5.3: coleta de ticks em blocos com retomada."""
import calendar
import datetime as dt

import pytest
from sqlalchemy import text

import mt5_extracao.tick_extractor as te_module
from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.tick_extractor import TickExtractor

START, END = dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 5)   # ter, qua, qui


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(te_module.time, "sleep", lambda s: None)


def epoch(d):
    return calendar.timegm(d.timetuple())


def count(db, where=""):
    with db.engine.connect() as c:
        return c.execute(text(f"SELECT COUNT(*) FROM win_n_ticks {where}")).scalar()


def test_connector_ticks_dataframe(connector):
    df = connector.get_ticks_range("WIN$N", dt.datetime(2024, 1, 2, 9), dt.datetime(2024, 1, 2, 9, 1))
    assert list(df.columns)[:3] == ["time", "bid", "ask"]
    assert len(df) == 6 and str(df["time"].iloc[-1]) == "2024-01-02 09:01:00.500000"
    empty = connector.get_ticks_range("WIN$N", dt.datetime(2024, 1, 6), dt.datetime(2024, 1, 7))  # sábado
    assert empty is not None and empty.empty
    assert connector.get_ticks_range("NAO_EXISTE", START, END) is None


def test_extract_keeps_same_millisecond_ticks_and_resumes(connector, fake_mt5, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    fake_mt5.fail_ranges = [(epoch(dt.datetime(2024, 1, 3, 9)), epoch(dt.datetime(2024, 1, 3, 18)))]
    ext = TickExtractor(connector, db)
    r = ext.extract("WIN$N", START, END)
    assert (r["ok"], r["failed"]) == (2, 1)
    per_day = 9 * 60 * 3
    assert count(db) == 2 * per_day          # 3 ticks/min, dois no mesmo milissegundo: nenhum perdido

    fake_mt5.fail_ranges = []
    fake_mt5.calls.clear()
    r = ext.extract("WIN$N", START, END)
    assert (r["ok"], r["skipped"], r["failed"]) == (1, 2, 0)
    assert len(fake_mt5.calls_named("copy_ticks_range")) == 1   # só o bloco que faltou
    assert count(db) == 3 * per_day
    assert count(db, "WHERE time LIKE '2024-01-03%'") == per_day

    # Reextrair tudo não duplica
    ext.extract("WIN$N", START, END, overwrite=True)
    assert count(db) == 3 * per_day


def test_weekend_block_is_empty_not_failed(connector, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    r = TickExtractor(connector, db).extract("WIN$N", dt.datetime(2024, 1, 6), dt.datetime(2024, 1, 8))
    assert r["empty"] == 2 and r["failed"] == 0
    with db.engine.connect() as c:
        assert [row[0] for row in c.execute(text("SELECT status FROM _extraction_log"))] == ["empty", "empty"]


def test_ticks_table_is_listed_and_time_basis_applies(connector, tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "u.db"), time_basis="utc", broker_utc_offset=-3)
    TickExtractor(connector, db, chunk_hours=1).extract("WIN$N", dt.datetime(2024, 1, 2, 9),
                                                         dt.datetime(2024, 1, 2, 10))
    assert "win_n_ticks" in db.get_all_tables()
    with db.engine.connect() as c:
        first = c.execute(text("SELECT time FROM win_n_ticks ORDER BY time_msc, seq LIMIT 1")).scalar()
    assert first.startswith("2024-01-02 12:00:00")   # 09:00 da corretora (UTC-3) gravado em UTC


def test_cli_ticks(fake_mt5, tmp_path, monkeypatch, capsys):
    import mt5_extracao.mt5_connector as connector_module
    monkeypatch.chdir(tmp_path)

    def fake_initialize(self, *a, **k):
        self.is_initialized = True
        return True
    monkeypatch.setattr(connector_module.MT5Connector, "initialize", fake_initialize)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "ticks", "--symbols", "WIN$N", "--from", "2024-01-02",
                     "--to", "2024-01-02"]) == 0
    out = capsys.readouterr().out
    assert "1620 ticks gravados" in out and "win_n_ticks" in out
