"""T5.5: fonte externa CSV para preencher lacunas M1."""
import calendar
import configparser
import datetime as dt

import pandas as pd
import pytest
from sqlalchemy import text

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import services
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.external_data_source import CsvExternalSource
from mt5_extracao.historical_extractor import HistoricalExtractor
from mt5_extracao.indicator_calculator import IndicatorCalculator


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)


def _minutes(day, n=60):
    return pd.date_range(f"{day} 09:00", periods=n, freq="min")


def write_standard(path, times):
    pd.DataFrame({"time": times, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
                  "real_volume": 7}).to_csv(path, index=False)


def write_mt5_export(path, times):
    """Formato do MT5: Barras -> Exportar (tab, datas com ponto)."""
    lines = ["<DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>\t<TICKVOL>\t<VOL>\t<SPREAD>"]
    lines += [f"{t:%Y.%m.%d}\t{t:%H:%M:%S}\t10\t11\t9\t10.5\t100\t7\t5" for t in times]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_reads_standard_and_mt5_formats(tmp_path):
    write_standard(tmp_path / "WIN$N.csv", _minutes("2024-01-03"))
    write_mt5_export(tmp_path / "petr4_M1.csv", _minutes("2024-01-03"))
    src = CsvExternalSource(str(tmp_path))
    assert src.is_configured()
    for symbol in ("WIN$N", "PETR4"):
        df = src.get_historical_m1_data(symbol, dt.datetime(2024, 1, 3, 9, 10), dt.datetime(2024, 1, 3, 9, 19))
        assert list(df.columns) == CsvExternalSource.COLUMNS
        assert len(df) == 10 and df["time"].iloc[0] == pd.Timestamp("2024-01-03 09:10")
    mt5 = src.get_historical_m1_data("PETR4", dt.datetime(2024, 1, 3), dt.datetime(2024, 1, 4))
    assert mt5["tick_volume"].iloc[0] == 100 and mt5["spread"].iloc[0] == 5 and mt5["real_volume"].iloc[0] == 7


def test_normalized_name_and_missing(tmp_path):
    write_standard(tmp_path / "win_n.csv", _minutes("2024-01-03", 5))
    src = CsvExternalSource(str(tmp_path))
    assert len(src.get_historical_m1_data("WIN$N", dt.datetime(2024, 1, 3), dt.datetime(2024, 1, 4))) == 5
    assert src.get_historical_m1_data("WDO$N", dt.datetime(2024, 1, 3), dt.datetime(2024, 1, 4)) is None
    empty = src.get_historical_m1_data("WIN$N", dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 2))
    assert empty is not None and empty.empty


def test_bad_file_and_missing_dir(tmp_path):
    (tmp_path / "X.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    src = CsvExternalSource(str(tmp_path))
    assert src.get_historical_m1_data("X", dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)) is None
    assert not CsvExternalSource(str(tmp_path / "nao_existe")).is_configured()
    with pytest.raises(ValueError, match="horário"):
        CsvExternalSource.read_csv(tmp_path / "X.csv")


def test_factory_from_config(tmp_path):
    cfg = configparser.ConfigParser()
    cfg.read_string(f"[FALLBACK]\nexternal_source_m1_fallback_enabled = True\n"
                    f"external_source_m1_type = Csv\ncsv_dir = {tmp_path}\n")
    src = services.create_external_source(cfg)
    assert isinstance(src, CsvExternalSource) and src.csv_dir == str(tmp_path)


def test_failed_mt5_block_is_filled_from_csv(connector, fake_mt5, tmp_path):
    epoch = lambda d: calendar.timegm(d.timetuple())
    fake_mt5.fail_ranges = [(epoch(dt.datetime(2024, 1, 3, 9)), epoch(dt.datetime(2024, 1, 3, 18)))]
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    write_mt5_export(csv_dir / "WIN$N.csv", _minutes("2024-01-03", 30))
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    ext = HistoricalExtractor(connector, db, IndicatorCalculator(), external_source=CsvExternalSource(str(csv_dir)),
                              chunk_config={"m1": 1, "m5_m15": 1, "default": 1})
    assert ext._process_symbol("WIN$N", 1, "1 minuto", dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 5),
                               include_indicators=False, overwrite=False) is True
    with db.engine.connect() as c:
        log = c.execute(text("SELECT block_start, status, source, rows FROM _extraction_log "
                             "ORDER BY block_start")).fetchall()
        day3 = c.execute(text("SELECT COUNT(*) FROM win_n_1_minuto WHERE time LIKE '2024-01-03%'")).scalar()
    assert [r[1] for r in log] == ["ok", "ok", "ok"]
    assert log[1][2] == "CsvExternalSource" and log[1][3] == 30
    assert day3 == 30
