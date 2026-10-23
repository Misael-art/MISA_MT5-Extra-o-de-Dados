"""BD05 2º recorte (R06): zona do corretor, fold/gap e offset desconhecido."""
from types import SimpleNamespace

import pandas as pd
import pytest

import bench
from mt5_extracao.analytics import datasets, resampling
from mt5_extracao.database_manager import DatabaseManager

SYMBOL = "EURUSD"
ZONE = "Europe/Athens"


def _hourly(local_times):
    n = len(local_times)
    return pd.DataFrame({"time": list(local_times), "open": 1.1, "high": 1.2, "low": 1.0,
                         "close": [1.1 + i / 1000 for i in range(n)], "tick_volume": 10, "spread": 1,
                         "real_volume": 0})


def _zone_db(path, zone=ZONE):
    return DatabaseManager(db_path=str(path), time_basis="utc", broker_timezone=zone)


def test_fold_keeps_both_real_hours_and_reopens(tmp_path):
    dst = bench.dst_transition()
    db = _zone_db(tmp_path / "zone.sqlite")
    assert db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(dst["bar_start_local"]))
    stored = db.load_ohlcv(SYMBOL, "1 hora")
    assert len(stored) == len(dst)                                   # 96 de 96, antes 95
    assert stored["time"].duplicated().sum() == 1                   # 03:00 local aparece duas vezes
    reopened = _zone_db(tmp_path / "zone.sqlite")
    assert len(reopened.load_ohlcv(SYMBOL, "1 hora")) == len(dst)
    assert reopened.time_context()["offset_state"] == "zone"


def test_nonexistent_local_hour_is_refused(tmp_path):
    db = _zone_db(tmp_path / "gap.sqlite")
    spring = pd.date_range("2026-03-29 01:00", periods=4, freq="h")  # 03:00 local não existe em Atenas
    assert not db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(spring))
    assert db.load_ohlcv(SYMBOL, "1 hora").empty


def test_reopening_with_another_zone_is_refused(tmp_path):
    db = _zone_db(tmp_path / "z.sqlite")
    assert db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(pd.date_range("2026-10-01", periods=3, freq="h")))
    other = _zone_db(tmp_path / "z.sqlite", zone="America/New_York")
    assert not other.save_ohlcv_data(SYMBOL, "1 hora", _hourly(pd.date_range("2026-10-02", periods=3, freq="h")))
    assert len(other.load_ohlcv(SYMBOL, "1 hora")) == 3


def test_as_of_uses_the_zone_for_close_and_receipt(tmp_path):
    db = _zone_db(tmp_path / "asof.sqlite")
    local = pd.to_datetime(["2026-10-25 02:00", "2026-10-25 03:00", "2026-10-25 03:00", "2026-10-25 04:00"])
    db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(local), received_at=pd.Timestamp("2026-10-26", tz="UTC"))
    seen = db.load_ohlcv_as_of(SYMBOL, "1 hora", pd.Timestamp("2026-10-26", tz="UTC"))
    assert len(seen) == 4
    single = _zone_db(tmp_path / "ambiguous.sqlite")  # 03:00 uma só vez: ambígua, sem ordem que a resolva
    assert not single.save_ohlcv_data(SYMBOL, "1 hora", _hourly(pd.date_range("2026-10-25 02:00", periods=3, freq="h")))
    # 02:00 local (+3) = 23:00 UTC do dia 24, fecha 00:00 UTC; às 23:30 UTC ainda está aberta
    db2 = _zone_db(tmp_path / "asof2.sqlite")
    db2.save_ohlcv_data(SYMBOL, "1 hora", _hourly(local[:1]), received_at=pd.Timestamp("2026-10-24 23:30", tz="UTC"))
    assert db2.load_ohlcv_as_of(SYMBOL, "1 hora", pd.Timestamp("2026-10-24 23:59", tz="UTC")).empty


def test_unknown_offset_is_explicit_and_causal_read_refuses_it(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "unknown.sqlite"))
    assert db.time_context()["offset_state"] == "unknown"
    db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(pd.date_range("2026-10-01", periods=2, freq="h")),
                       received_at=pd.Timestamp("2026-10-05", tz="UTC"))
    with pytest.raises(ValueError, match="fuso do corretor"):
        db.load_ohlcv_as_of(SYMBOL, "1 hora", pd.Timestamp("2026-10-06", tz="UTC"))
    declared = DatabaseManager(db_path=str(tmp_path / "declared.sqlite"), broker_utc_offset=2)
    assert declared.time_context()["offset_state"] == "declared"


def test_offset_only_consumers_refuse_zone_bases_explicitly(tmp_path):
    db = _zone_db(tmp_path / "consumers.sqlite")
    db.save_ohlcv_data(SYMBOL, "1 minuto", _hourly(pd.date_range("2026-10-01 10:00", periods=10, freq="min")))
    with pytest.raises(ValueError, match="zona"):
        resampling.resample_symbol(db, SYMBOL, ["M5"], symbol_info=SimpleNamespace(name=SYMBOL))
    with pytest.raises(ValueError, match="zona"):
        datasets.capture(db, SYMBOL, "M1")


def test_zone_works_without_system_tz_database_and_rejects_unknown_names(tmp_path, monkeypatch):
    """Caminho Windows: sem tzdata o zoneinfo falha; a base usa pytz e continua funcionando."""
    import zoneinfo

    def missing(*_args, **_kwargs):
        raise zoneinfo.ZoneInfoNotFoundError("sem base de fusos do sistema")

    monkeypatch.setattr(zoneinfo, "ZoneInfo", missing)
    db = _zone_db(tmp_path / "win.sqlite")
    assert db.save_ohlcv_data(SYMBOL, "1 hora", _hourly(bench.dst_transition()["bar_start_local"]))
    with pytest.raises(ValueError, match="IANA"):
        _zone_db(tmp_path / "bad.sqlite", zone="Marte/Olympus")
    with pytest.raises(ValueError, match="utc"):
        DatabaseManager(db_path=str(tmp_path / "broker.sqlite"), broker_timezone=ZONE)
