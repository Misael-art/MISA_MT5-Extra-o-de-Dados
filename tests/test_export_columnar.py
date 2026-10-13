"""T5.1: exportação Parquet e DuckDB."""
import builtins
import time

import pandas as pd
import pytest

from mt5_extracao import cli
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.error_handler import ExportError
from tests.market_data import random_walk

TABLE = "win_n_1_minuto"


@pytest.fixture
def db(tmp_path):
    manager = DatabaseManager(db_path=str(tmp_path / "e.db"))
    manager.save_ohlcv_data("WIN$N", "1 minuto", random_walk(500, freq="min", start="2024-01-02 09:00"))
    return manager


def test_parquet_roundtrip_with_timestamp(db, tmp_path):
    pytest.importorskip("pyarrow")
    out = DataExporter(db).export_to_parquet(TABLE, tmp_path / "w.parquet")
    df = pd.read_parquet(out)
    assert len(df) == 500 and pd.api.types.is_datetime64_any_dtype(df["time"])
    assert df["time"].iloc[0] == pd.Timestamp("2024-01-02 09:00")


def test_duckdb_tables_and_filter(db, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    db.save_ohlcv_data("WDO$N", "1 minuto", random_walk(50, freq="min", start="2024-01-02 09:00"))
    path = DataExporter(db).export_to_duckdb([TABLE, "wdo_n_1_minuto"], tmp_path / "base.duckdb")
    con = duckdb.connect(path, read_only=True)
    try:
        assert con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0] == 500
        assert con.execute("SELECT COUNT(*) FROM wdo_n_1_minuto").fetchone()[0] == 50
        assert "TIMESTAMP" in str(con.execute(f"SELECT typeof(time) FROM {TABLE} LIMIT 1").fetchone()[0])
    finally:
        con.close()
    # Reexportar substitui (não duplica)
    DataExporter(db).export_to_duckdb(TABLE, tmp_path / "base.duckdb")
    con = duckdb.connect(path, read_only=True)
    try:
        assert con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0] == 500
    finally:
        con.close()


def test_missing_optional_package_has_friendly_message(db, tmp_path, monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name in ("pyarrow", "duckdb"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ExportError, match="requirements-optional"):
        DataExporter(db).export_to_parquet(TABLE, tmp_path / "x.parquet")
    with pytest.raises(ExportError, match="duckdb"):
        DataExporter(db).export_to_duckdb(TABLE, tmp_path / "x.duckdb")


def test_cli_export_parquet(db, tmp_path, capsys):
    pytest.importorskip("pyarrow")
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    out = tmp_path / "cli.parquet"
    assert cli.main(["--config", str(cfg), "export", "--table", TABLE, "--format", "parquet",
                     "--out", str(out)]) == 0
    assert out.exists() and "Exportado" in capsys.readouterr().out


def test_parquet_one_million_rows_is_fast(tmp_path):
    """Critério de aceite: 1 milhão de linhas em menos de 10 s (escrita do Parquet)."""
    pytest.importorskip("pyarrow")
    n = 1_000_000
    df = pd.DataFrame({"time": pd.date_range("2020-01-01", periods=n, freq="min"),
                       "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "tick_volume": 10, "spread": 1,
                       "real_volume": 0})

    class FakeDb:
        def execute_query(self, query):
            return df.copy()
    exporter = DataExporter(FakeDb())
    t0 = time.perf_counter()
    exporter.export_to_parquet("big", tmp_path / "big.parquet")
    assert time.perf_counter() - t0 < 10
