"""SA-REF-01: gêmeo Python do candidato G19.15 sobre o código fixado; só dados sintéticos."""
import shutil
from pathlib import Path

import pytest

from mt5_extracao.robots import super_atr_reference as ref

FRAMEWORK = Path(__file__).resolve().parents[2] / "hadouken"


def framework():
    if not (FRAMEWORK / ref.PROTOCOL).exists() or not (shutil.which("g++") or shutil.which("clang++")):
        pytest.skip("HADOUKEN fixado ou compilador C++ indisponível")
    return FRAMEWORK


def test_contract_matches_frozen_protocol():
    operations, spec, source = ref._pinned_study(framework())
    assert spec["periods"][ref.CONTRACT["role"]] == ref.CONTRACT["window_broker"]
    assert spec["indicator"]["period"] == 20 and spec["indicator"]["mean"] == 0
    assert spec["account"]["risk_fraction"] == 0.0025 and spec["account"]["max_simultaneous_cycles"] == 1
    assert "flip1" in spec["entries"] and "variable2atr" in spec["stops"] and "target2r" in spec["managers"]
    rules = operations.management_rules("variable2atr", "target2r")
    assert rules.target_r == 2.0 and rules.eod_exit and not rules.trailing_atr and not rules.max_bars
    assert source["framework_commit"].startswith("b730ed4a")


def test_book_runs_on_synthetic_m1_without_opposite_flip_exits(tmp_path):
    framework()
    from mt5_extracao.database_manager import DatabaseManager
    from tests.market_data import random_walk
    db = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    bars = random_walk(23 * 1440, freq="min", start="2026-09-28 00:00")     # janela later + aquecimento, dentro do timeout
    assert db.save_ohlcv_data("EURUSD", "1 minuto", bars)
    book = ref.python_book(db, framework(), cost="base")
    assert book["contract"]["id"] == "SA-REF-01" and book["m1_rows"] > 0
    import math
    for trade in book["trades"]:
        assert trade["exit_reason"] != "signal"                         # target2r: sem saída por virada
        # stop do lado protetor da entrada (série sintética pode ter preço baixo: não exigir stop > 0)
        assert (trade["stop_price"] < trade["entry_price"]) if trade["direction"] > 0 else (trade["stop_price"] > trade["entry_price"])
    for decision in book["decisions"]:
        assert decision["side"] in ("long", "short") and math.isfinite(decision["stop"])
    db.close()


def _fingerprint(path):
    import hashlib, sqlite3
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        schema = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
    return schema, hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_readonly_reader_does_not_change_schema_or_data_and_refuses_missing_base(tmp_path):
    framework()
    from mt5_extracao.database_manager import DatabaseManager
    from tests.market_data import random_walk
    path = tmp_path / "base.sqlite"
    db = DatabaseManager(db_path=str(path))
    assert db.save_ohlcv_data("EURUSD", "1 minuto", random_walk(3000, freq="min", start="2026-10-01 00:00"))
    db.close()
    before = _fingerprint(path)
    ref._pinned_study(framework())
    rows = ref.m1_rows_readonly(path, "EURUSD", "2026-10-20T00:00:00")
    assert len(rows) == 3000 and _fingerprint(path) == before                # nada criado nem alterado
    missing = tmp_path / "nao_existe.sqlite"
    with pytest.raises(FileNotFoundError):
        ref.m1_rows_readonly(missing, "EURUSD", "2026-10-20T00:00:00")
    assert not missing.exists()


def test_book_reproduces_from_frozen_csv_without_database(tmp_path):
    framework()
    import gzip
    from tests.market_data import random_walk
    bars = random_walk(23 * 1440, freq="min", start="2026-09-28 00:00")
    frozen = tmp_path / "m1.csv.gz"
    with gzip.open(frozen, "wt", encoding="utf-8") as handle:
        handle.write("time,open,high,low,close\n")
        for row in bars.itertuples(index=False):
            handle.write(f"{str(row.time)[:19]},{row.open!r},{row.high!r},{row.low!r},{row.close!r}\n")
    ref._pinned_study(framework())
    book = ref.python_book(None, framework(), cost="zero", rows=ref.m1_rows_frozen(frozen))
    assert book["contract"]["id"] == "SA-REF-01" and book["m1_rows"] > 0
