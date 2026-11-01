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
    bars = random_walk(30 * 1440, freq="min", start="2026-09-20 00:00")
    assert db.save_ohlcv_data("EURUSD", "1 minuto", bars)
    book = ref.python_book(db, framework(), cost="base")
    assert book["contract"]["id"] == "SA-REF-01" and book["m1_rows"] > 0
    for trade in book["trades"]:
        assert trade["exit_reason"] != "signal"                         # target2r: sem saída por virada
    for decision in book["decisions"]:
        assert decision["side"] in ("long", "short") and decision["stop"] > 0
    db.close()
