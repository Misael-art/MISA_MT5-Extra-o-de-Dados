"""Cálculos e execução do estudo em bases SINTÉTICAS; nenhuma tentativa financeira."""
import copy
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import pf19_study as study


def protocol():
    root = Path(__file__).resolve().parents[1]
    return json.loads((root / "docs/pesquisa/PF19_PROTOCOLO_20261016.json").read_text(encoding="utf-8"))


def test_pf_bootstrap_keeps_trade_denominator_despite_every_day_net_positive():
    sessions = pd.bdate_range("2025-01-01", periods=20)
    pnl = np.tile([100., -50.], len(sessions))
    days = sessions.repeat(2)
    before = pnl.copy()
    ci = study.pf_interval(pnl, days, sessions, resamples=200)
    assert ci["defined"] == 200 and ci["undefined"] == 0
    assert ci["low"] == ci["high"] == 2
    np.testing.assert_array_equal(pnl, before)
    assert study.pf_interval(pnl, days, sessions, resamples=200) == ci


def test_pf_bootstrap_includes_zero_sessions_and_never_caps_undefined():
    sessions = pd.bdate_range("2025-01-01", periods=20)
    ci = study.pf_interval([10., 20.], sessions[:2], sessions, resamples=100)
    assert ci["market_sessions"] == 20 and ci["undefined"] == 100
    assert ci["low"] is ci["high"] is None
    json.dumps(ci, allow_nan=False)
    with pytest.raises(ValueError, match="sessão"):
        study.pf_interval([1.], ["2020-01-01"], sessions)
    with pytest.raises(ValueError, match="finitos"):
        study.pf_interval([np.nan], sessions[:1], sessions)


def test_empty_or_short_support_does_not_invent_interval():
    ci = study.pf_interval([], [], [], resamples=20)
    assert ci["low"] is None and ci["undefined"] == 20
    with pytest.raises(ValueError, match="positivos"):
        study.pf_interval([], [], [], block=0)


def test_high_or_exact_target_pf_cannot_bypass_exposure_and_unknown_costs():
    p = protocol()
    metrics = {"trades": 500, "losses": 100, "profit_factor": 1.9,
               "net_profit": 1000, "expectancy": 2, "max_drawdown_pct": .1}
    gates = study.eligibility(metrics, {"low": 1.1}, 100, .2, 4, p)
    assert gates["profit_factor"] and gates["closed_trades"] and gates["losses"]
    assert not gates["independent_untouched_oos"] and not gates["costs_and_currency_verified"]
    assert not all(gates.values())
    metrics["profit_factor"] = None
    assert not study.eligibility(metrics, {"low": None}, 100, .2, 4, p)["profit_factor"]


def test_trial_universe_and_budget_are_bounded_before_results():
    p = protocol()
    assert len(study.trial_cases(p)) == 8
    assert study.trial_cases(p)[0] == ("H1", "ema_atr", "base")
    for change in [{"max_trials": 7}, {"max_workers": 2}, {"max_wall_seconds": 601}, {"max_trial_seconds": 61}]:
        changed = copy.deepcopy(p)
        changed["budget"].update(change)
        with pytest.raises(ValueError):
            study.trial_cases(changed)
    p["universe"]["symbols"] = ["OTHER"]
    with pytest.raises(ValueError, match="universo"):
        study.trial_cases(p)


def test_study_refuses_to_start_before_ui_acceptance(tmp_path):
    p = tmp_path / "protocol.json"
    p.write_text(json.dumps(protocol()))
    a = tmp_path / "ui.json"
    a.write_text('{"accepted": false}')
    with pytest.raises(ValueError, match="aceite"):
        study.execute(tmp_path / "missing.db", p, tmp_path / "output", a)
    assert not (tmp_path / "output").exists()


def test_cli_workers_complete_synthetic_study_preserving_source_database(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    from tests.market_data import random_walk

    source = tmp_path / "SYNTHETIC.sqlite"
    db = DatabaseManager(db_path=str(source))
    try:
        frame = random_walk(650, seed=12, start="2025-10-01", freq="h", spread=3)
        db.save_ohlcv_data("EURUSD", "1 hora", frame)
        db.save_symbol_spec("EURUSD", {"point": .01, "digits": 2, "tick_size": .01, "tick_value": 1,
                                     "volume_min": .01, "volume_step": .01, "volume_max": 100,
                                     "contract_size": 1, "currency_profit": "TEST", "spread": 3})
    finally:
        db.close()
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    p = protocol()
    p["universe"].update(timeframes=["H1"], strategies=["donchian"])
    p["inference"]["resamples"] = 100
    p["periods"]["warmup_bars"] = 64
    proto = tmp_path / "protocol.json"
    proto.write_text(json.dumps(p), encoding="utf-8")
    acceptance = tmp_path / "ui.json"
    acceptance.write_text('{"accepted":true,"class":"SYNTHETIC TEST"}')
    out = tmp_path / "output"
    manifest = study.execute(source, proto, out, acceptance)
    assert manifest["status"] == "COMPLETED", manifest
    assert len(manifest["attempts"]) == 2 and manifest["approved"] == 0
    assert all(v["exit_code"] == 0 and v["peak_rss_mb"] > 0 for v in manifest["attempts"])
    for attempt in manifest["attempts"]:
        result = json.loads((out / attempt["result"]).read_text(encoding="utf-8"))
        assert result["spec"]["currency_profit"] == "TEST"
        assert result["metrics"]["trades"] == len(result["trades"])
        assert result["approved"] is False and result["costs_certified"] is False
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    with pytest.raises(FileExistsError):
        study.execute(source, proto, out, acceptance)
