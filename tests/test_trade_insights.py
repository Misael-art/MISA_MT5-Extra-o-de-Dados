"""T16.10: insights automáticos — efeito real é achado e validado; ruído não vira descoberta."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import insights


def _lab(n_days=300, seed=5, friday=-15.0, noon=0.0):
    rng = np.random.default_rng(seed)
    rows = []
    for k, d in enumerate(pd.bdate_range("2024-01-08", periods=n_days)):
        for h in (9, 12, 15):
            e = d + pd.Timedelta(hours=h, minutes=5)
            pnl = rng.normal(2, 10) + (friday if d.dayofweek == 4 else 0) + (noon if h == 12 else 0)
            rows.append({"run_id": "r", "seq": len(rows), "entry_time": e, "exit_time": e + pd.Timedelta(minutes=40),
                         "pnl": pnl, "day_of_week": d.dayofweek, "time_bucket": f"{h:02d}:00",
                         "event_impact": 0, "volatility_bucket": ("baixa", "média", "alta")[k % 3]})
    return pd.DataFrame(rows)


def test_planted_effects_are_found_and_validated():
    found = insights.discover(_lab(noon=-12))
    validated = {(i.dimension, i.value) for i in found if i.state == insights.VALIDATED}
    assert ("day_of_week", 4) in validated and ("time_bucket", "12:00") in validated
    friday = next(i for i in found if (i.dimension, i.value) == ("day_of_week", 4))
    assert friday.rule == {"tipo": "excluir_dia", "dias": [4]} and friday.folds_improved == friday.folds == 3
    assert friday.holdout_after > friday.holdout_before and friday.tested == len(found)
    assert "reserva final" in friday.text and "Testadas" in friday.text


def test_pure_noise_never_produces_a_validated_insight():
    validated = 0
    for seed in range(60):
        found = insights.discover(_lab(n_days=200, seed=seed, friday=0.0))
        validated += sum(i.state == insights.VALIDATED for i in found)
    assert validated == 0


def test_few_trades_stay_exploratory():
    found = insights.discover(_lab(n_days=25))
    assert all(i.state == insights.EXPLORATORY for i in found)


def test_save_and_load(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    found = insights.discover(_lab())
    assert insights.save(db, "robot:python:a", found) == len(found)
    insights.save(db, "robot:python:a", found[:1])                      # substitui
    rows = insights.load(db, "robot:python:a")
    assert len(rows) == 1 and rows[0]["state"] in (insights.VALIDATED, insights.EXPLORATORY)
    db.close()


def test_mode_insight_finds_where_a_mode_helps_and_ignores_noise():
    lab = _lab(friday=0.0)
    rng = np.random.default_rng(9)
    helped = lab["volatility_bucket"] == "alta"
    trailing = lab[["run_id", "seq"]].assign(pnl=lab["pnl"] + np.where(helped, 8.0, 0.0) + rng.normal(0, 1, len(lab)))
    found = insights.mode_insights(lab, {"trailing_atr": trailing})
    top = next(i for i in found if i.state == insights.VALIDATED)
    assert (top.dimension, top.value) == ("volatility_bucket", "alta")
    assert top.rule == {"tipo": "trocar_modo", "modo": "trailing_atr", "quando": {"volatility_bucket": ["alta"]}}
    noise = lab[["run_id", "seq"]].assign(pnl=lab["pnl"] + rng.normal(0, 3, len(lab)))
    assert not any(i.state == insights.VALIDATED for i in insights.mode_insights(lab, {"x": noise}))
