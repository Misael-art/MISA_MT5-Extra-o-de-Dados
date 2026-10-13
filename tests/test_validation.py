"""T6.4: walk-forward, robustez, Monte Carlo e veredito do Filtro C."""
import json

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import validation as v
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, Trade
from tests.market_data import ohlc_from_close, random_walk

SPEC = SymbolSpec(point=0.01, tick_value=0.01, volume_min=0.01, volume_step=0.01)


def trending_market(n=3000, seed=5):
    """Tendências longas alternadas: favorece estratégias de rompimento."""
    rng = np.random.default_rng(seed)
    drift = np.repeat(rng.choice([-0.4, 0.4], n // 150 + 1), 150)[:n]
    return ohlc_from_close(1000 + np.cumsum(drift + rng.normal(0, 1.0, n)), spread=2)


def test_walk_forward_windows_are_out_of_sample():
    df = random_walk(1400, seed=3)
    wf = v.walk_forward(df, "donchian", SPEC, BacktestConfig(), v.Criteria(n_windows=5, is_parts=2))
    assert len(wf["windows"]) == 5
    size = len(df) // 7
    first_oos = pd.Timestamp(wf["windows"][0]["oos_start"])
    assert first_oos == df["time"].iloc[2 * size]
    for t in wf["oos_trades"]:
        assert t.entry_time >= first_oos      # nenhuma operação fora da amostra antes do 1º teste
    assert 0 <= wf["positive_windows"] <= 1
    assert wf["oos_metrics"]["trades"] == len(wf["oos_trades"])
    for w in wf["windows"]:
        assert set(w["params"]) == {"entry_period", "exit_period"}


def test_walk_forward_needs_enough_data():
    with pytest.raises(ValueError, match="Poucos dados"):
        v.walk_forward(random_walk(200), "donchian", SPEC)


def test_robustness_varies_each_numeric_param_20pct():
    rob = v.robustness(random_walk(800, seed=4), "ema_atr", SPEC)
    params = {(x["param"], x["factor"]) for x in rob["variants"]}
    assert ("fast", 0.8) in params and ("trail_atr", 1.2) in params
    fast = {x["factor"]: x["value"] for x in rob["variants"] if x["param"] == "fast"}
    assert fast == {0.8: 16, 1.2: 24}
    assert rob["robust"] == all(x["profit_factor"] > 1 for x in rob["variants"])


def _trade(ret, equity=100.0):
    t0 = pd.Timestamp("2024-01-01")
    return Trade(1, t0, t0, 1, 1, 1, ret * equity, ret, 1, "alvo", equity)


def test_monte_carlo_is_deterministic_and_bounded():
    trades = [_trade(r) for r in [0.02, -0.01, 0.015, -0.01, 0.03, -0.02] * 10]
    a, b = v.monte_carlo(trades, runs=500, seed=1), v.monte_carlo(trades, runs=500, seed=1)
    assert a == b
    assert 0 < a["dd_median"] <= a["dd_percentile"] < 1
    only_losses = v.monte_carlo([_trade(-0.01)] * 20, runs=50)
    assert only_losses["dd_percentile"] == pytest.approx(1 - 0.99 ** 20)
    assert v.monte_carlo([], runs=10)["runs"] == 0


def test_validate_verdict_has_explained_checks():
    verdict = v.validate(trending_market(), "donchian", SPEC, BacktestConfig(), v.Criteria(mc_runs=200))
    names = [c["name"] for c in verdict["checks"]]
    assert names[:4] == ["Profit factor fora da amostra", "Operações fora da amostra",
                         "Janelas walk-forward positivas", "Robustez (parâmetros ±20%)"]
    assert names[4].startswith("Drawdown Monte Carlo")
    assert verdict["approved"] == all(c["passed"] for c in verdict["checks"])
    assert verdict["status"].startswith("✅" if verdict["approved"] else "❌ reprovada")
    json.dumps(verdict, default=v._json_default)   # serializável


def test_random_walk_is_not_approved_with_strict_criteria():
    """Sem vantagem real (passeio aleatório), o Filtro C não deve aprovar."""
    verdict = v.validate(random_walk(2000, seed=9, spread=20), "squeeze", SPEC, BacktestConfig(),
                         v.Criteria(mc_runs=100))
    assert not verdict["approved"]


def test_save_and_load_validation(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    assert v.load_validations(db).empty
    verdict = v.validate(trending_market(1500), "donchian", SPEC, BacktestConfig(), v.Criteria(mc_runs=50))
    v.save_validation(db, "WIN$N", "H1", verdict)
    v.save_validation(db, "WIN$N", "H1", verdict)   # upsert: não duplica
    rows = v.load_validations(db, ["WIN$N"], "H1")
    assert len(rows) == 1 and rows.loc[0, "strategy"] == "donchian"
    assert bool(rows.loc[0, "approved"]) == verdict["approved"]
    assert "_strategy_validation" not in db.get_all_tables()
