"""T19.52a: política condicional de estudo sobre o livro (seleção por contexto, posição única, piso de PF)."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.presets import conditional_policy as cp
from mt5_extracao.trade_analytics import ledger


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "policy.db"))
    yield database
    database.close()


def _trades(days, hour, pnl_of, minute=0):
    """Uma operação de 30 min por dia, na hora dada; pnl_of(i) decide o resultado."""
    rows = []
    for i, day in enumerate(pd.date_range("2026-01-05", periods=days, freq="D")):
        start = day + pd.Timedelta(hours=hour, minutes=minute)
        rows.append({"direction": 1, "entry_time": start, "exit_time": start + pd.Timedelta(minutes=30),
                     "entry_price": 100.0, "exit_price": 101.0, "stop_price": 99.0, "target_price": None,
                     "volume": 1.0, "pnl": float(pnl_of(i)), "r_multiple": 1.0, "exit_reason": "alvo"})
    return rows


def _save(db, run_id, robot, rows):
    ledger.save_trades(db, run_id, rows, robot_key=robot, symbol="X", timeframe="M15", source="python")


def _context_edge(db, days=100):
    """A ganha de manhã e perde à tarde; B o contrário. Cada um, sozinho, fica perto de zero."""
    win, lose = (lambda i: 3.0 if i % 4 else -1.0), (lambda i: -2.0 if i % 4 else 1.0)
    _save(db, "a", "python:a", _trades(days, 9, win) + _trades(days, 14, lose))
    _save(db, "b", "python:b", _trades(days, 9, lose, minute=45) + _trades(days, 14, win, minute=45))


def test_policy_routes_each_context_to_its_setup_and_passes_the_floor(db):
    _context_edge(db)
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    rules = report["policy"]["rules"]
    assert [item["setup"] for item in rules["08h"]] == ["python:a"]
    assert [item["setup"] for item in rules["12h"]] == ["python:b"]
    for name in ("validation", "holdout"):
        part = report["segments"][name]
        assert part["policy"]["profit_factor"] > 2 and part["blocked_no_rule"] > 0
        assert part["policy"]["net"] > part["static_best"]["net"]
        assert part["policy"]["net"] > part["accept_all"]["net"]
    assert report["status"] == "elegível" and report["reasons"] == []
    assert report["policy"]["activation_authorized"] is False and report["study_only"]
    again = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert again["policy"]["policy_id"] == report["policy"]["policy_id"]             # determinística


def test_noise_is_rejected_and_the_floor_is_not_a_promise(db):
    rng = np.random.default_rng(3)
    for name in ("a", "b", "c"):
        draws = rng.normal(0, 1, 400)
        _save(db, name, f"python:{name}", _trades(100, 9, lambda i: draws[i]) + _trades(100, 14, lambda i: draws[100 + i]))
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["status"] in ("reprovada", "sem candidato elegível") and report["reasons"]
    # o oráculo vê o próprio trecho e parece ótimo: fica rotulado e fora do parecer
    assert "oracle_not_executable" in report["segments"]["holdout"]


def test_rules_are_frozen_in_the_search_segment(db):
    """Um contexto que só fica bom DEPOIS da procura não vira regra."""
    late_only = lambda i: (3.0 if i % 5 else -1.0) if i >= 70 else -1.0
    _save(db, "a", "python:a", _trades(100, 9, late_only))
    _save(db, "b", "python:b", _trades(100, 14, lambda i: -1.0 if i % 3 else 0.5))
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["policy"]["rules"] == {} and report["status"] == "sem candidato elegível"
    assert report["segments"]["holdout"]["policy"]["n"] == 0                          # R00: bloqueia tudo
    assert report["segments"]["holdout"]["oracle_not_executable"]["net"] > 0


def test_single_position_and_one_run_per_setup(db):
    _context_edge(db)
    _save(db, "a2", "python:a", _trades(100, 9, lambda i: 3.0))                      # repetição do mesmo robô
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["policy"]["runs"] == ["a", "b"]                                    # o run repetido não conta
    with pytest.raises(ValueError, match="um run por robô"):
        cp.study(db, None, "X", "M15", runs=["a", "a2", "b"], dims=("period",))
    overlap = _trades(100, 9, lambda i: 3.0 if i % 4 else -1.0, minute=10)           # entra com a posição de A aberta
    _save(db, "c", "python:c", overlap)
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20, top=3)
    assert report["segments"]["holdout"]["blocked_position_open"] > 0


def test_invalid_requests_are_refused(db):
    _save(db, "a", "python:a", _trades(50, 9, lambda i: 1.0))
    with pytest.raises(ValueError, match="dois setups"):
        cp.study(db, None, "X", "M15")
    _save(db, "b", "python:b", _trades(50, 14, lambda i: 1.0))
    with pytest.raises(ValueError, match="piso"):
        cp.study(db, None, "X", "M15", min_profit_factor=0.5)
    with pytest.raises(ValueError, match="frações"):
        cp.study(db, None, "X", "M15", fractions=(0.5, 0.5, 0.5))
    with pytest.raises(ValueError, match="Dimensão"):
        cp.study(db, None, "X", "M15", dims=("lua",))


def test_cli_router(db, tmp_path, capsys):
    import json
    from mt5_extracao import cli
    _context_edge(db)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n[TRADE_ANALYTICS]\nmin_trades_cell = 15\n",
                   encoding="utf-8")
    code = cli.main(["--config", str(cfg), "--json", "trades", "router", "--symbol", "X", "--timeframe", "M15"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == cli.EXIT_OK and out["data"]["router"]["status"] in ("elegível", "reprovada", "sem candidato elegível")
    assert out["data"]["router"]["min_profit_factor"] == 2.0
