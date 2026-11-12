"""Repetir o mesmo backtest preserva o livro e não aumenta a amostra analítica."""
import configparser
import copy
import json

import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import backtest, catalog, store
from mt5_extracao.trade_analytics import aggregate, context, importers, ledger
from tests.market_data import random_walk


def repeated_runs(db, config):
    db.save_ohlcv_data("QA", "5 minutos", random_walk(600, seed=7, freq="5min", start="2026-01-05 09:00"))
    robot = next(r for r in catalog.python_robots() if r.key == "python:ema_atr")
    original = backtest.run_python(db, config, robot, "QA", "M5")
    trades = ledger.load(db, run_id=original["run_id"])
    assert len(trades) > 0 and original["metrics"]["execution_identity"]["cache_eligible"]

    def add(run_id, *, change=None, rows=None):
        record = copy.deepcopy(original)
        record["run_id"] = run_id
        if change:
            change(record)
        store.save_run(db, record)
        importers.record_python_run(db, record, trades.to_dict("records") if rows is None else rows)
        return record

    add("repeat-a")
    add("repeat-b")
    return original, trades, add


@pytest.fixture
def repeated(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "sample.sqlite"), broker_utc_offset=0)
    config = configparser.ConfigParser(interpolation=None)
    config.read_dict({"APP": {"broker_utc_offset": "0"},
                      "DATABASE": {"path": db.db_path, "type": "sqlite"}})
    original, trades, add = repeated_runs(db, config)
    yield db, config, original, trades, add
    db.close()


def test_proven_repeated_runs_do_not_inflate_lenses_and_remain_consultable(repeated):
    db, config, original, trades, _ = repeated
    lab = aggregate.frame(db, config, symbol="QA")
    assert len(lab) == len(trades)
    assert set(lab.run_id) == {original["run_id"]}
    assert len(ledger.load(db)) == 3 * len(trades)
    raw = aggregate.frame(db, config, symbol="QA", include_repeated_runs=True)
    assert len(raw) == 3 * len(trades)
    report = context.population(lab)
    assert report["repeated_runs_excluded"] == 2
    assert report["repeated_trades_excluded"] == 2 * len(trades)
    assert "repetidas" in context.summary(report)
    selected = aggregate.frame(db, config, run_id="repeat-a")
    assert set(selected.run_id) == {"repeat-a"} and len(selected) == len(trades)


def test_same_params_or_outcome_alone_do_not_prove_equivalence(repeated):
    db, config, _, trades, add = repeated

    def change_code(record):
        record["metrics"]["execution_identity"]["digest"] = "a" * 64

    add("different-code", change=change_code)
    altered = trades.to_dict("records")
    altered[0]["pnl"] += 1
    add("different-result", rows=altered)
    lab = aggregate.frame(db, config)
    assert len(lab) == 3 * len(trades)
    assert set(lab.run_id) >= {"different-code", "different-result"}


def test_unknown_execution_identity_preserves_rows_and_reports_uncertainty(repeated):
    db, config, _, trades, add = repeated
    add("legacy", change=lambda r: r["metrics"].pop("execution_identity"))
    lab = aggregate.frame(db, config)
    assert len(lab) == 2 * len(trades)
    assert context.population(lab)["unqualified_python_runs"] == 1


@pytest.mark.parametrize("version", [None, 2, True, 1.0, "1"])
def test_unknown_or_mistyped_identity_version_preserves_rows(repeated, version):
    db, config, _, trades, add = repeated
    add("legacy-version", change=lambda r: r["metrics"]["execution_identity"].update(version=version))
    lab = aggregate.frame(db, config)
    assert len(lab) == 2 * len(trades)
    assert context.population(lab)["unqualified_python_runs"] == 1


def test_filter_does_not_prove_equivalence_outside_visible_window(repeated):
    db, config, _, trades, add = repeated
    altered = trades.to_dict("records")
    altered[-1]["pnl"] += 1
    add("different-later", rows=altered)
    lab = aggregate.frame(db, config, end=trades.entry_time.max())
    assert "different-later" in set(lab.run_id)
    assert context.population(lab)["repeated_runs_excluded"] == 2


def test_declared_currency_is_part_of_equivalence(repeated):
    db, config, _, trades, add = repeated
    add("different-currency", change=lambda r: r["metrics"].update(currency="USD"))
    assert len(aggregate.frame(db, config)) == 2 * len(trades)


def test_missing_snapshot_reference_cannot_be_hidden_by_grouping(repeated):
    db, config, _, _, _ = repeated
    with db.engine.begin() as connection:
        connection.execute(text("DELETE FROM _dataset_references WHERE run_id=:run"), {"run": "repeat-a"})
    with pytest.raises(ValueError, match="Referência de dados indisponível"):
        aggregate.frame(db, config)


def test_router_keeps_explicit_selection_of_a_repeated_run(repeated):
    from mt5_extracao.presets import conditional_policy

    db, config, _, trades, _ = repeated
    peer = trades.assign(robot_key="python:peer").to_dict("records")
    ledger.save_trades(db, "peer", peer, source="python")
    report = conditional_policy.study(db, config, "QA", "M5", runs=["repeat-a", "peer"])
    assert report["policy"]["runs"] == ["peer", "repeat-a"]


def test_cli_all_runs_is_explicit_and_context_counts_follow_display_filter(repeated, tmp_path, capsys):
    db, config, _, trades, _ = repeated
    path = tmp_path / "qa.ini"
    with path.open("w", encoding="utf-8") as stream:
        config.write(stream)
    args = ["--config", str(path), "--json", "trades", "report", "--symbol", "QA", "--by", "day_of_week"]
    assert cli.main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["data"]["total"]["n"] == len(trades)
    assert cli.main([*args, "--include-repeated-runs"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["data"]["total"]["n"] == 3 * len(trades)
    lab = aggregate.frame(db, config).iloc[:1].copy()
    assert context.population(lab)["repeated_trades_excluded"] == 2
