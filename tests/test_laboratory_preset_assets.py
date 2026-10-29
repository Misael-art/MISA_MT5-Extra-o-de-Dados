"""LAB-06: mesma revisão gravada em ativos diferentes, sem disparar backtest."""
import json

import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import preset_assets
from mt5_extracao.robots import store


def _composition(symbol, fast=20):
    return dict(composition_schema_version=1, strategy=dict(key="ema_atr", params=dict(fast=fast)),
                context=dict(symbol=symbol, timeframe="M5"), risk=dict(mode="fixed_fraction", params=dict(fraction=0.01)))


def _run(run_id, symbol, created_at, *, fast=20, composition=None, initial=1000.0, equity=None,
         costs=None, currency="USD", composition_hash=None):
    composition = _composition(symbol, fast) if composition is None else composition
    metrics = dict(composition=composition, composition_hash=composition_hash or f"full-{symbol}-{fast}",
                   initial_equity=initial, currency=currency, real_spread=False, risk_per_trade=0.01,
                   instrument_context=dict(currency_profit=currency, point=0.00001, tick_value=1.0))
    if costs is not None:
        metrics["costs"] = costs
    return dict(run_id=run_id, robot_key="python:qa", source="backtest", symbol=symbol, timeframe="M5",
                data_start="2025-01-01", data_end="2025-02-01", bars=100, params=dict(fast=fast),
                metrics=metrics, equity=equity if equity is not None else [["2025-01-01", initial], ["2025-02-01", initial * 1.1]],
                trades=[], created_at=created_at)


def test_same_revision_normalizes_assets_and_latest_run_is_not_the_proof(tmp_path, monkeypatch):
    db = DatabaseManager(db_path=str(tmp_path / "preset.sqlite"))
    store.save_run(db, _run("old-eur", "EURUSD", "2025-03-01 00:00:00", initial=1000, costs=dict(commission=2.5, swap=0)))
    store.save_run(db, _run("gbp", "GBPUSD", "2025-03-02 00:00:00", initial=5000, currency="GBP",
                            equity=[["2025-01-01", 5000], ["2025-02-01", 5500]]))
    store.save_run(db, _run("new-eur", "EURUSD", "2025-03-03 00:00:00", fast=30))
    monkeypatch.setattr(store, "save_run", lambda *args, **kwargs: pytest.fail("a leitura não grava run"))
    report = preset_assets.compare(db, "python:qa")
    assert report["opened_backtest"] is False and report["latest_per_asset_is_proof"] is False
    assert report["latest_runs_share_body"] is False
    matched = report["groups"][0]
    assert matched["symbols"] == ["EURUSD", "GBPUSD"]
    assert {row["run_id"] for row in matched["runs"]} == {"old-eur", "gbp"}
    assert all(row["fraction"][0] == pytest.approx(1) and row["fraction"][-1] == pytest.approx(1.1)
               for row in report["curves"])
    eur = next(row for row in matched["runs"] if row["run_id"] == "old-eur")
    gbp = next(row for row in matched["runs"] if row["run_id"] == "gbp")
    assert eur["costs"]["commission"] == 2.5 and gbp["costs"] is None
    assert eur["spec"]["point"] == 0.00001 and gbp["spec"]["currency"] == "GBP"
    assert "moedas não convertidas" in matched["scale"]
    other = next(item for item in report["groups"] if item is not matched)
    assert other["runs"][0]["run_id"] == "new-eur"
    assert "não é a prova" in report["notice"]
    db.close()


def test_missing_composition_stays_out_and_empty_database_does_not_invent_runs(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "empty.sqlite"))
    assert "não dispara backtest" in preset_assets.compare(db, "python:qa")["reason"]
    bare = _run("bare", "EURUSD", "2025-03-01 00:00:00")
    bare["metrics"].pop("composition")
    store.save_run(db, bare)
    report = preset_assets.compare(db, "python:qa")
    assert report["groups"] == [] and report["unproven"][0]["why"].startswith("composição ausente")
    assert report["curves"] == []
    db.close()


def test_cli_reads_the_same_report_without_a_database_of_its_own(tmp_path, monkeypatch, capsys):
    from mt5_extracao import cli
    db = DatabaseManager(db_path=str(tmp_path / "cli.sqlite"))
    store.save_run(db, _run("eur", "EURUSD", "2025-03-01 00:00:00"))
    store.save_run(db, _run("jpy", "USDJPY", "2025-03-01 00:00:01", currency="JPY", initial=100000,
                            equity=[["2025-01-01", 100000], ["2025-02-01", 110000]]))
    monkeypatch.setattr(cli.Context, "db", property(lambda _: db))
    assert cli.main(["--json", "laboratory", "preset-assets", "--robot", "python:qa"]) == 0
    payload = json.loads(capsys.readouterr().out)["data"]
    assert payload == preset_assets.compare(db, "python:qa")
    assert payload["opened_backtest"] is False and len(payload["curves"]) == 2
    db.close()
