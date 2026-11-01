"""DASH-10: modo Robô sobre resultados persistidos; nada é executado."""
from datetime import datetime, timedelta, timezone

import pytest

from mt5_extracao.analytics import datasets
from mt5_extracao.dashboard import document, generator
from mt5_extracao.robots import store

LATER = datetime.now(timezone.utc) + timedelta(days=1)


@pytest.fixture
def db(tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    manager = DatabaseManager(db_path=str(tmp_path / "qa.sqlite"))
    datasets.identity(manager)
    store.save_record(manager, "qa.robot", "PY", "Robô QA", stage="bateria")
    yield manager
    manager.close()


def run(run_id, net=125.0, symbol="QA", tf="M5", created=None):
    return {"run_id": run_id, "robot_key": "qa.robot", "source": "python", "symbol": symbol, "timeframe": tf,
            "data_start": "2026-01-01 00:00:00", "data_end": "2026-06-30 23:55:00", "bars": 1000,
            "params": {"p": 1}, "equity": [["2026-01-01", 10000.0], ["2026-03-01", 10050.0], ["2026-06-30", 10000 + net]],
            "metrics": {"net_profit": net, "profit_factor": 1.3, "trades": 40, "max_drawdown_pct": 0.08,
                        "currency": "USD"},
            "created_at": created or datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}


def sections(snapshot):
    return {s["id"]: s for s in snapshot["sections"]}


def test_latest_run_is_selected_explicitly_and_others_not_aggregated(db):
    store.save_run(db, run("r-old", net=-50.0, created="2026-01-02 10:00:00.000000"))
    store.save_run(db, run("r-new", net=125.0))
    snapshot = generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat())
    generator.validate(snapshot)
    assert snapshot["subject"]["run_id"] == "r-new"
    assert snapshot["subject"]["run_selection"] == "latest_run_known_at_cut"
    found = sections(snapshot)
    assert found["robot:run"]["visual"]["value"] == 125.0                 # não somado ao r-old
    assert found["robot:run"]["evidence"]["other_runs_aggregated"] is False
    assert found["robot:run"]["evidence"]["is_realized_pnl"] is False
    assert [r["run_id"] for r in found["robot:other-runs"]["evidence"]["runs"]] == ["r-old"]
    assert not found["robot:other-runs"]["default_visible"]
    assert "regime" in found                                               # contexto do ativo/TF da execução


def test_explicit_run_is_respected(db):
    store.save_run(db, run("r-old", net=-50.0, created="2026-01-02 10:00:00.000000"))
    store.save_run(db, run("r-new"))
    snapshot = generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat(), run_id="r-old")
    assert snapshot["subject"]["run_selection"] == "explicit_run_id"
    assert sections(snapshot)["robot:run"]["visual"]["value"] == -50.0


def test_robot_without_result_stays_selectable(db):
    snapshot = generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat())
    generator.validate(snapshot)
    section = sections(snapshot)["robot:run"]
    assert section["default_visible"] and "Sem resultado neste recorte" in section["details"]["reason"]
    assert snapshot["capabilities"]["robots"]["state"] == "NO_DATA"


def test_run_after_cut_is_not_used(db):
    store.save_run(db, run("r-new"))
    snapshot = generator.generate_robot(db, robot_key="qa.robot", as_of=datetime.now(timezone.utc).isoformat())
    assert snapshot["subject"]["run_id"] is None and snapshot["provenance"]["robot_late_runs"] == 1
    with pytest.raises(ValueError, match="RUN_NOT_AT_CUT"):
        generator.generate_robot(db, robot_key="qa.robot", as_of=datetime.now(timezone.utc).isoformat(), run_id="r-new")


def test_battery_is_labelled_battery_not_hypothesis(db):
    store.save_run(db, run("r-new"))
    store.save_check(db, {"check_id": "c1", "robot_key": "qa.robot", "preset": "p", "approved": True,
                          "allows_real": False, "static_status": "ok", "statistical_status": "aprovada",
                          "run_id": "r-new", "items": [], "created_at": "2026-01-03 10:00:00"})
    section = sections(generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat()))["robot:battery"]
    assert section["source_status"] == "BATTERY_APPROVED" and section["status_namespace"] == "robot"
    assert section["evidence"]["battery_is_hypothesis"] is False and section["evidence"]["allows_real"] is False


def test_unknown_robot_and_bad_context_are_explicit(db):
    with pytest.raises(ValueError, match="ROBOT_NOT_FOUND"):
        generator.generate_robot(db, robot_key="nao.existe", as_of=LATER.isoformat())
    store.save_run(db, run("r-x", tf="NAO_NATIVO"))
    snapshot = generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat())
    assert snapshot["errors"][0]["code"] == "ROBOT_CONTEXT_UNAVAILABLE"
    assert sections(snapshot)["robot:run"]["visual"]["value"] == 125.0


def test_reading_does_not_write_runs_or_checks(db):
    store.save_run(db, run("r-new"))
    before = len(store.load_runs(db)), len(store.load_checks(db))
    generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat())
    assert (len(store.load_runs(db)), len(store.load_checks(db))) == before


def test_robot_document_renders_identity_in_header(db):
    store.save_run(db, run("r-new"))
    html, _ = document.render(generator.generate_robot(db, robot_key="qa.robot", as_of=LATER.isoformat()))
    assert "Robô QA" in html and "mais recente conhecida no corte" in html


def test_asset_snapshot_still_validates_after_robot_mode():
    snapshot = generator.generate(None, symbol="QA", timeframe="M5", as_of=LATER.isoformat())
    assert generator.validate(snapshot)["subject"]["mode"] == "asset"


def test_python_strategy_without_registry_row_is_selectable(db):
    from mt5_extracao.strategies import REGISTRY
    key = "python:" + next(iter(REGISTRY))
    snapshot = generator.generate_robot(db, robot_key=key, as_of=LATER.isoformat())
    assert snapshot["subject"]["robot_name"] and snapshot["subject"]["run_id"] is None
