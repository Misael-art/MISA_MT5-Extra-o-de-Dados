"""Catálogo de Estudo pela mesma sessão/canal; sem Tk, banco owner ou MT5 real."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

from mt5_extracao.automation import charts, open_workflow
from mt5_extracao.robots import sessions
from tests.test_automation_charts import channel, respond
from tests.test_robot_controls import robot


@pytest.mark.parametrize("ident", ["native:0", "native:8", "native:23", "native:999"])
def test_native_id_remains_text_without_guessing_remote_catalogue_size(ident):
    cells = charts.validate("study.indicator.add", {"catalog_id": ident, "chart": "9223372036854775807"})
    assert cells == ["study.indicator.add", "9223372036854775807", ident, "", "", "", "", ""]


@pytest.mark.parametrize("ident", [None, 8, True, "8", "native:-1", "native:08", "native:1000",
    "native:８", "native:8\n", "native:8\t", "feature:rsi", "native:../8", "native:"])
def test_untyped_ambiguous_or_foreign_ids_refuse_before_native_effect(ident):
    with pytest.raises(ValueError, match="catalog_id"):
        charts.validate("study.indicator.add", {"catalog_id": ident})


@pytest.mark.parametrize("method,params", [
    ("study.catalog", {"chart": "0"}), ("study.catalog", {"catalog_id": "native:8"}),
    ("study.indicator.add", {"catalog_id": "native:8", "period": 5}),
    ("study.indicator.add", {"catalog_id": "native:8", "mean": 1}),
    ("study.indicator.add", {"catalog_id": "native:8", "parameters": {"deviation": 4}}),
])
def test_catalogue_defaults_are_explicit_not_silently_overridden(method, params):
    with pytest.raises(ValueError):
        charts.validate(method, params)


def test_catalogue_and_add_share_request_id_ack_and_ownership(channel):
    ch, root = channel
    calls = []
    def native(cells):
        calls.append(cells)
        if cells[1] == "study.catalog":
            return {"data": {"available": True, "catalog_sha256": "f" * 64,
                "entries": [{"catalog_id": "native:8", "name": "Bands(20,2)"}]}}
        assert cells[1:4] == ["study.indicator.add", "0", "native:8"]
        return {"data": {"catalog_id": "native:8", "indicator": "3", "parameters_status": "observed_catalog_defaults",
            "inputs_verified": False, "buffer_values": [{"index": 0, "copied": 1, "value": 1.1}]}}
    w = open_workflow(None, {})
    w.controller.jobs[ch.session.id] = SimpleNamespace(visible=True, mode="play")
    w.controller.channels[ch.session.id] = ch
    for request in [
        dict(id="catalog", method="study.catalog", params={"session": ch.session.id}),
        dict(id="bands", method="study.indicator.add", params={"session": ch.session.id, "catalog_id": "native:8"}),
    ]:
        t = threading.Thread(target=respond, args=(ch, root, native)); t.start()
        result = w.call(request); t.join()
        assert result["ok"] and w.call(request) == result
    assert len(calls) == 2 and result["data"]["indicator"] == "3"
    # Nunca encaminha um gráfico/sessão de terceiro.
    assert not w.call(dict(id="foreign", method="study.catalog", params={"session": "foreign"}))["ok"]


@pytest.mark.parametrize("mode,visible", [("backtest", False), ("play", False)])
def test_study_refuses_nonvisual_session_before_writing(mode, visible):
    w = open_workflow(None, {})
    w.controller.jobs["own"] = SimpleNamespace(visible=visible, mode=mode)
    result = w.call(dict(id="one", method="study.catalog", params={"session": "own"}))
    assert not result["ok"] and "não visual" in result["errors"][0]


def test_catalogue_absent_is_an_explicit_capability_not_a_fabricated_list(channel):
    ch, root = channel
    t = threading.Thread(target=respond, args=(ch, root, {"data": {
        "available": False, "entries": [], "reason": "Expert sem catálogo na closure"}})); t.start()
    data = ch.request("study.catalog", {}, timeout=2); t.join()
    assert data["available"] is False and data["entries"] == []


def test_wrapper_hashes_actual_catalogue_bytes_only_for_opt_in(robot, tmp_path):
    path = tmp_path / "HDK_StudyCatalog.mqh"
    original = b"// canonical existing header\r\n#define HDK_STUDY_CATALOG_MQH\r\n"
    path.write_bytes(original); robot.files.append(str(path))
    before = sessions.telemetry_source(robot, "state.txt")
    wrapped = sessions.telemetry_source(robot, "state.txt", chart_control="b" * 32)
    digest = hashlib.sha256(original).hexdigest()
    assert '#define MT5X_AGENT_STUDY_HASH "' + digest + '"' in wrapped
    assert "MT5X_AgentStudyCatalog" not in before and path.read_bytes() == original
    assert "HDKStudyNativeAcquire(index,ChartSymbol(chart),ChartPeriod(chart))" in wrapped
    assert "@STUDY_HELPERS@" not in wrapped and "@SESSION@" not in wrapped
    assert "IndicatorRelease(" not in charts.source("b" * 32)


def test_package_contains_native_study_resource_and_discovers_new_methods():
    root = Path(__file__).resolve().parents[1]
    assert (root / "mt5_extracao/automation/native_studies.mqh").is_file()
    from mt5_extracao.automation.service import Workflow
    methods = {m["method"]: m for m in Workflow.methods()["methods"]}
    assert methods["study.catalog"]["required"] == ["session"]
    assert methods["study.indicator.add"]["required"] == ["session", "catalog_id"]
    assert '"automation/*.mqh"' in (root / "pyproject.toml").read_text()
    with pytest.raises(ValueError):
        charts.source("b" * 32, study_catalog_sha256="not a hash")
