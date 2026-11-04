"""P07: consumidores consultam o mesmo recibo P06, sem nova avaliação."""

from datetime import datetime, timedelta, timezone
import json

import pytest

from mt5_extracao import cli, experiments, sdk
from mt5_extracao.analytics.pattern_flow import evaluate_registered_attempt
from mt5_extracao.analytics.pattern_families import compile_family
from mt5_extracao.dashboard import generator
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory.pattern_report import render
from tests.test_pattern_campaign import campaign, source
from tests.test_backtester import bars


def persisted(db):
    body, _, _ = campaign(db)
    definition = compile_family("reversal", {"drop_fraction": "0.013"},
                                symbol="SYN", timeframe="M1", clock_version="UTC_fixed_v1",
                                calendar_version="synthetic_utc_day", cost_model_id="qa_cost", horizon_bars=1)
    body["grid"] = [definition.definition_hash]
    protocol = experiments.create_pattern_campaign(db, body)["protocol_id"]
    attempt = experiments.register_pattern_attempt(db, protocol, "consumer", definition)
    manifest = body["roster"][0]["data_manifest_id"]
    result = evaluate_registered_attempt(db, attempt["request_id"], manifest,
                                         session_mode="synthetic_utc_day")
    return protocol, attempt["request_id"], manifest, result


def test_sdk_cli_html_and_dashboard_read_persisted_exploratory_receipt(source, tmp_path, monkeypatch, capsys):
    db, path = source
    protocol, request, manifest, result = persisted(db)
    cut = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
    expected = sdk.pattern_evaluations(db, protocol_id=protocol, symbol="SYN", timeframe="M1", as_of=cut)
    assert len(expected) == 1 and expected[0]["receipt"] == result["receipt"]
    assert sdk.pattern_evaluations(db, as_of="2025-01-06T10:00:00Z") == []
    detail = sdk.pattern_evaluation(db, protocol, request, manifest, as_of=cut, include_facts=True)
    assert detail["receipt"] == expected[0]["receipt"]
    assert set(detail["facts"]) == {"signal", "outcome"}
    assert "Exploratória" in render(expected)
    assert "Nenhuma avaliação" in render([])
    config = tmp_path / "qa.ini"
    config.write_text("[APP]\nbroker_utc_offset=0\n", encoding="utf-8")
    monkeypatch.setattr(cli.Context, "db", property(lambda _: db))
    html = tmp_path / "patterns.html"
    assert cli.main(["--config", str(config), "--json", "laboratory", "pattern-evaluations",
                     "--protocol", protocol, "--as-of", cut, "--html", str(html)]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["evaluations"] == expected
    assert request in html.read_text(encoding="utf-8")
    assert cli.main(["--config", str(config), "--json", "laboratory", "pattern-evaluation",
                     "--protocol", protocol, "--request", request, "--manifest", manifest,
                     "--as-of", cut]) == 0
    assert json.loads(capsys.readouterr().out)["data"]["receipt"] == result["receipt"]
    snapshot = generator.generate(db, symbol="SYN", timeframe="M1", as_of=cut)
    section = next(row for row in snapshot["sections"] if row["id"].startswith("hypothesis:"))
    assert section["source_status"] == "EXPLORATORY" and not section["default_visible"]
    assert section["evidence"]["statistical_approval"] is False
    assert section["source_refs"][0]["data_manifest_id"] == manifest
    generator.validate(snapshot)
    reopened = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    try:
        assert sdk.pattern_evaluations(reopened, protocol_id=protocol, as_of=cut) == expected
    finally:
        reopened.close()


def test_query_isolated_by_base_and_cut(source, tmp_path):
    db, _ = source
    protocol, request, manifest, result = persisted(db)
    other = DatabaseManager(db_path=str(tmp_path / "other.sqlite"), broker_utc_offset=0)
    frame = bars([[100, 101, 99, 100]] * 15, start="2025-01-06 09:00", freq="min")
    frame["tick_volume"], frame["real_volume"] = 10, 0
    other.save_ohlcv_data("SYN", "1 minuto", frame)
    try:
        assert sdk.pattern_evaluations(other) == []
        with pytest.raises(ValueError, match="não encontrada"):
            sdk.pattern_evaluation(other, protocol, request, manifest)
        other_protocol, _, _, other_result = persisted(other)
        assert other_result["receipt"]["source_namespace"] != result["receipt"]["source_namespace"]
        assert sdk.pattern_evaluations(other, protocol_id=other_protocol)[0]["source_namespace"] != (
            sdk.pattern_evaluations(db, protocol_id=protocol)[0]["source_namespace"])
        with pytest.raises(ValueError, match="fuso"):
            sdk.pattern_evaluations(db, as_of="2025-01-06 10:00")
    finally:
        other.close()
