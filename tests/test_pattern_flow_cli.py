"""P06: entrada pública SDK/CLI do mesmo fluxo de campanha e recibo."""

import json

from mt5_extracao import cli, sdk
from mt5_extracao.analytics.pattern_families import compile_family
from tests.test_pattern_campaign import campaign, source


def test_cli_sdk_share_campaign_attempt_and_persisted_evaluation(source, tmp_path, monkeypatch, capsys):
    db, _ = source
    body, _, _ = campaign(db)
    definition = compile_family("reversal", {"drop_fraction": "0.013"},
                                symbol="SYN", timeframe="M1", clock_version="UTC_fixed_v1",
                                calendar_version="synthetic_utc_day", cost_model_id="qa_cost", horizon_bars=1)
    body["grid"] = [definition.definition_hash]
    campaign_file, definition_file = tmp_path / "campaign.json", tmp_path / "definition.json"
    campaign_file.write_text(json.dumps(body), encoding="utf-8")
    definition_file.write_text(json.dumps(definition.export()), encoding="utf-8")
    config = tmp_path / "qa.ini"
    config.write_text("[APP]\nbroker_utc_offset=0\n", encoding="utf-8")
    monkeypatch.setattr(cli.Context, "db", property(lambda _: db))

    def run(*args):
        assert cli.main(["--config", str(config), "--json", "experiments", *args]) == 0
        return json.loads(capsys.readouterr().out)["data"]

    created = run("pattern-create", "--file", str(campaign_file))
    protocol = created["protocol_id"]
    attempt = run("pattern-request", "--protocol", protocol, "--key", "first",
                  "--definition", str(definition_file))
    assert attempt["state"] == "requested"
    manifest = body["roster"][0]["data_manifest_id"]
    result = run("pattern-evaluate", attempt["request_id"], "--manifest", manifest,
                 "--session-mode", "synthetic_utc_day")
    assert result["receipt"]["request_id"] == attempt["request_id"]
    assert result["receipt"] == sdk.evaluate_pattern_attempt(
        db, attempt["request_id"], manifest, session_mode="synthetic_utc_day")["receipt"]
    assert not result["receipt"]["statistical_approval"]
    status = run("pattern-status", protocol)
    assert any(row["kind"] == "pattern_evaluated" for row in status["events"])
