from copy import deepcopy
from decimal import Decimal

import pytest

from mt5_extracao.laboratory import ultra_market_preset as preset
from mt5_extracao.robots import ultra_market_portfolio as portfolio


def test_lab_versions_preserve_selection_and_export_uses_frozen_inputs(tmp_path, monkeypatch):
    manifest = {"manifest_hash": "a" * 64, "time": {"start_msc": 0, "end_msc": 86400000},
                "coverage": {"last_msc": 17, "rows": 2}}
    monkeypatch.setattr(preset.ultra_market, "verify", lambda source: manifest)
    monkeypatch.setattr(portfolio, "market_contract", lambda state: {"symbol": "EURUSD"})

    class Sources:
        originals = {}

        def __init__(self):
            self.files = {}

        def write(self, path, data):
            self.files[str(path)] = "e" * 64
            (tmp_path / path).write_text(data)

    def resolve(config, state, *, strategies, pattern, edits, workspace=None, frozen=None):
        assert strategies == ["orb"] and pattern is None and state is manifest
        inputs = {"InpRangeMinutes": edits.get("orb", {}).get("InpRangeMinutes", "30"), "InpRiskPct": "0.5"}
        slot = dict(key="orb", magic=10, priority=0, inputs=inputs, preset_sha256="b" * 64,
                    inputs_sha256="c" * 64, reference="orb", include="owner_0.mqh")
        if frozen is not None:
            assert frozen == [slot]
        profile = dict(market={"symbol": "EURUSD"}, modules=[slot], source_closure_sha256="d" * 64,
                       canonical_files={}, portfolio_cap_pct="1.0")
        return profile, Sources(), "safety"

    monkeypatch.setattr(portfolio, "resolve", resolve)
    monkeypatch.setattr(portfolio, "contract_literal", lambda state, name: "0")
    version1 = preset.freeze(None, None, tmp_path / "v1.json", strategies=["orb"],
                             parameters={"orb": {"range_minutes": 15}})
    version2 = preset.freeze(None, None, tmp_path / "v2.json", previous=tmp_path / "v1.json",
                             parameters={"orb": {"range_minutes": 45}})
    assert version2["version"] == 2 and version2["parent_sha256"] == version1["preset_sha256"]
    assert version2["profile"]["modules"][0]["inputs"]["InpRangeMinutes"] == "45"
    expert = tmp_path / "portfolio.mq5"
    execution = portfolio.export(None, None, expert, attempt="f" * 32, preset=tmp_path / "v2.json")
    assert execution["preset"] == version2
    assert f'UltraPresetSha="{version2["preset_sha256"]}"' in expert.read_text()
    assert "UltraPresetVersion=2" in expert.read_text()
    with pytest.raises(ValueError, match="não combine"):
        portfolio.export(None, None, expert, attempt="f" * 32, strategies=["orb"], preset=version2)

    altered = deepcopy(version2)
    altered["profile"]["modules"][0]["inputs"]["InpRangeMinutes"] = "30"
    with pytest.raises(ValueError, match="preset_sha256"):
        preset.read(altered)


@pytest.mark.parametrize("inputs", [{"orb": {"range_minutes": True}}, {"orb": {"range_minutes": 0}},
                                     {"orb": {"stop": {"grid": [1, 2]}}}, {"pullback": {"pause": True}}])
def test_preset_rejects_unsupported_edits_before_routing(tmp_path, monkeypatch, inputs):
    monkeypatch.setattr(preset.ultra_market, "verify", lambda source: {})
    with pytest.raises(ValueError, match="parameters"):
        preset.freeze(None, None, tmp_path / "bad.json", parameters=inputs)
    assert not (tmp_path / "bad.json").exists()


def test_order_report_compares_protection_and_volume_by_ticket():
    from mt5_extracao.laboratory.ultra_market_portfolio_audit import _report_orders

    cells = ["2026.11.16 00:06:00", "2", "EURUSD", "buy", "0.33 / 0.33", "0.00000", "1.10214", "",
             "2026.11.16 00:06:00", "filled", "HX"]
    order = "2|20261040|2|1794787560022|0|4|0.33000000|0.00000000|0.00000|1.10214|0.00000".split("|")

    def compare():
        html = "<tr>" + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>"
        return _report_orders(html, {2: order}, "EURUSD", Decimal(".00001"), Decimal(".01"))

    assert compare() == {"status": "report_matches", "orders": 1}
    cells[6] = "1.10215"
    assert compare() == {"status": "diverged", "ticket": 2, "field": "sl", "expected": 110214, "observed": 110215}
