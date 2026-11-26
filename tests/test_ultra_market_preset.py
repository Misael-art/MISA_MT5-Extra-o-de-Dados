from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json

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


def test_market_parameters_cover_registry_and_reject_invalid_types():
    from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
    from mt5_extracao.laboratory import ultra_market_parameters as params
    from mt5_extracao.laboratory.ultra_catalog import MARKET_NATIVE

    assert params.INPUTS.keys() == MARKET_NATIVE.keys()
    for key, mapping in params.INPUTS.items():
        assert mapping.keys() == STRATEGY_PARAMETERS[key].keys()
    assert params.edits({"ema_atr": {"fast": 10}, "vwap_bands": {"volume_source": 1}}) == {
        "ema_atr": {"InpFastPeriod": "10"}, "vwap_bands": {"InpVolumeSource": "1"}}
    with pytest.raises(ValueError, match=r"parameters\.ema_atr\.fast"):
        params.edits({"ema_atr": {"fast": True}})
    with pytest.raises(ValueError, match=r"parameters\.madrid\.period_step"):
        params.validate_effective("madrid", {"InpFirstPeriod": "20", "InpPeriodStep": "110"})


def test_research_selects_by_net_with_strategy_uniqueness_and_can_hold_cash():
    from mt5_extracao.laboratory.ultra_market_research import select

    trials = [dict(id="first", strategy="ema_atr", index=0, net_cents=-1934, cycles=4),
              dict(id="next", strategy="ema_atr", index=1, net_cents=-1940, cycles=5),
              dict(id="other", strategy="orb", index=2, net_cents=-1934, cycles=2),
              dict(id="thin", strategy="pullback", index=3, net_cents=100, cycles=0)]
    assert select(dict(max_modules=2, min_cycles=1, min_net_cents=-2000), trials) == ["first", "other"]
    assert [t["decision"] for t in trials] == ["selected", "another_candidate_selected", "selected", "too_few_cycles"]
    assert select(dict(max_modules=2, min_cycles=1, min_net_cents=0), trials) == []


def test_local_risk_counts_partial_entry_order_once_and_carries_cycle_costs_across_day():
    from mt5_extracao.laboratory.ultra_market_portfolio_audit import _local_expected

    def deal(ticket, magic, owner_position, order, second, entry, steps, profit=0, commission=0):
        return dict(ticket=ticket, owner=magic, position=owner_position, order=order, msc=second * 1000,
                    entry="DEAL_ENTRY_" + entry, steps=steps, profit=profit, commission=commission, fee=0, swap=0)

    rows = [deal(2, 100, 2, 2, 86400, "IN", 4, commission=-20),
            deal(3, 100, 2, 2, 86401, "IN", 6, commission=-30),
            deal(4, 200, 4, 3, 86402, "IN", 1, commission=-100),
            deal(5, 100, 2, 4, 86403, "OUT", 5, profit=10),
            deal(6, 100, 2, 5, 172801, "OUT", 5, profit=20),
            deal(7, 100, 7, 6, 172802, "IN", 1)]
    assert _local_expected(rows, 100, 86400, 5, {"max_attempts": 1}) == (-40, 1, 0, "local_attempts")
    assert _local_expected(rows, 200, 86400, 5, {"daily_loss_cents": 100}) == (-100, 1, 0, "local_daily_loss")
    assert _local_expected(rows, 100, 172800, 6, {"max_losses": 1}) == (20, 0, 1, "local_losses")
    assert _local_expected(rows, 100, 172800, 7, {"daily_profit_cents": 20}) == (20, 1, 1, "local_daily_profit")


def test_signal_comparison_reads_native_journal_and_checks_its_hash(tmp_path):
    from mt5_extracao.laboratory.ultra_market_signals import compare

    journal = tmp_path / "agent.log"
    marker = portfolio.MARKER
    journal.write_text(
        f"{marker}RATE|1794787200|1.10000|1.10010|1.09990|1.10000|2|0\n"
        f"{marker}RATE|1794787260|1.10000|1.10030|1.10000|1.10020|3|0\n"
        f"{marker}RAW|1|1794787320001|1794787320|1794787200|1794787260|2|1|0|0|0|1.0999|1.1001|1.1005|1.0999\n"
        f"{marker}BAR|1|1794787320001|1794787320|1794787260|1|0|0|0|1.0999|1.1001|1.1005|1.0999\n",
        encoding="utf-16",
    )
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(dict(attempt="a", preset_sha256="b", journal=journal.name,
                                       journal_sha256=sha256(journal.read_bytes()).hexdigest(),
                                       slots=[dict(key="orb", magic=1, inputs=dict(InpRangeMinutes="1",
                                                   InpMinRangeAtr="0", InpTargetMult="1.5"))],
                                       comparison=dict(units=dict(tick_size="0.00001")))))
    result, = compare(receipt, strategies=["orb"])["results"]
    assert result["raw_matches"] == result["operable_matches"] == result["raw_level_matches"] == 1
    assert result["first_divergence"] == {}
    journal.write_bytes(journal.read_bytes() + b"xx")
    with pytest.raises(ValueError, match="journal_sha256"):
        compare(receipt)


def test_bollinger_accepts_roundoff_at_band_but_respects_price_ticks(monkeypatch):
    import pandas as pd

    from mt5_extracao.strategies import get_strategy, indicators

    close = pd.Series([1.10000, 1.10001, 1.09999])
    frame = pd.DataFrame(dict(time=pd.date_range("2026-11-16", periods=3, freq="min"),
                              open=close, high=close + 0.00005, low=close - 0.00005, close=close))
    monkeypatch.setattr(indicators, "bollinger", lambda *args: pd.DataFrame(
        dict(mid=1.100000000000001, upper=frame.high + 1e-15, lower=frame.low - 1e-15), index=frame.index))
    monkeypatch.setattr(indicators, "rsi", lambda *args: pd.Series([91., 90., 91.]))
    monkeypatch.setattr(indicators, "adx", lambda *args: pd.DataFrame(dict(adx=[19., 19., 20.])))
    signals = get_strategy("bb_rsi").gerar_sinais(frame)
    assert list(signals["exit_long"]) == [True, True, False]
    assert list(signals["exit_short"]) == [True, False, True]
    # O aquecimento do ATR mantém o sinal de entrada bruto fora do passe operável.
    from mt5_extracao.laboratory.ultra_market_signals import _raw
    assert list(_raw(get_strategy("bb_rsi"), frame)["entry_short"]) == [True, False, False]


def test_squeeze_keeps_roundoff_neutral_before_testing_momentum_crosses(monkeypatch):
    import pandas as pd

    from mt5_extracao.strategies import get_strategy, indicators

    close = pd.Series([1.10001, 1.1 - 1e-15, 1.1 + 1e-15, 1.09999, 1.10001])
    frame = pd.DataFrame(dict(time=pd.date_range("2026-11-16", periods=5, freq="min"),
                              open=close, high=close + 0.00010, low=close - 0.00010, close=close))
    bands = pd.DataFrame(dict(mid=1.1, upper=1.10002, lower=1.09998), index=frame.index)
    monkeypatch.setattr(indicators, "bollinger", lambda *args: bands)
    signals = get_strategy("squeeze").gerar_sinais(frame)
    assert list(signals["exit_long"]) == [False, False, False, True, False]
    assert list(signals["exit_short"]) == [False, False, False, False, True]
