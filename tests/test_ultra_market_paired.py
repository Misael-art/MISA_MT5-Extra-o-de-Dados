from decimal import Decimal
import pytest

from mt5_extracao.laboratory import ultra_market_paired as paired
from mt5_extracao.laboratory.ultra_pattern_native import _predicate


def test_paired_choice_requires_gain_over_both_ablations_and_original():
    p = dict(min_cycles=4, min_net_cents=1, max_equity_drawdown_pct=5)
    arms = {k: dict(cycles=5, equity_drawdown_pct=2, net_cents=n) for k,n in
            zip(("signal", "companion", "static", "filtered_signal", "conditional"), (3, 5, -1, 1, 8))}
    chosen, reasons = paired.choose(arms, p)
    assert chosen == "conditional"
    assert "no_increment_over_signal" in reasons["filtered_signal"]
    arms["conditional"]["equity_drawdown_pct"] = 6
    assert paired.choose(arms, p)[0] == "companion"
    for arm in arms.values():
        arm["net_cents"] = 0
    assert paired.choose(arms, p)[0] == "cash"


def test_ablation_distinguishes_rejected_capital_from_absent_signal():
    base = dict(net_cents=110, simultaneous=1, equity_drawdown_pct=3, profit_factor=None,
                entries=[[11, 20, "buy"], [11, 30, "buy"]], refusals=[])
    full = dict(base, net_cents=150, simultaneous=2, entries=[[11, 30, "buy"], [22, 50, "sell"]],
                refusals=[[11, 20, "buy", "orçamento global excedido"]])
    diff = paired.marginal(full, base)
    assert (diff["net_cents"], diff["profit_factor"], diff["capital_vetoes"]) == (40, None, 1)
    assert diff["displaced"][0]["tick_msc"] == 20
    assert paired.marginal(dict(full, refusals=[]), base)["capital_vetoes"] == 0


def test_reversal_exports_fraction_on_price_grid():
    left = dict(op="percent_change", value=dict(op="field", name="close"), reference=dict(op="field", name="prior_close"))
    node = dict(op="compare", comparison="lte", left=left, right=dict(op="literal", value="-0.0002", unit="fraction"))
    assert _predicate(node, Decimal(".00001")) == "PatternChange(closing,prior,-1,5000,1)"
    node["right"]["value"] = "-0.00000000001"
    with pytest.raises(ValueError, match="fração"):
        _predicate(node, Decimal(".00001"))


def test_runtime_revision_does_not_reorder_owners_or_relax_their_risk():
    from mt5_extracao.experiments.schema import digest
    from mt5_extracao.laboratory.ultra_market_preset import PORTABLE
    from mt5_extracao.robots.ultra_market_delivery import instruction
    from unittest.mock import patch

    def module(key, magic):
        inputs = {"InpRangeMinutes": "30"} if key == "orb" else {}
        contract = dict(inputs=inputs)
        return dict(key=key, magic=magic, inputs=inputs, preset_sha256=digest([key, contract]),
                    inputs_sha256=digest(contract))
    a, b = module("orb", 1), module("pullback", 2)
    base = dict(schema=PORTABLE, preset_sha256="a"*64, version=1,
                profile=dict(modules=[a,b], market="cash USD", portfolio_cap_pct="1.0"))
    successor = dict(base, version=2, parent_sha256=base["preset_sha256"], research=None)
    def revise(modules):
        return dict(successor, profile=dict(base["profile"], modules=modules))
    with patch("mt5_extracao.robots.ultra_market_delivery.preset.read", side_effect=lambda x: x):
        _, rows = instruction(base, revise([a]), None, 100)
        assert [r[1] for r in rows] == [1,0]
        with pytest.raises(ValueError, match="prioridade"):
            instruction(base, revise([b,a]), None, 100)
        with pytest.raises(ValueError, match="titularidade"):
            instruction(base, revise([dict(a, owner_rules={"risk": {"max_losses": 2}})]), None, 100)


def test_reserve_is_rejected_before_opening_any_quote_block(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    plan = json.loads((Path(__file__).resolve().parents[1] / "docs/repasse/perfis/EURUSD/reversal-m1.json").read_text())
    a, b = plan["economic_windows"]["reserve"]
    (tmp_path / "manifest.json").write_text(json.dumps({"time": {"start_inclusive": a, "end_exclusive": b}}))
    monkeypatch.setattr(paired.ultra_market, "verify", lambda source: pytest.fail("leu blocos da reserva"))
    with pytest.raises(ValueError, match="desenvolvimento"):
        paired.run(None, tmp_path, plan, tmp_path / "resultado", timeout=60, progress=None)
