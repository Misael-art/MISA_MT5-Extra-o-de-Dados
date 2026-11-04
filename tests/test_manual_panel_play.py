"""Painel manual reconhecido sem run; proteções e Experts automáticos preservados."""
import configparser
from dataclasses import replace
from pathlib import Path

import pytest

from mt5_extracao.robots import battery, catalog, controls, live, manual_panel, mql, risk


@pytest.fixture
def manual(tmp_path, monkeypatch):
    path = tmp_path / "Painel.mq5"
    path.write_text("int OnInit(){return 0;}\nvoid OnTick(){}\n")
    fields = (("InpProfile", "ENUM_HDK_PROFILE", "HDK_PROFILE_MODERATE"),
              ("InpStopMode", "ENUM_HDK_STOP_MODE", "HDK_STOP_ATR"),
              ("InpStopAtrMult", "double", "1.5"), ("InpStopPoints", "int", "300"),
              ("InpRiskPct", "double", "1"), ("InpDailyLossPct", "double", "3"),
              ("InpMaxDrawdownPct", "double", "15"))
    robot = catalog.Robot(manual_panel.ROBOT_KEY, catalog.MQL5, "Painel manual", path=str(path), files=[str(path)],
                          inputs=[mql.MqlInput(*item) for item in fields],
                          enums={"ENUM_HDK_PROFILE": {"HDK_PROFILE_CONSERVATIVE": 0, "HDK_PROFILE_MODERATE": 1,
                                                       "HDK_PROFILE_AGGRESSIVE": 2, "HDK_PROFILE_CUSTOM": 3},
                                 "ENUM_HDK_STOP_MODE": {"HDK_STOP_ATR": 0, "HDK_STOP_POINTS": 1}},
                          risk=risk.RiskRating(risk.MODERATE, ["perfil Moderado"], {"mandatory_stop": True}))
    monkeypatch.setattr(manual_panel, "APPROVED_SOURCE_FINGERPRINTS", {catalog.fingerprint(robot)})
    return robot


def test_manual_play_does_not_create_or_require_a_historical_run(manual):
    result = controls.selection(manual, "EURUSD", "H1")
    assert result["manual_panel"] and result["play_available"] and not result["reason"]
    assert result["run"] is None and result["headline"] is None and manual.runs == []
    assert controls.loss_scenario(result["run"]) is None


def test_exception_requires_the_reviewed_source_and_identity(manual):
    assert manual_panel.recognized(manual)
    assert not manual_panel.recognized(replace(manual, key="mql5:automatico"))
    assert not manual_panel.recognized(replace(manual, has_source=False))
    Path(manual.path).write_text("void OnTick(){ /* mudou */ }\n")
    assert not controls.selection(manual, "EURUSD", "H1")["play_available"]


@pytest.mark.parametrize("profile,expected", [(0, (.5, 2, 10)), (1, (1, 3, 15)), (2, (1.5, 4, 20))])
def test_named_limits_match_the_recognized_panel_profiles(manual, profile, expected):
    manual.presets["P"] = {"InpProfile": str(profile), "InpRiskPct": "999"}
    facts = controls.risk_budget(manual, "P", {"balance": 1000, "currency": "USD"})["facts"]
    assert tuple(facts[k] for k in ("risk_pct", "daily_loss_pct", "max_drawdown_pct")) == expected
    assert controls.risk_budget(manual, "P", {"balance": 1000, "currency": "USD"})["amounts"]["risk_pct"] == expected[0] * 10


def test_custom_limits_and_selected_stop_are_not_replaced_by_profile_defaults(manual):
    manual.presets["P"] = {"InpProfile": "3", "InpRiskPct": ".2", "InpDailyLossPct": "1",
                           "InpMaxDrawdownPct": "4", "InpStopMode": "1", "InpStopPoints": "0"}
    facts = controls.risk_budget(manual, "P")["facts"]
    assert facts["risk_pct"] == .2 and facts["daily_loss_pct"] == 1 and facts["max_drawdown_pct"] == 4
    assert facts["mandatory_stop"] is False  # ATR positivo não comprova stop em modo Pontos.


def test_play_keeps_account_stop_limits_static_failures_and_kill_checks(manual, monkeypatch):
    config = configparser.ConfigParser()
    account = {"login": 123, "server": "FakeDemo", "demo": True}
    items = [battery.Item(battery.STATIC, "compile", "Compilação", battery.FAIL, "fonte será compilada na sessão")]
    monkeypatch.setattr(battery, "static_items", lambda *a: items)
    monkeypatch.setattr(catalog, "load_catalog", lambda *a: [manual])
    monkeypatch.setattr(live, "stop_active", lambda *a: False)
    assert controls.play_issues(config, manual, "EURUSD", "H1", None, account) == []
    assert any("Conecte" in x for x in controls.play_issues(config, manual, "EURUSD", "H1", None, None))
    items.append(battery.Item(battery.STATIC, "orders", "Execução insegura", battery.FAIL, "ordem fora da Safety"))
    assert any("Safety" in x for x in controls.play_issues(config, manual, "EURUSD", "H1", None, account))
    items.pop()
    monkeypatch.setattr(live, "stop_active", lambda *a: True)
    assert any("PARAR TUDO" in x for x in controls.play_issues(config, manual, "EURUSD", "H1", None, account))
    monkeypatch.setattr(live, "stop_active", lambda *a: False)
    manual.presets["Inválido"] = {"InpProfile": "3", "InpRiskPct": "0", "InpDailyLossPct": "0"}
    assert any("Proteção" in x for x in controls.play_issues(config, manual, "EURUSD", "H1", "Inválido", account))
    real = dict(account, demo=False)
    assert any("REAL" in x for x in controls.play_issues(config, manual, "EURUSD", "H1", None, real))


def test_automatic_expert_does_not_inherit_manual_exception_from_zero_trades(manual):
    robot = replace(manual, key="mql5:automatico", runs=[])
    assert not controls.selection(robot, "EURUSD", "H1")["play_available"]
    assert "backtest" in controls.selection(robot, "EURUSD", "H1")["reason"]
