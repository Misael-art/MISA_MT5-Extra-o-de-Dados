"""T19.2: contrato separado, compatibilidade real e delegação ao motor legado."""
import json
from dataclasses import replace

import pandas as pd
import pytest

from mt5_extracao.composition import (BaseAdapter, Capabilities, Composition, CompositionError,
                                      base_composition, compatibility)
from mt5_extracao.composition.registry import BASE_MODES, STRATEGY_PARAMETERS
from mt5_extracao.composition.schema import canonical_json
from mt5_extracao.robots import catalog, contract
from mt5_extracao.strategies import REGISTRY, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec, run_backtest
from tests.market_data import random_walk
from tests.test_robots_contract import make_robot
from tests.test_strategies import intraday


def base(**kwargs):
    return base_composition("ema_atr", symbol="EURUSD", timeframe="H1", **kwargs)


def change_path(data, path, value):
    parts = path.split(".")
    for key in parts[:-1]:
        data = data[key]
    data[parts[-1]] = value


def caps_for(robot):
    return {"capabilities_schema_version": 1,
            "robot": {"key": robot.key, "version": robot.version, "fingerprint": catalog.fingerprint(robot)},
            "backend": "mql5", "strategy_key": "ema_atr",
            "modules": {**{slot: [mode] for slot, mode in BASE_MODES.items()}, "filters": []}, "bindings": {
                "strategy.params.fast": {"input": "Periodo", "type": "int", "unit": "bar"}}}


def test_canonical_roundtrip_hash_and_immutable_snapshot():
    original = base(params={"fast": 10}).to_dict()
    value = Composition(original)
    reordered = {k: original[k] for k in reversed(original)}
    assert Composition(reordered).config_hash == value.config_hash
    assert Composition(value.to_json()).to_dict() == original
    assert len(value.config_hash) == 64
    assert canonical_json({"b": True, "a": "ação"}) == '{"a":"ação","b":true}'
    original["strategy"]["params"]["fast"] = 15
    copy = value.to_dict()
    copy["strategy"]["params"]["fast"] = 16
    assert value.to_dict()["strategy"]["params"]["fast"] == 10
    assert base(params={"fast": 11}).config_hash != value.config_hash
    assert base_composition("ema_atr", symbol="EURUSD", timeframe="M5", params={"fast": 10}).config_hash != value.config_hash


@pytest.mark.parametrize("path,value,reason", [
    ("composition_schema_version", 2, "versão desconhecida"),
    ("composition_schema_version", True, "versão desconhecida"),
    ("composition_schema_version", "1", "versão desconhecida"),
    ("extra", 1, "desconhecidos"),
    ("strategy.params.fast", True, "int finito"),
    ("strategy.params.fast", 20.0, "int finito"),
    ("strategy.params.fast", 0, "int finito"),
    ("strategy.params.fast", "20", "int finito"),
    ("strategy.params.fast", 10**400, "finito"),
    ("strategy.params.stop_atr", float("nan"), "finito"),
    ("strategy.params.stop_atr", float("inf"), "finito"),
    ("strategy.params.stop_atr", -1, "valor inválido"),
    ("strategy.params.extra", 5, "desconhecidos"),
    ("strategy.key", "S09", "estratégia desconhecida"),
    ("context.timeframe", "Renko", "contrato próprio"),
    ("context.symbol", "", "texto não vazio"),
    ("orientation.mode", [], "modo desconhecido"),
    ("sizing.params.compound", 1, "bool finito"),
    ("sizing.params.risk_fraction", 5, "máximo 1"),
    ("sizing.params.risk_fraction", 0, "valor inválido"),
    ("bar_policy.position_limit", True, "uma posição"),
    ("bar_policy.position_limit", 2, "multiordem"),
    ("bar_policy.intrabar_priority", "target_first", "stop_first"),
    ("execution_order", ["next_open", "signal_close"], "precedência"),
    ("filters", {}, "lista ordenada"),
])
def test_typed_closed_schema_fails_with_actionable_field(path, value, reason):
    data = base().to_dict()
    change_path(data, path, value)
    with pytest.raises(CompositionError, match=reason) as err:
        Composition(data)
    assert err.value.problems[0].field


@pytest.mark.parametrize("bad", ['{"composition_schema_version":1,"composition_schema_version":2}',
                                  '{"x":NaN}', '{"x":Infinity}', '{bad'])
def test_bad_json_is_not_silently_reinterpreted(bad):
    with pytest.raises(CompositionError):
        Composition(bad)


def test_incompatible_partial_and_target_and_duplicate_filter():
    data = base().to_dict()
    data["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 2}}
    data["partial_exit_plan"] = {"mode": "fixed_r", "params": {
        "trigger_r": 2, "fraction": 0.5, "breakeven_after_confirmation": True}}
    with pytest.raises(CompositionError, match="anteceder"):
        Composition(data)
    data["partial_exit_plan"]["params"]["trigger_r"] = 1
    data["partial_exit_plan"]["params"]["fraction"] = 1
    with pytest.raises(CompositionError, match="menor que 1"):
        Composition(data)
    data = base().to_dict()
    data["filters"] = [{"mode": "adx_min", "params": {"period": 14, "threshold": 20}}] * 2
    with pytest.raises(CompositionError, match="duplicado"):
        Composition(data)


@pytest.mark.parametrize("slot,module", [
    ("orientation", {"mode": "inverted", "params": {}}),
    ("entry", {"mode": "limit", "params": {"distance_points": 10}}),
    ("entry", {"mode": "stop", "params": {"distance_points": 10}}),
    ("profit_exit", {"mode": "fixed_r", "params": {"target_r": 2}}),
    ("protection", {"mode": "fixed_points", "params": {"stop_points": 30}}),
    ("partial_exit_plan", {"mode": "fixed_r", "params": {
        "trigger_r": 1, "fraction": 0.5, "breakeven_after_confirmation": True}}),
    ("filters", [{"mode": "adx_min", "params": {"period": 14, "threshold": 20}}]),
])
def test_module_roundtrip_and_support_matches_backend(slot, module):
    data = base().to_dict()
    data[slot] = module
    draft = Composition(data)
    assert Composition(draft.to_json()).to_dict() == data
    if slot in {"profit_exit", "protection"}:
        # T19.18/19 implementaram estes modos em Python; artefatos antigos preservados.
        assert compatibility(draft) == []
        BaseAdapter(draft)
    else:
        assert any(p.field == slot for p in compatibility(draft))
        with pytest.raises(ValueError, match="sem adaptador"):
            BaseAdapter(draft)
    assert compatibility(draft, backend="mql5")  # declaração não libera executor nativo.


def test_native_support_cannot_be_granted_by_sidecar():
    assert "T19.23" in compatibility(base(), backend="mql5")[0].message
    assert "indisponível" in compatibility(base(), backend="anything")[0].message


def test_pullback_snapshot_preserved_but_new_adapter_waits_for_partial_confirmation_fix():
    composition = base_composition("pullback", symbol="EURUSD", timeframe="H1")
    assert composition.to_dict()["strategy"]["params"] == get_strategy("pullback").params
    assert composition.to_dict()["partial_exit_plan"]["mode"] == "strategy_default"
    with pytest.raises(ValueError, match="T19.57"):
        BaseAdapter(composition)


@pytest.mark.parametrize("key", list(REGISTRY))
def test_parameter_contract_covers_all_existing_defaults(key):
    assert set(STRATEGY_PARAMETERS[key]) == set(get_strategy(key).default_params)
    for name, spec in STRATEGY_PARAMETERS[key].items():
        assert spec.accepts(get_strategy(key).default_params[name])


@pytest.mark.parametrize("key", [key for key in REGISTRY if key != "pullback"])
def test_base_adapter_exact_legacy_parity_signals_exits_and_complete_backtest(key):
    bars = intraday(days=8) if key == "orb" else random_walk(800, seed=5)
    composition = base_composition(key, symbol="EURUSD", timeframe="M5", risk_fraction=0.005, compound=False)
    adapter = BaseAdapter(composition)
    legacy = get_strategy(key)
    pd.testing.assert_frame_equal(adapter.strategy().gerar_sinais(bars), legacy.gerar_sinais(bars))
    assert adapter.strategy().exit_rules() == legacy.exit_rules()
    cfg = BacktestConfig(initial_equity=20_000, risk_per_trade=0.009, slippage_points=2,
                         commission_per_lot=1.25, real_spread=True, swap=True)
    spec = SymbolSpec(symbol="EURUSD", point=0.01, tick_value=0.01,
                      volume_min=0.01, volume_step=0.01, swap_mode=1, swap_long=-2, swap_short=-1)
    expected = run_backtest(bars, legacy, spec, replace(cfg, risk_per_trade=0.005, compound=False))
    actual = adapter.run(bars, spec, cfg, timeframe="M5")
    pd.testing.assert_frame_equal(actual.trades_frame(), expected.trades_frame())
    assert not actual.trades_frame().empty
    assert actual.metrics == expected.metrics
    assert actual.config == expected.config
    assert cfg.risk_per_trade == 0.009 and cfg.compound is True
    for field in ("equity", "account_balance", "floating_equity", "performance_equity", "account_cashflows"):
        pd.testing.assert_series_equal(getattr(actual, field), getattr(expected, field))


def test_base_adapter_rejects_context_mismatch_and_does_not_mutate_config():
    adapter = BaseAdapter(base())
    with pytest.raises(ValueError, match="context.symbol"):
        adapter.run(pd.DataFrame(), SymbolSpec(symbol="WIN"), timeframe="H1")
    with pytest.raises(ValueError, match="context.timeframe"):
        adapter.run(pd.DataFrame(), SymbolSpec(symbol="EURUSD"), timeframe="M5")
    assert adapter.backtest_config().risk_per_trade == 0.01


def test_declared_binding_checks_actual_robot_without_extending_sdk1(tmp_path):
    robot = make_robot(tmp_path)
    data = caps_for(robot)
    caps = Capabilities(data, base())
    assert Capabilities(caps.to_json(), base()).to_dict() == data
    assert contract.validate_composition(robot, base(), caps) == []
    assert contract.validate(robot) == []
    assert "capabilities" not in contract.FIELDS
    robot.inputs = [replace(item, type="double") if item.name == "Periodo" else item for item in robot.inputs]
    assert any(p.field.endswith(".type") for p in contract.validate_composition(robot, base(), caps))


@pytest.mark.parametrize("path,value,reason", [
    ("capabilities_schema_version", True, "versão desconhecida"),
    ("capabilities_schema_version", 2, "versão desconhecida"),
    ("backend", "python", "mql5"),
    ("robot.version", "", "texto não vazio"),
    ("strategy_key", "donchian", "difere"),
    ("bindings.strategy.params.fast.type", "bool", "tipo int"),
    ("bindings.strategy.params.fast.unit", "minute", "unidade bar"),
])
def test_invalid_capability_sidecar(tmp_path, path, value, reason):
    data = caps_for(make_robot(tmp_path))
    if path.startswith("bindings.strategy.params.fast."):
        data["bindings"]["strategy.params.fast"][path.rsplit(".", 1)[1]] = value
    else:
        change_path(data, path, value)
    with pytest.raises(CompositionError, match=reason):
        Capabilities(data, base())


def test_stale_fingerprint_missing_input_duplicate_binding_and_unknown_path(tmp_path):
    robot = make_robot(tmp_path)
    data = caps_for(robot)
    data["robot"]["fingerprint"] = "old"
    data["bindings"]["strategy.params.fast"]["input"] = "InpGuess"
    problems = contract.validate_composition(robot, base(), data)
    assert any("fingerprint" in p.field for p in problems)
    assert any("input não existe" in p.message for p in problems)
    data["bindings"]["strategy.params.slow"] = data["bindings"]["strategy.params.fast"]
    with pytest.raises(CompositionError, match="duplicado"):
        Capabilities(data, base())
    data["bindings"] = {"guessed.path": {"input": "Periodo", "type": "int", "unit": "bar"}}
    with pytest.raises(CompositionError, match="caminho desconhecido"):
        Capabilities(data, base())


def test_sdk_sidecar_validation_retains_sdk_errors_and_actionable_new_errors(tmp_path):
    robot = make_robot(tmp_path, manifest={"contract": 2})
    problems = contract.validate_composition(robot, {"composition_schema_version": 2}, {})
    assert any(p.field == "contract" for p in problems)
    assert any(p.field == "composition" for p in problems)


def test_capability_modes_are_typed_and_missing_support_has_reason(tmp_path):
    robot = make_robot(tmp_path)
    data = caps_for(robot)
    data["modules"]["orientation"] = []
    assert any(p.field == "capabilities.modules.orientation" for p in
               contract.validate_composition(robot, base(), data))
    data["modules"]["orientation"] = [True]
    with pytest.raises(CompositionError, match="modos conhecidos"):
        Capabilities(data, base())
    data["modules"]["orientation"] = ["normal", "normal"]
    with pytest.raises(CompositionError, match="duplicado"):
        Capabilities(data, base())
