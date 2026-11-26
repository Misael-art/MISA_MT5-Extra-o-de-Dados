from mt5_extracao.laboratory import ultra_catalog, ultra_tester
from mt5_extracao.strategies import REGISTRY


def test_registered_strategies_have_inventory_and_use_same_gate_as_tester():
    result = ultra_tester.capabilities()
    catalog = result["catalog"]
    assert not catalog["unmapped"]
    assert catalog["python_count"] == len(REGISTRY)
    rows = {row["key"]: row for row in catalog["strategies"]}
    admitted = {key for key in rows if ultra_catalog.physical_signal_supported(key)}
    assert admitted == set(result["simulated_platform_orders"]["strategies"])
    assert set(REGISTRY) <= rows.keys()
    assert rows["ema_atr"]["reference_exits"]["trailing_atr"] == 3.0
    assert rows["vwap_bands"]["eurusd_data_constraint"]
    assert set(REGISTRY) < set(catalog["market_native_strategies"])
    assert rows["super_atr_triple"]["eurusd_market"] == "native_reference"
    assert rows["pattern_definition"]["eurusd_market"] == "m1_price_definition"
    assert "super_atr_triple_plot" in catalog["missing_portfolio"]
    assert not catalog["full_portfolio_coverage"]


def test_new_registered_strategy_flags_inventory_and_is_rejected_by_tester(monkeypatch):
    monkeypatch.setitem(REGISTRY, "new_strategy", REGISTRY["donchian"])
    catalog = ultra_catalog.catalog()
    assert catalog["unmapped"] == ["new_strategy"]
    assert "new_strategy" in catalog["missing_portfolio"]
    assert not ultra_catalog.physical_signal_supported("new_strategy")
