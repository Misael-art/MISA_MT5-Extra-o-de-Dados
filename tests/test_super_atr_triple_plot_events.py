"""Conferência de apresentação e par sintético; não é replay MT5 nem QA visual."""
import copy

import pytest

from mt5_extracao.composition import super_atr_triple_native as profile
from mt5_extracao.presets import execution
from mt5_extracao.presets import super_atr_triple_events as events
from tests.test_super_atr_triple_events import example


def test_plot_values_match_the_exact_confirmed_decision_without_entering_its_hash(example):
    old=example["validate"]()[0]
    log=example["log"];log.write_text(log.read_text()+f"HDK_TRIPLE_PLOT schema=1 bar_at={example['at']} first=90 second=88 third=86\n"
        +f"HDK_TRIPLE_PLOT_STATE schema=1 at={example['available']} enabled=1\n")
    reference=example["validate"](visual=True)[0]
    assert reference["presentation"]["status"]=="observed"
    assert reference["presentation"]["points"][0]["levels"]==[90,88,86]
    assert events.semantic_reference(old)==events.semantic_reference(reference)
    assert not reference["presentation"]["native_visual_qualified"]
    with pytest.raises(ValueError,match="plot difere"):example["validate"](visual=False)
    log.write_text(log.read_text().replace("second=88", "second=89"))
    with pytest.raises(ValueError):example["validate"](visual=True)


@pytest.mark.parametrize("tail", ["HDK_TRIPLE_PLOT schema=2 bar_at=100 first=90 second=88 third=86", "HDK_TRIPLE_PLOT_STATE schema=1 at=100 enabled=2"])
def test_unknown_or_malformed_presentation_markers_are_not_ignored(example,tail):
    log=example["log"];log.write_text(log.read_text()+tail+"\n")
    with pytest.raises(ValueError):example["validate"](visual=True)


def pair(example):
    reference,sizing,deals=example["validate"]()
    # Recibos sintéticos completos para exercitar o comparador, sem promover a
    # API gráfica/modelo falso da fixture a evidência real.
    reference["source"].update(framework_commit=profile.PLOT_HADOUKEN_REVISION,qualification="PINNED_TRIPLE_HEADER_CPP_ADAPTER")
    hidden=dict(run_id="synthetic-hidden",status="reconciled_replay",functional_native_activity=True,
        actual_mode=dict(tester=True,visual=True,demo=True),
        source=dict(key=profile.ROBOT_KEY,version=profile.PLOT_ROBOT_VERSION,fingerprint=profile.PLOT_APPROVED_FINGERPRINT),
        context=dict(symbol="EURUSD",timeframe="M5"),test_context=dict(deposit=10000),tester_model="synthetic_ticks",
        inputs=dict(values={k:False if k=="InpPlotEnabled" else 0 for k in profile.PLOT_INPUT_TYPES},runtime_binding=dict(native_verified=True)),
        strategy_reference=reference,sizing=sizing,deals=deals,ledger=example["rows"],
        signals=execution.native_signals(example["directory"],marker="HDK_STRATEGY_SIGNAL"))
    visible=copy.deepcopy(hidden);visible["run_id"]="synthetic-visible";visible["inputs"]["values"]["InpPlotEnabled"]=True
    visible["strategy_reference"]["presentation"].update(status="observed",points=[dict(bar_at=example["at"],levels=[90,88,86])])
    return visible,hidden


def test_pair_compares_activity_decisions_sizing_and_closed_fills_without_claiming_qualification(example):
    on,off=pair(example);result=events.compare_presentation_pair(on,off)
    assert result["same_observed_decisions_and_closed_fills"] and not result["operational_acceptance"]
    assert not result["native_visual_qualified"]
    on["ledger"][0]["pnl"]=-3
    result=events.compare_presentation_pair(on,off)
    assert not result["same_observed_decisions_and_closed_fills"] and result["first_difference"]["section"]=="ledger"


@pytest.mark.parametrize("fault", ["empty", "params", "headless", "source", "failure", "toggle", "off_plot", "missing_binding"])
def test_pair_does_not_promote_incompatible_or_unobserved_runs(example,fault):
    on,off=pair(example)
    if fault=="empty":on["functional_native_activity"]=False
    elif fault=="params":on["inputs"]["values"]["InpPeriod"]=30
    elif fault=="headless":on["actual_mode"]["visual"]=False
    elif fault=="source":on["source"]["fingerprint"]="unknown"
    elif fault=="failure":on["strategy_reference"]["presentation"]["failures"]=["creation failed"]
    elif fault=="toggle":on["strategy_reference"]["presentation"]["states"]=[dict(at=1,enabled=False)]
    elif fault=="off_plot":off["strategy_reference"]["presentation"]["points"]=[dict(bar_at=1,levels=[1,2,3])]
    else:on["inputs"]["runtime_binding"]["native_verified"]=False
    with pytest.raises(ValueError):events.compare_presentation_pair(on,off)
